"""Transactional change capture for the synthetic PostgreSQL sources."""
import argparse
import json

from bank.adapters import TABLES, raw_store


def install(db):
    from psycopg import sql
    db.execute("""CREATE SCHEMA IF NOT EXISTS bank_cdc;
        CREATE TABLE IF NOT EXISTS bank_cdc.origin (
          singleton boolean PRIMARY KEY CHECK(singleton), source_id uuid NOT NULL DEFAULT gen_random_uuid());
        INSERT INTO bank_cdc.origin(singleton) VALUES (true) ON CONFLICT DO NOTHING;
        CREATE TABLE IF NOT EXISTS bank_cdc.events (
          event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          source_schema text NOT NULL, source_table text NOT NULL, operation text NOT NULL,
          transaction_id bigint NOT NULL, before_row jsonb, after_row jsonb,
          delivered boolean NOT NULL DEFAULT false);
        CREATE INDEX IF NOT EXISTS undelivered ON bank_cdc.events(event_id) WHERE NOT delivered;
        CREATE OR REPLACE FUNCTION bank_cdc.capture() RETURNS trigger
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
        BEGIN
          IF TG_OP = 'UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN RETURN NULL; END IF;
          INSERT INTO bank_cdc.events(source_schema,source_table,operation,transaction_id,before_row,after_row)
          VALUES (TG_TABLE_SCHEMA,TG_TABLE_NAME,TG_OP,txid_current(),
            CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END,
            CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END);
          RETURN NULL;
        END $$;
        REVOKE ALL ON FUNCTION bank_cdc.capture() FROM PUBLIC""")
    for table, (schema, _) in TABLES.items():
        target = sql.Identifier(schema, table)
        db.execute(sql.SQL("CREATE OR REPLACE TRIGGER bank_capture AFTER INSERT OR UPDATE OR DELETE ON {} "
                           "FOR EACH ROW EXECUTE FUNCTION bank_cdc.capture()").format(target))


def drain(db, raw, limit=500):
    """One batch per transaction. A sequence gap never acknowledges another writer's event."""
    if limit < 1:
        raise ValueError('Positive batch limit required')
    with db.transaction():
        db.execute("SET LOCAL TIME ZONE 'UTC'")
        origin = db.execute("SELECT source_id FROM bank_cdc.origin WHERE singleton").fetchone()[0]
        rows = db.execute("""SELECT event_id, (to_jsonb(e) - 'delivered')::text
          FROM bank_cdc.events e WHERE NOT delivered ORDER BY event_id
          LIMIT %s FOR UPDATE SKIP LOCKED""", (limit,)).fetchall()
        for event_id, payload in rows:
            raw.put_verified(f'cdc/postgres/{origin}/{event_id:020}.json', payload.encode())
        if rows:
            db.execute("UPDATE bank_cdc.events SET delivered=true WHERE event_id=ANY(%s)",
                       ([event_id for event_id, _ in rows],))
    return len(rows)


def main():
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['install', 'drain'])
    parser.add_argument('--batch-size', type=int, default=500)
    args = parser.parse_args()
    with psycopg.connect(connect_timeout=10, autocommit=True) as db:
        if args.action == 'install':
            with db.transaction():
                install(db)
        else:
            store = raw_store()
            delivered = 0
            while count := drain(db, store, args.batch_size):
                delivered += count
            print(json.dumps({'delivered': delivered}))


if __name__ == '__main__':
    main()
