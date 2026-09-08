import hashlib
from decimal import Decimal
import json
import os
from bank.generator import generate
from bank.ingestion import canonical, utc


def event(transaction):
    row = dict(transaction)
    for name in ['event_time', 'updated_at']:
        row[name] = utc(row[name])
    row['amount'] = format(Decimal(row['amount']), '.2f')
    identity = f"{row['transaction_id']}|{row['updated_at']}"
    kind = 'transaction_reversed' if row['transaction_type'].startswith('REVERSAL_') else (
        'transaction_failed' if row['status'] == 'FAILED' else 'transaction_created')
    return dict(event_id=hashlib.sha256(identity.encode()).hexdigest(),event_type=kind,
                event_time=utc(row['event_time']),payload=canonical(row).decode())


def main():
    from confluent_kafka import Producer
    producer = Producer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS'],
                         'enable.idempotence':True, 'acks':'all'})
    failures = []
    def delivered(error, message):
        if error:
            failures.append(type(error).__name__)
    rows = generate()['transactions']
    # Same deterministic bank events deliberately overlap the batch path. Replay
    # includes duplicates and reversed delivery order to exercise recovery.
    for row in list(reversed(rows)) + rows[:5]:
        producer.produce('bank.transactions',key=row['account_id'],value=json.dumps(event(row)),callback=delivered)
        producer.poll(0)
    remaining = producer.flush(30)
    if failures or remaining:
        raise RuntimeError('Producer delivery failed')
    print(json.dumps({'delivered':len(rows)+5,'synthetic':True}))


if __name__ == '__main__':
    main()
