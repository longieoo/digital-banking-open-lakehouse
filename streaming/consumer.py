"""Microbatch replay is safe across partial writes; checkpoint advances only on success."""
import os
import re
from processing.silver import session, process_table, merge
from streaming.rules import risk_amount, risk_sql

ALERT_COLUMNS = ['transaction_id','account_id','event_time','rule','alert_id']
ALERT_MODE = 'current-v1'


def affected_accounts(history, transaction_ids):
    """Recover both former and current accounts from immutable source versions.

    Reading only pre-update Silver would lose the former account after a partial
    batch succeeded and retried. Bronze retains that dependency across retries.
    """
    from pyspark.sql import functions as F
    return (history.join(transaction_ids.select('record_id').distinct(),'record_id','left_semi')
            .select(F.get_json_object('payload','$.account_id').alias('account_id'))
            .filter('account_id IS NOT NULL').distinct())


def _append_observations(observations, target):
    """Append-only state-transition audit. Identical computation retries add no row."""
    spark = observations.sparkSession
    if not spark.catalog.tableExists(target):
        observations.limit(0).writeTo(target).using('iceberg').tableProperty('format-version','2').create()
    observations.createOrReplaceTempView('risk_observation_input')
    spark.sql(f'''MERGE INTO {target} t USING risk_observation_input s
        ON t.observation_id=s.observation_id WHEN NOT MATCHED THEN INSERT *''')


def _merge_current_alerts(changes, target):
    """One Iceberg commit installs new alerts and retracts stale alerts together."""
    changes.createOrReplaceTempView('risk_state_changes')
    assignments = ', '.join(f't.{name}=s.{name}' for name in ALERT_COLUMNS)
    columns = ', '.join(ALERT_COLUMNS)
    values = ', '.join('s.'+name for name in ALERT_COLUMNS)
    changes.sparkSession.sql(f'''MERGE INTO {target} t USING risk_state_changes s
        ON t.alert_id=s.alert_id
        WHEN MATCHED AND s._delete THEN DELETE
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED AND NOT s._delete THEN INSERT ({columns}) VALUES ({values})''')


def reconcile_current_alerts(spark, accounts, cause_token, silver_schema='silver', threshold=None):
    """Reconcile CURRENT alerts and retain immutable ACTIVE/RETRACTED observations.

    The first invocation migrates the former append-only risk_alerts table using
    all validated Silver accounts. Later invocations evaluate complete histories
    only for affected accounts. This limits the risk query's scope, not retained
    state; Silver conformance itself remains a full-history operation.
    Passing accounts=None requests global reconciliation after a batch refresh.

    Audit is written before the atomic current-state MERGE. A crash between those
    commits leaves an observation of the computation; retry repairs current state
    without duplicating that observation. Callers hold the shared writer lock.
    """
    from pyspark.sql import functions as F
    if not re.fullmatch('[a-z][a-z0-9_]*',silver_schema):
        raise ValueError('Invalid Silver schema')
    current_target = f'lake.{silver_schema}.risk_alerts'
    audit_target = f'lake.{silver_schema}.risk_alert_observations'
    transactions = spark.table(f'lake.{silver_schema}.transactions')
    configured = os.environ.get('RISK_AMOUNT_THRESHOLD','10000.00') if threshold is None else threshold
    amount = str(risk_amount(configured))
    exists = spark.catalog.tableExists(current_target)
    properties = {r.key:r.value for r in spark.sql(f'SHOW TBLPROPERTIES {current_target}').collect()} if exists else {}
    bootstrap = (properties.get('bank.risk_alert_state') != ALERT_MODE or
                 properties.get('bank.risk_amount_threshold') != amount)
    if bootstrap or accounts is None:
        accounts = transactions.select('account_id')
        if exists:
            accounts = accounts.union(spark.table(current_target).select('account_id'))
    accounts = accounts.select('account_id').distinct()
    scoped = transactions.join(accounts,'account_id','left_semi')
    scoped.createOrReplaceTempView('risk_events')
    alerts = spark.sql(risk_sql(amount)).withColumn(
        'alert_id',F.sha2(F.concat_ws('|','transaction_id','rule'),256)).select(*ALERT_COLUMNS)
    if not exists:
        alerts.limit(0).writeTo(current_target).using('iceberg').tableProperty('format-version','2').create()
    previous = spark.table(current_target).select(*ALERT_COLUMNS).join(accounts,'account_id','left_semi')
    stale = previous.join(alerts.select('alert_id'),'alert_id','left_anti').withColumn('_delete',F.lit(True))
    if bootstrap:
        added = alerts
    else:
        old = previous.alias('old')
        new = alerts.alias('new')
        joined = new.join(old,F.col('new.alert_id')==F.col('old.alert_id'),'left')
        different = F.col('old.alert_id').isNull()
        for column in ALERT_COLUMNS:
            different = different | ~F.col('new.'+column).eqNullSafe(F.col('old.'+column))
        added = joined.filter(different).select('new.*')
    changes = added.withColumn('_delete',F.lit(False)).unionByName(stale).persist()
    try:
        count = changes.count()  # Materialize the pre-commit state used by both writes.
        observations = (changes.withColumn('action',F.when(F.col('_delete'),'RETRACTED').otherwise('ACTIVE'))
                        .drop('_delete').withColumn('cause_token',F.lit(str(cause_token)))
                        .withColumn('policy_version',F.lit(f'{ALERT_MODE}:{amount}')))
        observations = observations.withColumn('observation_id',F.sha2(
            F.to_json(F.struct(*[F.col(name) for name in [*ALERT_COLUMNS,'action','cause_token','policy_version']])),256))
        if count:
            _append_observations(observations,audit_target)
            _merge_current_alerts(changes,current_target)
        elif not spark.catalog.tableExists(audit_target):
            observations.limit(0).writeTo(audit_target).using('iceberg').tableProperty('format-version','2').create()
        if bootstrap:
            spark.sql(f"ALTER TABLE {current_target} SET TBLPROPERTIES "
                      f"('bank.risk_alert_state'='{ALERT_MODE}','bank.risk_amount_threshold'='{amount}')")
        return {'state_changes':count,'bootstrap':bootstrap,'current_table':current_target,'audit_table':audit_target}
    finally:
        changes.unpersist()


def consume_batch(batch, batch_number):
    from bank.ingestion import State
    with State(os.environ.get("CONTROL_PATH", "runtime/control.db")).exclusive():
        _consume_batch(batch, batch_number)


def _consume_batch(batch, batch_number):
    from pyspark.sql import functions as F
    spark = batch.sparkSession
    if batch.isEmpty():
        return
    for ns in ['bronze','silver','quarantine']:
        spark.sql(f'CREATE NAMESPACE IF NOT EXISTS lake.{ns}')
    invalid = batch.filter('event_id IS NULL OR payload IS NULL OR event_time IS NULL')
    rejected = invalid.select('raw_event').withColumn('reject_reason',F.lit('invalid_event_envelope')).withColumn(
        'reject_id', F.sha2('raw_event',256)).dropDuplicates(['reject_id'])
    merge(rejected,'lake.quarantine.stream_events',['reject_id'],versioned=False)
    valid = batch.filter('event_id IS NOT NULL AND payload IS NOT NULL AND event_time IS NOT NULL')
    if valid.groupBy('event_id').agg(F.countDistinct('payload').alias('versions')).filter('versions > 1').count():
        raise ValueError('Conflicting duplicate event payload')
    valid = valid.dropDuplicates(['event_id'])
    if valid.isEmpty():
        return
    valid = valid.withColumn('batch_number',F.lit(batch_number))
    late = valid.limit(0)
    if spark.catalog.tableExists('lake.silver.stream_events'):
        previous = spark.table('lake.silver.stream_events')
        conflicts = valid.alias('s').join(previous.alias('t'),'event_id').filter('s.payload <> t.payload')
        if conflicts.count():
            raise ValueError('Conflicting historical event payload')
        new_events = valid.join(previous.select('event_id'), 'event_id', 'left_anti')
        maximum = previous.select(F.max('event_time').alias('max_time'))
        late = new_events.crossJoin(maximum).filter("event_time < max_time - INTERVAL 10 MINUTE").drop('max_time')
    # Persist classification before changing its reference history; replay is idempotent.
    merge(late,'lake.silver.late_events',['event_id'],versioned=False)
    merge(valid,'lake.silver.stream_events',['event_id'],versioned=False)
    envelopes = valid.select(
        F.get_json_object('payload','$.transaction_id').alias('record_id'),
        F.get_json_object('payload','$.updated_at').alias('source_updated_at'),
        F.get_json_object('payload','$.business_date').alias('business_date'),
        'payload',F.col('event_id').alias('batch_id'),F.lit('core').alias('source_system'),
        F.lit('transactions').alias('source_table'))
    if envelopes.filter('record_id IS NULL OR source_updated_at IS NULL').count():
        raise ValueError('Invalid transaction version in stream')
    if envelopes.groupBy('record_id','source_updated_at').agg(
            F.countDistinct('payload').alias('versions')).filter('versions > 1').count():
        raise ValueError('Conflicting duplicate transaction version')
    envelopes = envelopes.dropDuplicates(['record_id','source_updated_at'])
    # Shared immutable Bronze versions make batch/stream convergence explicit.
    existing = spark.table('lake.bronze.transactions')
    conflicts = envelopes.alias('s').join(existing.alias('t'),
        (F.col('s.record_id') == F.col('t.record_id')) &
        (F.col('s.source_updated_at') == F.col('t.source_updated_at'))).filter('s.payload <> t.payload')
    if conflicts.count():
        raise ValueError('Conflicting stream source version')
    envelopes.createOrReplaceTempView('event_versions')
    spark.sql('''MERGE INTO lake.bronze.transactions t USING event_versions s
        ON t.record_id=s.record_id AND t.source_updated_at=s.source_updated_at
        WHEN NOT MATCHED THEN INSERT *''')
    process_table(spark,'transactions')
    accounts = affected_accounts(spark.table('lake.bronze.transactions'),envelopes.select('record_id'))
    cause_token = valid.select(F.sha2(F.to_json(F.sort_array(F.collect_set('event_id'))),256).alias('token')).first().token
    reconcile_current_alerts(spark,accounts,cause_token)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--available-now',action='store_true',help='Drain available offsets then exit; preserve checkpoint')
    modes.add_argument('--reconcile-alerts',action='store_true',help='Reconcile all current alerts after an independent batch refresh')
    parser.add_argument('--max-offsets-per-trigger',type=int,default=10000)
    args = parser.parse_args()
    if args.max_offsets_per_trigger < 1:
        parser.error('Positive max offsets required')
    spark = session()
    if args.reconcile_alerts:
        import json
        from bank.ingestion import State
        try:
            with State(os.environ.get('CONTROL_PATH','runtime/control.db')).exclusive():
                # The newest historical snapshot need not be the current one
                # after a rollback. Use the actual main reference for provenance.
                snapshot = spark.sql("SELECT snapshot_id FROM lake.silver.transactions.refs WHERE name='main'").first()
                token = f'manual-silver-snapshot:{snapshot.snapshot_id}' if snapshot else 'manual-empty-silver'
                result = reconcile_current_alerts(spark,None,token)
            print(json.dumps(result))
        finally:
            spark.stop()
        return
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType,StructField,StringType
    schema = StructType([StructField(c,StringType()) for c in ['event_id','event_type','event_time','payload']])
    kafka = (spark.readStream.format('kafka').option('kafka.bootstrap.servers',os.environ['KAFKA_BOOTSTRAP_SERVERS'])
        .option('subscribe','bank.transactions').option('startingOffsets','earliest')
        .option('maxOffsetsPerTrigger',args.max_offsets_per_trigger).option('failOnDataLoss','true').load())
    events = kafka.select(F.col('value').cast('string').alias('raw_event')).select(
        'raw_event',F.from_json('raw_event',schema).alias('event')).select('raw_event','event.*')
    events = events.withColumn('event_time',F.to_timestamp('event_time'))
    writer = (events.writeStream.foreachBatch(consume_batch)
        .option('checkpointLocation',os.environ.get('STREAM_CHECKPOINT','/opt/bank/runtime/checkpoints/transactions')))
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime='10 seconds')
    try:
        writer.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
