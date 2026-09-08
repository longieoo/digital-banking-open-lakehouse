"""Microbatch replay is safe across partial writes; checkpoint advances only on success."""
import os
from processing.silver import session, process_table, merge
from streaming.rules import risk_sql


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
    valid = valid.dropDuplicates(['event_id'])
    valid = valid.withColumn('batch_number',F.lit(batch_number))
    merge(valid,'lake.silver.stream_events',['event_id'],versioned=False)
    envelopes = valid.select(
        F.get_json_object('payload','$.transaction_id').alias('record_id'),
        F.get_json_object('payload','$.updated_at').alias('source_updated_at'),
        F.get_json_object('payload','$.business_date').alias('business_date'),
        'payload',F.col('event_id').alias('batch_id'),F.lit('core').alias('source_system'),
        F.lit('transactions').alias('source_table'))
    if envelopes.filter('record_id IS NULL OR source_updated_at IS NULL').count():
        raise ValueError('Invalid transaction version in stream')
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
    spark.table('lake.silver.transactions').createOrReplaceTempView('risk_events')
    alerts = spark.sql(risk_sql(os.environ.get('RISK_AMOUNT_THRESHOLD','10000.00'))).withColumn(
        'alert_id',F.sha2(F.concat_ws('|','transaction_id','rule'),256))
    merge(alerts,'lake.silver.risk_alerts',['alert_id'],versioned=False)
    # Late observations are retained, not silently dropped by a watermark.
    all_events = spark.table('lake.silver.stream_events')
    maximum = all_events.select(F.max('event_time').alias('max_time'))
    late = all_events.crossJoin(maximum).filter("event_time < max_time - INTERVAL 10 MINUTE").drop('max_time')
    merge(late,'lake.silver.late_events',['event_id'],versioned=False)


def main():
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType,StructField,StringType
    spark = session()
    schema = StructType([StructField(c,StringType()) for c in ['event_id','event_type','event_time','payload']])
    kafka = (spark.readStream.format('kafka').option('kafka.bootstrap.servers',os.environ['KAFKA_BOOTSTRAP_SERVERS'])
        .option('subscribe','bank.transactions').option('startingOffsets','earliest').option('failOnDataLoss','true').load())
    events = kafka.select(F.col('value').cast('string').alias('raw_event')).select(
        'raw_event',F.from_json('raw_event',schema).alias('event')).select('raw_event','event.*')
    events = events.withColumn('event_time',F.to_timestamp('event_time'))
    query = (events.writeStream.foreachBatch(consume_batch)
        .option('checkpointLocation',os.environ.get('STREAM_CHECKPOINT','/opt/bank/runtime/checkpoints/transactions'))
        .trigger(processingTime='10 seconds').start())
    query.awaitTermination()


if __name__ == '__main__':
    main()
