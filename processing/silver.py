"""Real Spark Bronze -> Silver. Invoke with spark-submit from repository root."""
import os
from bank.adapters import TABLES
from bank.contracts import fields, column_type, primary_key, REFERENCES, NULLABLE, CREDIT_TYPES, DEBIT_TYPES, DECIMALS


def session():
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.appName('bank-silver')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.lake', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.lake.type', 'rest')
        .config('spark.sql.catalog.lake.uri', os.environ['CATALOG_URI'])
        .config('spark.sql.catalog.lake.warehouse', os.environ.get('WAREHOUSE_URI','s3://warehouse'))
        .config('spark.sql.shuffle.partitions', os.environ.get('SPARK_SHUFFLE_PARTITIONS', '4')))
    if os.environ.get('STORAGE_BACKEND', 's3') == 's3':
        builder = (builder.config('spark.sql.catalog.lake.io-impl','org.apache.iceberg.aws.s3.S3FileIO')
            .config('spark.sql.catalog.lake.s3.endpoint', os.environ['S3_ENDPOINT'])
            .config('spark.sql.catalog.lake.s3.path-style-access','true'))
    else:
        builder = builder.config('spark.sql.catalog.lake.io-impl','org.apache.iceberg.azure.adlsv2.ADLSFileIO')
    return builder.getOrCreate()


def parse_frame(bronze, table):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField(c, StringType()) for c in fields(table)])
    parsed = bronze.select(F.from_json('payload', schema).alias('r'), 'payload', 'batch_id')
    frame = parsed.select('r.*', 'payload', 'batch_id')
    errors = []
    for col in fields(table):
        if col in DECIMALS:
            errors.append(F.when(~F.get_json_object('payload', f'$.{col}').rlike(r'^-?[0-9]+(\.[0-9]{1,2})?$'), F.lit(f'{col}:invalid_scale')))
        frame = frame.withColumn(col, F.expr(f'try_cast(`{col}` as {column_type(col)})'))
        if col not in NULLABLE:
            errors.append(F.when(F.col(col).isNull(), F.lit(f'{col}:required_or_invalid')))
    if 'amount' in frame.columns:
        errors.append(F.when(F.col('amount') <= 0, F.lit('amount:nonpositive')))
    if 'currency' in frame.columns:
        frame = frame.withColumn('currency', F.upper(F.trim('currency')))
        errors.append(F.when(~F.col('currency').isin('USD', 'EUR', 'VND'), F.lit('currency:unsupported')))
    if table == 'transactions':
        errors.extend([
            F.when(~F.col('status').isin('SUCCESS','FAILED','PENDING'), F.lit('status:invalid')),
            F.when(~F.col('transaction_type').isin(*sorted(CREDIT_TYPES | DEBIT_TYPES)), F.lit('type:invalid')),
            F.when(~F.col('channel').isin('MOBILE','CARD','BRANCH','ATM'), F.lit('channel:invalid')),
            F.when(F.col('transaction_type').startswith('REVERSAL_') &
                   F.col('original_transaction_id').isNull(), F.lit('reversal:missing_original')),
        ])
    if table == 'transfers':
        errors.append(F.when(F.col('source_account_id') == F.col('destination_account_id'),
                             F.lit('transfer:self')))
    if table == 'journal_lines':
        errors.append(F.when(~(((F.col('debit') > 0) & (F.col('credit') == 0)) |
                              ((F.col('credit') > 0) & (F.col('debit') == 0))), F.lit('journal:invalid_side')))
    return frame.withColumn('reject_reason', F.concat_ws(';', *errors))


def merge(frame, target, keys, versioned=True):
    from pyspark.sql import functions as F
    spark = frame.sparkSession
    view = 'merge_input'
    frame.createOrReplaceTempView(view)
    if not spark.catalog.tableExists(target):
        writer = frame.limit(0).writeTo(target).using('iceberg').tableProperty('format-version','2')
        writer = writer.tableProperty('write.parquet.compression-codec','zstd')
        if 'business_date' in frame.columns:
            writer = writer.partitionedBy(F.col('business_date'))
        writer.create()
    on = ' AND '.join(f't.`{k}` = s.`{k}`' for k in keys)
    condition = ' AND s.updated_at >= t.updated_at' if versioned and 'updated_at' not in keys else ''
    spark.sql(f'MERGE INTO {target} t USING {view} s ON {on} '
              f'WHEN MATCHED{condition} THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *')


def process_table(spark, table, bronze_schema='bronze', silver_schema='silver',
                  quarantine_schema='quarantine', strict=False):
    from pyspark.sql import functions as F, Window
    bronze = spark.table(f'lake.{bronze_schema}.{table}')
    if table=='source_totals':
        # A control epoch is a complete snapshot of groups, including disappeared groups.
        epoch = bronze.select(F.max('source_updated_at').alias('epoch')).first().epoch
        bronze = bronze.filter(F.col('source_updated_at')==epoch)
    if strict:
        # An invalid current version must block publication instead of using an old valid row.
        history_keys = ['record_id','source_updated_at'] if table=='customer_history' else ['record_id']
        bronze = bronze.withColumn('_version',F.row_number().over(
            Window.partitionBy(*history_keys).orderBy(F.col('source_updated_at').desc())))
        bronze = bronze.filter('_version = 1').drop('_version')
    frame = parse_frame(bronze, table)
    for col, parent, parent_key in REFERENCES.get(table, []):
        parent_frame = spark.table(f'lake.{silver_schema}.{parent}')
        currency_check = 'currency' in frame.columns and 'currency' in parent_frame.columns
        ref_columns = [F.col(parent_key).alias('_ref')]
        if currency_check:
            ref_columns.append(F.col('currency').alias('_ref_currency'))
        ref = parent_frame.select(*ref_columns).distinct()
        frame = frame.join(ref, frame[col] == ref['_ref'], 'left').withColumn('reject_reason',
            F.concat_ws(';', F.col('reject_reason'), F.when(F.col('_ref').isNull() & F.col(col).isNotNull(), F.lit(f'{col}:orphan'))))
        if currency_check:
            frame = frame.withColumn('reject_reason', F.concat_ws(';', 'reject_reason',
                F.when(F.col('currency') != F.col('_ref_currency'), F.lit(f'{col}:currency_mismatch'))))
        frame = frame.drop('_ref', '_ref_currency')
    if table == 'transactions':
        candidates = frame.filter("reject_reason = ''").withColumn('_rn', F.row_number().over(
            Window.partitionBy('transaction_id').orderBy(F.col('updated_at').desc())))
        originals = candidates.filter('_rn = 1').select(
            F.col('transaction_id').alias('_original_id'), F.col('account_id').alias('_original_account'),
            F.col('currency').alias('_original_currency'), F.col('amount').alias('_original_amount'),
            F.col('transaction_type').alias('_original_type'), F.col('status').alias('_original_status'))
        frame = frame.join(originals, frame.original_transaction_id == originals._original_id, 'left')
        reversal = F.col('transaction_type').startswith('REVERSAL_')
        valid = ((F.col('_original_id').isNotNull()) & (F.col('transaction_id') != F.col('_original_id')) &
                 (F.col('account_id') == F.col('_original_account')) &
                 (F.col('currency') == F.col('_original_currency')) &
                 (F.col('amount') == F.col('_original_amount')) & (F.col('_original_status') == 'SUCCESS') &
                 (((F.col('transaction_type') == 'REVERSAL_CREDIT') &
                   F.col('_original_type').isin('WITHDRAWAL','CARD_PAYMENT','TRANSFER_OUT')) |
                  ((F.col('transaction_type') == 'REVERSAL_DEBIT') &
                   F.col('_original_type').isin('DEPOSIT','TRANSFER_IN'))))
        frame = frame.withColumn('reject_reason', F.concat_ws(';', 'reject_reason',
            F.when(reversal & ~F.coalesce(valid,F.lit(False)), F.lit('reversal:invalid_original'))))
        frame = frame.drop(*[c for c in frame.columns if c.startswith('_original_')])
    rejected = frame.filter("reject_reason <> ''").select('payload','batch_id','reject_reason').withColumn(
        'reject_id', F.sha2(F.concat_ws('|','payload','reject_reason'),256)).dropDuplicates(['reject_id'])
    merge(rejected, f'lake.{quarantine_schema}.{table}', ['reject_id'], versioned=False)
    rejected_count = rejected.count()
    if strict and rejected_count:
        raise ValueError(f'Silver quality gate rejected {rejected_count} rows in {table}')
    good = frame.filter("reject_reason = ''").drop('payload','reject_reason','batch_id')
    keys = primary_key(table)
    good = good.withColumn('_rn', F.row_number().over(
        Window.partitionBy(*keys).orderBy(F.col('updated_at').desc())))
    good = good.filter('_rn = 1').drop('_rn')
    if table=='source_totals':
        good.writeTo(f'lake.{silver_schema}.{table}').using('iceberg').tableProperty('format-version','2').createOrReplace()
    else:
        merge(good, f'lake.{silver_schema}.{table}', keys)
    accepted_count = good.count()
    print(f'{{"table":"{table}","rejected":{rejected_count},"accepted":{accepted_count}}}')
    return dict(table=table,rejected=rejected_count,accepted=accepted_count)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-id')
    args = parser.parse_args()
    spark = session()
    if args.release_id:
        release(spark,args.release_id)
        spark.stop()
        return
    for namespace in ['silver','quarantine']:
        spark.sql(f'CREATE NAMESPACE IF NOT EXISTS lake.{namespace}')
    for table in TABLES:
        process_table(spark, table)
    spark.stop()


def release(spark, value):
    from bank.release import load, directory, atomic_json
    from bank.pipeline import DATED_TABLES, RELATED_TABLES
    from bank.ingestion import utc
    from pyspark.sql import functions as F
    document = load(value)
    if document['status']=='CERTIFIED':
        return
    bronze, silver = document['bronze_schema'], document['silver_schema']
    quarantine = f'quarantine_r_{document["release_id"]}'
    for namespace in (bronze,silver,quarantine):
        spark.sql(f'CREATE NAMESPACE IF NOT EXISTS lake.{namespace}')
    frames = {}
    for table,snapshot in document['bronze_snapshots'].items():
        reader = spark.read.format('iceberg')
        frame = reader.option('snapshot-id',str(snapshot)).load(f'lake.bronze.{table}') if snapshot else (
            reader.load(f'lake.bronze.{table}').limit(0))
        if table in DATED_TABLES:
            frame = frame.filter(f"business_date BETWEEN '{document['start_date']}' AND '{document['as_of_date']}'")
        if table=='source_totals':
            frame = frame.filter(F.col('source_updated_at')==utc(document['source_cutoff']['captured_at']))
        frames[table] = frame
    for table,frame in frames.items():
        if table in RELATED_TABLES:
            child_key,parent,parent_key = RELATED_TABLES[table]
            parent_column = F.col('record_id') if parent_key=='record_id' else F.get_json_object('payload',f'$.{parent_key}')
            included = frames[parent].select(parent_column.alias('_included')).distinct()
            frame = frame.join(included,F.get_json_object(frame.payload,f'$.{child_key}')==included._included,'left_semi')
        # Isolate both SQL engines from ongoing batch and streaming writes.
        frame.writeTo(f'lake.{bronze}.{table}').using('iceberg').createOrReplace()
    measurements = [process_table(spark,table,bronze,silver,quarantine,strict=True) for table in TABLES]
    document.update(status='SILVER_READY',silver_quality={'rejected':sum(r['rejected'] for r in measurements),
                                                       'tables':measurements})
    atomic_json(directory(value)/'release.json',document)


if __name__ == '__main__':
    main()
