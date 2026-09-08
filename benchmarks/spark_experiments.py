"""Run with the Spark container. Results appear only after a real Spark action."""
import argparse
import json
import os
from pathlib import Path
import platform
from time import perf_counter
from processing.silver import session


def experiments(spark, volumes, jdbc=False):
    from pyspark.sql import functions as F
    output = []
    for n in volumes:
        for partitions in [1, 4, 8]:
            for strategy in ['broadcast','merge']:
                facts = spark.range(n).select('id', (F.col('id') % 100).alias('branch_id'),
                    (F.col('id') % 10000).cast('decimal(22,2)').alias('amount')).repartition(partitions)
                dim = spark.range(100).select(F.col('id').alias('branch_id')).hint(strategy)
                query = facts.join(dim, 'branch_id').groupBy('branch_id').sum('amount')
                query.explain(mode='formatted')
                start = perf_counter()
                result = query.collect()
                output.append(dict(rows=n,partitions=partitions,join=strategy,
                                   seconds=perf_counter()-start,result_groups=len(result)))
    if jdbc:
        for partitions in [1,4]:
            reader = spark.read.format('jdbc').option('url','jdbc:postgresql://postgres:5432/bank')
            reader = reader.options(user=os.environ['PGUSER'],password=os.environ['PGPASSWORD'],
                driver='org.postgresql.Driver',dbtable="(SELECT *, substring(transaction_id from 6)::bigint AS pid FROM core.transactions) t",
                partitionColumn='pid',lowerBound='0',upperBound=str(max(volumes)*3),numPartitions=str(partitions))
            start = perf_counter()
            result = reader.load().agg(F.count('*').alias('rows'),F.sum('amount').alias('total')).collect()[0]
            output.append(dict(jdbc_partitions=partitions,seconds=perf_counter()-start,
                               rows=result['rows'],amount=str(result['total'])))
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows',nargs='+',type=int,default=[100000,1000000])
    parser.add_argument('--jdbc',action='store_true')
    args = parser.parse_args()
    spark = session()
    measurements = experiments(spark,args.rows,args.jdbc)
    path = Path('runtime/spark-benchmark.json')
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(dict(environment=platform.platform(),spark=spark.version,
                                   measurements=measurements),indent=2))
    spark.stop()
