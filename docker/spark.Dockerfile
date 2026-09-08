FROM apache/spark:3.5.5
USER root
WORKDIR /opt/bank
COPY . .
ENV PYTHONPATH=/opt/bank
RUN mkdir -p /opt/bank/runtime /opt/spark/.ivy2 && chmod -R 777 /opt/bank/runtime /opt/spark/.ivy2
USER spark
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1,org.apache.iceberg:iceberg-azure-bundle:1.9.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.5"]
CMD ["processing/silver.py"]
