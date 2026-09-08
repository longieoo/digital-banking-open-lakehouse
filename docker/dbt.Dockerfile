FROM python:3.11.12-slim
RUN pip install --no-cache-dir dbt-core==1.11.14 dbt-trino==1.9.3
WORKDIR /opt/bank/dbt
COPY dbt .
RUN useradd --uid 50000 --gid 0 --create-home bank && chown -R 50000:0 /opt/bank
USER bank
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false
ENTRYPOINT ["dbt"]
CMD ["build", "--profiles-dir", "."]
