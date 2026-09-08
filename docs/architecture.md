# Implemented architecture

```mermaid
flowchart TB
  subgraph Sources
    Core[Core PostgreSQL]
    Card[Card PostgreSQL schema]
    GL[General Ledger PostgreSQL schema]
    FX[Public FX JSON / labeled fixture]
    Events[Synthetic transaction events]
  end
  subgraph Batch
    Airflow[Airflow + Python extraction]
    Raw[Immutable raw landing]
    Bronze[Bronze Iceberg v2]
    Spark[Spark conformance + quarantine]
    Silver[Silver Iceberg v2]
    Trino[Trino Iceberg connector]
    DBT[dbt dimensions / facts / quality]
    Gold[Gold Iceberg]
    State[Persistent manifests + composite watermark]
  end
  Core & Card & GL --> Airflow
  FX --> Airflow
  Airflow --> Raw --> Bronze --> Spark --> Silver --> Trino --> DBT --> Gold
  Airflow <--> State
  Events --> Redpanda[Redpanda / Kafka API]
  Redpanda --> Streaming[Spark Structured Streaming]
  Streaming --> Bronze
  Streaming --> Alerts[Silver rule alerts + late-event audit]
  Streaming -. invokes shared conformance .-> Spark
  subgraph Storage alternatives
    MinIO[Local MinIO]
    ADLS[Cloud ADLS Gen2]
    REST[Persistent Iceberg REST catalog]
  end
  Raw & Bronze & Silver & Gold -. local objects .-> MinIO
  Raw & Bronze & Silver & Gold -. cloud objects .-> ADLS
  Bronze & Silver & Gold -. table metadata .-> REST
  Gold --> C360[Customer 360]
  Gold --> Tx[Transaction analytics]
  Gold --> Balances[Daily balances]
  Gold --> Recon[Reconciliation]
  C360 & Tx & Balances & Recon --> BI[Power BI specification]
  Terraform[Terraform] --> ADLS
  Docker[Docker Compose profiles] -. local runtime .-> Batch
  CI[GitHub Actions] -. tests / config validation .-> Batch
```

The stream deliberately adds a Bronze version merge before invoking the common
Silver code. This shares deduplication/reversal semantics with batch. The FX fixture
has its own explicit ingestion entry point and is not automatically fetched by the
scheduled core DAG. BI is a serving specification, not a generated PBIX.

## Table model

```mermaid
erDiagram
  dim_customer ||--o{ fct_transactions : "customer_sk at event time"
  dim_account ||--o{ fct_transactions : account_id
  dim_account ||--o{ fct_account_balance_daily : account_id
  dim_account ||--o{ dim_card : account_id
  dim_card ||--o{ fct_card_transactions : card_id
  dim_branch ||--o{ dim_account : branch_id
  dim_date ||--o{ fct_transactions : business_date
  dim_date ||--o{ fct_account_balance_daily : business_date
  dim_transaction_type ||--o{ fct_transactions : transaction_type
  dim_account ||--o{ fct_transfers : "source / destination roles"
```

| Layer | Grain / purpose | Persistence |
|---|---|---|
| Raw | Canonical source batch, content hash | Immutable local/object file |
| Bronze | Business key + source_updated_at | Iceberg history of observed versions |
| Silver transactions | Latest valid transaction event | Typed Iceberg MERGE |
| Silver customer_history | Customer + updated_at | Preserved history |
| dim_customer | Contiguous segment/branch/status interval | Deterministic SCD2 |
| fct_transactions | One core posting/event, including failed events | Gold Iceberg |
| fct_transfers | One transfer, independent of its two posting legs | Gold Iceberg |
| fct_card_transactions | One card purchase | Gold Iceberg |
| fct_account_balance_daily | Account/date including no-activity dates | Gold Iceberg |
| mart_customer_360 | One customer, explicit date/reporting currency | Gold Iceberg |
| mart_reconciliation_daily | Date/currency, three agreement levels | Gold Iceberg |

Atomicity is per Iceberg table and local control transaction. No cross-table or
cross-engine atomic snapshot is claimed. See ADRs for boundaries and consequences.
