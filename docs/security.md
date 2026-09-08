# Security scope

All customer records are synthetic, but they are treated as sensitive. Raw and
Bronze contain names, email, phone and DOB (restricted); account IDs, balances and
transaction history are confidential analytical data. Gold omits names/email/phone/
DOB and supplies masked display labels. Stable SYN identifiers are technical join
keys, not anonymization. Analysts can still link behavioral data; masking is not
anonymity or a compliance claim.

PostgreSQL `sources/security.sql` creates NOLOGIN reader roles and a real masked
view. Bind a separately provisioned login to those roles; credentials come from
environment/secret storage. Demo seeding requires an owner; switch ingestion to the
reader/control-writer role afterward. Do not grant analysts source-schema access.

Trino file rules grant transformer DDL/DML and analyst SELECT only on Gold. The
local HTTP server trusts supplied usernames, so these rules demonstrate authorization
and are NOT an authenticated security boundary. Bind ports to loopback. Before any
shared deployment, add TLS and authenticated identities and verify deny behavior.
The REST fixture/MinIO admin credentials are local-demo conveniences; deploy a
proper authenticated REST catalog and scoped bucket credentials outside the laptop.

No .env or key material is tracked. Manifest errors contain exception type only.
CLI failures emit sanitized types. Avoid debug SDK logging, full environment dumps
and Terraform state in Git. Terraform uses Azure RBAC and managed/workload identity;
Key Vault stores secrets separately if required. No secret values are provisioned
by this repository. Local mounted volumes are not encrypted by this project; rely
on OS disk encryption and restrict filesystem access.
