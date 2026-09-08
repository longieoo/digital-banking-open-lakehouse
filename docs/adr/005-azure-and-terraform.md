# ADR 005: ADLS is the cloud alternative to MinIO

## Context
The same open table architecture should demonstrate a cloud storage deployment without always-on compute.
## Decision
Terraform provisions Azure ADLS Gen2, limited RBAC, restricted networking and Key
Vault. MinIO remains the local backend. URI and FileIO configuration choose the
environment. No other cloud, cluster orchestrator or expensive compute is added.
## Alternatives
Running MinIO and ADLS simultaneously duplicates storage; deploying a full cloud
cluster adds cost and operational scope before local integration is accepted.
## Consequences
A cloud REST catalog and compute/identity host still need explicit deployment.
Terraform validate proves configuration structure, not identity authorization,
reachability, running pipelines or actual Azure costs.
