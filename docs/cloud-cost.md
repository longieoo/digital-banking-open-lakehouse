# Cost and teardown

No Azure costs were measured and no paid resources were deployed. Storage bytes,
read/write/list operations, egress and Key Vault operations can incur charges.
LRS is selected for an inexpensive demonstration, not banking resilience. This
configuration creates no continuously running compute.

Use subscription budgets/alerts before deployment, confirm student-credit expiry,
and run only small datasets. `terraform destroy -var-file=demo.tfvars` removes the
managed demo resources and their data. Review the plan: do not import unrelated
resources into this state. Key Vault soft deletion can retain a recoverable name
for seven days; state, logs and exported files remain locally until managed
separately. Do not apply age-based deletion to Iceberg data files: it can invalidate
snapshots. Use Iceberg snapshot expiration/orphan cleanup with a reviewed retention
policy when the catalog is running.
