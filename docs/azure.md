# Azure storage demonstration

Terraform provisions a Resource Group, ADLS Gen2 storage, raw/warehouse filesystems,
pipeline container RBAC and an RBAC Key Vault with restricted public egress IPs.
No compute, catalog service, credentials or secret values are provisioned. Azure
deployment is UNAVAILABLE without an authorized account; validation is not deployment.

With an authorized Azure subscription and existing workload identity:

```sh
az login
cd infra/terraform
cp demo.tfvars.example demo.tfvars
# Edit subscription ID, globally unique name, pipeline object ID and your public IP.
terraform init
terraform fmt -check
terraform validate
terraform plan -var-file=demo.tfvars -out=demo.plan
terraform apply demo.plan
terraform output
# After the demo, when the data is no longer needed:
terraform destroy -var-file=demo.tfvars
```

The provisioning caller needs resource creation and role-assignment permissions.
Data-plane RBAC can take minutes to propagate: after the role exists, retry apply
if filesystem creation initially gets 403. Network rules must include the runner's
public IPv4. Never weaken them to all internet addresses. Protect local Terraform
state and plan files; use an independently provisioned secure remote backend for teams.

Use config/azure.env.example for URI/backend selection. Python raw uses
DefaultAzureCredential; Spark uses Iceberg ADLSFileIO and its Azure default
credential chain. The PyIceberg Azure path needs `fsspec`/`adlfs`; verify workload
identity support in the deployed credential chain. Trino's example uses OAuth
credentials supplied at runtime; production identity choices need environment
validation. Configure the REST catalog itself for ADLS and Azure identity too.
Do not leave its local S3 overrides enabled. Changing backend changes configuration,
not the ingestion state machine or Gold SQL. Existing MinIO tables are not migrated
by changing an environment variable: use a fresh cloud catalog/warehouse or a
deliberate migration. Catalog hosting/authentication is a manual cloud prerequisite.

Authoring validation: Terraform init/fmt/validate PASS with AzureRM 4.26.0.
The local mirror archives were SHA256-checked against HashiCorp release checksums;
the lock file includes verified Windows/Linux archive hashes. Azure was not contacted.

Fresh-clone recheck: local provider checksum calculation returned Access is denied
with both archive and unpacked mirrors. This initialization attempt is recorded
as FAIL; working-repository validation remains PASS. Re-run init on the developer
machine/CI before plan/apply. No checksum mismatch or cloud result was inferred.
