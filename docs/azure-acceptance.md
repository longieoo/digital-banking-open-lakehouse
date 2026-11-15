# Azure acceptance and raw storage probe

The Azure template and adapters are implemented, but a deployed, authenticated
cloud pipeline is a separate acceptance gate. This tool makes the account gate
inspectable without representing an offline configuration check as deployment.

## Offline readiness

```sh
python -m bank.cloud_check
```

This command does not authenticate or contact Azure. It checks process environment
configuration and installed Azure identity/blob SDK modules. It returns exit 2
when configuration or dependencies are missing; READY_FOR_PROBE still means
cloud_validation=NOT_RUN. It never prints credential values, tenant/client IDs
or token content. Credential hints describe locally present fields, not whether
those credentials work. Managed identity availability remains NOT_CHECKED.

Install the project's existing runtime extra when needed:

```sh
python -m pip install -e '.[runtime]'
```

Configure the actual values from Terraform outputs using
[config/azure.env.example](../config/azure.env.example). The checker reads process
environment variables; it does not load .env files automatically. It requires
STORAGE_BACKEND=azure, AZURE_STORAGE_ACCOUNT_NAME, same-account abfss RAW_URI and
WAREHOUSE_URI with different filesystems, and a configured HTTPS CATALOG_URI.
Placeholder names are not accepted. This helper currently targets Azure public
cloud endpoint names; sovereign clouds need a deliberate endpoint configuration.

## Explicit real storage smoke test

After authentication and network/RBAC setup on the existing storage account:

```sh
python -m bank.cloud_check --probe
```

Only --probe creates Azure SDK clients and contacts the data plane. It uses
DefaultAzureCredential with interactive browser authentication disabled. Use an
existing workload/managed identity or a deliberately authenticated Azure CLI
session. The tool creates no resource groups, storage accounts, filesystems,
catalogs or role assignments and does not run Terraform apply.

The operation creates one short synthetic JSON blob below the configured raw
prefix at `_acceptance/<probe_id>.json`, with overwrite=False. It downloads using
the upload's ETag, verifies exact bytes, and deletes only that same ETag. It does
not list, overwrite or delete any unrelated blob. The existing storage account
must support ordinary blob data-plane access to the ADLS Gen2 filesystem.

If upload fails or its response is ambiguous, creation ownership is unconfirmed
and the tool does not issue a speculative delete. If the ETag is missing or has
changed, conditional cleanup cannot safely complete. Such a run returns FAIL and
reports that its unique probe may remain; investigate only that probe under the
configured raw prefix. It never retries cleanup without the ETag condition.
Deleting the probe follows the account's configured soft-delete/retention policy;
the tool does not purge retained storage versions.

## What PASS proves

A real PASS with cloud_validation=RAW_IO_ONLY proves that the selected credential
and network path could create, read and conditionally delete one raw blob at that
time. Azure's data-plane call uses existing storage resources and can incur the
normal negligible storage/transaction charges for that probe.

It does not prove hierarchical namespace/ACL behavior, denied identities, warehouse
permissions, REST catalog deployment/authentication, Iceberg ADLS compatibility
for every engine, migration of MinIO tables, or a Spark/Trino/dbt cloud release.
Those gates require the intended deployed environment and separate evidence.
No Azure PASS should be recorded when only mocked tests or offline checks ran.

The upload and conditional delete options follow the
[Microsoft BlobClient SDK reference](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python).
