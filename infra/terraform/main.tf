terraform {
  required_version = ">= 1.11, < 2.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "4.26.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id
  storage_use_azuread = true
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bank" {
  name     = "rg-${var.storage_account_name}"
  location = var.location
  tags     = { project = "educational-banking-lakehouse", environment = "demo" }
}

resource "azurerm_storage_account" "lake" {
  name                            = var.storage_account_name
  resource_group_name             = azurerm_resource_group.bank.name
  location                        = azurerm_resource_group.bank.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  public_network_access_enabled   = true
  network_rules {
    default_action = "Deny"
    ip_rules       = var.allowed_ipv4
    bypass         = ["None"]
  }
  tags = azurerm_resource_group.bank.tags
}

# Terraform's filesystem operations use the data plane and the caller identity.
resource "azurerm_role_assignment" "provisioner" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["raw", "warehouse"])
  name               = each.value
  storage_account_id = azurerm_storage_account.lake.id
  depends_on         = [azurerm_role_assignment.provisioner]
}

resource "azurerm_role_assignment" "pipeline" {
  for_each             = azurerm_storage_data_lake_gen2_filesystem.layers
  scope                = "${azurerm_storage_account.lake.id}/blobServices/default/containers/${each.key}"
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.pipeline_principal_id
}

resource "azurerm_key_vault" "bank" {
  name                       = "kv-${var.storage_account_name}"
  location                   = azurerm_resource_group.bank.location
  resource_group_name        = azurerm_resource_group.bank.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  network_acls {
    bypass         = "None"
    default_action = "Deny"
    ip_rules       = var.allowed_ipv4
  }
  tags = azurerm_resource_group.bank.tags
}

resource "azurerm_role_assignment" "secrets_reader" {
  scope                = azurerm_key_vault.bank.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.pipeline_principal_id
}
