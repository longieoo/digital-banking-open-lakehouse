output "resource_group" {
  value = azurerm_resource_group.bank.name
}
output "storage_account" {
  value = azurerm_storage_account.lake.name
}
output "raw_uri" {
  value = "abfss://raw@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
}
output "warehouse_uri" {
  value = "abfss://warehouse@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
}
output "key_vault_uri" {
  value = azurerm_key_vault.bank.vault_uri
}
