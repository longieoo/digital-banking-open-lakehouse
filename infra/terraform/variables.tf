variable "subscription_id" {
  type        = string
  description = "Azure subscription ID; use a dedicated demo subscription/resource group."
}
variable "location" {
  type    = string
  default = "southeastasia"
}
variable "storage_account_name" {
  type        = string
  description = "Globally unique lowercase name; also forms the Key Vault name."
  validation {
    condition     = can(regex("^[a-z][a-z0-9]{2,19}$", var.storage_account_name))
    error_message = "Use 3-20 lowercase letters/digits, starting with a letter."
  }
}
variable "pipeline_principal_id" {
  type        = string
  description = "Existing managed identity or service principal object ID (not client ID)."
}
variable "allowed_ipv4" {
  type        = list(string)
  description = "Public egress IPv4 addresses/CIDRs of the Terraform runner and demo clients."
  validation {
    condition     = length(var.allowed_ipv4) > 0 && !contains(var.allowed_ipv4, "0.0.0.0/0")
    error_message = "Supply limited public runner/client IPs; never the entire internet."
  }
}
