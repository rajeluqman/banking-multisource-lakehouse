# Every environment-specific value is a variable with no default. Hardcoding an account id, a
# bucket name or a workspace host would both leak infrastructure detail into a public repo and
# make this configuration non-portable when the disposable trial workspace is replaced (D-14).

variable "aws_region" {
  description = "AWS region hosting the lake bucket."
  type        = string
}

variable "lake_bucket_name" {
  description = "S3 bucket that is the sole source of truth for the lakehouse (ADR-002)."
  type        = string
}

variable "databricks_host" {
  description = "Databricks workspace URL, e.g. https://<workspace>.cloud.databricks.com."
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account id used in the IAM trust policy's ExternalId condition."
  type        = string
  sensitive   = true
}

variable "databricks_unity_catalog_role_arn" {
  description = <<-EOT
    AWS role ARN Databricks Unity Catalog assumes to reach the bucket. This is self-referential
    by design: the trust policy must name the very role it is attached to, which is why the role
    is created first and the trust policy applied second (see iam.tf).
  EOT
  type        = string
}

variable "uc_catalog_name" {
  description = "Unity Catalog catalog holding the medallion schemas."
  type        = string
  default     = "banking"
}

variable "snowflake_organization_name" {
  description = "Snowflake organization name (provider v1 splits the old account identifier in two)."
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name within the organization."
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake user Terraform authenticates as."
  type        = string
}

variable "snowflake_terraform_role" {
  description = "Snowflake role Terraform assumes; needs privileges to manage warehouses and monitors."
  type        = string
  default     = "SYSADMIN"
}

variable "serving_warehouse_name" {
  description = "Snowflake virtual warehouse serving the Gold external tables."
  type        = string
  default     = "BANKING_SERVING_WH"
}

variable "serving_warehouse_auto_suspend_seconds" {
  description = "Idle seconds before the serving warehouse suspends. The primary idle-spend guardrail."
  type        = number
  default     = 60
}

variable "monthly_credit_quota" {
  description = "Monthly credit ceiling for the serving warehouse's resource monitor."
  type        = number
  default     = 5
}
