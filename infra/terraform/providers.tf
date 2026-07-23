# Provider configuration. Every credential arrives from the environment (TF_VAR_* or the
# provider's own env vars) — nothing authenticating is written in this repo. journey/09 §1.

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "banking-multisource-lakehouse"
      ManagedBy = "terraform"
      Layer     = "cloud-primitives"
    }
  }
}

# Host comes from DATABRICKS_HOST, auth from DATABRICKS_TOKEN — the same environment contract
# cd.yml already uses for the Asset Bundle deploy, so there is one auth story, not two.
provider "databricks" {
  host = var.databricks_host
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  role              = var.snowflake_terraform_role
}
