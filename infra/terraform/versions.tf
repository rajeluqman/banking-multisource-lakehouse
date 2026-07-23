# Pinned toolchain + providers. Versions are constrained, not floating: a plan that cannot be
# reproduced months later is not evidence. `.terraform.lock.hcl` is committed alongside this file.

terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  # No `backend` block. State is not kept as a record here (see README "State"): this
  # configuration reconciles primitives that already exist, and the artifact of value is the
  # plan diff, not a stored state file that would rot when the trial workspace is deleted (D-14).

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
}
