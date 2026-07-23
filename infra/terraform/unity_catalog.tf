# Unity Catalog's storage seam: the credential (which IAM role UC assumes) and the external
# location (which S3 prefix that credential is allowed to govern).
#
# OWNERSHIP BOUNDARY — read this before adding anything here.
#   * Terraform owns  : cloud primitives, the storage credential, the external location, and
#                       the *storage-level* grants below.
#   * Asset Bundles own: the Databricks Job (databricks.yml). Never declare a `databricks_job`
#                       in this configuration — two tools reconciling one Job is drift.
#   * SQL DDL owns     : catalog/schema/table grants (pipeline/gold/grants/uc_grants.sql), which
#                       are data-object permissions, not infrastructure.
# One owner per resource. The seam is deliberate and is the reason nothing here overlaps.

resource "databricks_storage_credential" "lake" {
  name    = "banking-lakehouse-cred"
  comment = "Credential UC uses to reach the banking lakehouse bucket (Terraform-managed)."

  aws_iam_role {
    role_arn = aws_iam_role.unity_catalog.arn
  }
}

resource "databricks_external_location" "lake" {
  name            = "banking-lakehouse-root"
  url             = "s3://${aws_s3_bucket.lake.id}/banking"
  credential_name = databricks_storage_credential.lake.name
  comment         = "Medallion root. Layer prefixes live beneath: landing/ bronze/ silver/ gold/."

  depends_on = [aws_iam_role_policy.unity_catalog]
}

# Gold is the only layer a serving identity may reach. Scoping a second external location at the
# gold/ prefix is what makes R-32 ("serving_ro read-only, scoped to banking/gold/ only")
# enforceable at the storage layer instead of relying on table grants alone.
resource "databricks_external_location" "gold" {
  name            = "banking-lakehouse-gold"
  url             = "s3://${aws_s3_bucket.lake.id}/banking/gold"
  credential_name = databricks_storage_credential.lake.name
  read_only       = true
  comment         = "Read-only Gold serving surface (R-32)."

  depends_on = [aws_iam_role_policy.unity_catalog]
}

# Storage-level grants. The pipeline service identity is the only one that may write raw layers;
# every consumer role is read-only and confined to Gold (R-31: no analyst or serving role may
# read Landing/Bronze, which hold unmasked PII).
resource "databricks_grants" "lake_root" {
  external_location = databricks_external_location.lake.id

  grant {
    principal  = "pipeline_svc"
    privileges = ["READ_FILES", "WRITE_FILES"]
  }

  grant {
    principal  = "data_engineer"
    privileges = ["READ_FILES"]
  }
}

resource "databricks_grants" "gold" {
  external_location = databricks_external_location.gold.id

  grant {
    principal  = "serving_ro"
    privileges = ["READ_FILES"]
  }

  grant {
    principal  = "analyst_marketing"
    privileges = ["READ_FILES"]
  }

  grant {
    principal  = "fraud_ops"
    privileges = ["READ_FILES"]
  }

  grant {
    principal  = "risk"
    privileges = ["READ_FILES"]
  }
}
