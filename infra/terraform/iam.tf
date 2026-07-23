# The role Unity Catalog assumes to reach the lake bucket.
#
# THE SELF-REFERENTIAL TRUST POLICY. Databricks requires this role to trust *itself* in addition
# to the Databricks control-plane principal. That makes the policy depend on the ARN of the role
# the policy is attached to — a cycle Terraform cannot resolve inside a single resource. It is
# broken by constructing the ARN from known parts (`var.databricks_unity_catalog_role_arn`)
# rather than reading it back from the resource, which is what turns a documented manual console
# step into a reconcilable resource.

data "aws_iam_policy_document" "uc_trust" {
  # Databricks' own control-plane account assumes the role, scoped by ExternalId to this account
  # so a confused-deputy cannot borrow it.
  statement {
    sid     = "DatabricksControlPlaneAssume"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::414351767826:root"]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }

  # The self-assumption leg Unity Catalog requires.
  statement {
    sid     = "SelfAssume"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.databricks_unity_catalog_role_arn]
    }
  }
}

resource "aws_iam_role" "unity_catalog" {
  name               = "banking-lakehouse-uc-access"
  assume_role_policy = data.aws_iam_policy_document.uc_trust.json
  description        = "Assumed by Unity Catalog to read/write the banking lakehouse bucket."
}

# Least privilege: object access is scoped to the bucket, and the bucket-level actions are the
# minimum set Unity Catalog needs to enumerate and manage multipart uploads. No wildcard resource.
data "aws_iam_policy_document" "uc_access" {
  statement {
    sid    = "ObjectAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    sid    = "BucketAccess"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]

    resources = [aws_s3_bucket.lake.arn]
  }
}

resource "aws_iam_role_policy" "unity_catalog" {
  name   = "banking-lakehouse-uc-access"
  role   = aws_iam_role.unity_catalog.id
  policy = data.aws_iam_policy_document.uc_access.json
}
