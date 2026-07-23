# The lake bucket — sole source of truth for the medallion (ADR-002). Landing/Bronze hold
# unmasked PII (R-27, R-31), so the public-access block and encryption below are load-bearing
# controls, not boilerplate.

resource "aws_s3_bucket" "lake" {
  bucket = var.lake_bucket_name
}

# Versioning is the recovery path for the failure mode this pipeline actually hit: a transform
# overwriting a Delta table with a bad full-volume run. Without it, recovery means re-ingesting
# from five source systems.
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# R-31: raw layers hold unmasked PII. Public exposure is not a risk this project accepts, so all
# four switches are set explicitly rather than relying on the account default.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Landing is a staging area, not an archive — ADR-007's landing-TTL reasoning. Expiring it keeps
# the PII blast radius small and storage cost flat, while Bronze remains the replayable record.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-landing"
    status = "Enabled"

    filter {
      prefix = "banking/landing/"
    }

    expiration {
      days = 30
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}
