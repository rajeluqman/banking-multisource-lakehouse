output "lake_bucket_arn" {
  description = "ARN of the lakehouse bucket."
  value       = aws_s3_bucket.lake.arn
}

output "unity_catalog_role_arn" {
  description = "Role UC assumes. Feed back into var.databricks_unity_catalog_role_arn on first apply (see README)."
  value       = aws_iam_role.unity_catalog.arn
}

output "external_location_gold_url" {
  description = "Read-only Gold serving prefix (R-32)."
  value       = databricks_external_location.gold.url
}

output "serving_warehouse_name" {
  description = "Snowflake warehouse serving Gold external tables."
  value       = snowflake_warehouse.serving.name
}
