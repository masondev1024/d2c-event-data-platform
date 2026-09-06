output "aws_region" {
  description = "AWS region used by this stack."
  value       = var.aws_region
}

output "bucket_name" {
  description = "S3 data lake bucket containing bronze Parquet and Firehose errors."
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_kms_key_arn" {
  description = "Customer-managed KMS key used for S3 data lake encryption."
  value       = aws_kms_key.data_lake.arn
}

output "firehose_name" {
  description = "DirectPut Firehose delivery stream consumed by Vector."
  value       = aws_kinesis_firehose_delivery_stream.sensor_events.name
}

output "glue_database_name" {
  description = "Glue Data Catalog database."
  value       = aws_glue_catalog_database.sensor.name
}

output "glue_table_name" {
  description = "Glue/Athena table for canonical sensor events."
  value       = aws_glue_catalog_table.sensor_events.name
}

output "firehose_log_group_name" {
  description = "CloudWatch log group for Firehose delivery diagnostics."
  value       = aws_cloudwatch_log_group.firehose.name
}
