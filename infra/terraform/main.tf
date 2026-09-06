data "aws_caller_identity" "current" {}

resource "random_id" "suffix" {
  byte_length = 4

  keepers = {
    project     = var.project_name
    environment = var.environment
  }
}

locals {
  resource_suffix    = random_id.suffix.hex
  bucket_name        = coalesce(var.bucket_name, "${var.project_name}-${var.environment}-${data.aws_caller_identity.current.account_id}-${local.resource_suffix}")
  firehose_name      = coalesce(var.firehose_name, "${var.project_name}-${var.environment}-${local.resource_suffix}")
  glue_database_name = replace("${var.project_name}-${var.environment}-${local.resource_suffix}", "-", "_")
  lake_prefix        = "bronze/ingest_year=!{timestamp:yyyy}/ingest_month=!{timestamp:MM}/ingest_day=!{timestamp:dd}/"
  error_prefix       = "errors/!{firehose:error-output-type}/ingest_year=!{timestamp:yyyy}/ingest_month=!{timestamp:MM}/ingest_day=!{timestamp:dd}/"

  common_tags = merge(
    {
      DataProduct = "factory-sensor"
      DataLayer   = "bronze"
    },
    var.tags,
  )
}

resource "aws_s3_bucket" "data_lake" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy
  tags          = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "data_lake_kms" {
  statement {
    sid    = "EnableAccountRootPermissions"
    effect = "Allow"

    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }

  statement {
    sid    = "AllowFirehoseEncryption"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:GenerateDataKey",
    ]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.firehose.arn]
    }
  }
}

resource "aws_kms_key" "data_lake" {
  description             = "Customer-managed key for the factory sensor bronze data lake."
  deletion_window_in_days = 7
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.data_lake_kms.json
  tags                    = local.common_tags
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/${var.project_name}-${var.environment}-${local.resource_suffix}-data-lake"
  target_key_id = aws_kms_key.data_lake.key_id
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket     = aws_s3_bucket.data_lake.id
  depends_on = [aws_s3_bucket_versioning.data_lake]

  rule {
    id     = "expire-bronze-objects"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    expiration {
      days = var.data_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = var.data_retention_days
    }
  }

  rule {
    id     = "expire-error-objects"
    status = "Enabled"

    filter {
      prefix = "errors/"
    }

    expiration {
      days = var.data_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = var.data_retention_days
    }
  }
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.firehose_name}"
  retention_in_days = var.cloudwatch_log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "DestinationDelivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_glue_catalog_database" "sensor" {
  name        = local.glue_database_name
  description = "Glue catalog for factory sensor bronze Parquet events."
  tags        = local.common_tags
}

resource "aws_glue_catalog_table" "sensor_events" {
  name          = "sensor_events"
  database_name = aws_glue_catalog_database.sensor.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Canonical factory-sensor.v1 events landed as partitioned Parquet."

  parameters = {
    classification                   = "parquet"
    typeOfData                       = "file"
    "projection.enabled"             = "true"
    "projection.ingest_year.type"    = "integer"
    "projection.ingest_year.range"   = "2020,2100"
    "projection.ingest_month.type"   = "integer"
    "projection.ingest_month.range"  = "1,12"
    "projection.ingest_month.digits" = "2"
    "projection.ingest_day.type"     = "integer"
    "projection.ingest_day.range"    = "1,31"
    "projection.ingest_day.digits"   = "2"
    "storage.location.template"      = "s3://${local.bucket_name}/bronze/ingest_year=$${ingest_year}/ingest_month=$${ingest_month}/ingest_day=$${ingest_day}/"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "schema_version"
      type = "string"
    }
    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "event_time"
      type = "timestamp"
    }
    columns {
      name = "ingested_at"
      type = "timestamp"
    }
    columns {
      name = "sensor_id"
      type = "string"
    }
    columns {
      name = "temperature"
      type = "double"
    }
    columns {
      name = "humidity"
      type = "double"
    }
    columns {
      name = "status"
      type = "string"
    }
    columns {
      name = "source"
      type = "struct<topic:string,partition:int,offset:bigint>"
    }
    columns {
      name = "vector_ingest_at"
      type = "timestamp"
    }
    columns {
      name = "pipeline"
      type = "string"
    }
  }

  partition_keys {
    name = "ingest_year"
    type = "int"
  }
  partition_keys {
    name = "ingest_month"
    type = "int"
  }
  partition_keys {
    name = "ingest_day"
    type = "int"
  }
}

data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    sid     = "FirehoseAssumeRole"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${local.firehose_name}-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "firehose_access" {
  statement {
    sid = "ListLakeBucket"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]

    resources = [aws_s3_bucket.data_lake.arn]
  }

  statement {
    sid = "WriteLakeObjects"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]

    resources = ["${aws_s3_bucket.data_lake.arn}/*"]
  }

  statement {
    sid = "EncryptLakeObjects"

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:GenerateDataKey",
    ]

    resources = [aws_kms_key.data_lake.arn]
  }

  statement {
    sid = "ReadGlueSchema"

    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
    ]

    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.sensor.name}",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.sensor.name}/${aws_glue_catalog_table.sensor_events.name}",
    ]
  }

  statement {
    sid = "WriteDeliveryLogs"

    actions = [
      "logs:CreateLogStream",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]

    resources = [
      aws_cloudwatch_log_group.firehose.arn,
      "${aws_cloudwatch_log_group.firehose.arn}:log-stream:*",
    ]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${local.firehose_name}-access"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_access.json
}

resource "aws_kinesis_firehose_delivery_stream" "sensor_events" {
  name        = local.firehose_name
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.data_lake.arn
    prefix              = local.lake_prefix
    error_output_prefix = local.error_prefix

    buffering_size     = var.enable_parquet_conversion ? 64 : 5
    buffering_interval = var.firehose_buffer_interval_seconds
    compression_format = var.enable_parquet_conversion ? "UNCOMPRESSED" : "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }

    data_format_conversion_configuration {
      enabled = var.enable_parquet_conversion

      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }

      output_format_configuration {
        serializer {
          parquet_ser_de {
            compression = "SNAPPY"
          }
        }
      }

      schema_configuration {
        catalog_id    = data.aws_caller_identity.current.account_id
        database_name = aws_glue_catalog_database.sensor.name
        region        = var.aws_region
        role_arn      = aws_iam_role.firehose.arn
        table_name    = aws_glue_catalog_table.sensor_events.name
        version_id    = "LATEST"
      }
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
}
