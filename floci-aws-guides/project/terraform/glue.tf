resource "aws_glue_catalog_database" "main" {
  name = "floci_demo_db"
}

# Floci crawlers do not write tables, so declare them explicitly.
resource "aws_glue_catalog_table" "orders_raw" {
  name          = "orders_raw"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification            = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.this["raw"].bucket}/raw/orders/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters            = { "field.delim" = "," }
    }

    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "amount"
      type = "double"
    }
    columns {
      name = "status"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "orders_clean" {
  name          = "orders_clean"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification            = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.this["curated"].bucket}/clean/orders/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters            = { "field.delim" = "," }
    }

    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "amount"
      type = "double"
    }
    columns {
      name = "status"
      type = "string"
    }
  }
}

resource "aws_s3_object" "clean_orders_script" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "glue/clean_orders_shell.py"
  source = "${path.module}/../glue_scripts/clean_orders_shell.py"
  etag   = filemd5("${path.module}/../glue_scripts/clean_orders_shell.py")
}

resource "aws_glue_crawler" "orders" {
  name          = "${var.prefix}-orders-crawler"
  role          = aws_iam_role.this["glue"].arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.this["raw"].bucket}/raw/orders/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_job" "clean_orders" {
  name         = "${var.prefix}-clean-orders"
  role_arn     = aws_iam_role.this["glue"].arn
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 30

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.this["scripts"].bucket}/${aws_s3_object.clean_orders_script.key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"   = "python"
    "--SOURCE_BUCKET"  = aws_s3_bucket.this["raw"].bucket
    "--SOURCE_KEY"     = "raw/orders/orders.csv"
    "--TARGET_BUCKET"  = aws_s3_bucket.this["curated"].bucket
    "--TARGET_KEY"     = "clean/orders/orders.csv"
  }
}
