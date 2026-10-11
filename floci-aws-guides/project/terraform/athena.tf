resource "aws_athena_workgroup" "main" {
  name          = "${var.prefix}-wg"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena-results"].bucket}/results/"
    }
  }
}

resource "aws_athena_named_query" "top_customers" {
  name      = "top-customers"
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.main.name
  query     = file("${path.module}/../sql/athena/top_customers.sql")
}
