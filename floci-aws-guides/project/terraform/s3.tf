resource "aws_s3_bucket" "this" {
  for_each      = toset(["raw", "curated", "scripts", "athena-results"])
  bucket        = "${var.prefix}-${each.key}"
  force_destroy = true
  tags          = local.tags
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.this["raw"].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.this["raw"].id
  rule {
    id     = "expire-raw"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    expiration {
      days = 30
    }
  }
  depends_on = [aws_s3_bucket_versioning.raw]
}

# S3 -> SQS notification (queue policy lives in sqs.tf)
resource "aws_s3_bucket_notification" "raw" {
  bucket = aws_s3_bucket.this["raw"].id

  queue {
    queue_arn     = aws_sqs_queue.main.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
    filter_suffix = ".csv"
  }

  depends_on = [aws_sqs_queue_policy.main]
}
