# DirectPut. Kinesis -> Firehose is bridged by the kinesis_to_firehose Lambda (lambda.tf).
resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = "${var.prefix}-to-s3"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.this["firehose"].arn
    bucket_arn          = aws_s3_bucket.this["curated"].arn
    prefix              = "events/"
    error_output_prefix = "events/errors/!{firehose:error-output-type}/"
    buffering_size      = 1
    buffering_interval  = 60
    compression_format  = "UNCOMPRESSED"
  }

  tags = local.tags
}
