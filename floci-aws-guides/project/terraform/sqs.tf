resource "aws_sqs_queue" "dlq" {
  name                      = "${var.prefix}-jobs-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = local.tags
}

resource "aws_sqs_queue" "main" {
  name                       = "${var.prefix}-jobs"
  visibility_timeout_seconds = 60 # keep >= the Lambda timeout
  message_retention_seconds  = 345600
  tags                       = local.tags
}

resource "aws_sqs_queue_redrive_policy" "main" {
  queue_url = aws_sqs_queue.main.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main.arn]
  })
}

resource "aws_sqs_queue_policy" "main" {
  queue_url = aws_sqs_queue.main.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.main.arn
      Condition = { ArnLike = { "aws:SourceArn" = aws_s3_bucket.this["raw"].arn } }
    }]
  })
}
