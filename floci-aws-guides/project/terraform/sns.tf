resource "aws_sns_topic" "alerts" {
  name = "${var.prefix}-alerts"
  tags = local.tags
}

resource "aws_sqs_queue" "alerts_errors" {
  name = "${var.prefix}-alerts-errors"
}

resource "aws_sqs_queue_policy" "alerts_errors" {
  queue_url = aws_sqs_queue.alerts_errors.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.alerts_errors.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_sns_topic.alerts.arn } }
    }]
  })
}

resource "aws_sns_topic_subscription" "errors_only" {
  topic_arn            = aws_sns_topic.alerts.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.alerts_errors.arn
  raw_message_delivery = true
  filter_policy        = jsonencode({ severity = ["ERROR", "CRITICAL"] })
  depends_on           = [aws_sqs_queue_policy.alerts_errors]
}
