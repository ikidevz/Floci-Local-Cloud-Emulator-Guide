resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/${var.prefix}-ingest"
  retention_in_days = 14
  tags              = local.tags
}

resource "aws_cloudwatch_log_metric_filter" "errors" {
  name           = "${var.prefix}-errors"
  log_group_name = aws_cloudwatch_log_group.ingest.name
  pattern        = "ERROR"

  metric_transformation {
    name          = "ErrorCount"
    namespace     = "FlociDemo/Pipeline"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "failed_jobs" {
  alarm_name          = "${var.prefix}-failed-jobs"
  namespace           = "FlociDemo/Pipeline"
  metric_name         = "FailedJobs"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  dimensions          = { stage = "ingest" }
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.prefix}-dlq-not-empty"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
