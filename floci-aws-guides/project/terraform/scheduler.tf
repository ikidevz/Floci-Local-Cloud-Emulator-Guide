resource "aws_scheduler_schedule_group" "main" {
  name = "${var.prefix}-group"
}

resource "aws_scheduler_schedule" "nightly_etl" {
  name                         = "${var.prefix}-nightly-etl"
  group_name                   = aws_scheduler_schedule_group.main.name
  schedule_expression          = "cron(0 2 * * ? *)"
  schedule_expression_timezone = "Asia/Manila"
  state                        = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.etl.arn
    role_arn = aws_iam_role.this["scheduler"].arn
    input    = jsonencode({ source = "scheduler" })

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}
