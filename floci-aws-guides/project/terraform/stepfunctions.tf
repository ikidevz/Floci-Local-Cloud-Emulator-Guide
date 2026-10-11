resource "aws_sfn_state_machine" "etl" {
  name     = "${var.prefix}-etl"
  role_arn = aws_iam_role.this["sfn"].arn
  type     = "STANDARD"

  definition = templatefile("${path.module}/../sfn_definitions/etl_pipeline.asl.json", {
    GLUE_RUNNER_ARN = aws_lambda_function.glue_runner.arn
    GLUE_JOB_NAME   = aws_glue_job.clean_orders.name
    SNS_TOPIC_ARN   = aws_sns_topic.alerts.arn
  })

  tags = local.tags
}
