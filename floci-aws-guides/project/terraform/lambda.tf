data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_src/ingest"
  output_path = "${path.module}/build/ingest.zip"
}

data "archive_file" "glue_runner" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_src/glue_runner"
  output_path = "${path.module}/build/glue_runner.zip"
}

data "archive_file" "kinesis_to_firehose" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_src/kinesis_to_firehose"
  output_path = "${path.module}/build/kinesis_to_firehose.zip"
}

resource "aws_lambda_function" "ingest" {
  function_name    = "${var.prefix}-ingest"
  role             = aws_iam_role.this["lambda"].arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.ingest.output_path
  source_code_hash = data.archive_file.ingest.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      TABLE_NAME  = aws_dynamodb_table.events.name
      STREAM_NAME = aws_kinesis_stream.events.name
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }
  tags = local.tags
}

resource "aws_lambda_function" "glue_runner" {
  function_name    = "${var.prefix}-glue-runner"
  role             = aws_iam_role.this["lambda"].arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.glue_runner.output_path
  source_code_hash = data.archive_file.glue_runner.output_base64sha256
  timeout          = 30
  tags             = local.tags
}

resource "aws_lambda_function" "kinesis_to_firehose" {
  function_name    = "${var.prefix}-kinesis-to-firehose"
  role             = aws_iam_role.this["lambda"].arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.kinesis_to_firehose.output_path
  source_code_hash = data.archive_file.kinesis_to_firehose.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      FIREHOSE_STREAM = aws_kinesis_firehose_delivery_stream.to_s3.name
    }
  }
  tags = local.tags
}

resource "aws_lambda_event_source_mapping" "from_queue" {
  event_source_arn        = aws_sqs_queue.main.arn
  function_name           = aws_lambda_function.ingest.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}

resource "aws_lambda_event_source_mapping" "from_stream" {
  event_source_arn               = aws_kinesis_stream.events.arn
  function_name                  = aws_lambda_function.kinesis_to_firehose.arn
  starting_position              = "TRIM_HORIZON"
  batch_size                     = 100
  maximum_retry_attempts         = 2
  bisect_batch_on_function_error = true
}
