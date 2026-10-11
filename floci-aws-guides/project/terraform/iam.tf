locals {
  roles = {
    lambda    = "lambda.amazonaws.com"
    glue      = "glue.amazonaws.com"
    sfn       = "states.amazonaws.com"
    scheduler = "scheduler.amazonaws.com"
    firehose  = "firehose.amazonaws.com"
  }

  s3_rw = {
    Effect = "Allow"
    Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
    "s3:GetBucketLocation", "s3:AbortMultipartUpload"]
    Resource = ["arn:aws:s3:::${var.prefix}-*", "arn:aws:s3:::${var.prefix}-*/*"]
  }

  logs = {
    Effect   = "Allow"
    Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    Resource = "*"
  }
}

resource "aws_iam_role" "this" {
  for_each = local.roles
  name     = "${var.prefix}-${each.key}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = each.value }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "lambda" {
  name = "inline"
  role = aws_iam_role.this["lambda"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.logs,
      local.s3_rw,
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query"]
        Resource = "arn:aws:dynamodb:*:*:table/${var.prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:*:*:${var.prefix}-*"
      },
      {
        Effect = "Allow"
        Action = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
        "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords"]
        Resource = "arn:aws:kinesis:*:*:stream/${var.prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
        Resource = "arn:aws:firehose:*:*:deliverystream/${var.prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:StartCrawler", "glue:GetCrawler"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "glue" {
  name = "inline"
  role = aws_iam_role.this["glue"].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [local.logs, local.s3_rw, { Effect = "Allow", Action = ["glue:*"], Resource = "*" }]
  })
}

resource "aws_iam_role_policy" "sfn" {
  name = "inline"
  role = aws_iam_role.this["sfn"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.logs,
      { Effect = "Allow", Action = ["lambda:InvokeFunction"], Resource = "arn:aws:lambda:*:*:function:${var.prefix}-*" },
      { Effect = "Allow", Action = ["sns:Publish"], Resource = "arn:aws:sns:*:*:${var.prefix}-*" },
    ]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "inline"
  role = aws_iam_role.this["scheduler"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = "arn:aws:states:*:*:stateMachine:${var.prefix}-*"
    }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "inline"
  role = aws_iam_role.this["firehose"].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [local.s3_rw, local.logs]
  })
}
