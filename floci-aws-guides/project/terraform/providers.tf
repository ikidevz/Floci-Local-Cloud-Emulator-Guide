terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0" # the version Floci's own compatibility suite runs against
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

variable "endpoint" {
  type    = string
  default = "http://localhost:4566"
}

provider "aws" {
  region     = var.region
  access_key = "test"
  secret_key = "test"

  # Floci is not real AWS: skip the checks that need real AWS endpoints
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3             = var.endpoint
    sqs            = var.endpoint
    sns            = var.endpoint
    dynamodb       = var.endpoint
    lambda         = var.endpoint
    iam            = var.endpoint
    sts            = var.endpoint
    kinesis        = var.endpoint
    firehose       = var.endpoint
    glue           = var.endpoint
    athena         = var.endpoint
    rds            = var.endpoint
    cloudwatch     = var.endpoint
    logs           = var.endpoint
    events         = var.endpoint
    sfn            = var.endpoint
    scheduler      = var.endpoint
  }
}
