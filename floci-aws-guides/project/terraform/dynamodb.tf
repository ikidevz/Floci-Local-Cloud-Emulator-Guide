resource "aws_dynamodb_table" "events" {
  name         = "${var.prefix}-events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"
  range_key    = "ts"

  attribute {
    name = "run_id"
    type = "S"
  }
  attribute {
    name = "ts"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }

  global_secondary_index {
    name            = "status-ts-index"
    hash_key        = "status"
    range_key       = "ts"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"
  tags             = local.tags
}
