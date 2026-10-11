resource "aws_kinesis_stream" "events" {
  name             = "${var.prefix}-events"
  shard_count      = 2
  retention_period = 24
  tags             = local.tags
}
