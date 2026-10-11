resource "aws_db_instance" "postgres" {
  identifier              = "${var.prefix}-pg"
  engine                  = "postgres"
  engine_version          = "16"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  db_name                 = "appdb"
  username                = "app"
  password                = var.db_password
  publicly_accessible     = true
  skip_final_snapshot     = true
  backup_retention_period = 0
  tags                    = local.tags
}
