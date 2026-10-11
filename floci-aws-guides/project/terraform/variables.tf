variable "region" {
  type    = string
  default = "us-east-1"
}

variable "prefix" {
  type    = string
  default = "floci-demo"
}

variable "db_password" {
  type      = string
  default   = "ChangeMe123!" # local only; use a secret manager on real AWS
  sensitive = true
}

locals {
  tags = {
    Project = var.prefix
    Env     = "floci-local"
  }
}
