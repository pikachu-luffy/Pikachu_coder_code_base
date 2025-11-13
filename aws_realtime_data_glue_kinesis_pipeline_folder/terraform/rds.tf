resource "aws_db_instance" "postgres_instance" {
  identifier          = var.db_instance_identifier
  allocated_storage   = var.db_allocated_storage
  engine              = "postgres"
  engine_version      = "16.6"
  instance_class      = var.db_instance_class
  username            = var.db_username
  password            = var.db_password
  skip_final_snapshot = true
  db_subnet_group_name    = aws_db_subnet_group.rds_subnet_group.name
  publicly_accessible = true
}
