resource "aws_db_parameter_group" "postevent" {
  name   = "postevent"
  family = "postgres17"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "shared_preload_libraries"
    value        = "pglogical"
    apply_method = "pending-reboot"
  }
}

resource "aws_db_subnet_group" "postevent" {
  name       = "main"
  subnet_ids = var.subnet_ids

  tags = {
    Name = "Postevent subnet group"
  }
}

resource "aws_db_instance" "postevent" {
  count = 4

  identifier                   = "postevent-${count.index}"
  engine                       = "postgres"
  engine_version               = "17"
  instance_class               = "db.t3.micro"
  allocated_storage            = 20
  storage_encrypted            = true
  performance_insights_enabled = true
  db_name                      = "postgres"
  username                     = "postgres"
  password                     = var.db_password
  publicly_accessible          = true

  parameter_group_name = aws_db_parameter_group.postevent.name
  skip_final_snapshot  = true

  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.postevent.name
}
