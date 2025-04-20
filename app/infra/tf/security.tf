# ALB Security Group
resource "aws_security_group" "alb" {
  name        = "postevent-alb"
  description = "Security group for ALB"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 50052
    to_port         = 50052
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
