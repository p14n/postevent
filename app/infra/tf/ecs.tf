
locals {
  container_environment = [
    [
      { name = "APP_WRITE_TOPICS", value = "topicone" },
      { name = "APP_DB_HOST", value = split(":", aws_db_instance.postevent[0].endpoint)[0] }
    ],
    [
      { name = "APP_READ_TOPICS", value = "topicone" },
      { name = "APP_WRITE_TOPICS", value = "topictwo" },
      { name = "APP_DB_HOST", value = split(":", aws_db_instance.postevent[1].endpoint)[0] },
      { name = "APP_TOPIC_HOST", value = aws_lb.postevent[0].dns_name }
    ],
    [
      { name = "APP_READ_TOPICS", value = "topictwo" },
      { name = "APP_WRITE_TOPICS", value = "topicthree" },
      { name = "APP_DB_HOST", value = split(":", aws_db_instance.postevent[2].endpoint)[0] },
      { name = "APP_TOPIC_HOST", value = aws_lb.postevent[1].dns_name }
    ],
    [
      { name = "APP_READ_TOPICS", value = "topicthree" },
      { name = "APP_WRITE_TOPICS", value = "topicfour" },
      { name = "APP_DB_HOST", value = split(":", aws_db_instance.postevent[3].endpoint)[0] },
      { name = "APP_TOPIC_HOST", value = aws_lb.postevent[2].dns_name }
    ]
  ]
}


resource "aws_ecs_task_definition" "postevent" {
  count                    = 4
  family                   = "postevent-${var.service_names[count.index]}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name        = "app"
      image       = "937578967415.dkr.ecr.eu-west-1.amazonaws.com/p14n/postevent-app:${var.image_tag}"
      environment = local.container_environment[count.index]
      portMappings = [
        {
          containerPort = 50052
          hostPort      = 50052
          protocol      = "tcp"
        },
        {
          containerPort = 8080
          hostPort      = 8080
          protocol      = "tcp"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.postevent.name
          "awslogs-region"        = "eu-west-1"
          "awslogs-stream-prefix" = var.service_names[count.index]
        }
      }
    },
    {
      name    = "aws-otel-collector"
      image   = "amazon/aws-otel-collector"
      command = ["--config=/etc/ecs/ecs-default-config.yaml"]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.postevent.name
          "awslogs-region"        = "eu-west-1"
          "awslogs-stream-prefix" = "${var.service_names[count.index]}-otel"
        }
      }
    }
  ])
}

# Create ALB for each service
resource "aws_lb" "postevent" {
  count              = 4
  name               = "postevent-${var.service_names[count.index]}"
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.subnet_ids
}

resource "aws_lb_target_group" "postevent" {
  count       = 4
  name        = "postevent-${var.service_names[count.index]}"
  port        = 50052
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    healthy_threshold   = 2
    interval            = 30
    matcher             = "200"
    path                = "/health"
    port                = "8080"
    timeout             = 5
    unhealthy_threshold = 3
    protocol            = "HTTP"
  }
}

resource "aws_lb_listener" "postevent" {
  count             = 4
  load_balancer_arn = aws_lb.postevent[count.index].arn
  port              = "50052"
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.postevent[count.index].arn
  }
}

# Update ECS service to use target groups
resource "aws_ecs_service" "postevent" {
  count           = 4
  name            = var.service_names[count.index]
  cluster         = aws_ecs_cluster.postevent.id
  task_definition = aws_ecs_task_definition.postevent[count.index].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.postevent[count.index].arn
    container_name   = "app"
    container_port   = 50052
  }
}
