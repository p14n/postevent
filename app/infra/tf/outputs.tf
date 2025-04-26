output "rds_endpoints" {
  value = aws_db_instance.postevent[*].endpoint
}

output "cluster_arn" {
  value = aws_ecs_cluster.postevent.arn
}

output "service_names" {
  value = aws_ecs_service.postevent[*].name
}