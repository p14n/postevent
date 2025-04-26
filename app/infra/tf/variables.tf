
variable "vpc_id" {
  type        = string
  description = "VPC ID for the private subnets"
}

variable "vpc_cidr" {
  type        = string
  description = "VPC CIDR block"
}

variable "subnet_ids" {
  type        = list(string)
  description = "List of subnet IDs for the private subnets"
}

variable "route_table_id" {
  type        = string
  description = "Route table ID for the private subnets"
}

variable "db_password" {
  type    = string
  default = "postgres"
}

variable "image_tag" {
  type = string
}

variable "service_names" {
  type    = list(string)
  default = ["writer", "reader1", "reader2", "reader3"]
}
