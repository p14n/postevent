
variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "route_table_id" {
  type        = string
  description = "Route table ID for the private subnets"
}

variable "db_password" {
  type = string
}

variable "image_tag" {
  type = string
}

variable "service_names" {
  type    = list(string)
  default = ["service1", "service2", "service3", "service4"]
}
