variable "prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for Lambda functions"
  type        = list(string)
}

variable "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket name for artifacts"
  type        = string
}

variable "event_rate_per_second" {
  description = "Event generation rate"
  type        = number
  default     = 1.0
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
