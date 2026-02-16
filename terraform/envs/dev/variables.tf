variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "prefix" {
  description = "Resource name prefix"
  type        = string
  default     = "cat-demo"
}

variable "owner_email" {
  description = "Owner email for tagging"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = "Availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "low_cost_mode" {
  description = "Enable low-cost mode (single AZ, minimal capacity)"
  type        = bool
  default     = true
}

variable "watermark_delay_seconds" {
  description = "Watermark delay for late event handling"
  type        = number
  default     = 30
}

variable "event_rate_per_second" {
  description = "Event generation rate"
  type        = number
  default     = 2.0
}

variable "data_retention_days" {
  description = "S3 data retention in days"
  type        = number
  default     = 30
}
