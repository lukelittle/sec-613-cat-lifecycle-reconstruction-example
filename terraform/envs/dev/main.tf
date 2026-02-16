terraform {
  required_version = ">= 1.6"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  
  default_tags {
    tags = {
      Project     = "cat-demo"
      Environment = var.environment
      Owner       = var.owner_email
      ManagedBy   = "terraform"
    }
  }
}

locals {
  prefix = "${var.prefix}-${var.environment}"
  
  common_tags = {
    Project     = "cat-demo"
    Environment = var.environment
    Owner       = var.owner_email
  }
}

# VPC and Networking
module "vpc" {
  source = "../../modules/vpc"
  
  prefix        = local.prefix
  cidr_block    = var.vpc_cidr
  azs           = var.azs
  low_cost_mode = var.low_cost_mode
  tags          = local.common_tags
}

# S3 Bucket for data and artifacts
resource "aws_s3_bucket" "cat_data" {
  bucket = "${local.prefix}-data"
  
  tags = merge(local.common_tags, {
    Name = "${local.prefix}-data-bucket"
  })
}

resource "aws_s3_bucket_versioning" "cat_data" {
  bucket = aws_s3_bucket.cat_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "cat_data" {
  bucket = aws_s3_bucket.cat_data.id
  
  rule {
    id     = "expire-old-data"
    status = "Enabled"
    
    expiration {
      days = var.data_retention_days
    }
  }
}

# Upload Spark job
resource "aws_s3_object" "spark_job" {
  bucket = aws_s3_bucket.cat_data.id
  key    = "spark-jobs/lifecycle_streaming.py"
  source = "../../../spark/lifecycle_job/lifecycle_streaming.py"
  etag   = filemd5("../../../spark/lifecycle_job/lifecycle_streaming.py")
}

# MSK Serverless Cluster
module "msk" {
  source = "../../modules/msk"
  
  prefix                     = local.prefix
  vpc_id                     = module.vpc.vpc_id
  subnet_ids                 = module.vpc.private_subnet_ids
  client_security_group_ids  = [module.lambda.security_group_id]
  tags                       = local.common_tags
}

# Lambda Functions
module "lambda" {
  source = "../../modules/lambda"
  
  prefix                  = local.prefix
  vpc_id                  = module.vpc.vpc_id
  subnet_ids              = module.vpc.private_subnet_ids
  kafka_bootstrap_servers = module.msk.bootstrap_brokers
  s3_bucket_name          = aws_s3_bucket.cat_data.id
  event_rate_per_second   = var.event_rate_per_second
  tags                    = local.common_tags
}

# API Gateway
resource "aws_apigatewayv2_api" "operator" {
  name          = "${local.prefix}-operator-api"
  protocol_type = "HTTP"
  
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["*"]
  }
  
  tags = local.common_tags
}

resource "aws_apigatewayv2_integration" "operator" {
  api_id           = aws_apigatewayv2_api.operator.id
  integration_type = "AWS_PROXY"
  
  integration_uri        = module.lambda.operator_api_invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "operator" {
  for_each = toset([
    "POST /generator/start",
    "POST /generator/stop",
    "POST /generator/mode",
    "GET /health",
    "GET /stats"
  ])
  
  api_id    = aws_apigatewayv2_api.operator.id
  route_key = each.value
  target    = "integrations/${aws_apigatewayv2_integration.operator.id}"
}

resource "aws_apigatewayv2_stage" "operator" {
  api_id      = aws_apigatewayv2_api.operator.id
  name        = "$default"
  auto_deploy = true
  
  tags = local.common_tags
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda.operator_api_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.operator.execution_arn}/*/*"
}

# DynamoDB for lifecycle snapshots
resource "aws_dynamodb_table" "lifecycles" {
  name           = "${local.prefix}-lifecycles"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "customer_order_id"
  range_key      = "snapshot_type"
  
  attribute {
    name = "customer_order_id"
    type = "S"
  }
  
  attribute {
    name = "snapshot_type"
    type = "S"
  }
  
  attribute {
    name = "symbol"
    type = "S"
  }
  
  attribute {
    name = "ts_snapshot"
    type = "N"
  }
  
  global_secondary_index {
    name            = "symbol-index"
    hash_key        = "symbol"
    range_key       = "ts_snapshot"
    projection_type = "ALL"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  
  tags = merge(local.common_tags, {
    Name = "${local.prefix}-lifecycles-table"
  })
}

# EMR Serverless Application
resource "aws_emrserverless_application" "spark" {
  name          = "${local.prefix}-spark-app"
  release_label = "emr-7.0.0"
  type          = "Spark"
  
  initial_capacity {
    initial_capacity_type = "Driver"
    
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
      }
    }
  }
  
  initial_capacity {
    initial_capacity_type = "Executor"
    
    initial_capacity_config {
      worker_count = var.low_cost_mode ? 1 : 2
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "8 GB"
      }
    }
  }
  
  maximum_capacity {
    cpu    = var.low_cost_mode ? "16 vCPU" : "64 vCPU"
    memory = var.low_cost_mode ? "32 GB" : "128 GB"
  }
  
  auto_start_configuration {
    enabled = true
  }
  
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
  
  network_configuration {
    subnet_ids         = module.vpc.private_subnet_ids
    security_group_ids = [module.lambda.security_group_id]
  }
  
  tags = local.common_tags
}

# IAM Role for EMR Serverless
resource "aws_iam_role" "emr_job" {
  name = "${local.prefix}-emr-job-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "emr-serverless.amazonaws.com"
      }
    }]
  })
  
  tags = local.common_tags
}

resource "aws_iam_role_policy" "emr_job" {
  name = "${local.prefix}-emr-job-policy"
  role = aws_iam_role.emr_job.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.cat_data.arn,
          "${aws_s3_bucket.cat_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:CreateTopic"
        ]
        Resource = [
          module.msk.cluster_arn,
          "${module.msk.cluster_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${aws_emrserverless_application.spark.id}"
  retention_in_days = 7
  
  tags = local.common_tags
}

# Outputs
output "msk_bootstrap_servers" {
  value       = module.msk.bootstrap_brokers
  description = "MSK bootstrap servers"
}

output "api_gateway_url" {
  value       = aws_apigatewayv2_api.operator.api_endpoint
  description = "API Gateway URL"
}

output "s3_bucket_name" {
  value       = aws_s3_bucket.cat_data.id
  description = "S3 bucket name"
}

output "emr_application_id" {
  value       = aws_emrserverless_application.spark.id
  description = "EMR Serverless application ID"
}

output "emr_job_role_arn" {
  value       = aws_iam_role.emr_job.arn
  description = "EMR job execution role ARN"
}

output "generator_function_name" {
  value       = module.lambda.generator_function_name
  description = "Event generator Lambda function name"
}

output "dynamodb_table_name" {
  value       = aws_dynamodb_table.lifecycles.name
  description = "DynamoDB table name"
}
