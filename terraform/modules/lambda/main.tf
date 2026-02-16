# Lambda Functions Module

# Security group for Lambda functions
resource "aws_security_group" "lambda" {
  name        = "${var.prefix}-lambda-sg"
  description = "Security group for Lambda functions"
  vpc_id      = var.vpc_id
  
  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${var.prefix}-lambda-sg"
  })
}

# IAM Role for Lambda functions
resource "aws_iam_role" "lambda" {
  name = "${var.prefix}-lambda-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_custom" {
  name = "${var.prefix}-lambda-policy"
  role = aws_iam_role.lambda.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeTopic"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "arn:aws:s3:::${var.s3_bucket_name}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "*"
      }
    ]
  })
}

# Package Lambda functions
data "archive_file" "generator" {
  type        = "zip"
  source_dir  = "${path.module}/../../../services/event_generator"
  output_path = "${path.module}/generator.zip"
}

data "archive_file" "operator_api" {
  type        = "zip"
  source_dir  = "${path.module}/../../../services/operator_api"
  output_path = "${path.module}/operator_api.zip"
}

# Event Generator Lambda
resource "aws_lambda_function" "generator" {
  filename         = data.archive_file.generator.output_path
  function_name    = "${var.prefix}-event-generator"
  role            = aws_iam_role.lambda.arn
  handler         = "generator.lambda_handler"
  source_code_hash = data.archive_file.generator.output_base64sha256
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 512
  
  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }
  
  environment {
    variables = {
      KAFKA_BOOTSTRAP_SERVERS = var.kafka_bootstrap_servers
      EVENT_RATE_PER_SECOND   = tostring(var.event_rate_per_second)
      MODE                    = "normal"
    }
  }
  
  tags = merge(var.tags, {
    Name = "${var.prefix}-event-generator"
  })
}

# Operator API Lambda
resource "aws_lambda_function" "operator_api" {
  filename         = data.archive_file.operator_api.output_path
  function_name    = "${var.prefix}-operator-api"
  role            = aws_iam_role.lambda.arn
  handler         = "api.lambda_handler"
  source_code_hash = data.archive_file.operator_api.output_base64sha256
  runtime         = "python3.11"
  timeout         = 30
  memory_size     = 256
  
  environment {
    variables = {
      GENERATOR_FUNCTION_NAME = aws_lambda_function.generator.function_name
    }
  }
  
  tags = merge(var.tags, {
    Name = "${var.prefix}-operator-api"
  })
}

# Outputs
output "security_group_id" {
  value       = aws_security_group.lambda.id
  description = "Lambda security group ID"
}

output "generator_function_name" {
  value       = aws_lambda_function.generator.function_name
  description = "Event generator function name"
}

output "generator_function_arn" {
  value       = aws_lambda_function.generator.arn
  description = "Event generator function ARN"
}

output "operator_api_function_name" {
  value       = aws_lambda_function.operator_api.function_name
  description = "Operator API function name"
}

output "operator_api_function_arn" {
  value       = aws_lambda_function.operator_api.arn
  description = "Operator API function ARN"
}

output "operator_api_invoke_arn" {
  value       = aws_lambda_function.operator_api.invoke_arn
  description = "Operator API invoke ARN"
}
