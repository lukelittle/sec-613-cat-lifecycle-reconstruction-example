# MSK Serverless Cluster for CAT system

resource "aws_msk_serverless_cluster" "cat" {
  cluster_name = "${var.prefix}-cat-cluster"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = merge(var.tags, {
    Name = "${var.prefix}-cat-msk-cluster"
  })
}

resource "aws_security_group" "msk" {
  name        = "${var.prefix}-msk-sg"
  description = "Security group for MSK cluster"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka IAM auth"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = var.client_security_group_ids
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.prefix}-msk-sg"
  })
}

output "bootstrap_brokers" {
  value       = aws_msk_serverless_cluster.cat.bootstrap_brokers_sasl_iam
  description = "MSK bootstrap brokers with IAM auth"
}

output "cluster_arn" {
  value       = aws_msk_serverless_cluster.cat.arn
  description = "MSK cluster ARN"
}

output "security_group_id" {
  value       = aws_security_group.msk.id
  description = "MSK security group ID"
}
