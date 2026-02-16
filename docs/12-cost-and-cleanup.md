# Cost Management and Cleanup

## Cost Breakdown

### Low-Cost Mode (Recommended for Students)

**Monthly estimates** (assuming 8 hours/day, 20 days/month):

| Service | Configuration | Monthly Cost |
|---------|--------------|--------------|
| MSK Serverless | 1 MCU, ~10 GB/day | $30-40 |
| EMR Serverless | 1 driver + 1 executor, 8h/day | $15-25 |
| Lambda | 100K invocations, 512 MB | $5-10 |
| S3 | 50 GB storage, 100 GB transfer | $5-10 |
| DynamoDB | On-demand, 1M reads/writes | $5-10 |
| Data Transfer | VPC endpoints, minimal NAT | $5-10 |
| CloudWatch | Logs and metrics | $5-10 |

**Total**: $70-115/month

### Production Mode

**Monthly estimates** (24/7 operation):

| Service | Configuration | Monthly Cost |
|---------|--------------|--------------|
| MSK Serverless | 4 MCUs, ~100 GB/day | $200-300 |
| EMR Serverless | 2 drivers + 4 executors, 24/7 | $300-500 |
| Lambda | 1M invocations | $20-30 |
| S3 | 500 GB storage, 1 TB transfer | $30-50 |
| DynamoDB | Provisioned capacity | $50-100 |
| Data Transfer | Multi-AZ, NAT gateways | $50-100 |
| CloudWatch | Extensive logging | $20-40 |

**Total**: $670-1,120/month

## Cost Optimization Strategies

### 1. Use Low-Cost Mode

```hcl
# terraform.tfvars
low_cost_mode = true
```

This enables:
- Single AZ deployment (no cross-AZ data transfer)
- Minimal MSK capacity (1 MCU)
- Minimal EMR capacity (1 executor)
- VPC endpoints instead of NAT gateway

**Savings**: ~40-50% vs. production mode

### 2. Stop When Not in Use

```bash
# Stop EMR application
aws emr-serverless stop-application \
  --application-id $EMR_APP_ID

# Delete Lambda functions (if not needed)
aws lambda delete-function --function-name cat-demo-dev-event-generator
```

**Savings**: ~$20-30/day when stopped

### 3. Reduce Retention

```hcl
# Shorter retention for derived topics
data_retention_days = 7  # Instead of 30
```

**Savings**: ~$5-10/month on storage

### 4. Use Spot Instances (EMR Provisioned)

If using EMR on EC2 instead of serverless:

```hcl
instance_fleet {
  instance_type_configs {
    instance_type     = "m5.xlarge"
    bid_price_as_percentage_of_on_demand_price = 50
  }
}
```

**Savings**: ~50-70% on compute

### 5. Optimize Spark Job

- Reduce checkpoint frequency
- Tune batch interval (longer = fewer micro-batches)
- Reduce shuffle partitions
- Use broadcast joins where possible

**Savings**: ~20-30% on EMR costs

### 6. Set Up Billing Alerts

```bash
# Create billing alarm
aws cloudwatch put-metric-alarm \
  --alarm-name cat-demo-billing-alert \
  --alarm-description "Alert when monthly costs exceed $100" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --evaluation-periods 1 \
  --threshold 100 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=Currency,Value=USD
```

### 7. Use AWS Free Tier

Some services have free tiers:
- Lambda: 1M requests/month free
- DynamoDB: 25 GB storage free
- CloudWatch: 10 custom metrics free
- S3: 5 GB storage free (first 12 months)

### 8. Tag Everything for Cost Tracking

```hcl
tags = {
  Project     = "cat-demo"
  Environment = "dev"
  Owner       = "student@uncc.edu"
  CostCenter  = "education"
}
```

Use AWS Cost Explorer to filter by tags.

## Monitoring Costs

### AWS Cost Explorer

1. Go to AWS Cost Explorer
2. Filter by tag: `Project = cat-demo`
3. Group by: Service
4. View daily costs

### CLI

```bash
# Get current month costs
aws ce get-cost-and-usage \
  --time-period Start=2026-02-01,End=2026-02-28 \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --filter file://cost-filter.json

# cost-filter.json
{
  "Tags": {
    "Key": "Project",
    "Values": ["cat-demo"]
  }
}
```

### CloudWatch Dashboard

Create dashboard showing:
- MSK data ingress/egress
- EMR job duration
- Lambda invocations
- S3 storage size
- DynamoDB read/write units

## Cleanup

### Option 1: Terraform Destroy (Recommended)

```bash
cd terraform/envs/dev

# Preview what will be deleted
terraform plan -destroy

# Destroy all resources
terraform destroy

# Confirm by typing: yes
```

**Time**: ~10-15 minutes

**What's deleted**:
- VPC and subnets
- MSK cluster
- EMR application
- Lambda functions
- API Gateway
- S3 bucket (if empty)
- DynamoDB table
- IAM roles and policies
- CloudWatch log groups

### Option 2: Manual Cleanup

If Terraform state is lost:

```bash
# Delete MSK cluster
aws kafka delete-cluster --cluster-arn $CLUSTER_ARN

# Delete EMR application
aws emr-serverless delete-application --application-id $APP_ID

# Delete Lambda functions
aws lambda delete-function --function-name cat-demo-dev-event-generator
aws lambda delete-function --function-name cat-demo-dev-operator-api

# Delete API Gateway
aws apigatewayv2 delete-api --api-id $API_ID

# Empty and delete S3 bucket
aws s3 rm s3://cat-demo-dev-data --recursive
aws s3 rb s3://cat-demo-dev-data

# Delete DynamoDB table
aws dynamodb delete-table --table-name cat-demo-dev-lifecycles

# Delete VPC (after deleting dependencies)
aws ec2 delete-vpc --vpc-id $VPC_ID
```

### Option 3: Partial Cleanup (Keep Infrastructure)

To save costs while keeping infrastructure:

```bash
# Stop EMR application
aws emr-serverless stop-application --application-id $APP_ID

# Delete S3 data (keep bucket)
aws s3 rm s3://cat-demo-dev-data/iceberg/ --recursive
aws s3 rm s3://cat-demo-dev-data/checkpoints/ --recursive

# Truncate DynamoDB table (keep table)
# (No direct truncate - must scan and delete items)
```

## Verifying Cleanup

### Check for Remaining Resources

```bash
# List MSK clusters
aws kafka list-clusters-v2 | grep cat-demo

# List EMR applications
aws emr-serverless list-applications | grep cat-demo

# List Lambda functions
aws lambda list-functions | grep cat-demo

# List S3 buckets
aws s3 ls | grep cat-demo

# List DynamoDB tables
aws dynamodb list-tables | grep cat-demo
```

### Check Final Bill

Wait 24-48 hours after cleanup, then:

1. Go to AWS Billing Dashboard
2. Check "Bills" for current month
3. Verify charges stopped after cleanup date
4. Check for any lingering charges (data transfer, storage)

## Common Cleanup Issues

### S3 Bucket Not Empty

```bash
# Error: BucketNotEmpty
# Solution: Empty bucket first
aws s3 rm s3://cat-demo-dev-data --recursive
aws s3 rb s3://cat-demo-dev-data
```

### VPC Has Dependencies

```bash
# Error: DependencyViolation
# Solution: Delete in order
# 1. MSK cluster
# 2. EMR application
# 3. Lambda functions (ENIs)
# 4. Security groups
# 5. Subnets
# 6. VPC
```

### IAM Role In Use

```bash
# Error: DeleteConflict
# Solution: Wait for Lambda/EMR to fully terminate
# Then delete role
```

### Terraform State Drift

```bash
# If resources deleted manually
terraform refresh

# If state is corrupted
terraform state rm <resource>
```

## Cost Estimation Before Deployment

Use AWS Pricing Calculator:
1. Go to https://calculator.aws/
2. Add services: MSK Serverless, EMR Serverless, Lambda, S3, DynamoDB
3. Configure based on your usage
4. Get monthly estimate

## Student Budget Recommendations

For a semester-long project:

- **Budget**: $300-400 total ($75-100/month)
- **Strategy**: Use low-cost mode, stop when not actively developing
- **Monitoring**: Set billing alert at $100/month
- **Cleanup**: Destroy resources during breaks

## Questions?

Contact your instructor or AWS support for cost-related questions.

## Summary

- **Low-cost mode**: ~$70-115/month
- **Stop when not in use**: Save ~$20-30/day
- **Always clean up**: Use `terraform destroy`
- **Monitor costs**: Set billing alerts
- **Tag resources**: Track spending by project

**Remember**: AWS charges for running resources. Always clean up when done!
