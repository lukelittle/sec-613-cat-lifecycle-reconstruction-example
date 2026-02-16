# Deploy to AWS

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.6
- AWS account with permissions for VPC, MSK, EMR, Lambda, API Gateway, S3, DynamoDB

## Step 1: Configure Terraform Variables

```bash
cd terraform/envs/dev
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
region          = "us-east-1"
prefix          = "cat-demo"
low_cost_mode   = true  # Set false for production-like setup
owner_email     = "your-email@uncc.edu"

# Watermark configuration
watermark_delay_seconds = 30
event_rate_per_second   = 2.0
```

## Step 2: Deploy Infrastructure

```bash
terraform init
terraform plan
terraform apply
```

This creates:
- VPC with private subnets
- MSK Serverless cluster
- EMR Serverless application
- Lambda functions (generator, operator API)
- API Gateway
- S3 bucket
- DynamoDB table
- IAM roles and policies

**Deployment time**: ~15-20 minutes

## Step 3: Create Kafka Topics

```bash
# Get MSK bootstrap servers from Terraform output
BOOTSTRAP_SERVERS=$(terraform output -raw msk_bootstrap_servers)

# Create topics (requires kafka-topics CLI or use provided script)
export KAFKA_BROKER=$BOOTSTRAP_SERVERS
../../../tools/create_topics.sh
```

## Step 4: Deploy Spark Job

```bash
# Package and upload Spark job
./deploy_spark_job.sh
```

This script:
1. Packages `spark/lifecycle_job/lifecycle_streaming.py`
2. Uploads to S3
3. Starts EMR Serverless job

## Step 5: Start Event Generator

```bash
# Get API Gateway URL
API_URL=$(terraform output -raw api_gateway_url)

# Start generator
curl -X POST $API_URL/generator/start \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "normal",
    "duration": 300,
    "rate": 2.0
  }'
```

## Step 6: Monitor

### CloudWatch Logs

```bash
# Spark job logs
aws logs tail /aws/emr-serverless/applications/$(terraform output -raw emr_application_id) --follow

# Generator logs
aws logs tail /aws/lambda/$(terraform output -raw generator_function_name) --follow
```

### Kafka Topics

```bash
# Install kcat (formerly kafkacat)
brew install kcat  # macOS
# or: apt-get install kafkacat  # Linux

# Consume lifecycle snapshots
kcat -b $BOOTSTRAP_SERVERS \
  -t cat.lifecycle.v1 \
  -C \
  -o end
```

### Query Lifecycle

```bash
# Query via API (if implemented)
curl $API_URL/lifecycle/COID-20260216-A123-0001
```

## Step 7: Run Demo Scenarios

### Normal Mode
```bash
curl -X POST $API_URL/generator/start \
  -d '{"mode": "normal", "duration": 60, "rate": 1.0}'
```

### Late Event Mode
```bash
curl -X POST $API_URL/generator/start \
  -d '{"mode": "late", "duration": 120, "rate": 1.0}'
```

Watch for late event reconciliations in Spark logs.

### Chaos Mode
```bash
curl -X POST $API_URL/generator/start \
  -d '{"mode": "chaos", "duration": 60, "rate": 2.0}'
```

Check exceptions topic for validation errors.

## Cost Management

### Estimated Costs (Low-Cost Mode)

- MSK Serverless: ~$30-50/month
- EMR Serverless: ~$10-20/month (with pre-initialized capacity)
- Lambda: ~$5/month
- S3: ~$5/month
- DynamoDB: ~$5/month
- Data transfer: ~$5/month

**Total**: ~$60-90/month for demo usage

### Cost Optimization

1. **Use low_cost_mode**: Single AZ, minimal capacity
2. **Stop when not in use**: Terminate EMR jobs
3. **Set retention**: 7 days for derived topics
4. **Use spot instances**: For EMR workers (if using provisioned)
5. **Monitor**: Set up billing alerts

## Cleanup

```bash
# Destroy all resources
terraform destroy

# Confirm deletion
# Type: yes
```

**Warning**: This deletes all data. Export important data first.

## Troubleshooting

### MSK Connection Issues

```bash
# Test connectivity from Lambda
aws lambda invoke \
  --function-name $(terraform output -raw generator_function_name) \
  --payload '{"test": true}' \
  response.json
```

### Spark Job Not Starting

```bash
# Check EMR application status
aws emr-serverless get-application \
  --application-id $(terraform output -raw emr_application_id)

# Check job run status
aws emr-serverless list-job-runs \
  --application-id $(terraform output -raw emr_application_id)
```

### No Events in Topics

```bash
# Check generator logs
aws logs tail /aws/lambda/$(terraform output -raw generator_function_name) --since 10m

# List topics
kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVERS
```

## Next Steps

Continue to [07-run-demo.md](07-run-demo.md) for a 10-minute demo script.
