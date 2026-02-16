# Quick Reference Guide

## Common Commands

### Local Development

```bash
# Start environment
make local-up

# Create topics
make local-topics

# Run Spark job
make local-spark

# Generate events
make local-generate

# Tail events
./tools/tail_topics.sh cat.events.v1

# Stop environment
make local-down
```

### AWS Deployment

```bash
# Deploy infrastructure
cd terraform/envs/dev
terraform init
terraform apply

# Create topics
make aws-topics

# Deploy Spark job
make aws-spark

# Start generator
make aws-generate

# Destroy everything
make aws-destroy
```

## Kafka Topics

| Topic | Purpose | Retention |
|-------|---------|-----------|
| cat.events.v1 | Raw lifecycle events | 30 days |
| cat.linkages.v1 | Parent-child edges | 7 days |
| cat.lifecycle.v1 | Lifecycle snapshots | 7 days |
| cat.exceptions.v1 | Validation errors | 7 days |
| audit.v1 | Correction audit trail | 30 days |

## Event Types

| Type | Description | Key Fields |
|------|-------------|------------|
| NEW | Order creation | customer_order_id, firm_order_id |
| ROUTE | Route to venue | parent_firm_order_id, route_id |
| ACK | Venue acknowledgment | route_id |
| FILL | Execution | exec_id, exec_price |
| REPLACE | Order modification | parent_firm_order_id (old) |
| CANCEL | Order cancellation | firm_order_id |
| BUST | Execution reversal | exec_id |

## Lifecycle Statuses

- OPEN - Order created, not yet routed
- ROUTED - Sent to venue(s)
- PARTIALLY_FILLED - Some quantity filled
- FILLED - Completely filled
- CANCELED - Canceled by user
- REPLACED - Modified (new version exists)

## Generator Modes

```bash
# Normal: In-order events
python generator.py --mode normal

# Late: Delayed events (60s late)
python generator.py --mode late

# Duplicate: Resend events with same event_id
python generator.py --mode duplicate

# Chaos: Out-of-order, missing events
python generator.py --mode chaos
```

## Useful Queries

### Count Events by Type
```bash
kcat -b $KAFKA_BROKER -t cat.events.v1 -C -e \
  | jq -r '.event_type' | sort | uniq -c
```

### Find Late Events
```bash
kcat -b $KAFKA_BROKER -t audit.v1 -C -o end \
  | jq 'select(.action == "LATE_EVENT_RECEIVED")'
```

### Exception Summary
```bash
kcat -b $KAFKA_BROKER -t cat.exceptions.v1 -C -e \
  | jq -r '.exception_type' | sort | uniq -c
```

### Lifecycle Status Distribution
```bash
kcat -b $KAFKA_BROKER -t cat.lifecycle.v1 -C -c 100 \
  | jq -r '.status' | sort | uniq -c
```

## AWS CLI Commands

### Check Spark Job Status
```bash
aws emr-serverless get-job-run \
  --application-id $EMR_APP_ID \
  --job-run-id $JOB_RUN_ID
```

### Tail Spark Logs
```bash
aws logs tail /aws/emr-serverless/applications/$EMR_APP_ID --follow
```

### Query DynamoDB
```bash
aws dynamodb get-item \
  --table-name cat-demo-dev-lifecycles \
  --key '{"customer_order_id": {"S": "COID-123"}, "snapshot_type": {"S": "LATEST"}}'
```

### Check MSK Cluster
```bash
aws kafka describe-cluster-v2 --cluster-arn $CLUSTER_ARN
```

## Configuration

### Terraform Variables
```hcl
region          = "us-east-1"
prefix          = "cat-demo"
low_cost_mode   = true
owner_email     = "student@uncc.edu"
watermark_delay_seconds = 30
event_rate_per_second   = 2.0
```

### Spark Configuration
```python
spark.sql.shuffle.partitions = 6
spark.sql.streaming.checkpointLocation = "s3://bucket/checkpoints/"
spark.executor.memory = "4g"
spark.driver.memory = "2g"
```

### Environment Variables
```bash
# Lambda
KAFKA_BOOTSTRAP_SERVERS=...
EVENT_RATE_PER_SECOND=2.0
MODE=normal

# Spark
CHECKPOINT_LOCATION=/tmp/checkpoints
WATERMARK_DELAY=30 seconds
DEBUG_MODE=false
```

## Monitoring

### CloudWatch Log Groups
- `/aws/lambda/cat-demo-dev-event-generator`
- `/aws/lambda/cat-demo-dev-operator-api`
- `/aws/emr-serverless/applications/{app-id}`

### Key Metrics
- Kafka: BytesInPerSec, BytesOutPerSec
- Spark: numInputRows, processedRowsPerSecond
- Lambda: Invocations, Duration, Errors

### Dashboards
- Kafka UI: http://localhost:8080 (local)
- Spark UI: http://localhost:4040 (local)
- CloudWatch: AWS Console → CloudWatch → Dashboards

## Troubleshooting

### No Events Flowing
1. Check generator logs
2. Verify Kafka connectivity
3. List topics: `kafka-topics --list`
4. Test produce: `echo '{"test":"event"}' | kcat -b $BROKER -t cat.events.v1 -P`

### Spark Job Not Processing
1. Check EMR application status
2. Review CloudWatch logs
3. Verify checkpoint location accessible
4. Check IAM permissions

### High Costs
1. Stop EMR application
2. Delete unnecessary S3 data
3. Enable low_cost_mode
4. Set billing alerts

## File Locations

### Code
- Spark job: `spark/lifecycle_job/lifecycle_streaming.py`
- Generator: `services/event_generator/generator.py`
- Operator API: `services/operator_api/api.py`

### Infrastructure
- Main Terraform: `terraform/envs/dev/main.tf`
- VPC module: `terraform/modules/vpc/`
- MSK module: `terraform/modules/msk/`
- Lambda module: `terraform/modules/lambda/`

### Documentation
- Overview: `docs/00-overview.md`
- Deployment: `docs/06-deploy-aws.md`
- Demo script: `docs/07-run-demo.md`
- Exercises: `docs/10-exercises.md`
- Troubleshooting: `docs/11-troubleshooting.md`

### Tools
- Create topics: `tools/create_topics.sh`
- Tail topics: `tools/tail_topics.sh`
- Deploy Spark: `terraform/envs/dev/deploy_spark_job.sh`

## URLs and Endpoints

### Local
- Kafka: localhost:9092
- Kafka UI: http://localhost:8080
- Spark UI: http://localhost:4040

### AWS
- API Gateway: `terraform output api_gateway_url`
- MSK Bootstrap: `terraform output msk_bootstrap_servers`
- S3 Bucket: `terraform output s3_bucket_name`

## Cost Estimates

### Low-Cost Mode (8h/day)
- MSK Serverless: $30-40/month
- EMR Serverless: $15-25/month
- Lambda: $5-10/month
- S3: $5-10/month
- DynamoDB: $5-10/month
- **Total: $70-115/month**

### Production Mode (24/7)
- MSK Serverless: $200-300/month
- EMR Serverless: $300-500/month
- Lambda: $20-30/month
- S3: $30-50/month
- DynamoDB: $50-100/month
- **Total: $670-1,120/month**

## Support

- Documentation: `docs/`
- Troubleshooting: `docs/11-troubleshooting.md`
- Exercises: `docs/10-exercises.md`
- Issues: GitHub Issues
- Course: Discussion forum

## Quick Links

- [README](README.md)
- [Project Summary](PROJECT_SUMMARY.md)
- [Contributing](CONTRIBUTING.md)
- [License](LICENSE)
- [Blog Post](blog/posts/simplified-cat-order-lifecycle-streaming.md)

---

**Need more details?** See the full documentation in `docs/` directory.
