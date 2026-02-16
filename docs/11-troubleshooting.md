# Troubleshooting Guide

## Common Issues and Solutions

### 1. Kafka Connection Failures

**Symptom**: Lambda or Spark can't connect to MSK

**Possible Causes**:
- Security group misconfiguration
- IAM authentication issues
- VPC networking problems

**Solutions**:

```bash
# Check security group rules
aws ec2 describe-security-groups --group-ids $MSK_SG_ID

# Verify Lambda is in correct VPC/subnets
aws lambda get-function-configuration --function-name $FUNCTION_NAME

# Test connectivity from Lambda
aws lambda invoke \
  --function-name $FUNCTION_NAME \
  --payload '{"test": "connectivity"}' \
  response.json
```

### 2. Spark Job Not Starting

**Symptom**: EMR Serverless job fails to start

**Possible Causes**:
- Insufficient IAM permissions
- Invalid checkpoint location
- Missing Spark packages

**Solutions**:

```bash
# Check job status
aws emr-serverless get-job-run \
  --application-id $APP_ID \
  --job-run-id $JOB_RUN_ID

# Check logs
aws logs tail /aws/emr-serverless/applications/$APP_ID --follow

# Verify IAM role permissions
aws iam get-role-policy --role-name $ROLE_NAME --policy-name $POLICY_NAME
```

**Fix**: Ensure role has:
- S3 read/write for checkpoints
- Kafka cluster permissions
- CloudWatch logs permissions

### 3. No Events in Topics

**Symptom**: Topics are empty or no new events

**Possible Causes**:
- Generator not running
- Kafka topic doesn't exist
- Producer errors

**Solutions**:

```bash
# List topics
kafka-topics --list --bootstrap-server $KAFKA_BROKER

# Check generator logs
aws logs tail /aws/lambda/$GENERATOR_FUNCTION --since 10m

# Manually produce test event
echo '{"test": "event"}' | kcat -b $KAFKA_BROKER -t cat.events.v1 -P
```

### 4. High Processing Lag

**Symptom**: Spark job falling behind

**Possible Causes**:
- Insufficient compute capacity
- Inefficient aggregations
- Large state size

**Solutions**:

```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group spark-lifecycle-job \
  --describe

# Increase EMR capacity
aws emr-serverless update-application \
  --application-id $APP_ID \
  --initial-capacity ...

# Tune Spark configuration
spark.sql.shuffle.partitions=12  # Increase parallelism
spark.sql.streaming.stateStore.providerClass=...  # Use RocksDB
```

### 5. Checkpoint Failures

**Symptom**: "Failed to read checkpoint" errors

**Possible Causes**:
- S3 permissions issues
- Corrupted checkpoint
- Checkpoint location changed

**Solutions**:

```bash
# Check S3 permissions
aws s3 ls s3://$BUCKET/checkpoints/

# Delete corrupted checkpoint (WARNING: loses state)
aws s3 rm s3://$BUCKET/checkpoints/lifecycle-job/ --recursive

# Restart job with new checkpoint location
```

### 6. Duplicate Events Not Deduplicated

**Symptom**: Seeing duplicate events in output

**Possible Causes**:
- Deduplication not working
- event_id not unique
- Watermark too short

**Solutions**:

```python
# Verify deduplication logic
events.dropDuplicates(["event_id"])

# Check event_id uniqueness
events.groupBy("event_id").count().filter("count > 1").show()

# Increase deduplication watermark
events.withWatermark("event_time", "2 hours")  # Longer window
```

### 7. Late Events Not Reconciled

**Symptom**: Late events ignored or not updating lifecycles

**Possible Causes**:
- Watermark too short
- State already dropped
- Reconciliation logic missing

**Solutions**:

```python
# Increase watermark delay
.withWatermark("event_time", "60 seconds")  # Longer delay

# Check if late events detected
events.filter(col("is_late")).count()

# Verify state retention
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "100")
```

### 8. Terraform Apply Failures

**Symptom**: Terraform fails to create resources

**Common Errors**:

**Error**: "VPC limit exceeded"
```bash
# Solution: Delete unused VPCs or request limit increase
aws ec2 describe-vpcs
aws ec2 delete-vpc --vpc-id $VPC_ID
```

**Error**: "Insufficient permissions"
```bash
# Solution: Verify IAM user/role has required permissions
aws sts get-caller-identity
aws iam get-user-policy --user-name $USER --policy-name $POLICY
```

**Error**: "Resource already exists"
```bash
# Solution: Import existing resource or destroy and recreate
terraform import aws_s3_bucket.cat_data $BUCKET_NAME
# or
terraform destroy -target=aws_s3_bucket.cat_data
```

### 9. High AWS Costs

**Symptom**: Unexpected high bills

**Possible Causes**:
- Resources left running
- High data transfer
- Inefficient queries

**Solutions**:

```bash
# Check running resources
aws emr-serverless list-applications --state STARTED
aws lambda list-functions
aws kafka list-clusters-v2

# Stop unnecessary resources
aws emr-serverless stop-application --application-id $APP_ID

# Enable low-cost mode
# In terraform.tfvars:
low_cost_mode = true
```

### 10. DynamoDB Throttling

**Symptom**: "ProvisionedThroughputExceededException"

**Solutions**:

```bash
# Switch to on-demand billing
aws dynamodb update-table \
  --table-name $TABLE_NAME \
  --billing-mode PAY_PER_REQUEST

# Or increase provisioned capacity
aws dynamodb update-table \
  --table-name $TABLE_NAME \
  --provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits=10
```

## Debugging Techniques

### Enable Debug Logging

**Spark**:
```python
spark.sparkContext.setLogLevel("DEBUG")
```

**Lambda**:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Inspect Kafka Messages

```bash
# Consume with full metadata
kcat -b $KAFKA_BROKER -t cat.events.v1 -C -f '\
Topic: %t\n\
Partition: %p\n\
Offset: %o\n\
Timestamp: %T\n\
Key: %k\n\
Value: %s\n\
---\n'
```

### Check Spark State

```python
# Query state store
state_df = spark.read \
    .format("state") \
    .load("s3://$BUCKET/checkpoints/lifecycle-job/state")

state_df.show()
```

### Trace Event Flow

```bash
# Follow a specific customer_order_id through pipeline
COID="COID-20260216-A123-0001"

# Raw events
kcat -b $KAFKA_BROKER -t cat.events.v1 -C -e \
  | jq "select(.customer_order_id == \"$COID\")"

# Linkages
kcat -b $KAFKA_BROKER -t cat.linkages.v1 -C -e \
  | jq "select(.customer_order_id == \"$COID\")"

# Lifecycle
kcat -b $KAFKA_BROKER -t cat.lifecycle.v1 -C -e \
  | jq "select(.customer_order_id == \"$COID\")"
```

## Performance Profiling

### Spark UI Analysis

1. Access Spark UI (port 4040 or EMR console)
2. Check "Streaming" tab for:
   - Input rate
   - Processing time
   - Scheduling delay
3. Check "Stages" tab for:
   - Task duration
   - Shuffle read/write
   - GC time

### Identify Bottlenecks

```python
# Add timing metrics
from pyspark.sql.functions import current_timestamp

events_with_timing = events \
    .withColumn("processing_start", current_timestamp())

# Later in pipeline
.withColumn("processing_end", current_timestamp()) \
.withColumn("processing_duration", 
           col("processing_end").cast("long") - col("processing_start").cast("long"))
```

## Getting Help

### Check Logs First

1. CloudWatch Logs for Lambda and EMR
2. Spark UI for job metrics
3. Kafka logs (if self-hosted)

### Gather Information

When asking for help, provide:
- Error messages (full stack trace)
- Relevant logs (last 100 lines)
- Configuration (Terraform variables, Spark config)
- Steps to reproduce
- Expected vs. actual behavior

### Resources

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [AWS MSK Documentation](https://docs.aws.amazon.com/msk/)
- [AWS EMR Serverless Documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)
- Project GitHub Issues
- Course discussion forum

## Prevention

### Best Practices

1. **Test locally first**: Use docker-compose before AWS
2. **Start small**: Low event rates, short durations
3. **Monitor continuously**: Set up CloudWatch alarms
4. **Version control**: Commit working configurations
5. **Document changes**: Note what worked and what didn't
6. **Clean up regularly**: Don't leave resources running

### Pre-Deployment Checklist

- [ ] Terraform plan reviewed
- [ ] IAM permissions verified
- [ ] VPC/networking configured
- [ ] Cost estimates calculated
- [ ] Billing alerts set
- [ ] Backup/rollback plan ready

### Post-Deployment Checklist

- [ ] All resources created successfully
- [ ] Kafka topics exist
- [ ] Spark job running
- [ ] Events flowing
- [ ] No errors in logs
- [ ] Metrics look healthy

## Emergency Procedures

### System Down

1. Check AWS Service Health Dashboard
2. Verify all resources running
3. Check CloudWatch alarms
4. Review recent changes
5. Rollback if necessary

### Data Loss

1. Check S3 versioning
2. Restore from checkpoint
3. Replay from Kafka (if within retention)
4. Document incident

### Cost Spike

1. Stop all non-essential resources
2. Check Cost Explorer for culprit
3. Review recent deployments
4. Enable low-cost mode

## Still Stuck?

Contact your instructor or post in the course forum with:
- Detailed problem description
- Error messages and logs
- What you've tried
- Your environment (local vs. AWS)

Good luck debugging!
