# Observe and Query

## Observing Event Flow

### Using kcat (Recommended)

```bash
# Install kcat
brew install kcat  # macOS
# or: apt-get install kafkacat  # Linux

# Tail events topic
kcat -b $KAFKA_BROKER -t cat.events.v1 -C -o end

# Tail with formatting
kcat -b $KAFKA_BROKER -t cat.events.v1 -C -o end \
  -f 'Key: %k\nValue: %s\n---\n' | jq .

# Consume specific number of messages
kcat -b $KAFKA_BROKER -t cat.lifecycle.v1 -C -c 10
```

### Using Kafka Console Consumer

```bash
# Consume from beginning
kafka-console-consumer --bootstrap-server $KAFKA_BROKER \
  --topic cat.events.v1 \
  --from-beginning

# Consume from end
kafka-console-consumer --bootstrap-server $KAFKA_BROKER \
  --topic cat.lifecycle.v1 \
  --from-beginning \
  --max-messages 10
```

### Using Kafka UI (Local)

1. Open http://localhost:8080
2. Navigate to Topics
3. Select topic (e.g., cat.events.v1)
4. View messages in real-time

## Querying Lifecycles

### DynamoDB (Latest Snapshots)

```bash
# Query by customer_order_id
aws dynamodb get-item \
  --table-name cat-demo-dev-lifecycles \
  --key '{"customer_order_id": {"S": "COID-20260216-A123-0001"}, "snapshot_type": {"S": "LATEST"}}'

# Query by symbol (using GSI)
aws dynamodb query \
  --table-name cat-demo-dev-lifecycles \
  --index-name symbol-index \
  --key-condition-expression "symbol = :symbol" \
  --expression-attribute-values '{":symbol": {"S": "AAPL"}}'
```

### S3/Iceberg (Historical Data)

```bash
# Query with Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM cat_lifecycles WHERE symbol = 'AAPL' AND date = '2026-02-16'" \
  --result-configuration OutputLocation=s3://cat-demo-dev-data/athena-results/
```

### Spark SQL (Direct Query)

```python
# Read Iceberg table
lifecycles = spark.read \
    .format("iceberg") \
    .load("s3://cat-demo-dev-data/iceberg/lifecycles")

# Query
lifecycles.filter("symbol = 'AAPL'") \
    .filter("status = 'FILLED'") \
    .show()
```

## Monitoring Spark Job

### CloudWatch Logs

```bash
# Tail Spark logs
aws logs tail /aws/emr-serverless/applications/$EMR_APP_ID --follow

# Filter for errors
aws logs tail /aws/emr-serverless/applications/$EMR_APP_ID \
  --filter-pattern "ERROR" \
  --since 1h
```

### Streaming Query Metrics

```python
# In Spark job
query = lifecycles.writeStream.start()

# Get metrics
while query.isActive:
    progress = query.lastProgress
    if progress:
        print(f"Input rows: {progress['numInputRows']}")
        print(f"Processing rate: {progress['processedRowsPerSecond']}")
        print(f"Batch duration: {progress['batchDuration']}")
    time.sleep(10)
```

### Spark UI

Access Spark UI:
- Local: http://localhost:4040
- EMR Serverless: Via AWS Console → EMR → Applications → Job Runs → Spark UI

## CloudWatch Dashboards

Create custom dashboard:

```bash
aws cloudwatch put-dashboard \
  --dashboard-name cat-demo-metrics \
  --dashboard-body file://dashboard.json
```

dashboard.json:
```json
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["AWS/Kafka", "BytesInPerSec", {"stat": "Sum"}]
        ],
        "period": 300,
        "stat": "Sum",
        "region": "us-east-1",
        "title": "Kafka Ingress"
      }
    }
  ]
}
```

## Useful Queries

### Count Events by Type

```bash
kcat -b $KAFKA_BROKER -t cat.events.v1 -C -e \
  | jq -r '.event_type' \
  | sort | uniq -c
```

### Find Late Events

```bash
kcat -b $KAFKA_BROKER -t audit.v1 -C -o end \
  | jq 'select(.action == "LATE_EVENT_RECEIVED")'
```

### Exception Summary

```bash
kcat -b $KAFKA_BROKER -t cat.exceptions.v1 -C -e \
  | jq -r '.exception_type' \
  | sort | uniq -c
```

### Lifecycle Status Distribution

```bash
kcat -b $KAFKA_BROKER -t cat.lifecycle.v1 -C -c 100 \
  | jq -r '.status' \
  | sort | uniq -c
```

## Performance Metrics

### Kafka Lag

```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group spark-lifecycle-job \
  --describe
```

### Processing Latency

```bash
# Calculate end-to-end latency
kcat -b $KAFKA_BROKER -t cat.lifecycle.v1 -C -c 10 \
  | jq '.ts_snapshot - .ts_first_event' \
  | awk '{sum+=$1; count++} END {print "Avg latency:", sum/count, "ms"}'
```

## Troubleshooting Queries

### Find Missing Linkages

```sql
-- In Athena or Spark SQL
SELECT e.customer_order_id, e.firm_order_id
FROM cat_events e
WHERE e.event_type = 'ROUTE'
  AND e.parent_firm_order_id NOT IN (
    SELECT firm_order_id FROM cat_events WHERE event_type = 'NEW'
  )
```

### Find Overfilled Orders

```sql
SELECT customer_order_id, total_qty, filled_qty
FROM cat_lifecycles
WHERE filled_qty > total_qty
```

### Find Slow Lifecycles

```sql
SELECT customer_order_id, 
       (ts_last_event - ts_first_event) / 1000 as duration_seconds
FROM cat_lifecycles
WHERE (ts_last_event - ts_first_event) > 60000  -- > 60 seconds
ORDER BY duration_seconds DESC
```

## Next Steps

Continue to [09-data-quality-controls.md](09-data-quality-controls.md) for validation details.
