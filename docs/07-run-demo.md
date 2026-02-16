# 10-Minute Demo Script

This script walks through a live demonstration of the CAT lifecycle reconstruction system.

## Prerequisites

- System deployed to AWS (or running locally)
- Terminal access
- Browser for Kafka UI (optional)

## Demo Flow (10 minutes)

### Minute 0-1: Introduction

**Say**: "Today I'll show you how to reconstruct order lifecycles across distributed trading systems using event sourcing and streaming. This is inspired by the SEC's Consolidated Audit Trail."

**Show**: Architecture diagram from README

### Minute 1-2: Start Event Generation

```bash
# Get API URL
API_URL=$(cd terraform/envs/dev && terraform output -raw api_gateway_url)

# Start generator in normal mode
curl -X POST $API_URL/generator/start \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "normal",
    "duration": 300,
    "rate": 2.0
  }'
```

**Say**: "I'm generating synthetic order lifecycle events - NEW orders, ROUTE to venues, ACKs, and FILLs - at 2 events per second."

### Minute 2-3: Show Raw Events

```bash
# Consume raw events (show a few)
kcat -b $BOOTSTRAP_SERVERS \
  -t cat.events.v1 \
  -C \
  -c 5 \
  -f 'Key: %k\nValue: %s\n---\n'
```

**Say**: "Here are raw events flowing through Kafka. Notice each has an event_id for idempotency, timestamps, and linkage identifiers."

**Point out**:
- event_type (NEW, ROUTE, FILL)
- customer_order_id (stable across lifecycle)
- parent_firm_order_id (links child to parent)

### Minute 3-4: Show Linkage Graph

```bash
# Consume linkages
kcat -b $BOOTSTRAP_SERVERS \
  -t cat.linkages.v1 \
  -C \
  -c 10 \
  -f 'Value: %s\n'
```

**Say**: "The Spark job constructs a linkage graph - edges connecting parent orders to child routes to executions."

**Show**: Example edge JSON with source_id and target_id

### Minute 4-5: Show Lifecycle Snapshots

```bash
# Consume lifecycle snapshots
kcat -b $BOOTSTRAP_SERVERS \
  -t cat.lifecycle.v1 \
  -C \
  -c 3 \
  -f 'Value: %s\n' | jq .
```

**Say**: "These are materialized lifecycle snapshots - one per customer order. Notice the status, filled_qty, and executions array."

**Point out**:
- status (OPEN, PARTIALLY_FILLED, FILLED)
- filled_qty vs. total_qty
- avg_exec_price
- is_provisional flag

### Minute 5-6: Inject Late Events

```bash
# Switch to late mode
curl -X POST $API_URL/generator/start \
  -d '{
    "mode": "late",
    "duration": 120,
    "rate": 1.0
  }'
```

**Say**: "Now I'm injecting late-arriving events - ACKs that arrive 60 seconds after they should. This simulates network delays or system failures."

### Minute 6-7: Show Reconciliation

```bash
# Watch audit log
kcat -b $BOOTSTRAP_SERVERS \
  -t audit.v1 \
  -C \
  -o end \
  -f 'Late event correction: %s\n'
```

**Say**: "The system detects late events, retrieves the affected lifecycle, recomputes it, and logs the correction to the audit trail."

**Show**: Audit entry with action="LIFECYCLE_CORRECTION"

### Minute 7-8: Show Data Quality Exceptions

```bash
# Switch to chaos mode
curl -X POST $API_URL/generator/start \
  -d '{
    "mode": "chaos",
    "duration": 60,
    "rate": 2.0
  }'

# Watch exceptions
kcat -b $BOOTSTRAP_SERVERS \
  -t cat.exceptions.v1 \
  -C \
  -o end \
  -f 'Exception: %s\n' | jq .
```

**Say**: "In chaos mode, events arrive out of order and some are missing. The validation logic detects these issues."

**Point out**:
- exception_type (MISSING_PARENT, ORPHANED_EXECUTION, etc.)
- severity (ERROR, WARNING)
- description

### Minute 8-9: Show Spark Metrics

```bash
# Get Spark streaming metrics
aws logs tail /aws/emr-serverless/applications/$EMR_APP_ID \
  --since 5m \
  --filter-pattern "numInputRows" \
  | grep "processedRowsPerSecond"
```

**Say**: "The Spark job processes events in micro-batches. Here's the throughput - we're processing X events per second with Y milliseconds latency."

**Show**: Metrics like:
- numInputRows
- processedRowsPerSecond
- stateMemoryUsedBytes

### Minute 9-10: Wrap Up and Q&A

**Say**: "Let me summarize what we've seen:

1. **Event sourcing**: All state changes stored as immutable events
2. **Linkages**: Identifiers connect distributed events into lifecycles
3. **Streaming**: Real-time processing with Spark Structured Streaming
4. **Late data**: Watermarks and reconciliation handle out-of-order events
5. **Data quality**: Validation rules detect anomalies
6. **Audit trail**: Complete history for regulatory compliance

This is a simplified version of what the real CAT system does for U.S. securities markets - processing billions of events daily to enable market oversight."

**Open for questions**

## Alternative: Local Demo

If running locally:

```bash
# Start local environment
cd local
docker-compose up -d

# Create topics
../tools/create_topics.sh

# Start Spark job (in separate terminal)
docker exec -it cat-spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/lifecycle_streaming.py

# Generate events (in separate terminal)
cd services/event_generator
python generator.py --mode normal --duration 300 --rate 2.0

# Follow the same demo flow above
```

## Tips for Effective Demo

1. **Prepare**: Test the full flow before presenting
2. **Have backups**: Pre-record terminal output if live demo fails
3. **Explain as you go**: Don't just show commands, explain what they do
4. **Use jq**: Format JSON output for readability
5. **Highlight key concepts**: Event time, watermarks, idempotency
6. **Connect to real world**: Mention Flash Crash, market manipulation cases
7. **Engage audience**: Ask questions, invite participation

## Common Issues

**Events not flowing**: Check generator logs, verify Kafka connectivity

**Spark job not processing**: Check EMR logs, verify checkpoint location

**No late events**: Increase late event injection rate in generator

**Too much output**: Use `-c` flag with kcat to limit messages

## Extended Demo (20 minutes)

If you have more time:

- Show Kafka UI for visual topic browsing
- Query DynamoDB for lifecycle snapshots
- Demonstrate idempotency with duplicate events
- Show CloudWatch metrics and dashboards
- Walk through Spark code and explain watermarking logic
- Discuss cost optimization strategies

## Post-Demo Resources

Point audience to:
- Full documentation in `docs/`
- Student exercises in `docs/10-exercises.md`
- Blog post explaining concepts
- GitHub repository for code

Good luck with your demo!
