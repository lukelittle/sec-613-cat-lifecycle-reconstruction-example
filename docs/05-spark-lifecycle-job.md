# Spark Lifecycle Job: Implementation Deep Dive

## Job Overview

The Spark Structured Streaming job is the heart of the system. It:
1. Consumes raw events from Kafka
2. Enforces idempotency
3. Constructs linkage graphs
4. Materializes lifecycle snapshots
5. Validates data quality
6. Handles late-arriving events
7. Maintains audit trails

## Job Structure

```python
# lifecycle_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    spark = create_spark_session()
    
    # Read events
    events = read_events_stream(spark)
    
    # Deduplicate
    unique_events = deduplicate_events(events)
    
    # Apply watermark
    watermarked = apply_watermark(unique_events)
    
    # Build linkages
    linkages = build_linkages(watermarked)
    
    # Materialize lifecycles
    lifecycles = materialize_lifecycles(watermarked)
    
    # Validate
    exceptions = validate_lifecycles(lifecycles)
    
    # Write outputs
    write_linkages(linkages)
    write_lifecycles(lifecycles)
    write_exceptions(exceptions)
    write_to_iceberg(lifecycles)
    
    # Start streaming
    spark.streams.awaitAnyTermination()
```

## Step 1: Read Events from Kafka

```python
def read_events_stream(spark):
    """Read raw events from cat.events.v1 topic"""
    
    # Event schema
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("ts_event", LongType(), False),
        StructField("ts_ingest", LongType(), False),
        StructField("firm_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("customer_order_id", StringType(), False),
        StructField("firm_order_id", StringType(), False),
        StructField("parent_firm_order_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("venue", StringType(), True),
        StructField("exec_id", StringType(), True),
        StructField("symbol", StringType(), False),
        StructField("qty", IntegerType(), False),
        StructField("side", StringType(), False),
        StructField("order_type", StringType(), True),
        StructField("limit_price", DoubleType(), True),
        StructField("exec_price", DoubleType(), True),
        StructField("reason", StringType(), True),
        StructField("corr_id", StringType(), True)
    ])
    
    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", get_bootstrap_servers()) \
        .option("subscribe", "cat.events.v1") \
        .option("startingOffsets", "latest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
        .option("kafka.sasl.jaas.config", get_iam_config()) \
        .option("kafka.sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
        .load()
    
    # Parse JSON
    parsed = raw_stream \
        .select(
            from_json(col("value").cast("string"), event_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ) \
        .select("data.*", "kafka_timestamp", "partition", "offset")
    
    # Convert ts_event to timestamp
    with_timestamp = parsed \
        .withColumn("event_time", 
                   (col("ts_event") / 1000).cast("timestamp"))
    
    return with_timestamp
```

## Step 2: Deduplication (Idempotency)

```python
def deduplicate_events(events):
    """Remove duplicate events based on event_id"""
    
    # Spark's dropDuplicates with watermark maintains bounded state
    # Only keeps event_ids within watermark window
    deduplicated = events \
        .withWatermark("event_time", "1 hour") \
        .dropDuplicates(["event_id"])
    
    return deduplicated
```

**How it works**:
- Spark maintains a hash set of seen `event_id` values
- State is bounded by watermark (1 hour)
- Events older than watermark are not deduplicated (acceptable trade-off)
- Prevents memory overflow from unbounded state

## Step 3: Apply Watermark for Late Data

```python
def apply_watermark(events):
    """Apply watermark for late event handling"""
    
    watermark_delay = get_config("watermark_delay", "30 seconds")
    
    watermarked = events \
        .withWatermark("event_time", watermark_delay)
    
    # Tag late events
    watermarked = watermarked \
        .withColumn("is_late", 
                   col("event_time") < current_timestamp() - expr(f"INTERVAL {watermark_delay}"))
    
    return watermarked
```

**Watermark semantics**:
- Watermark = max(event_time) - delay
- Events with `event_time < watermark` are "late"
- Late events still processed but trigger reconciliation

## Step 4: Build Linkage Graph

```python
def build_linkages(events):
    """Construct parent-child edges from events"""
    
    @udf(ArrayType(StructType([
        StructField("edge_id", StringType()),
        StructField("edge_type", StringType()),
        StructField("source_id", StringType()),
        StructField("target_id", StringType()),
        StructField("customer_order_id", StringType()),
        StructField("ts_event", LongType())
    ])))
    def construct_edges(event_type, customer_order_id, firm_order_id, 
                       parent_firm_order_id, route_id, exec_id, ts_event):
        import uuid
        edges = []
        
        if event_type == "NEW":
            # Root edge: customer_order_id → firm_order_id
            edges.append({
                "edge_id": str(uuid.uuid4()),
                "edge_type": "ROOT",
                "source_id": customer_order_id,
                "target_id": firm_order_id,
                "customer_order_id": customer_order_id,
                "ts_event": ts_event
            })
        
        elif event_type == "ROUTE":
            # Parent → child edge
            if parent_firm_order_id:
                edges.append({
                    "edge_id": str(uuid.uuid4()),
                    "edge_type": "ROUTE",
                    "source_id": parent_firm_order_id,
                    "target_id": firm_order_id,
                    "customer_order_id": customer_order_id,
                    "ts_event": ts_event
                })
            # Child → route_id edge
            if route_id:
                edges.append({
                    "edge_id": str(uuid.uuid4()),
                    "edge_type": "ROUTE",
                    "source_id": firm_order_id,
                    "target_id": route_id,
                    "customer_order_id": customer_order_id,
                    "ts_event": ts_event
                })
        
        elif event_type == "FILL":
            # route_id → exec_id edge
            if route_id and exec_id:
                edges.append({
                    "edge_id": str(uuid.uuid4()),
                    "edge_type": "FILL",
                    "source_id": route_id,
                    "target_id": exec_id,
                    "customer_order_id": customer_order_id,
                    "ts_event": ts_event
                })
        
        elif event_type == "REPLACE":
            # parent → new version edge
            if parent_firm_order_id:
                edges.append({
                    "edge_id": str(uuid.uuid4()),
                    "edge_type": "REPLACE",
                    "source_id": parent_firm_order_id,
                    "target_id": firm_order_id,
                    "customer_order_id": customer_order_id,
                    "ts_event": ts_event
                })
        
        return edges
    
    # Explode edges
    linkages = events \
        .withColumn("edges", construct_edges(
            col("event_type"),
            col("customer_order_id"),
            col("firm_order_id"),
            col("parent_firm_order_id"),
            col("route_id"),
            col("exec_id"),
            col("ts_event")
        )) \
        .select(explode(col("edges")).alias("edge")) \
        .select("edge.*")
    
    return linkages
```

## Step 5: Materialize Lifecycle Snapshots

```python
def materialize_lifecycles(events):
    """Aggregate events into lifecycle snapshots"""
    
    # Define aggregation logic
    lifecycle_schema = StructType([
        StructField("customer_order_id", StringType()),
        StructField("root_firm_order_id", StringType()),
        StructField("account_id", StringType()),
        StructField("symbol", StringType()),
        StructField("side", StringType()),
        StructField("total_qty", IntegerType()),
        StructField("filled_qty", IntegerType()),
        StructField("avg_exec_price", DoubleType()),
        StructField("status", StringType()),
        StructField("active_routes", ArrayType(StringType())),
        StructField("executions", ArrayType(StructType([
            StructField("exec_id", StringType()),
            StructField("qty", IntegerType()),
            StructField("price", DoubleType()),
            StructField("ts_event", LongType())
        ]))),
        StructField("ts_first_event", LongType()),
        StructField("ts_last_event", LongType()),
        StructField("ts_snapshot", LongType()),
        StructField("is_provisional", BooleanType())
    ])
    
    @pandas_udf(lifecycle_schema, PandasUDFType.GROUPED_MAP)
    def aggregate_lifecycle(pdf):
        import pandas as pd
        import time
        
        # Sort by event time
        pdf = pdf.sort_values("ts_event")
        
        # Initialize lifecycle
        lifecycle = {
            "customer_order_id": pdf.iloc[0]["customer_order_id"],
            "root_firm_order_id": None,
            "account_id": pdf.iloc[0]["account_id"],
            "symbol": pdf.iloc[0]["symbol"],
            "side": pdf.iloc[0]["side"],
            "total_qty": 0,
            "filled_qty": 0,
            "avg_exec_price": 0.0,
            "status": "OPEN",
            "active_routes": [],
            "executions": [],
            "ts_first_event": pdf.iloc[0]["ts_event"],
            "ts_last_event": pdf.iloc[-1]["ts_event"],
            "ts_snapshot": int(time.time() * 1000),
            "is_provisional": False
        }
        
        # Process events
        for _, event in pdf.iterrows():
            if event["event_type"] == "NEW":
                lifecycle["root_firm_order_id"] = event["firm_order_id"]
                lifecycle["total_qty"] = event["qty"]
            
            elif event["event_type"] == "ROUTE":
                if event["route_id"]:
                    lifecycle["active_routes"].append(event["route_id"])
            
            elif event["event_type"] == "FILL":
                lifecycle["filled_qty"] += event["qty"]
                lifecycle["executions"].append({
                    "exec_id": event["exec_id"],
                    "qty": event["qty"],
                    "price": event["exec_price"],
                    "ts_event": event["ts_event"]
                })
                # Update avg price
                total_value = sum(e["qty"] * e["price"] for e in lifecycle["executions"])
                lifecycle["avg_exec_price"] = total_value / lifecycle["filled_qty"]
            
            elif event["event_type"] == "CANCEL":
                lifecycle["status"] = "CANCELED"
            
            elif event["event_type"] == "REPLACE":
                lifecycle["status"] = "REPLACED"
        
        # Determine final status
        if lifecycle["filled_qty"] == lifecycle["total_qty"]:
            lifecycle["status"] = "FILLED"
        elif lifecycle["filled_qty"] > 0:
            lifecycle["status"] = "PARTIALLY_FILLED"
        
        # Check if provisional (has late events)
        lifecycle["is_provisional"] = pdf["is_late"].any()
        
        return pd.DataFrame([lifecycle])
    
    # Group by customer_order_id and aggregate
    lifecycles = events \
        .groupBy("customer_order_id") \
        .apply(aggregate_lifecycle)
    
    return lifecycles
```

## Step 6: Validate Data Quality

```python
def validate_lifecycles(lifecycles):
    """Detect data quality exceptions"""
    
    exceptions = []
    
    # Exception 1: Overfill
    overfill = lifecycles \
        .filter(col("filled_qty") > col("total_qty")) \
        .select(
            lit(uuid.uuid4()).alias("exception_id"),
            lit("OVERFILL").alias("exception_type"),
            lit("ERROR").alias("severity"),
            col("customer_order_id"),
            col("root_firm_order_id").alias("firm_order_id"),
            lit("Filled quantity exceeds total quantity").alias("description"),
            current_timestamp().alias("ts_detected")
        )
    exceptions.append(overfill)
    
    # Exception 2: Negative remaining quantity
    negative_remaining = lifecycles \
        .filter(col("filled_qty") < 0) \
        .select(
            lit(uuid.uuid4()).alias("exception_id"),
            lit("NEGATIVE_FILL").alias("exception_type"),
            lit("ERROR").alias("severity"),
            col("customer_order_id"),
            col("root_firm_order_id").alias("firm_order_id"),
            lit("Negative filled quantity").alias("description"),
            current_timestamp().alias("ts_detected")
        )
    exceptions.append(negative_remaining)
    
    # Union all exceptions
    all_exceptions = exceptions[0]
    for exc in exceptions[1:]:
        all_exceptions = all_exceptions.union(exc)
    
    return all_exceptions
```

## Step 7: Write Outputs

```python
def write_linkages(linkages):
    """Write linkages to Kafka topic"""
    
    query = linkages \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", get_bootstrap_servers()) \
        .option("topic", "cat.linkages.v1") \
        .option("checkpointLocation", "s3://bucket/checkpoints/linkages") \
        .outputMode("append") \
        .start()
    
    return query

def write_lifecycles(lifecycles):
    """Write lifecycle snapshots to Kafka and DynamoDB"""
    
    # To Kafka
    kafka_query = lifecycles \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", get_bootstrap_servers()) \
        .option("topic", "cat.lifecycle.v1") \
        .option("checkpointLocation", "s3://bucket/checkpoints/lifecycle") \
        .outputMode("update") \
        .start()
    
    return kafka_query

def write_to_iceberg(lifecycles):
    """Write lifecycle snapshots to S3 Iceberg table"""
    
    query = lifecycles \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("path", "s3://bucket/iceberg/lifecycles") \
        .option("checkpointLocation", "s3://bucket/checkpoints/iceberg") \
        .partitionBy("symbol", "date") \
        .start()
    
    return query
```

## Handling Late Events

### Detection

```python
# Tag late events
events = events \
    .withColumn("is_late", 
               col("event_time") < 
               (current_timestamp() - expr("INTERVAL 30 SECONDS")))
```

### Reconciliation

When a late event arrives:
1. Spark's stateful aggregation automatically retrieves the existing lifecycle state
2. The late event is included in the aggregation
3. A new snapshot is emitted (with `is_provisional=True` if still within watermark)
4. An audit entry is written

```python
# Audit late events
late_audit = events \
    .filter(col("is_late")) \
    .select(
        lit(uuid.uuid4()).alias("audit_id"),
        lit("LATE_EVENT_RECEIVED").alias("action"),
        col("customer_order_id"),
        lit("Event arrived after watermark").alias("reason"),
        col("event_id").alias("triggering_event_id"),
        current_timestamp().alias("ts_audit")
    )

late_audit.writeStream \
    .format("kafka") \
    .option("topic", "audit.v1") \
    .start()
```

## State Management

### Checkpoint Location

```python
.option("checkpointLocation", "s3://bucket/checkpoints/lifecycle-job")
```

Checkpoints store:
- Kafka offsets (for exactly-once semantics)
- Stateful operator state (deduplication sets, aggregations)
- Watermark state

### State TTL

```python
# State is automatically cleaned up after watermark passes
.withWatermark("event_time", "30 seconds")
```

State older than watermark is dropped, preventing unbounded growth.

## Performance Tuning

### Parallelism

```python
spark.conf.set("spark.sql.shuffle.partitions", "6")  # Match Kafka partitions
```

### Batch Interval

```python
.trigger(processingTime="10 seconds")  # Micro-batch every 10s
```

### Memory

```python
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.driver.memory", "2g")
```

## Monitoring

### Streaming Query Metrics

```python
query = lifecycles.writeStream.start()

# Get metrics
metrics = query.lastProgress
print(f"Input rows: {metrics['numInputRows']}")
print(f"Processing rate: {metrics['processedRowsPerSecond']}")
print(f"State memory: {metrics['stateOperators'][0]['memoryUsedBytes']}")
```

### Custom Metrics

```python
from pyspark.sql.functions import count

# Count events by type
event_counts = events \
    .groupBy("event_type") \
    .agg(count("*").alias("count"))

event_counts.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()
```

## Testing

### Unit Tests

```python
def test_construct_edges():
    event = {
        "event_type": "NEW",
        "customer_order_id": "COID-123",
        "firm_order_id": "FOID-456",
        "ts_event": 1710000000000
    }
    
    edges = construct_edges(**event)
    
    assert len(edges) == 1
    assert edges[0]["edge_type"] == "ROOT"
    assert edges[0]["source_id"] == "COID-123"
    assert edges[0]["target_id"] == "FOID-456"
```

### Integration Tests

```python
def test_end_to_end():
    # Create test events
    events = [
        {"event_type": "NEW", "customer_order_id": "COID-123", ...},
        {"event_type": "ROUTE", "customer_order_id": "COID-123", ...},
        {"event_type": "FILL", "customer_order_id": "COID-123", ...}
    ]
    
    # Write to Kafka
    produce_events(events)
    
    # Wait for processing
    time.sleep(30)
    
    # Query lifecycle
    lifecycle = query_lifecycle("COID-123")
    
    assert lifecycle["status"] == "FILLED"
    assert lifecycle["filled_qty"] == 100
```

## What's Next?

Continue to [06-deploy-aws.md](06-deploy-aws.md) to deploy this job to AWS.

## Key Takeaways

- Spark Structured Streaming provides exactly-once semantics
- Watermarks enable bounded state and late data handling
- Stateful aggregations maintain lifecycle state
- Idempotency prevents duplicate event corruption
- Checkpointing ensures fault tolerance
- Performance tuning is critical for production workloads
- Monitoring provides visibility into streaming health
