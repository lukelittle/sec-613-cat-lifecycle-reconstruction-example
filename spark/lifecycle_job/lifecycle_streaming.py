#!/usr/bin/env python3
"""
Spark Structured Streaming job for CAT lifecycle reconstruction.
Consumes events from Kafka, builds linkages, materializes lifecycles,
validates data quality, and handles late-arriving events.
"""

import os
import sys
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, explode, lit, current_timestamp,
    window, count, sum as _sum, avg, max as _max, min as _min,
    udf, pandas_udf, PandasUDFType, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, ArrayType, TimestampType
)


# Event schema
EVENT_SCHEMA = StructType([
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


def get_config(key: str, default: Any = None) -> Any:
    """Get configuration from environment or use default"""
    return os.environ.get(key, default)


def create_spark_session() -> SparkSession:
    """Create and configure Spark session"""
    builder = SparkSession.builder \
        .appName("CAT-Lifecycle-Streaming") \
        .config("spark.sql.streaming.checkpointLocation", 
                get_config("CHECKPOINT_LOCATION", "/tmp/checkpoints")) \
        .config("spark.sql.shuffle.partitions", "6") \
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
    
    # Add Kafka and Iceberg packages if needed
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
        "software.amazon.msk:aws-msk-iam-auth:2.0.3"
    ]
    builder = builder.config("spark.jars.packages", ",".join(packages))
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def read_events_stream(spark: SparkSession) -> DataFrame:
    """Read raw events from cat.events.v1 Kafka topic"""
    
    bootstrap_servers = get_config("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    use_iam_auth = get_config("USE_IAM_AUTH", "false").lower() == "true"
    
    reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "cat.events.v1") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false")
    
    if use_iam_auth:
        reader = reader \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
            .option("kafka.sasl.jaas.config",
                   "software.amazon.msk.auth.iam.IAMLoginModule required;") \
            .option("kafka.sasl.client.callback.handler.class",
                   "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
    
    raw_stream = reader.load()
    
    # Parse JSON and extract fields
    parsed = raw_stream \
        .select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ) \
        .select("data.*", "kafka_timestamp", "partition", "offset")
    
    # Convert ts_event to timestamp for watermarking
    with_timestamp = parsed \
        .withColumn("event_time", 
                   (col("ts_event") / 1000).cast("timestamp"))
    
    return with_timestamp


def deduplicate_events(events: DataFrame) -> DataFrame:
    """Remove duplicate events based on event_id (idempotency)"""
    
    # Apply watermark for bounded deduplication state
    deduplicated = events \
        .withWatermark("event_time", "1 hour") \
        .dropDuplicates(["event_id"])
    
    return deduplicated


def apply_watermark(events: DataFrame) -> DataFrame:
    """Apply watermark for late event handling"""
    
    watermark_delay = get_config("WATERMARK_DELAY", "30 seconds")
    
    watermarked = events \
        .withWatermark("event_time", watermark_delay)
    
    # Tag late events (for audit purposes)
    watermarked = watermarked \
        .withColumn("is_late", 
                   col("kafka_timestamp") > 
                   col("event_time") + expr(f"INTERVAL {watermark_delay}"))
    
    return watermarked


def build_linkages(events: DataFrame) -> DataFrame:
    """Construct parent-child edges from events"""
    
    edge_schema = ArrayType(StructType([
        StructField("edge_id", StringType()),
        StructField("edge_type", StringType()),
        StructField("source_id", StringType()),
        StructField("target_id", StringType()),
        StructField("customer_order_id", StringType()),
        StructField("ts_event", LongType())
    ]))
    
    @udf(edge_schema)
    def construct_edges(event_type, customer_order_id, firm_order_id, 
                       parent_firm_order_id, route_id, exec_id, ts_event):
        edges = []
        
        if event_type == "NEW":
            edges.append({
                "edge_id": str(uuid.uuid4()),
                "edge_type": "ROOT",
                "source_id": customer_order_id,
                "target_id": firm_order_id,
                "customer_order_id": customer_order_id,
                "ts_event": ts_event
            })
        
        elif event_type == "ROUTE":
            if parent_firm_order_id:
                edges.append({
                    "edge_id": str(uuid.uuid4()),
                    "edge_type": "ROUTE",
                    "source_id": parent_firm_order_id,
                    "target_id": firm_order_id,
                    "customer_order_id": customer_order_id,
                    "ts_event": ts_event
                })
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
            if parent_firm_order_id:
                edges.append({
                    "edge_id": str(uuid.uuid4()),
                    "edge_type": "REPLACE",
                    "source_id": parent_firm_order_id,
                    "target_id": firm_order_id,
                    "customer_order_id": customer_order_id,
                    "ts_event": ts_event
                })
        
        elif event_type == "CANCEL":
            edges.append({
                "edge_id": str(uuid.uuid4()),
                "edge_type": "CANCEL",
                "source_id": firm_order_id,
                "target_id": f"CANCEL-{firm_order_id}",
                "customer_order_id": customer_order_id,
                "ts_event": ts_event
            })
        
        elif event_type == "BUST":
            if exec_id:
                edges.append({
                    "edge_id": str(uuid.uuid4()),
                    "edge_type": "BUST",
                    "source_id": exec_id,
                    "target_id": f"BUST-{exec_id}",
                    "customer_order_id": customer_order_id,
                    "ts_event": ts_event
                })
        
        return edges
    
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


def materialize_lifecycles(events: DataFrame) -> DataFrame:
    """Aggregate events into lifecycle snapshots"""
    
    # Use stateful aggregation with custom logic
    lifecycle_agg = events \
        .groupBy("customer_order_id") \
        .agg(
            _max(struct(col("event_type"), col("ts_event"))).alias("last_event"),
            _min("ts_event").alias("ts_first_event"),
            _max("ts_event").alias("ts_last_event"),
            _max(col("firm_order_id")).alias("root_firm_order_id"),
            _max(col("account_id")).alias("account_id"),
            _max(col("symbol")).alias("symbol"),
            _max(col("side")).alias("side"),
            _max(col("qty")).alias("total_qty"),
            _sum(expr("CASE WHEN event_type = 'FILL' THEN qty ELSE 0 END")).alias("filled_qty"),
            avg(expr("CASE WHEN event_type = 'FILL' THEN exec_price ELSE NULL END")).alias("avg_exec_price"),
            count(expr("CASE WHEN event_type = 'ROUTE' THEN 1 END")).alias("route_count"),
            count(expr("CASE WHEN event_type = 'FILL' THEN 1 END")).alias("fill_count"),
            _max(expr("CASE WHEN event_type = 'CANCEL' THEN 1 ELSE 0 END")).alias("is_canceled"),
            _max(col("is_late")).alias("has_late_events")
        )
    
    # Determine status
    lifecycles = lifecycle_agg \
        .withColumn("status", 
                   expr("""
                       CASE 
                           WHEN is_canceled = 1 THEN 'CANCELED'
                           WHEN filled_qty >= total_qty THEN 'FILLED'
                           WHEN filled_qty > 0 THEN 'PARTIALLY_FILLED'
                           WHEN route_count > 0 THEN 'ROUTED'
                           ELSE 'OPEN'
                       END
                   """)) \
        .withColumn("is_provisional", col("has_late_events")) \
        .withColumn("ts_snapshot", (current_timestamp().cast("long") * 1000))
    
    return lifecycles


def validate_lifecycles(lifecycles: DataFrame) -> DataFrame:
    """Detect data quality exceptions"""
    
    # Exception 1: Overfill
    overfill = lifecycles \
        .filter(col("filled_qty") > col("total_qty")) \
        .select(
            lit(str(uuid.uuid4())).alias("exception_id"),
            lit("OVERFILL").alias("exception_type"),
            lit("ERROR").alias("severity"),
            col("customer_order_id"),
            col("root_firm_order_id").alias("firm_order_id"),
            lit("Filled quantity exceeds total quantity").alias("description"),
            (current_timestamp().cast("long") * 1000).alias("ts_detected"),
            struct(
                col("total_qty"),
                col("filled_qty")
            ).alias("metadata")
        )
    
    # Exception 2: Negative fill
    negative_fill = lifecycles \
        .filter(col("filled_qty") < 0) \
        .select(
            lit(str(uuid.uuid4())).alias("exception_id"),
            lit("NEGATIVE_FILL").alias("exception_type"),
            lit("ERROR").alias("severity"),
            col("customer_order_id"),
            col("root_firm_order_id").alias("firm_order_id"),
            lit("Negative filled quantity").alias("description"),
            (current_timestamp().cast("long") * 1000).alias("ts_detected"),
            struct(col("filled_qty")).alias("metadata")
        )
    
    # Union all exceptions
    all_exceptions = overfill.union(negative_fill)
    
    return all_exceptions


def write_to_kafka(df: DataFrame, topic: str, checkpoint_suffix: str):
    """Write DataFrame to Kafka topic"""
    
    bootstrap_servers = get_config("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    checkpoint_base = get_config("CHECKPOINT_LOCATION", "/tmp/checkpoints")
    
    query = df \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic) \
        .option("checkpointLocation", f"{checkpoint_base}/{checkpoint_suffix}") \
        .outputMode("append") \
        .start()
    
    return query


def write_lifecycles_to_kafka(lifecycles: DataFrame):
    """Write lifecycle snapshots to Kafka (update mode)"""
    
    bootstrap_servers = get_config("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    checkpoint_base = get_config("CHECKPOINT_LOCATION", "/tmp/checkpoints")
    
    query = lifecycles \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", "cat.lifecycle.v1") \
        .option("checkpointLocation", f"{checkpoint_base}/lifecycle") \
        .outputMode("update") \
        .start()
    
    return query


def write_to_console(df: DataFrame, name: str):
    """Write DataFrame to console for debugging"""
    
    query = df \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()
    
    return query


def main():
    """Main streaming job"""
    
    print("Starting CAT Lifecycle Streaming Job...")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read events
    print("Reading events from Kafka...")
    events = read_events_stream(spark)
    
    # Deduplicate
    print("Applying deduplication...")
    unique_events = deduplicate_events(events)
    
    # Apply watermark
    print("Applying watermark...")
    watermarked = apply_watermark(unique_events)
    
    # Build linkages
    print("Building linkages...")
    linkages = build_linkages(watermarked)
    
    # Materialize lifecycles
    print("Materializing lifecycles...")
    lifecycles = materialize_lifecycles(watermarked)
    
    # Validate
    print("Validating lifecycles...")
    exceptions = validate_lifecycles(lifecycles)
    
    # Write outputs
    print("Starting output streams...")
    
    queries = []
    
    # Write linkages
    queries.append(write_to_kafka(linkages, "cat.linkages.v1", "linkages"))
    
    # Write lifecycles
    queries.append(write_lifecycles_to_kafka(lifecycles))
    
    # Write exceptions
    queries.append(write_to_kafka(exceptions, "cat.exceptions.v1", "exceptions"))
    
    # Optional: Console output for debugging
    if get_config("DEBUG_MODE", "false").lower() == "true":
        queries.append(write_to_console(lifecycles, "lifecycles"))
    
    print(f"Started {len(queries)} streaming queries")
    print("Waiting for termination...")
    
    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
