#!/usr/bin/env python3
"""
Spark Structured Streaming job for CAT lifecycle reconstruction.
Consumes events from Kafka, builds linkages, materializes lifecycles,
validates data quality, and handles late-arriving events.
"""

import os
import hashlib
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, explode, lit, current_timestamp,
    window, count, sum as _sum, avg, max as _max, min as _min,
    udf, expr
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


def use_iam_auth() -> bool:
    return get_config("USE_IAM_AUTH", "false").lower() == "true"


def kafka_options() -> Dict[str, str]:
    """Connection options shared by every Kafka reader and writer"""
    options = {"kafka.bootstrap.servers": get_config("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
    if use_iam_auth():
        options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "kafka.sasl.client.callback.handler.class":
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        })
    return options


def create_spark_session() -> SparkSession:
    """Create and configure Spark session"""
    builder = SparkSession.builder \
        .appName("CAT-Lifecycle-Streaming") \
        .config("spark.sql.streaming.checkpointLocation", 
                get_config("CHECKPOINT_LOCATION", "/tmp/checkpoints")) \
        .config("spark.sql.shuffle.partitions", "6") \
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
    
    # Kafka connector always; MSK IAM auth only when running against MSK.
    # Under spark-submit these only apply if no JVM is running yet, so the
    # Makefile and deploy script also pass them with --packages / --conf.
    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]
    if use_iam_auth():
        packages.append("software.amazon.msk:aws-msk-iam-auth:2.0.3")
    builder = builder.config("spark.jars.packages", ",".join(packages))
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def read_events_stream(spark: SparkSession) -> DataFrame:
    """Read raw events from cat.events.v1 Kafka topic"""
    
    raw_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_options()) \
        .option("subscribe", "cat.events.v1") \
        .option("startingOffsets", get_config("STARTING_OFFSETS", "latest")) \
        .option("failOnDataLoss", "false") \
        .load()
    
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


def watermark_delay() -> str:
    return get_config("WATERMARK_DELAY", "2 minutes")


def apply_watermark(events: DataFrame) -> DataFrame:
    """Apply the event-time watermark and tag events that arrived late.

    An event counts as late when it reached Kafka more than the watermark
    delay after it happened. Stateful operators silently drop events that are
    behind the watermark, so late events are also routed to their own topic
    (cat.late_events.v1) for batch reconciliation.
    """
    delay = watermark_delay()
    return events \
        .withWatermark("event_time", delay) \
        .withColumn("is_late",
                    col("kafka_timestamp") > col("event_time") + expr(f"INTERVAL {delay}"))


def deduplicate_events(events: DataFrame) -> DataFrame:
    """Remove duplicate events by event_id (idempotency).

    Deduplicating on event_id plus the watermarked event_time column lets
    Spark evict each id once the watermark passes it, so state stays
    bounded. Deduplicating on event_id alone would keep every id forever.
    A resent event carries the same ts_event, so both keys match.

    (dropDuplicatesWithinWatermark would be the natural choice, but in
    Spark 3.5 it fails at runtime once downstream projections prune columns.)
    """
    return events.dropDuplicates(["event_id", "event_time"])


EDGE_SCHEMA = ArrayType(StructType([
    StructField("edge_id", StringType()),
    StructField("edge_type", StringType()),
    StructField("source_id", StringType()),
    StructField("target_id", StringType()),
    StructField("customer_order_id", StringType()),
    StructField("ts_event", LongType())
]))


def edges_for_event(event_id, event_type, customer_order_id, firm_order_id,
                    parent_firm_order_id, route_id, exec_id, ts_event) -> List[Dict[str, Any]]:
    """Parent-child edges implied by one event.

    Edge ids are derived from the event, so replaying the same event produces
    the same edges instead of new random ids.
    """
    pairs = []
    if event_type == "NEW":
        pairs.append(("ROOT", customer_order_id, firm_order_id))
    elif event_type == "ROUTE":
        if parent_firm_order_id:
            pairs.append(("ROUTE", parent_firm_order_id, firm_order_id))
        if route_id:
            pairs.append(("ROUTE", firm_order_id, route_id))
    elif event_type == "FILL":
        if route_id and exec_id:
            pairs.append(("FILL", route_id, exec_id))
    elif event_type == "REPLACE":
        if parent_firm_order_id:
            pairs.append(("REPLACE", parent_firm_order_id, firm_order_id))
    elif event_type == "CANCEL":
        pairs.append(("CANCEL", firm_order_id, f"CANCEL-{firm_order_id}"))
    elif event_type == "BUST":
        if exec_id:
            pairs.append(("BUST", exec_id, f"BUST-{exec_id}"))

    edges = []
    for i, (edge_type, source_id, target_id) in enumerate(pairs):
        digest = hashlib.sha256(f"{event_id}:{i}:{edge_type}".encode()).hexdigest()[:32]
        edges.append({
            "edge_id": digest,
            "edge_type": edge_type,
            "source_id": source_id,
            "target_id": target_id,
            "customer_order_id": customer_order_id,
            "ts_event": ts_event,
        })
    return edges


def build_linkages(events: DataFrame) -> DataFrame:
    """Construct parent-child edges from events (stateless, append mode)"""
    construct_edges = udf(edges_for_event, EDGE_SCHEMA)
    return events \
        .withColumn("edges", construct_edges(
            col("event_id"),
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


def materialize_lifecycles(events: DataFrame) -> DataFrame:
    """Aggregate events into lifecycle snapshots"""
    
    # Grouping by trading day as well as order id lets the watermark close
    # each day and evict its state. Without an event-time key, state would
    # grow forever. (Orders that live across days, like GTC, would need
    # arbitrary stateful processing instead; see docs/10-exercises.md.)
    lifecycle_agg = events \
        .groupBy(window(col("event_time"), "1 day").alias("trade_date_window"),
                 col("customer_order_id")) \
        .agg(
            # ts_event first so max() picks the latest event, not the
            # alphabetically largest event type
            _max(struct(col("ts_event"), col("event_type"))).alias("last_event"),
            _min("ts_event").alias("ts_first_event"),
            _max("ts_event").alias("ts_last_event"),
            _max(col("firm_order_id")).alias("root_firm_order_id"),
            _max(col("account_id")).alias("account_id"),
            _max(col("symbol")).alias("symbol"),
            _max(col("side")).alias("side"),
            # Order quantity comes from the NEW event; taking max(qty) across
            # all events would hide overfills
            _max(expr("CASE WHEN event_type = 'NEW' THEN qty END")).alias("total_qty"),
            # Busts reverse fills
            _sum(expr("""CASE WHEN event_type = 'FILL' THEN qty
                              WHEN event_type = 'BUST' THEN -qty
                              ELSE 0 END""")).alias("filled_qty"),
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
        .withColumn("trade_date", col("trade_date_window.start").cast("date").cast("string")) \
        .withColumn("ts_snapshot", (current_timestamp().cast("long") * 1000)) \
        .drop("trade_date_window")
    
    return lifecycles


def validate_lifecycles(lifecycles: DataFrame) -> DataFrame:
    """Detect data quality exceptions in a batch of lifecycle snapshots.

    Runs inside foreachBatch on each micro-batch of updated lifecycles, so it
    works on a plain (non-streaming) DataFrame.
    """
    def exceptions(condition, exception_type, description, metadata):
        return lifecycles \
            .filter(condition) \
            .select(
                expr("uuid()").alias("exception_id"),
                lit(exception_type).alias("exception_type"),
                lit("ERROR").alias("severity"),
                col("customer_order_id"),
                col("root_firm_order_id").alias("firm_order_id"),
                lit(description).alias("description"),
                (current_timestamp().cast("long") * 1000).alias("ts_detected"),
                to_json(metadata).alias("metadata")
            )

    overfill = exceptions(col("filled_qty") > col("total_qty"), "OVERFILL",
                          "Filled quantity exceeds total quantity",
                          struct(col("total_qty"), col("filled_qty")))
    negative_fill = exceptions(col("filled_qty") < 0, "NEGATIVE_FILL",
                               "Negative filled quantity (more busted than filled)",
                               struct(col("total_qty"), col("filled_qty")))
    return overfill.unionByName(negative_fill)


def checkpoint(name: str) -> str:
    return f"{get_config('CHECKPOINT_LOCATION', '/tmp/checkpoints')}/{name}"


def write_to_kafka(df: DataFrame, topic: str, checkpoint_suffix: str):
    """Write a stateless stream to a Kafka topic (append mode)"""
    return df \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .options(**kafka_options()) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint(checkpoint_suffix)) \
        .outputMode("append") \
        .start()


def write_lifecycles_and_exceptions(lifecycles: DataFrame):
    """Write updated lifecycles, and the exceptions found in them, per micro-batch.

    Exceptions are derived from an aggregation, which Spark can't emit in
    append mode, so both outputs are written from foreachBatch in update mode.
    """
    def write_batch(batch: DataFrame, batch_id: int):
        batch.persist()
        for df, topic in ((batch, "cat.lifecycle.v1"),
                          (validate_lifecycles(batch), "cat.exceptions.v1")):
            df.select(col("customer_order_id").alias("key"),
                      to_json(struct("*")).alias("value")) \
                .write \
                .format("kafka") \
                .options(**kafka_options()) \
                .option("topic", topic) \
                .save()
        batch.unpersist()

    return lifecycles \
        .writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", checkpoint("lifecycle")) \
        .outputMode("update") \
        .start()


def write_to_console(df: DataFrame, name: str):
    """Write DataFrame to console for debugging"""
    
    query = df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
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
    
    # Watermark first, so dedup and aggregation state are bounded
    print("Applying watermark...")
    watermarked = apply_watermark(events)

    # Deduplicate
    print("Applying deduplication...")
    unique_events = deduplicate_events(watermarked)

    # Build linkages
    print("Building linkages...")
    linkages = build_linkages(unique_events)

    # Materialize lifecycles
    print("Materializing lifecycles...")
    lifecycles = materialize_lifecycles(unique_events)

    # Write outputs
    print("Starting output streams...")

    queries = []

    # Linkages (stateless)
    queries.append(write_to_kafka(linkages, "cat.linkages.v1", "linkages"))

    # Lifecycles and the exceptions detected in them
    queries.append(write_lifecycles_and_exceptions(lifecycles))

    # Late events, for batch reconciliation (read before the stateful
    # operators, which drop events behind the watermark)
    late_events = watermarked.filter(col("is_late")) \
        .drop("kafka_timestamp", "partition", "offset", "event_time")
    queries.append(write_to_kafka(late_events, "cat.late_events.v1", "late_events"))

    # Optional: Console output for debugging
    if get_config("DEBUG_MODE", "false").lower() == "true":
        queries.append(write_to_console(lifecycles, "lifecycles"))
    
    print(f"Started {len(queries)} streaming queries")
    print("Waiting for termination...")
    
    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
