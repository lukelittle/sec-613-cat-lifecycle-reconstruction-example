"""Tests for the lifecycle job's core logic.

Run with `make test` (inside the local Spark container). Streaming-specific
behavior (watermarks, late-event routing) is exercised by the demo itself;
these tests run the same functions on small batch DataFrames.
"""

import unittest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import lifecycle_streaming as job

BASE = datetime(2026, 1, 15, 14, 30, 0)


def event(event_id, event_type, qty, offset_ms, **extra):
    ts = BASE + timedelta(milliseconds=offset_ms)
    row = {
        "event_id": event_id, "event_type": event_type,
        "ts_event": int(ts.timestamp() * 1000), "ts_ingest": int(ts.timestamp() * 1000),
        "firm_id": "DEMO_BROKER", "account_id": "A100",
        "customer_order_id": "CO-1", "firm_order_id": "FO-1",
        "parent_firm_order_id": None, "route_id": None, "venue": None, "exec_id": None,
        "symbol": "AAPL", "qty": qty, "side": "BUY", "order_type": None,
        "limit_price": None, "exec_price": None, "reason": None, "corr_id": None,
        "kafka_timestamp": ts, "partition": 0, "offset": 0, "event_time": ts,
        "is_late": False,
    }
    row.update(extra)
    return row


class EdgeTests(unittest.TestCase):
    def test_route_produces_parent_and_venue_edges(self):
        edges = job.edges_for_event("E1", "ROUTE", "CO-1", "FO-2", "FO-1", "R-1", None, 1)
        self.assertEqual([(e["source_id"], e["target_id"]) for e in edges],
                         [("FO-1", "FO-2"), ("FO-2", "R-1")])

    def test_edge_ids_are_deterministic(self):
        first = job.edges_for_event("E1", "NEW", "CO-1", "FO-1", None, None, None, 1)
        again = job.edges_for_event("E1", "NEW", "CO-1", "FO-1", None, None, None, 1)
        other = job.edges_for_event("E2", "NEW", "CO-1", "FO-1", None, None, None, 1)
        self.assertEqual(first[0]["edge_id"], again[0]["edge_id"])
        self.assertNotEqual(first[0]["edge_id"], other[0]["edge_id"])

    def test_fill_without_exec_id_has_no_edge(self):
        self.assertEqual(job.edges_for_event("E1", "FILL", "CO-1", "FO-1", None, "R-1", None, 1), [])


class LifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.ui.enabled", "false").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def lifecycle(self, rows):
        df = self.spark.createDataFrame(rows, schema=self.schema())
        return job.materialize_lifecycles(job.deduplicate_events(df))

    def schema(self):
        from pyspark.sql.types import StructType, StructField, TimestampType, BooleanType, IntegerType, LongType
        return StructType(job.EVENT_SCHEMA.fields + [
            StructField("kafka_timestamp", TimestampType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("event_time", TimestampType()),
            StructField("is_late", BooleanType()),
        ])

    def test_filled_order(self):
        rows = [event("E1", "NEW", 100, 0), event("E2", "ROUTE", 100, 100),
                event("E3", "FILL", 100, 200, exec_id="X1", route_id="R-1")]
        result = self.lifecycle(rows).collect()[0]
        self.assertEqual(result["status"], "FILLED")
        self.assertEqual(result["total_qty"], 100)
        self.assertEqual(result["last_event"]["event_type"], "FILL")
        self.assertEqual(result["trade_date"], "2026-01-15")

    def test_last_event_is_latest_not_alphabetical(self):
        # ROUTE sorts after FILL alphabetically but happened earlier
        rows = [event("E1", "NEW", 100, 0), event("E2", "FILL", 50, 300, exec_id="X1"),
                event("E3", "ROUTE", 100, 100)]
        self.assertEqual(self.lifecycle(rows).collect()[0]["last_event"]["event_type"], "FILL")

    def test_duplicate_events_count_once(self):
        fill = event("E2", "FILL", 50, 200, exec_id="X1")
        rows = [event("E1", "NEW", 100, 0), fill, dict(fill)]
        result = self.lifecycle(rows).collect()[0]
        self.assertEqual(result["filled_qty"], 50)
        self.assertEqual(result["status"], "PARTIALLY_FILLED")

    def test_overfill_is_flagged(self):
        rows = [event("E1", "NEW", 100, 0), event("E2", "FILL", 60, 100, exec_id="X1"),
                event("E3", "FILL", 60, 200, exec_id="X2")]
        exceptions = job.validate_lifecycles(self.lifecycle(rows)).collect()
        self.assertEqual([e["exception_type"] for e in exceptions], ["OVERFILL"])

    def test_bust_reverses_fill_and_negative_fill_is_flagged(self):
        rows = [event("E1", "NEW", 100, 0), event("E2", "FILL", 50, 100, exec_id="X1"),
                event("E3", "BUST", 50, 200, exec_id="X1"), event("E4", "BUST", 50, 300, exec_id="X1")]
        lifecycle = self.lifecycle(rows)
        self.assertEqual(lifecycle.collect()[0]["filled_qty"], -50)
        types = [e["exception_type"] for e in job.validate_lifecycles(lifecycle).collect()]
        self.assertEqual(types, ["NEGATIVE_FILL"])

    def test_exception_ids_are_unique(self):
        rows = [event("E1", "NEW", 100, 0), event("E2", "FILL", 150, 100, exec_id="X1")]
        rows += [dict(r, customer_order_id="CO-2", event_id=r["event_id"] + "b") for r in rows]
        ids = [e["exception_id"] for e in job.validate_lifecycles(self.lifecycle(rows)).collect()]
        self.assertEqual(len(ids), 2)
        self.assertEqual(len(set(ids)), 2)


if __name__ == "__main__":
    unittest.main()
