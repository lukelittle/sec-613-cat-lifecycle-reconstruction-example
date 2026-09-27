#!/usr/bin/env python3
"""
Event generator for CAT lifecycle events.
Produces synthetic order lifecycle events with configurable modes:
- normal: In-order events
- late: Inject delayed events
- duplicate: Resend events with same event_id
- chaos: Out-of-order and missing events
"""

import os
import json
import time
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("Warning: kafka-python not installed. Install with: pip install kafka-python")
    KafkaProducer = None


@dataclass
class OrderLifecycle:
    """Represents a complete order lifecycle"""
    customer_order_id: str
    root_firm_order_id: str
    account_id: str
    symbol: str
    side: str
    qty: int
    order_type: str
    limit_price: Optional[float]
    routes: List[Dict[str, Any]]
    
    def __post_init__(self):
        self.events = []


class EventGenerator:
    """Generates synthetic CAT lifecycle events"""
    
    VENUES = ["XNAS", "XNYS", "IEX", "BATS", "EDGX"]
    SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "BAC", "WMT"]
    SIDES = ["BUY", "SELL"]
    
    def __init__(self, bootstrap_servers: str, mode: str = "normal"):
        self.bootstrap_servers = bootstrap_servers
        self.mode = mode
        self.producer = None
        self.sequence = 0
        self.late_event_buffer = []
        
        if KafkaProducer:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
    
    def generate_event_id(self) -> str:
        """Generate unique event ID"""
        return str(uuid.uuid4())
    
    def generate_customer_order_id(self) -> str:
        """Generate customer order ID"""
        date_str = datetime.now().strftime("%Y%m%d")
        account = f"A{random.randint(100, 999)}"
        seq = self.sequence
        self.sequence += 1
        return f"COID-{date_str}-{account}-{seq:04d}"
    
    def generate_firm_order_id(self) -> str:
        """Generate firm order ID"""
        date_str = datetime.now().strftime("%Y%m%d")
        seq = self.sequence
        self.sequence += 1
        return f"FOID-{date_str}-DEMO-{seq:04d}"
    
    def generate_route_id(self, venue: str) -> str:
        """Generate route ID"""
        date_str = datetime.now().strftime("%Y%m%d")
        seq = self.sequence
        self.sequence += 1
        return f"RID-{date_str}-{venue}-{seq:04d}"
    
    def generate_exec_id(self, venue: str) -> str:
        """Generate execution ID"""
        date_str = datetime.now().strftime("%Y%m%d")
        seq = self.sequence
        self.sequence += 1
        return f"EID-{date_str}-{venue}-{seq:04d}"
    
    def create_base_event(self, event_type: str, customer_order_id: str, 
                         firm_order_id: str, symbol: str, side: str, 
                         qty: int, account_id: str) -> Dict[str, Any]:
        """Create base event structure"""
        ts_now = int(time.time() * 1000)
        
        return {
            "event_id": self.generate_event_id(),
            "event_type": event_type,
            "ts_event": ts_now,
            "ts_ingest": ts_now,
            "firm_id": "DEMO_BROKER",
            "account_id": account_id,
            "customer_order_id": customer_order_id,
            "firm_order_id": firm_order_id,
            "parent_firm_order_id": None,
            "route_id": None,
            "venue": None,
            "exec_id": None,
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "order_type": None,
            "limit_price": None,
            "exec_price": None,
            "reason": None,
            "corr_id": str(uuid.uuid4())
        }
    
    def generate_simple_lifecycle(self) -> List[Dict[str, Any]]:
        """Generate a simple order lifecycle: NEW -> ROUTE -> ACK -> FILL"""
        
        events = []
        base_ts = int(time.time() * 1000)
        
        # Order parameters
        customer_order_id = self.generate_customer_order_id()
        root_firm_order_id = self.generate_firm_order_id()
        account_id = f"A{random.randint(100, 999)}"
        symbol = random.choice(self.SYMBOLS)
        side = random.choice(self.SIDES)
        qty = random.choice([100, 200, 500, 1000])
        limit_price = round(random.uniform(100, 300), 2)
        
        # Event 1: NEW
        new_event = self.create_base_event(
            "NEW", customer_order_id, root_firm_order_id, 
            symbol, side, qty, account_id
        )
        new_event["order_type"] = "LIMIT"
        new_event["limit_price"] = limit_price
        new_event["ts_event"] = base_ts
        events.append(new_event)
        
        # Decide routing strategy
        num_routes = random.choice([1, 2])
        route_qty = qty // num_routes
        
        for i in range(num_routes):
            venue = random.choice(self.VENUES)
            child_firm_order_id = self.generate_firm_order_id()
            route_id = self.generate_route_id(venue)
            
            # Event 2: ROUTE
            route_event = self.create_base_event(
                "ROUTE", customer_order_id, child_firm_order_id,
                symbol, side, route_qty, account_id
            )
            route_event["parent_firm_order_id"] = root_firm_order_id
            route_event["route_id"] = route_id
            route_event["venue"] = venue
            route_event["order_type"] = "LIMIT"
            route_event["limit_price"] = limit_price
            route_event["ts_event"] = base_ts + (i + 1) * 100
            events.append(route_event)
            
            # Event 3: ACK
            ack_event = self.create_base_event(
                "ACK", customer_order_id, child_firm_order_id,
                symbol, side, route_qty, account_id
            )
            ack_event["route_id"] = route_id
            ack_event["venue"] = venue
            ack_event["ts_event"] = base_ts + (i + 1) * 100 + 50
            
            # In late mode, hold back some ACKs: they keep their original
            # event time but are sent later, so they arrive late
            if self.mode == "late" and random.random() < 0.3:
                self.late_event_buffer.append(ack_event)
            else:
                events.append(ack_event)
            
            # Event 4: FILL
            exec_id = self.generate_exec_id(venue)
            exec_price = limit_price + random.uniform(-0.5, 0.5)
            
            fill_event = self.create_base_event(
                "FILL", customer_order_id, child_firm_order_id,
                symbol, side, route_qty, account_id
            )
            fill_event["route_id"] = route_id
            fill_event["venue"] = venue
            fill_event["exec_id"] = exec_id
            fill_event["exec_price"] = round(exec_price, 2)
            fill_event["ts_event"] = base_ts + (i + 1) * 100 + 100
            events.append(fill_event)
        
        # In duplicate mode, duplicate some events
        if self.mode == "duplicate" and len(events) > 0:
            dup_event = random.choice(events).copy()
            dup_event["ts_ingest"] = int(time.time() * 1000)
            events.append(dup_event)
        
        # In chaos mode, shuffle events
        if self.mode == "chaos":
            random.shuffle(events)
            # Randomly drop an event
            if len(events) > 2 and random.random() < 0.2:
                events.pop(random.randint(0, len(events) - 1))
        
        return events
    
    def generate_complex_lifecycle(self) -> List[Dict[str, Any]]:
        """Generate complex lifecycle with REPLACE and CANCEL"""
        
        events = []
        base_ts = int(time.time() * 1000)
        
        # Order parameters
        customer_order_id = self.generate_customer_order_id()
        root_firm_order_id = self.generate_firm_order_id()
        account_id = f"A{random.randint(100, 999)}"
        symbol = random.choice(self.SYMBOLS)
        side = random.choice(self.SIDES)
        qty = random.choice([100, 200, 500])
        limit_price = round(random.uniform(100, 300), 2)
        
        # Event 1: NEW
        new_event = self.create_base_event(
            "NEW", customer_order_id, root_firm_order_id,
            symbol, side, qty, account_id
        )
        new_event["order_type"] = "LIMIT"
        new_event["limit_price"] = limit_price
        new_event["ts_event"] = base_ts
        events.append(new_event)
        
        # Event 2: REPLACE (price change)
        replaced_firm_order_id = self.generate_firm_order_id()
        new_limit_price = round(limit_price + random.uniform(-5, 5), 2)
        
        replace_event = self.create_base_event(
            "REPLACE", customer_order_id, replaced_firm_order_id,
            symbol, side, qty, account_id
        )
        replace_event["parent_firm_order_id"] = root_firm_order_id
        replace_event["order_type"] = "LIMIT"
        replace_event["limit_price"] = new_limit_price
        replace_event["reason"] = "Price improvement"
        replace_event["ts_event"] = base_ts + 500
        events.append(replace_event)
        
        # Event 3: CANCEL
        cancel_event = self.create_base_event(
            "CANCEL", customer_order_id, replaced_firm_order_id,
            symbol, side, qty, account_id
        )
        cancel_event["reason"] = "Customer request"
        cancel_event["ts_event"] = base_ts + 1000
        events.append(cancel_event)
        
        return events
    
    def send_event(self, event: Dict[str, Any]):
        """Send event to Kafka"""
        if self.producer:
            try:
                future = self.producer.send(
                    "cat.events.v1",
                    key=event["customer_order_id"],
                    value=event
                )
                future.get(timeout=10)
            except KafkaError as e:
                print(f"Error sending event: {e}")
        else:
            # Print to console if Kafka not available
            print(json.dumps(event, indent=2))
    
    def run(self, duration_seconds: int = 60, events_per_second: float = 1.0):
        """Run generator for specified duration"""
        
        print(f"Starting event generator in '{self.mode}' mode")
        print(f"Duration: {duration_seconds}s, Rate: {events_per_second} events/s")
        
        start_time = time.time()
        event_count = 0
        
        while time.time() - start_time < duration_seconds:
            # Generate lifecycle
            if random.random() < 0.8:
                lifecycle_events = self.generate_simple_lifecycle()
            else:
                lifecycle_events = self.generate_complex_lifecycle()
            
            # Send events
            for event in lifecycle_events:
                self.send_event(event)
                event_count += 1
            
            # Send buffered late events once they're older than the
            # watermark delay (LATE_AFTER_SECONDS, default 150s)
            late_after_ms = int(os.environ.get("LATE_AFTER_SECONDS", "150")) * 1000
            now_ms = int(time.time() * 1000)
            while self.late_event_buffer and \
                    now_ms - self.late_event_buffer[0]["ts_event"] > late_after_ms:
                late_event = self.late_event_buffer.pop(0)
                late_event["ts_ingest"] = int(time.time() * 1000)
                self.send_event(late_event)
                print(f"Sent late event: {late_event['event_type']} for {late_event['customer_order_id']}")
                event_count += 1
            
            # Rate limiting
            time.sleep(1.0 / events_per_second)
        
        # Anything still held back goes out now (late by however long the run was)
        for late_event in self.late_event_buffer:
            late_event["ts_ingest"] = int(time.time() * 1000)
            self.send_event(late_event)
            event_count += 1
        self.late_event_buffer.clear()

        if self.producer:
            self.producer.flush()
        
        print(f"Generated {event_count} events in {duration_seconds}s")
        print(f"Buffered late events: {len(self.late_event_buffer)}")


def lambda_handler(event, context):
    """AWS Lambda handler"""
    
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    mode = os.environ.get("MODE", "normal")
    duration = int(os.environ.get("DURATION_SECONDS", "60"))
    rate = float(os.environ.get("EVENT_RATE_PER_SECOND", "1.0"))
    
    generator = EventGenerator(bootstrap_servers, mode)
    generator.run(duration, rate)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Event generation complete",
            "mode": mode,
            "duration": duration
        })
    }


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="CAT Event Generator")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--mode", default="normal",
                       choices=["normal", "late", "duplicate", "chaos"],
                       help="Generation mode")
    parser.add_argument("--duration", type=int, default=60,
                       help="Duration in seconds")
    parser.add_argument("--rate", type=float, default=1.0,
                       help="Events per second")
    
    args = parser.parse_args()
    
    generator = EventGenerator(args.bootstrap_servers, args.mode)
    generator.run(args.duration, args.rate)


if __name__ == "__main__":
    main()
