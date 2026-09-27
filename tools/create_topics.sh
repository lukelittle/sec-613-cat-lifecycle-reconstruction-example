#!/bin/bash
# Create Kafka topics for CAT system

set -e

KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"

# Use a local kafka-topics if installed; otherwise run it inside the local
# Kafka container (the default for `make local-topics`)
if command -v kafka-topics >/dev/null 2>&1; then
    kafka_topics() { kafka-topics "$@"; }
else
    KAFKA_BROKER="kafka:29092"
    kafka_topics() { docker exec cat-kafka kafka-topics "$@"; }
fi
PARTITIONS="${PARTITIONS:-6}"
REPLICATION="${REPLICATION:-1}"

echo "Creating Kafka topics on $KAFKA_BROKER..."

# Function to create topic
create_topic() {
    local topic=$1
    local retention_ms=$2
    
    echo "Creating topic: $topic"
    
    kafka_topics --create \
        --bootstrap-server "$KAFKA_BROKER" \
        --topic "$topic" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --config retention.ms="$retention_ms" \
        --config compression.type=gzip \
        --if-not-exists
}

# Create topics
# Raw events: 30 days retention
create_topic "cat.events.v1" 2592000000

# Derived topics: 7 days retention
create_topic "cat.linkages.v1" 604800000
create_topic "cat.lifecycle.v1" 604800000
create_topic "cat.exceptions.v1" 604800000
create_topic "cat.late_events.v1" 604800000
create_topic "audit.v1" 2592000000

# Customer reference data (optional)
create_topic "cat.customers.v1" 2592000000

echo "Topics created successfully!"
echo ""
echo "List of topics:"
kafka_topics --list --bootstrap-server "$KAFKA_BROKER"
