#!/bin/bash
# Tail Kafka topics to observe events in real-time

set -e

KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
TOPIC="${1:-cat.events.v1}"

echo "Tailing topic: $TOPIC on $KAFKA_BROKER"
echo "Press Ctrl+C to stop"
echo "---"

# Check if kcat is installed
if command -v kcat &> /dev/null; then
    kcat -b "$KAFKA_BROKER" \
        -t "$TOPIC" \
        -C \
        -o end \
        -f 'Partition: %p | Offset: %o | Key: %k\nValue: %s\n---\n'
elif command -v kafkacat &> /dev/null; then
    kafkacat -b "$KAFKA_BROKER" \
        -t "$TOPIC" \
        -C \
        -o end \
        -f 'Partition: %p | Offset: %o | Key: %k\nValue: %s\n---\n'
else
    # Fall back to the console consumer inside the local Kafka container
    docker exec -it cat-kafka kafka-console-consumer \
        --bootstrap-server kafka:29092 \
        --topic "$TOPIC" \
        --property print.key=true
fi
