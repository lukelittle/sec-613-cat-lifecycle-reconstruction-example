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
    echo "Error: kcat (or kafkacat) not installed"
    echo "Install with: brew install kcat (macOS) or apt-get install kafkacat (Linux)"
    exit 1
fi
