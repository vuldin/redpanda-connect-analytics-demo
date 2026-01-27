#!/bin/bash
set -e

echo "========================================"
echo "Creating Redpanda Topics for Demo"
echo "========================================"

# Wait for Redpanda to be fully ready
echo "Waiting for Redpanda to be ready..."
until rpk cluster info --brokers redpanda:9092 2>/dev/null; do
  echo "Redpanda is not ready yet. Retrying in 2 seconds..."
  sleep 2
done

echo ""
echo "Redpanda is ready! Creating topics..."
echo ""

# Create source topic - raw user events
rpk topic create user-events-raw \
  --brokers redpanda:9092 \
  --partitions 3 \
  --replicas 1 \
  --topic-config retention.ms=86400000 \
  || echo "Topic user-events-raw already exists or creation failed"

# Create processed topic - enriched user events
rpk topic create user-events-enriched \
  --brokers redpanda:9092 \
  --partitions 3 \
  --replicas 1 \
  --topic-config retention.ms=86400000 \
  || echo "Topic user-events-enriched already exists or creation failed"

# Create analytics topic - aggregated metrics
rpk topic create user-analytics \
  --brokers redpanda:9092 \
  --partitions 3 \
  --replicas 1 \
  --topic-config retention.ms=86400000 \
  || echo "Topic user-analytics already exists or creation failed"

# Create dead letter queue for failed processing
rpk topic create user-events-dlq \
  --brokers redpanda:9092 \
  --partitions 1 \
  --replicas 1 \
  --topic-config retention.ms=604800000 \
  || echo "Topic user-events-dlq already exists or creation failed"

echo ""
echo "========================================"
echo "Topics Created Successfully!"
echo "========================================"
echo ""
echo "Topic List:"
rpk topic list --brokers redpanda:9092
echo ""
echo "Topic Details:"
rpk topic describe user-events-raw user-events-enriched user-analytics user-events-dlq \
  --brokers redpanda:9092 || true
echo ""
echo "Setup complete!"
