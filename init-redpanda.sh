#!/bin/sh
# Wait for Redpanda to be ready
echo "Waiting for Redpanda cluster..."

# Use -X brokers=... for connection
# We loop on 'cluster info' as a generic connectivity check
until rpk cluster info -X brokers=redpanda:9092; do 
  echo "Redpanda not ready yet..."
  sleep 2
done

echo "Cluster is reachable. Creating topic 'iot-stream'..."
# Create topic
# If it exists, rpk might return an error, so we allow it to 'fail' verbally but we exit 0
rpk topic create iot-stream -X brokers=redpanda:9092 -p 1 -r 1 || true

echo "Topic creation step done. Exiting."
exit 0
