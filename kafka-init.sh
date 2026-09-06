#!/usr/bin/env bash
set -euo pipefail

topics=(
  factory.sensor.raw.json.v1
  factory.sensor.raw.text.v1
  factory.sensor.clean.v1
  factory.sensor.dlq.v1
  factory.sensor.sink.dlq.v1
  d2c.application.approved.v1
  d2c.application.consumer.dlq.v1
)

for topic in "${topics[@]}"; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000
done
