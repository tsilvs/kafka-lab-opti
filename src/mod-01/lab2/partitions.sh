#!/usr/bin/env bash
# Module 1 - Exercises 2.2 & 2.3: Create a Properly Partitioned Topic and
# observe partition assignment.
#
# Usage: ./lab2-partitions.sh [partition-count]
#   partition-count: your calculated value from Exercise 2.1 (default: 26)
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"
PARTITIONS="${1:-26}"

# --- Create the topic with the calculated partition count ---
kafka-topics --create \
	--topic analytics-events \
	--partitions "$PARTITIONS" \
	--replication-factor 3 \
	--config min.insync.replicas=2

# --- Verify the distribution across brokers ---
kafka-topics --describe --topic analytics-events

# --- Produce 10 test messages ---
for i in {1..10}; do echo "message-$i"; done | kafka-console-producer --topic analytics-events

# --- Consume with partition visibility ---
kafka-console-consumer --topic analytics-events \
	--from-beginning \
	--property print.partition=true \
	--timeout-ms 5000 || true
