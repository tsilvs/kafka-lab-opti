#!/usr/bin/env bash
# Module 3 - Exercises 2.2 & 2.3: Consumer Fetch Optimization
#
# Creates a test topic, then runs the consumer twice: once with real-time
# settings (low latency) and once with batch settings (high throughput).
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Create test topic ---
kafka-topics --create \
	--topic realtime-events \
	--partitions 6 \
	--replication-factor 3

# --- Produce test data ---
for i in {1..10000}; do echo "event-$i"; done |
	kafka-console-producer --topic realtime-events

# --- Section 1: Consume with real-time settings ---
# fetch.min.bytes=1   -> return immediately, don't wait for data
# fetch.max.wait.ms=100 -> max 100ms wait (low latency)
# max.poll.records=100  -> small batches, frequent polling
kafka-console-consumer \
	--topic realtime-events \
	--from-beginning \
	--group realtime-consumer \
	--max-messages 1000 \
	--consumer-property fetch.min.bytes=1 \
	--consumer-property fetch.max.wait.ms=100 \
	--consumer-property max.poll.records=100

# --- Section 2: Consume with high-throughput settings ---
# Delete previous consumer group so the second run starts fresh.
kafka-consumer-groups --delete --group batch-consumer 2>/dev/null || true

# fetch.min.bytes=102400 (100KB) -> wait for substantial data
# fetch.max.wait.ms=500 -> max 500ms wait
# max.poll.records=1000  -> large batches, fewer polls
kafka-console-consumer \
	--topic realtime-events \
	--from-beginning \
	--group batch-consumer \
	--max-messages 1000 \
	--consumer-property fetch.min.bytes=102400 \
	--consumer-property fetch.max.wait.ms=500 \
	--consumer-property max.poll.records=1000
