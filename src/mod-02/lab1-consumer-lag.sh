#!/usr/bin/env bash
# Module 2 - Exercise 1.1: Create a Topic with Lag
# Builds a realistic lag scenario: fast producer, one slow consumer.
# Monitor lag afterwards with the Exercise 1.2 / 1.3 commands.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Create topic ---
kafka-topics --create \
	--topic order-events \
	--partitions 12 \
	--replication-factor 3 \
	--config min.insync.replicas=2

# --- Produce 5,000 messages quickly ---
for i in {1..5000}; do
	echo "order-$i"
done | kafka-console-producer --topic order-events

# --- Start ONE slow consumer in the background (will fall behind) ---
kafka-console-consumer \
	--topic order-events \
	--group order-processor \
	--from-beginning >/dev/null 2>&1 &
echo "Background consumer started (stop it later with: pkill -f kafka-console-consumer)"

# --- Keep producing in the background to build lag ---
# 0.05s per message: producer outpaces the single consumer
for i in {5001..10000}; do
	echo "order-$i"
	sleep 0.05
done | kafka-console-producer --topic order-events &
echo "Background producer started - lag is now building"
