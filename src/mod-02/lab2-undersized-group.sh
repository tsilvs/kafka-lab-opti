#!/usr/bin/env bash
# Module 2 - Exercise 2.1: Create Undersized Consumer Group
# 2 consumers for a 12-partition topic: each ends up handling 6 partitions.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Create topic with 12 partitions ---
kafka-topics --create \
	--topic inventory-updates \
	--partitions 12 \
	--replication-factor 3

# --- Produce test data ---
for i in {1..10000}; do echo "inventory-$i"; done |
	kafka-console-producer --topic inventory-updates

# --- Start only 2 consumers (undersized for 12 partitions) ---
kafka-console-consumer \
	--topic inventory-updates \
	--group inventory-sync \
	--from-beginning >/dev/null 2>&1 &

kafka-console-consumer \
	--topic inventory-updates \
	--group inventory-sync \
	--from-beginning >/dev/null 2>&1 &

# --- Wait for rebalance ---
sleep 5

# --- Check member assignment ---
kafka-consumer-groups --describe \
	--group inventory-sync \
	--members --verbose
