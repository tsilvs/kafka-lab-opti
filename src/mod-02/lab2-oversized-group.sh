#!/usr/bin/env bash
# Module 2 - Exercise 2.3: Observe Oversized Consumer Group
# 20 consumers for a 12-partition topic: 8 of them end up idle.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Stop previous consumers ---
pkill -f kafka-console-consumer || true

# --- Delete consumer group to start fresh ---
kafka-consumer-groups --delete --group test-oversized 2>/dev/null || true
sleep 2

# --- Start 20 consumers for a 12-partition topic ---
for i in {1..20}; do
	kafka-console-consumer \
		--topic inventory-updates \
		--group test-oversized \
		--from-beginning >/dev/null 2>&1 &
done

# --- Wait for rebalance ---
sleep 10

# --- Check assignment (idle consumers show #PARTITIONS = 0) ---
kafka-consumer-groups --describe \
	--group test-oversized \
	--members | head -25
