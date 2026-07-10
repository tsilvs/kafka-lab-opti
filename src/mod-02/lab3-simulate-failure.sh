#!/usr/bin/env bash
# Module 2 - Exercise 3.2: Simulate Broker Failure
# Creates a test topic, stops broker-2, and shows the resulting
# under-replicated partitions. Recover with lab3-observe-recovery.sh.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Create a test topic ---
kafka-topics --create \
	--topic health-test \
	--partitions 9 \
	--replication-factor 3

# --- Produce some test data ---
for i in {1..1000}; do echo "message-$i"; done |
	kafka-console-producer --topic health-test

# --- Check initial state ---
echo "=== Before Failure ==="
kafka-topics --describe --topic health-test

# --- Stop broker-2 ---
docker stop broker-2

# --- Wait for cluster to detect failure ---
echo "Waiting 10 seconds for cluster to detect failure..."
sleep 10

# --- Check for under-replicated partitions ---
echo "=== After Failure ==="
kafka-topics --describe --under-replicated-partitions
