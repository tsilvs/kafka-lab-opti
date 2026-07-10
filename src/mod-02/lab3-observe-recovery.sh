#!/usr/bin/env bash
# Module 2 - Exercise 3.3: Observe Self-Healing
# Restarts broker-2 and verifies the cluster recovered automatically.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Restart broker-2 ---
docker start broker-2

# --- Wait for it to rejoin ---
echo "Waiting for broker-2 to rejoin cluster..."
sleep 15

# --- Under-replicated partitions should be gone (empty output = healed) ---
echo "=== After Recovery ==="
kafka-topics --describe --under-replicated-partitions

# --- Verify all partitions are healthy ---
kafka-topics --describe --topic health-test | grep -E "Partition|Isr"
