#!/usr/bin/env bash
# Module 2 - Lab Cleanup: stop background clients, delete test consumer
# groups and topics, ensure all brokers are running for the next module.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Stop all background processes ---
pkill -f kafka-console-consumer || true
pkill -f kafka-console-producer || true

# --- Delete test consumer groups ---
kafka-consumer-groups --delete --group order-processor 2>/dev/null || true
kafka-consumer-groups --delete --group inventory-sync 2>/dev/null || true
kafka-consumer-groups --delete --group test-oversized 2>/dev/null || true

# --- Delete test topics ---
kafka-topics --delete --topic order-events 2>/dev/null || true
kafka-topics --delete --topic inventory-updates 2>/dev/null || true
kafka-topics --delete --topic health-test 2>/dev/null || true

# --- Ensure all brokers are running for next module ---
docker start broker-1 broker-2 broker-3
