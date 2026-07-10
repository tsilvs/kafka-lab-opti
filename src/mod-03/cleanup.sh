#!/usr/bin/env bash
# Module 3 - Lab Cleanup: stop background clients and delete test consumer
# groups and topics.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Stop background processes ---
pkill -f kafka-console-consumer || true
pkill -f kafka-console-producer || true

# --- Delete test consumer groups ---
kafka-consumer-groups --delete --group realtime-consumer 2>/dev/null || true
kafka-consumer-groups --delete --group batch-consumer 2>/dev/null || true

# --- Delete test topics ---
kafka-topics --delete --topic perf-test 2>/dev/null || true
kafka-topics --delete --topic realtime-events 2>/dev/null || true

echo "✅ Module 3 labs complete!"
