#!/usr/bin/env bash
# Module 1 - Exercises 3.1-3.3: Topic Configuration Best Practices
# Creates the three production configuration patterns, verifies them, and
# runs the compaction behaviour test.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Section 1: Analytics topic (time-series, delete after 30 days) ---
kafka-topics --create \
	--topic clickstream-events \
	--partitions 20 \
	--replication-factor 3 \
	--config min.insync.replicas=2 \
	--config retention.ms=2592000000 \
	--config segment.bytes=1073741824 \
	--config compression.type=lz4 \
	--config cleanup.policy=delete

# --- Section 2: State topic (compaction keeps latest value per key) ---
kafka-topics --create \
	--topic user-profiles \
	--partitions 12 \
	--replication-factor 3 \
	--config min.insync.replicas=2 \
	--config cleanup.policy=compact \
	--config segment.bytes=104857600 \
	--config compression.type=zstd \
	--config min.compaction.lag.ms=3600000

# --- Section 3: Audit log (compliance: 90-day retention, max durability) ---
kafka-topics --create \
	--topic audit-log \
	--partitions 6 \
	--replication-factor 3 \
	--config min.insync.replicas=3 \
	--config retention.ms=7776000000 \
	--config segment.ms=86400000 \
	--config segment.bytes=536870912 \
	--config compression.type=gzip \
	--config cleanup.policy=delete

# --- Section 4: Verify each topic's configuration ---
kafka-configs --describe --entity-type topics --entity-name clickstream-events
kafka-configs --describe --entity-type topics --entity-name user-profiles
kafka-configs --describe --entity-type topics --entity-name audit-log

# --- Section 5: Compaction behaviour test ---
# Produce multiple updates for the same key; after compaction runs (may take
# several minutes) only user1:v3 remains.
echo "user1:v1" | kafka-console-producer --topic user-profiles \
	--property "parse.key=true" --property "key.separator=:"

echo "user1:v2" | kafka-console-producer --topic user-profiles \
	--property "parse.key=true" --property "key.separator=:"

echo "user1:v3" | kafka-console-producer --topic user-profiles \
	--property "parse.key=true" --property "key.separator=:"

# Consume all records (before compaction all three versions are visible)
kafka-console-consumer --topic user-profiles \
	--from-beginning \
	--property print.key=true \
	--timeout-ms 5000 || true
