#!/usr/bin/env bash
# Module 3 - Exercises 1.1-1.4: Producer Batching and Compression
#
# Runs the full producer performance sweep sequentially: baseline, increasing
# batch sizes, adding linger, then comparing compression algorithms. Inspect
# each section's output to record throughput and latency numbers.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Create test topic ---
kafka-topics --create \
	--topic perf-test \
	--partitions 12 \
	--replication-factor 3 \
	--config min.insync.replicas=2

# --- Section 1: Baseline (16KB batch, 0ms linger, no compression) ---
kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=16384 \
	linger.ms=0 \
	compression.type=none

# --- Section 2: Optimize Batch Size (32KB / 64KB / 128KB) ---
kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=32768 \
	linger.ms=0 \
	compression.type=none

kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=65536 \
	linger.ms=0 \
	compression.type=none

kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=131072 \
	linger.ms=0 \
	compression.type=none

# --- Section 3: Add Linger Time (128KB batch + 10ms linger) ---
kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=131072 \
	linger.ms=10 \
	compression.type=none

# --- Section 4: Test Compression Algorithms (lz4 / snappy / zstd / gzip) ---
kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=131072 \
	linger.ms=10 \
	compression.type=lz4

kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=131072 \
	linger.ms=10 \
	compression.type=snappy

kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=131072 \
	linger.ms=10 \
	compression.type=zstd

kafka-producer-perf-test \
	--topic perf-test \
	--num-records 1000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=131072 \
	linger.ms=10 \
	compression.type=gzip
