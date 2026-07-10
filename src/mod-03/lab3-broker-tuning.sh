#!/usr/bin/env bash
# Module 3 - Exercises 3.1 & 3.3: Broker-Side Performance Tuning
#
# Views current broker threading/buffer settings and runs a 5M-record
# performance test to establish a baseline with default broker config.
# Broker tuning itself requires a rolling restart, which is out of scope
# for this lab — see Exercise 3.2 for the recommended calculation.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Section 1: View current broker configuration ---
# Inspects threading (network/io) and socket buffer sizes on broker 1.
docker exec broker-1 kafka-configs \
	--bootstrap-server broker-1:29092 \
	--entity-type brokers \
	--entity-name 1 \
	--describe --all | grep -E "num.network.threads|num.io.threads|socket"

# --- Section 2: Performance test with current settings ---
# Baseline run against default broker config. Compare against the tuned
# producer settings (128KB batch, 10ms linger, lz4) from Lab 1.
kafka-producer-perf-test \
	--topic perf-test \
	--num-records 5000000 \
	--record-size 1024 \
	--throughput -1 \
	--producer-props \
	acks=all \
	batch.size=131072 \
	linger.ms=10 \
	compression.type=lz4
