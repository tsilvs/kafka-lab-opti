#!/usr/bin/env bash
# Module 1 - Exercise 1.1: Create Topics with Different Replication
# Creates one unsafe topic (no redundancy) and one production-ready topic,
# then prints both configurations for verification.
source "$(cd "$(dirname "$0")" && pwd)/../kafka-env.sh"

# --- Topic with NO redundancy (unsafe for production) ---
kafka-topics --create \
	--topic orders-unsafe \
	--partitions 3 \
	--replication-factor 1

# --- Topic with redundancy (production-ready) ---
kafka-topics --create \
	--topic orders-safe \
	--partitions 3 \
	--replication-factor 3 \
	--config min.insync.replicas=2

# --- Verify the configuration ---
kafka-topics --describe --topic orders-unsafe
kafka-topics --describe --topic orders-safe
