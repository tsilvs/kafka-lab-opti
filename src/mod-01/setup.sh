#!/usr/bin/env bash
# Module 1 - Step 1: Create Kafka Cluster
# Creates the working directory, copies the cluster definition, starts the
# cluster, waits for it to come up, and verifies the setup.
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# --- Create working directory ---
mkdir -p ~/kafka-labs
cd ~/kafka-labs

# --- Copy the cluster definition into place ---
cp "$SCRIPT_DIR/docker-compose.yml" .

# --- Start the cluster ---
docker-compose up -d

# --- Wait for cluster to start (this takes about 60 seconds) ---
echo "Waiting for Kafka cluster to start..."
sleep 60

# --- Verify setup: all containers "Up", topic list reachable ---
docker-compose ps
source "$SCRIPT_DIR/../kafka-env.sh"
kafka-topics --list
