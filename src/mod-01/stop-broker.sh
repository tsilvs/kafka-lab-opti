#!/usr/bin/env bash
# Module 1 - Broker Shutdown: gracefully stop a single cluster broker.
#
# Used in the failover labs (see Module 1, replication exercises) to
# simulate a broker outage. Restart it later with:
#   docker start broker-<N>
#
# Usage: ./stop-broker.sh [N]   # N = broker number 1-3 (default: 2)
set -e

BROKER_NUM="${1:-2}"
BROKER="broker-${BROKER_NUM}"

case "$BROKER_NUM" in
1 | 2 | 3) ;;
*)
	echo "Error: broker number must be 1, 2 or 3 (got: $BROKER_NUM)" >&2
	exit 1
	;;
esac

# --- Graceful stop: Kafka gets SIGTERM, flushes and leaves the cluster cleanly ---
echo "Stopping $BROKER..."
docker stop "$BROKER"

echo "$BROKER stopped. Restart with: docker start $BROKER"
