#!/usr/bin/env bash
# Module 1 - Cluster Pause: temporarily stop the whole cluster to free RAM/CPU.
#
# Unlike cleanup.sh (docker-compose down), this keeps containers and data
# intact — resume exactly where you left off with:
#   docker-compose start        (from ~/kafka-labs)
#   or: ./stop-cluster.sh -r    (resume shortcut)
#
# Usage: ./stop-cluster.sh      # stop all brokers, keep containers + data
#        ./stop-cluster.sh -r   # resume previously stopped cluster
set -e
cd ~/kafka-labs

if [ "${1:-}" = "-r" ]; then
	echo "Resuming cluster..."
	docker-compose start
	docker-compose ps
else
	echo "Stopping cluster (containers and data preserved)..."
	docker-compose stop
	echo "Cluster stopped. Resume with: $0 -r"
fi
