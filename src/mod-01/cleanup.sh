#!/usr/bin/env bash
# Module 1 - Lab Cleanup: stop and remove all containers.
#
# Usage: ./cleanup.sh        # stop containers, keep data
#        ./cleanup.sh -v     # also remove all data (fresh start next time)
set -e
# cd ~/kafka-labs

cd ~/Documents/mbrok/kafka/learn/practice/kafka-labs

# --- Stop and remove all containers (any extra flags are passed through) ---
docker-compose down "$@"
