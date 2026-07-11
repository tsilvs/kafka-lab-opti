#!/usr/bin/env bash
# Shared Kafka CLI wrappers for the lab scripts.
#
# The lab guides use shell aliases (see Module 1, Step 2). Aliases are not
# available in non-interactive scripts, so the same commands are provided
# here as functions with identical names — script bodies can therefore
# match the guide commands verbatim.

BOOTSTRAP='broker-1:29092,broker-2:29092,broker-3:29092'

kafka-topics() { docker exec broker-1 kafka-topics --bootstrap-server "$BOOTSTRAP" "$@"; }
kafka-console-producer() { docker exec -i broker-1 kafka-console-producer --bootstrap-server "$BOOTSTRAP" "$@"; }
kafka-console-consumer() { docker exec broker-1 kafka-console-consumer --bootstrap-server "$BOOTSTRAP" "$@"; }
kafka-consumer-groups() { docker exec broker-1 kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" "$@"; }
kafka-configs() { docker exec broker-1 kafka-configs --bootstrap-server "$BOOTSTRAP" "$@"; }

# kafka-producer-perf-test takes the bootstrap servers inside --producer-props,
# so it is appended after the caller's arguments (always call this function
# with --producer-props as the last option group).
kafka-producer-perf-test() { docker exec broker-1 kafka-producer-perf-test "$@" bootstrap.servers="$BOOTSTRAP"; }

export -f kafka-topics kafka-console-producer kafka-console-consumer kafka-consumer-groups kafka-configs kafka-producer-perf-test
