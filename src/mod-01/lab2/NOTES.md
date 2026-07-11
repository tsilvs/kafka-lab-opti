# Lab 2 Output Explained: Single Partition & TimeoutException

<!-- links -->

[kafka/kip-480]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner "KIP-480: Sticky Partitioner"

<!-- doc -->

Observed output from `lab2/partitions.sh`:

```
Partition:6     message-1
...
Partition:6     message-10
[...] ERROR Error processing message, terminating consumer process:  (org.apache.kafka.tools.consumer.ConsoleConsumer)
org.apache.kafka.common.errors.TimeoutException
Processed a total of 10 messages
```

Both behaviours are expected — nothing is broken.

## 1. All messages land in one partition

Messages were produced **without a key**. Since Kafka 2.4, keyless records use the
**sticky partitioner** ([KIP-480][kafka/kip-480]): the producer picks one random
partition and keeps writing to it until the current batch is full or `linger.ms`
expires, then switches. Ten tiny messages fit in a single batch, so all ten go to
the same (randomly chosen) partition — here, partition 6. It is *not* round-robin
per message.

To see messages spread across partitions, produce **keyed** records — the key is
hashed (murmur2) modulo partition count:

```bash
for i in {1..10}; do echo "key-$i:message-$i"; done | kafka-console-producer \
	--topic analytics-events \
	--property parse.key=true \
	--property key.separator=:
```

## 2. `TimeoutException` at the end

`kafka-console-consumer` was started with `--timeout-ms 5000`. After 5 seconds
with no new messages it terminates itself by throwing
`org.apache.kafka.common.errors.TimeoutException` and logging it as an ERROR.
This is the intended shutdown path for a bounded read — note the final line
`Processed a total of 10 messages`. The `|| true` in the script exists precisely
to swallow this non-zero exit code.

## 3. Deprecation warning (cosmetic)

```
Option --property is deprecated and will be removed in a future version. Use --formatter-property instead.
```

Consumer-side formatter options should migrate to `--formatter-property`, e.g.
`--formatter-property print.partition=true`. Behaviour is unchanged for now.

## 4. Querying broker partition count via JMX (`jmxterm`)

Each broker exposes its hosted partition count as a JMX metric:
`kafka.server:type=ReplicaManager,name=PartitionCount`.

### Enable JMX on the brokers

The lab `docker-compose.yml` does not expose JMX by default. Add per broker
(unique host port each):

```yaml
    ports:
      - "9101:9101"   # JMX (broker-1; use 9102/9103 for brokers 2/3)
    environment:
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
```

Then `docker compose up -d` to recreate.

### Query with jmxterm

```bash
# Download once
wget https://github.com/jiaqi/jmxterm/releases/latest/download/jmxterm-uber.jar

# Interactive session against broker-1
java -jar jmxterm-uber.jar -l localhost:9101
```

Inside the session:

```
$> get -b kafka.server:type=ReplicaManager,name=PartitionCount Value
```

Non-interactive one-liner (handy for loops over all brokers):

```bash
echo "get -b kafka.server:type=ReplicaManager,name=PartitionCount Value" \
	| java -jar jmxterm-uber.jar -l localhost:9101 -n 2>/dev/null
```

Related beans worth checking after the lab:

- `kafka.server:type=ReplicaManager,name=LeaderCount` — partitions this broker leads
- `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` — should be 0
- `kafka.controller:type=KafkaController,name=GlobalPartitionCount` — cluster-wide total (controller only)

Sum of `PartitionCount` across brokers = partitions × replication factor
(e.g. 26 × 3 = 78 replicas for `analytics-events`, plus internal topics).
