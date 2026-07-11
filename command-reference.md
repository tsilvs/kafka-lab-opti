# Kafka Command Reference
**Complete command-line reference for every lab exercise**

---

## 📖 Overview

This guide contains every command used across the lab modules, organized by module and topic. Use this as a quick reference while working through the labs.

**Format:** Command → Brief explanation → Expected result

---

## 🔧 Initial Setup (One-Time)

Before running any commands below, set up your environment:

```bash
# Create working directory
mkdir -p ~/kafka-labs
cd ~/kafka-labs

# Start the Kafka cluster (docker-compose.yml from Module 1 guide)
docker-compose up -d
sleep 60

# Set up aliases for easier command usage
alias kafka-topics='docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'
alias kafka-console-producer='docker exec -i broker-1 kafka-console-producer --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'
alias kafka-console-consumer='docker exec broker-1 kafka-console-consumer --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'
alias kafka-consumer-groups='docker exec broker-1 kafka-consumer-groups --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'
alias kafka-producer-perf='docker exec broker-1 kafka-producer-perf-test --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'
```

---

## 📦 Module 1: Configure Topics for High Availability

### Replication Factors and Data Durability

#### Check Cluster Status
```bash
docker-compose ps
```
**Purpose:** Verify all 3 brokers are running  
**Expected:** All containers show status "Up"

---

#### Create Unsafe Topic (No Redundancy)
```bash
kafka-topics --create \
  --topic orders-unsafe \
  --partitions 3 \
  --replication-factor 1
```
**Purpose:** Demonstrate a single point of failure configuration  
**Result:** Topic created with only 1 copy of each partition

---

#### Create Safe Topic (With Redundancy)
```bash
kafka-topics --create \
  --topic orders-safe \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```
**Purpose:** Production-ready configuration with fault tolerance  
**Result:** Topic created with 3 copies, requiring 2 for writes

---

#### Inspect Topic Configuration
```bash
kafka-topics --describe --topic orders-unsafe
kafka-topics --describe --topic orders-safe
```
**Purpose:** View partition distribution and replica status  
**Key fields:** Replicas (which brokers), Isr (in-sync replicas), Leader

---

#### Produce Test Messages
```bash
echo -e "order-1001\norder-1002\norder-1003" | \
  kafka-console-producer --topic orders-unsafe
```
**Purpose:** Add sample data to demonstrate failure scenarios  
**Result:** 3 messages sent to the topic

---

#### Simulate Broker Failure
```bash
# Check which broker has partition 0
kafka-topics --describe --topic orders-unsafe | grep "Partition: 0"

# Stop that broker (example: broker-2)
docker stop broker-2
```
**Purpose:** Demonstrate what happens when a broker fails  
**Result:** Partitions on that broker become unavailable

---

#### Try to Read from Failed Topic
```bash
kafka-console-consumer --topic orders-unsafe \
  --from-beginning --timeout-ms 5000
```
**Purpose:** Show data loss with replication-factor=1  
**Expected:** Errors or missing messages for partitions on failed broker

---

#### Check Safe Topic Still Works
```bash
kafka-topics --describe --topic orders-safe
```
**Purpose:** Verify redundancy prevents data loss  
**Result:** ISR list shrinks but topic remains available

---

#### Produce to Safe Topic During Failure
```bash
echo -e "order-2001\norder-2002" | \
  kafka-console-producer --topic orders-safe
```
**Purpose:** Prove writes still work with one broker down  
**Result:** Messages accepted successfully

---

#### Restore Failed Broker
```bash
docker start broker-2
sleep 10

kafka-topics --describe --topic orders-safe | grep "Partition: 0"
```
**Purpose:** Show automatic recovery and ISR rejoin  
**Result:** Broker-2 rejoins ISR list automatically

---

### Partition Strategy for Parallelism

#### Calculate Optimal Partitions
```
# This is shown on screen, not a command:
Producers need: 500 MB/sec ÷ 50 MB/sec = 10 partitions
Consumers need: 500 MB/sec ÷ 25 MB/sec = 20 partitions
Take MAX = 20 partitions
Add 30% headroom = 26 partitions
```
**Purpose:** Demonstrate partition sizing methodology  
**Formula:** max(throughput/producer capacity, throughput/consumer capacity) × 1.3

---

#### Create Properly Partitioned Topic
```bash
kafka-topics --create \
  --topic high-throughput-events \
  --partitions 26 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```
**Purpose:** Apply calculated partition count  
**Result:** 26 partitions distributed across 3 brokers

---

#### View Partition Distribution
```bash
kafka-topics --describe --topic high-throughput-events
```
**Purpose:** Verify even distribution across brokers  
**Look for:** Round-robin assignment of leaders

---

#### Test Message Distribution
```bash
# Produce 10 test messages
for i in {1..10}; do echo "message-$i"; done | \
  kafka-console-producer --topic high-throughput-events

# Consume with partition visibility
kafka-console-consumer --topic high-throughput-events \
  --from-beginning \
  --property print.partition=true \
  --timeout-ms 3000
```
**Purpose:** Show messages distributed across partitions  
**Output format:** Partition:0 message-5

---

### Topic Configuration Best Practices

#### Pattern 1: Analytics Topic (Time-Series)
```bash
kafka-topics --create \
  --topic clickstream-events \
  --partitions 20 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000 \
  --config segment.bytes=1073741824 \
  --config compression.type=lz4 \
  --config cleanup.policy=delete
```
**Purpose:** Configure for analytics use case  
**Key settings:** 30-day retention, 1GB segments, lz4 compression, delete old data

---

#### Pattern 2: State Topic (Compaction)
```bash
kafka-topics --create \
  --topic user-profiles \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=compact \
  --config segment.bytes=104857600 \
  --config compression.type=zstd \
  --config min.compaction.lag.ms=3600000
```
**Purpose:** Configure for state/snapshot use case  
**Key settings:** Compaction (keeps latest per key), 100MB segments, 1-hour lag

---

#### Pattern 3: Audit Log (Compliance)
```bash
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
```
**Purpose:** Configure for compliance/audit use case  
**Key settings:** 90-day retention, min.insync.replicas=3 (all must ack), daily segments

---

#### Verify Configuration
```bash
kafka-configs --describe \
  --entity-type topics \
  --entity-name user-profiles
```
**Purpose:** View all configuration for a topic  
**Shows:** All non-default settings

---

## 📊 Module 2: Monitor Performance and Identify Bottlenecks

### Understanding Consumer Lag

#### Check Consumer Group Status
```bash
kafka-consumer-groups --describe \
  --group storefront-inventory-sync
```
**Purpose:** View current lag for each partition  
**Key columns:** CURRENT-OFFSET, LOG-END-OFFSET, LAG

---

#### Calculate Total Lag
```bash
kafka-consumer-groups --describe \
  --group storefront-inventory-sync | \
  awk 'NR>1 {sum+=$5} END {print "Total Lag:", sum}'
```
**Purpose:** Sum lag across all partitions  
**Result:** Single number showing total messages behind

---

### Consumer Group Sizing

#### Inspect Consumer Group Members
```bash
kafka-consumer-groups --describe \
  --group storefront-inventory-sync \
  --members --verbose
```
**Purpose:** See partition assignment per consumer  
**Shows:** How many partitions each consumer is handling

---

#### Show Properly Sized Group (After Scaling)
```bash
kafka-consumer-groups --describe \
  --group inventory-sync \
  --members
```
**Purpose:** Demonstrate optimal 1:1 consumer-to-partition ratio  
**Expected:** 12 consumers, each assigned 1 partition

---

#### Show Oversized Group
```bash
kafka-consumer-groups --describe \
  --group analytics-processors \
  --members
```
**Purpose:** Show wasted resources with too many consumers  
**Expected:** Some consumers with #PARTITIONS = 0 (idle)

---

### Broker Health Monitoring

#### Check for Under-Replicated Partitions (Healthy)
```bash
kafka-topics --describe --under-replicated-partitions
```
**Purpose:** Find partitions missing replicas  
**Expected (healthy):** Empty output

---

#### Simulate Broker Failure
```bash
docker stop broker-2
sleep 5
```
**Purpose:** Create under-replicated partitions  
**Result:** Some partitions will lose a replica

---

#### Check Under-Replicated Partitions (Unhealthy)
```bash
kafka-topics --describe --under-replicated-partitions
```
**Purpose:** See which partitions are at risk  
**Expected:** Lists partitions that had broker-2 as a replica

---

#### Restore Broker
```bash
docker start broker-2
sleep 10

kafka-topics --describe --under-replicated-partitions
```
**Purpose:** Verify automatic healing  
**Expected:** Empty output (all partitions back in sync)

---

## ⚡ Module 3: Optimize Producer and Consumer Performance

### Producer Batching and Compression

#### Baseline Performance Test (Defaults)
```bash
kafka-producer-perf-test \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    batch.size=16384 \
    linger.ms=0 \
    compression.type=none
```
**Purpose:** Establish baseline throughput  
**Key metrics:** Records/sec, avg latency, P95 latency

---

#### Optimized with Batching
```bash
kafka-producer-perf-test \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    batch.size=131072 \
    linger.ms=10 \
    compression.type=none
```
**Purpose:** Show throughput improvement with larger batches  
**Expected:** 3x+ improvement in records/sec

---

#### Optimized with Compression
```bash
kafka-producer-perf-test \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    batch.size=131072 \
    linger.ms=10 \
    compression.type=lz4
```
**Purpose:** Show further improvement with lz4 compression  
**Expected:** 4x+ improvement vs baseline

---

### Consumer Fetch Optimization

#### Consume with Real-Time Settings
```bash
kafka-console-consumer \
  --topic consumer-test \
  --group realtime-consumer \
  --property fetch.min.bytes=1 \
  --property fetch.max.wait.ms=100 \
  --property max.poll.records=100 \
  --from-beginning \
  --timeout-ms 5000
```
**Purpose:** Demonstrate low-latency configuration  
**Use case:** Fraud detection, alerting

---

#### Consume with High-Throughput Settings
```bash
kafka-console-consumer \
  --topic consumer-test \
  --group batch-consumer \
  --property fetch.min.bytes=102400 \
  --property fetch.max.wait.ms=500 \
  --property max.poll.records=1000 \
  --from-beginning \
  --timeout-ms 5000
```
**Purpose:** Demonstrate batch processing configuration  
**Use case:** ETL, analytics, data warehouse loading

---

### Broker-Side Performance Tuning

#### View Current Broker Configuration
```bash
docker exec broker-1 kafka-configs \
  --bootstrap-server broker-1:29092 \
  --entity-type brokers \
  --entity-name 1 \
  --describe --all | grep -E "num.network.threads|num.io.threads|socket"
```
**Purpose:** See current threading and buffer settings  
**Shows:** Network threads, I/O threads, socket buffer sizes

---

#### Performance Test with Current Settings
```bash
kafka-producer-perf-test \
  --topic perf-test \
  --num-records 5000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    batch.size=131072 \
    linger.ms=10 \
    compression.type=lz4
```
**Purpose:** Baseline with default broker settings  
**Note:** Broker tuning requires restart (out of scope for this lab)

---

## 🧹 Cleanup Commands

#### Stop All Background Processes
```bash
pkill -f kafka-console-consumer
pkill -f kafka-console-producer
```

---

#### Delete Test Topics
```bash
kafka-topics --delete --topic orders-unsafe
kafka-topics --delete --topic orders-safe
kafka-topics --delete --topic high-throughput-events
kafka-topics --delete --topic clickstream-events
kafka-topics --delete --topic user-profiles
kafka-topics --delete --topic audit-log
kafka-topics --delete --topic perf-test
kafka-topics --delete --topic consumer-test
```

---

#### Delete Consumer Groups
```bash
kafka-consumer-groups --delete --group storefront-inventory-sync
kafka-consumer-groups --delete --group inventory-sync
kafka-consumer-groups --delete --group analytics-processors
kafka-consumer-groups --delete --group realtime-consumer
kafka-consumer-groups --delete --group batch-consumer
```

---

#### Restart All Brokers
```bash
docker restart broker-1 broker-2 broker-3
sleep 15
```

---

#### Complete Cluster Reset
```bash
docker-compose down -v
docker-compose up -d
sleep 60
```

---

## 📊 Command Categories

### Topic Management
- `kafka-topics --create` - Create new topic
- `kafka-topics --describe` - View topic details
- `kafka-topics --list` - List all topics
- `kafka-topics --delete` - Remove topic
- `kafka-configs --describe` - View topic configuration

### Producing Messages
- `kafka-console-producer` - Send messages interactively
- `echo | kafka-console-producer` - Pipe messages in
- `kafka-producer-perf-test` - Performance testing

### Consuming Messages
- `kafka-console-consumer` - Read messages
- `kafka-console-consumer --from-beginning` - Read all messages
- `kafka-console-consumer --property print.partition=true` - Show partitions

### Consumer Groups
- `kafka-consumer-groups --describe` - View lag and offsets
- `kafka-consumer-groups --describe --members` - View assignments
- `kafka-consumer-groups --delete` - Remove consumer group

### Cluster Management
- `docker-compose ps` - Check container status
- `docker stop/start broker-X` - Control brokers
- `docker-compose down -v` - Complete teardown

---

## 💡 Common Patterns

### Create Production-Ready Topic
```bash
kafka-topics --create \
  --topic MY-TOPIC \
  --partitions N \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

### Produce with Key
```bash
echo "key1:value1" | kafka-console-producer \
  --topic MY-TOPIC \
  --property "parse.key=true" \
  --property "key.separator=:"
```

### Consume Specific Partition
```bash
kafka-console-consumer \
  --topic MY-TOPIC \
  --partition 0 \
  --from-beginning \
  --timeout-ms 5000
```

### Monitor Consumer Lag
```bash
watch -n 2 "kafka-consumer-groups --describe --group MY-GROUP"
```

---

## 🔍 Quick Reference by Use Case

### "I want to test replication"
1. Create topic with `--replication-factor 3`
2. Produce messages
3. `docker stop broker-2`
4. Try to consume
5. `docker start broker-2`

### "I want to measure throughput"
1. Create topic
2. Run `kafka-producer-perf-test`
3. Vary `batch.size`, `linger.ms`, `compression.type`
4. Compare results

### "I want to check consumer lag"
1. `kafka-consumer-groups --describe --group GROUP-NAME`
2. Look at LAG column
3. Sum with awk if needed

### "I want to optimize partitions"
1. Calculate: max(producer needs, consumer needs)
2. Add 30% headroom
3. Create topic with calculated partition count

---

## 📝 Notes

- All commands assume aliases are set up (see Initial Setup section)
- Replace `broker-2` with actual broker ID when simulating failures
- Performance test numbers vary by hardware
- Some commands produce a lot of output - use `| head` or `| grep` to filter

---

## 🔗 Related Resources

- [Full Lab Guides](README.md) - Detailed exercises with explanations
- [Module 1 Guide](module-1-guide.md) - Topic configuration labs
- [Module 2 Guide](module-2-guide.md) - Monitoring labs
- [Module 3 Guide](module-3-guide.md) - Optimization labs

---

**Last Updated:** November 2024  
**Total Commands:** 50+ across 9 sections
