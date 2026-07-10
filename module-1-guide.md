# Module 1: Configure Topics for High Availability and Performance

## Student Lab Guide

---

## 📚 Module Overview

In this module, you'll learn to configure Apache Kafka topics for high availability and optimal performance. You'll understand how replication prevents data loss, how to calculate optimal partition counts, and how to apply production-ready configuration patterns.

**Learning Objectives:**

- Configure topics with appropriate replication factors and partition counts
- Understand the relationship between replication, availability, and durability
- Calculate partition counts based on throughput requirements
- Apply production configuration patterns for different use cases

**Estimated Time:** 60 minutes (including videos and hands-on practice)

---

## 🔧 Lab Environment Setup

### Prerequisites

- Docker Desktop installed on your Mac/Windows/Linux machine
- At least 8GB RAM allocated to Docker
- Basic familiarity with command-line terminals

### Step 1: Create Kafka Cluster

Open your terminal and run:

```bash
bash src/mod-01/setup.sh
```

The cluster definition is at [`src/mod-01/docker-compose.yml`](src/mod-01/docker-compose.yml).

### Step 2: Set Up Command Aliases (Optional but Recommended)

Add these to your `~/.zshrc` or `~/.bash_profile`:

```bash
alias kafka-topics='docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'
alias kafka-console-producer='docker exec -i broker-1 kafka-console-producer --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'
alias kafka-console-consumer='docker exec broker-1 kafka-console-consumer --bootstrap-server broker-1:29092,broker-2:29092,broker-3:29092'

# Reload your shell
source ~/.zshrc  # or source ~/.bash_profile
```

### Step 3: Verify Setup

```bash
# Check containers are running
docker-compose ps
# You should see: zookeeper, broker-1, broker-2, broker-3 all "Up"

# Test Kafka commands
kafka-topics --list
# Should return (possibly empty, or system topics like __consumer_offsets)
```

✅ **If these commands work, you're ready to start the labs!**

---

## 🎓 Lab 1: Replication Factors and Data Durability

### Key Concepts

**Replication Factor:** Number of copies Kafka maintains for each partition

- `replication-factor=1`: Single copy (no redundancy) ⚠️ Risk of data loss
- `replication-factor=3`: Three copies (production standard) ✅

**In-Sync Replicas (ISR):** Replicas that are fully caught up with the leader

- Only ISRs can become the new leader if the current leader fails
- `min.insync.replicas=2` means at least 2 replicas must acknowledge writes

**Key Insight:** Higher replication = better availability, but more disk space and network overhead

---

### Exercise 1.1: Create Topics with Different Replication

**Task:** Create two topics to compare replication behavior.

Covered by [`src/mod-01/lab1-replication.sh`](src/mod-01/lab1-replication.sh) (creates both topics, verifies config).

**Verify the configuration:**

```bash
kafka-topics --describe --topic orders-unsafe
kafka-topics --describe --topic orders-safe
```

**Questions to Answer:**

1. For `orders-unsafe`, how many replicas does each partition have?
2. For `orders-safe`, which brokers hold replicas for Partition 0?
3. What does the `Isr` column show for each topic?

---

### Exercise 1.2: Simulate Broker Failure

**Task:** See what happens when a broker fails.

Run [`src/mod-01/lab1-replication.sh`](src/mod-01/lab1-replication.sh) to produce test messages and verify the setup, then simulate a broker failure:

```bash
docker stop broker-X  # replace X with the broker number holding Partition 0
kafka-topics --describe --topic orders-unsafe | grep "Partition: 0"
docker start broker-X
sleep 10
kafka-topics --describe --topic orders-safe
```

**Questions to Answer:**

1. What happened to `orders-unsafe` when the broker failed?
2. Was `orders-safe` still available during the failure?
3. How long did it take for the failed broker to rejoin the ISR?

---

### 💡 Key Takeaway

**Replication-factor=3 with min.insync.replicas=2** is the production standard because:

- Survives single broker failures without data loss
- Maintains availability during failures
- Balances durability with performance

---

## 🎓 Lab 2: Partition Strategy for Parallelism

### Key Concepts

**Partitions = Unit of Parallelism**

- Each partition can only be consumed by ONE consumer in a consumer group
- More partitions = more potential parallelism
- Maximum active consumers = number of partitions

**Partition Calculation Formula:**

```
Partitions = max(
  Target Throughput / Producer Capacity per Partition,
  Target Throughput / Consumer Capacity per Partition
)
× (1 + Safety Margin)
```

**Key Insight:** Size for the bottleneck (usually consumers) and add 20-30% headroom

---

### Exercise 2.1: Calculate Partition Count

**Scenario:** You need to build a real-time analytics topic with these requirements:

- Target throughput: 500 MB/sec
- Producer capacity: 50 MB/sec per partition (tested)
- Consumer capacity: 25 MB/sec per partition (tested)

**Your Task:** Calculate the optimal partition count.

```
Step 1: Producer needs = 500 MB/sec ÷ 50 MB/sec = ___ partitions

Step 2: Consumer needs = 500 MB/sec ÷ 25 MB/sec = ___ partitions

Step 3: Take the MAX = ___ partitions

Step 4: Add 30% headroom = ___ × 1.3 = ___ partitions (round up)
```

**Answer:** **\_** partitions

---

### Exercise 2.2: Create a Properly Partitioned Topic

Covered by [`src/mod-01/lab2-partitions.sh`](src/mod-01/lab2-partitions.sh) (creates topic, verifies distribution, produces 10 messages, consumes with partition visibility):

```bash
bash src/mod-01/lab2-partitions.sh YOUR_CALCULATED_NUMBER
```

**Questions to Answer:**

1. How are the partitions distributed across your 3 brokers?
2. Why is this distribution beneficial?
3. What's the maximum number of consumers that can read from this topic in parallel?

---

### Exercise 2.3: Observe Partition Assignment

Covered by sections "Produce 10 test messages" and "Consume with partition visibility" in [`src/mod-01/lab2-partitions.sh`](src/mod-01/lab2-partitions.sh).

**Questions to Answer:**

1. Are messages evenly distributed across partitions?
2. What determines which partition a message goes to?
3. How would you ensure messages for a specific customer always go to the same partition?

---

### 💡 Key Takeaway

**Always size partitions for consumer capacity** (the bottleneck), add headroom, but stay under 4,000 partitions per broker.

---

## 🎓 Lab 3: Topic Configuration Best Practices

### Key Concepts

**Configuration Dimensions:**

- **Retention:** How long data lives (`retention.ms`, `retention.bytes`)
- **Segments:** File size for deletion/compaction (`segment.bytes`, `segment.ms`)
- **Cleanup:** Delete old data vs. compact to keep latest (`cleanup.policy`)
- **Compression:** Reduce disk/network usage (`compression.type`: lz4, zstd, gzip)

**Three Production Patterns:**

1. **Time-series/Analytics:** Delete old data, moderate retention
2. **State/Compaction:** Keep latest value per key forever
3. **Audit/Compliance:** Long retention, strict durability

---

### Exercise 3.1: Create Three Configuration Patterns

**Pattern 1: Analytics Topic (Time-Series Data)**

Covered by section "Analytics topic" in [`src/mod-01/lab3-config-patterns.sh`](src/mod-01/lab3-config-patterns.sh).

**Use Case:** Clickstream data, logs, metrics
**Retention:** 30 days (2,592,000,000 ms)
**Why:** Data becomes less valuable over time, delete to save space

---

**Pattern 2: State Topic (Compaction)**

Covered by section "State topic" in [`src/mod-01/lab3-config-patterns.sh`](src/mod-01/lab3-config-patterns.sh).

**Use Case:** User profiles, inventory state, configuration
**Compaction:** Keeps only the latest update per key
**Why:** You only care about current state, not history

---

**Pattern 3: Audit Log (Compliance)**

Covered by section "Audit log" in [`src/mod-01/lab3-config-patterns.sh`](src/mod-01/lab3-config-patterns.sh).

**Use Case:** Financial transactions, compliance logs, legal records
**Retention:** 90 days
**Durability:** `min.insync.replicas=3` (all replicas must ack)
**Why:** Regulatory requirements, zero tolerance for data loss

---

### Exercise 3.2: Verify and Compare Configurations

Covered by section "Verify each topic's configuration" in [`src/mod-01/lab3-config-patterns.sh`](src/mod-01/lab3-config-patterns.sh).

**Questions to Answer:**

1. Which topic has the longest retention period?
2. Which topic will take up the least disk space long-term? Why?
3. Which topic has the strictest durability guarantees?
4. Why use different compression types for different use cases?

---

### Exercise 3.3: Test Compaction Behavior (Advanced)

Covered by section "Compaction behaviour test" in [`src/mod-01/lab3-config-patterns.sh`](src/mod-01/lab3-config-patterns.sh).

---

### 💡 Key Takeaway

**Match configuration to use case:**

- Ephemeral data → Short retention, delete cleanup
- State data → Compaction, high compression
- Compliance → Long retention, max durability

---

## 🧹 Lab Cleanup

After completing all exercises:

```bash
bash src/mod-01/cleanup.sh         # stop containers, keep data
bash src/mod-01/cleanup.sh -v      # also remove all data (fresh start)
```

---

## 📝 Module 1 Quiz

Test your understanding:

1. **If you have a 5-broker cluster and set `replication-factor=3`, how many broker failures can you survive without data loss?**
   - A) 1
   - B) 2
   - C) 3
   - D) 4

2. **You have 20 partitions. What's the maximum number of active consumers in a single consumer group?**
   - A) 10
   - B) 20
   - C) 40
   - D) Unlimited

3. **Which configuration keeps only the latest value per key?**
   - A) `cleanup.policy=delete`
   - B) `cleanup.policy=compact`
   - C) `retention.ms=0`
   - D) `min.insync.replicas=1`

4. **For production systems, the recommended replication configuration is:**
   - A) `replication-factor=1`
   - B) `replication-factor=2, min.insync.replicas=1`
   - C) `replication-factor=3, min.insync.replicas=2`
   - D) `replication-factor=5, min.insync.replicas=5`

5. **When calculating partitions, you should size for:**
   - A) Producer capacity only
   - B) Consumer capacity only (the bottleneck)
   - C) Number of brokers × 10
   - D) Always use exactly 12 partitions

**Answers:** 1-B, 2-B, 3-B, 4-C, 5-B

---

## 🔗 Additional Resources

### Official Documentation

- [Kafka Topic Configuration](https://kafka.apache.org/documentation/#topicconfigs)
- [Replication](https://kafka.apache.org/documentation/#replication)
- [Partitioning](https://kafka.apache.org/documentation/#intro_topics)

### Real-World Examples

- [Netflix Kafka Usage](https://netflixtechblog.com/kafka-inside-keystone-pipeline-dd5aeabaf6bb)
- [LinkedIn Kafka at Scale](https://engineering.linkedin.com/kafka/running-kafka-scale)
- [Uber's Kafka Infrastructure](https://www.uber.com/blog/kafka/)

---

## ✅ Module 1 Completion Checklist

- [ ] Set up 3-broker Kafka cluster
- [ ] Completed Lab 1: Replication and Durability
- [ ] Completed Lab 2: Partition Strategy
- [ ] Completed Lab 3: Configuration Patterns
- [ ] Passed Module 1 Quiz (4/5 correct minimum)
- [ ] Ready to move to Module 2: Monitor Performance

---

**Congratulations on completing Module 1! 🎉**

You now understand how to configure Kafka topics for production use. In Module 2, you'll learn to monitor consumer lag and identify performance bottlenecks.
