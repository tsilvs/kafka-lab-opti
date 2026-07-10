# Module 2: Monitor Performance and Identify Bottlenecks
## Student Lab Guide

---

## 📚 Module Overview

In this module, you'll learn to monitor Kafka performance by tracking consumer lag, analyzing consumer group health, and identifying bottlenecks before they impact production systems.

**Learning Objectives:**
- Monitor and interpret consumer lag metrics
- Calculate optimal consumer group sizes based on partition count
- Diagnose performance bottlenecks using broker health indicators
- Set up alerting for critical metrics

**Estimated Time:** 60 minutes (including videos and hands-on practice)

---

## 🔧 Lab Environment Setup

### Prerequisites
- Completed Module 1 (Docker cluster should already be set up)
- Kafka cluster running (`docker-compose ps` shows all containers "Up")

### Verify Your Environment

```bash
cd ~/kafka-labs
docker-compose up -d
sleep 30
kafka-topics --list
```

If you need to set up from scratch, refer to Module 1 Student Guide.

---

## 🎓 Lab 1: Understanding Consumer Lag

### Key Concepts

**Consumer Lag:** The difference between what's been produced and what's been consumed
- **Formula:** `LAG = LOG-END-OFFSET - CURRENT-OFFSET`
- **Healthy:** Lag < 1 second for real-time systems
- **Warning:** Lag growing consistently
- **Critical:** Lag > 30 seconds or continuing to grow

**Why Lag Matters:**
- **Walmart Example:** 30-second inventory lag = customers buy unavailable items
- **Uber Example:** Surge pricing lag = wrong prices shown
- **Netflix Example:** Recommendation lag = stale suggestions

**Key Insight:** Consumer lag is the **#1 indicator** of system health in streaming architectures.

---

### Exercise 1.1: Create a Topic with Lag

**Task:** Set up a realistic scenario where consumers fall behind.

Covered by [`src/mod-02/lab1-consumer-lag.sh`](src/mod-02/lab1-consumer-lag.sh) (creates topic, produces 5K messages, starts slow consumer, keeps producing to build lag).

---

### Exercise 1.2: Monitor Consumer Lag

**Task:** Check the lag that's building up.

```bash
# Wait 5 seconds for lag to build
sleep 5

# Check consumer group status
kafka-consumer-groups --describe --group order-processor
```

**Expected Output:**
```
GROUP           TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-processor order-events  0          423             658             235
order-processor order-events  1          401             612             211
order-processor order-events  2          389             645             256
...
```

**Questions to Answer:**
1. What is the total lag across all partitions?
2. Is the lag the same for every partition? Why or why not?
3. How many consumers are in the group?

---

### Exercise 1.3: Calculate Total Lag

**Task:** Use command-line tools to sum lag across all partitions.

```bash
# Calculate total lag
kafka-consumer-groups --describe --group order-processor | \
  awk 'NR>1 {sum+=$5} END {print "Total Lag:", sum, "messages"}'
```

**Now check again 10 seconds later:**
```bash
sleep 10
kafka-consumer-groups --describe --group order-processor | \
  awk 'NR>1 {sum+=$5} END {print "Total Lag:", sum, "messages"}'
```

**Questions to Answer:**
1. Did the lag increase or decrease?
2. What does this tell you about the consumer's health?
3. At what point would you trigger an alert?

---

### Exercise 1.4: Cleanup

```bash
# Stop the background consumer
kill $CONSUMER_PID 2>/dev/null

# Stop background producer if still running
pkill -f kafka-console-producer
```

---

### 💡 Key Takeaway

**Consumer lag thresholds:**
- **< 1 sec:** Healthy real-time system
- **1-5 sec:** Acceptable for analytics
- **5-30 sec:** Warning - investigate
- **> 30 sec or growing:** Critical - scale immediately

---

## 🎓 Lab 2: Consumer Group Sizing and Parallelism

### Key Concepts

**The Partition-Consumer Rule:**
- Each partition can be consumed by **exactly ONE** consumer in a group
- Maximum parallelism = number of partitions
- Undersized group → consumers overloaded, lag builds
- Oversized group → idle consumers, wasted resources
- Perfect size = **one consumer per partition**

**Sizing Formula:**
```
Message Rate: 800 msg/sec
Consumer Capacity: 80 msg/sec per consumer
Minimum Consumers: 800 ÷ 80 = 10 consumers
Optimal Consumers: Number of partitions (if ≥ 10)
```

**Key Insight:** You can't have more **active** consumers than partitions.

---

### Exercise 2.1: Create Undersized Consumer Group

**Task:** Demonstrate what happens when you have too few consumers.

Covered by [`src/mod-02/lab2-undersized-group.sh`](src/mod-02/lab2-undersized-group.sh) (creates 12-partition topic, produces 10K messages, starts 2 consumers, shows member assignment).

**Expected Output:**
```
GROUP           CONSUMER-ID        HOST        #PARTITIONS
inventory-sync  consumer-1-xxx     /172.x.x.x  6
inventory-sync  consumer-2-xxx     /172.x.x.x  6
```

**Questions to Answer:**
1. How many partitions is each consumer handling?
2. Is this balanced? What's the problem?
3. If each consumer can handle 100 msg/sec and you're getting 800 msg/sec, what happens?

---

### Exercise 2.2: Calculate Optimal Group Size

**Scenario:** Your topic has these characteristics:
- 12 partitions
- 800 messages/sec inbound rate
- Each consumer can process 80 messages/sec

**Your Task:** Calculate the optimal consumer group size.

```
Step 1: Minimum consumers needed for throughput
        = 800 msg/sec ÷ 80 msg/sec per consumer
        = ___ consumers

Step 2: Maximum consumers possible
        = Number of partitions
        = ___ consumers

Step 3: Optimal size
        = min(capacity needed, partitions available)
        = ___ consumers

Answer: You need ___ consumers for optimal performance.
```

---

### Exercise 2.3: Observe Oversized Consumer Group

**Task:** See what happens with too many consumers.

Covered by [`src/mod-02/lab2-oversized-group.sh`](src/mod-02/lab2-oversized-group.sh) (stops previous consumers, starts 20 consumers for 12 partitions, shows idle members).

**Expected Output:**
```
GROUP           CONSUMER-ID        #PARTITIONS
test-oversized  consumer-1-xxx     1
test-oversized  consumer-2-xxx     1
...
test-oversized  consumer-12-xxx    1
test-oversized  consumer-13-xxx    0  ← IDLE
test-oversized  consumer-14-xxx    0  ← IDLE
...
test-oversized  consumer-20-xxx    0  ← IDLE
```

**Questions to Answer:**
1. How many consumers are actively consuming?
2. How many consumers are idle?
3. What's the waste in running 20 consumers for 12 partitions?

---

### Exercise 2.4: Cleanup

```bash
# Stop all consumers
pkill -f kafka-console-consumer

# Delete consumer groups
kafka-consumer-groups --delete --group inventory-sync 2>/dev/null || true
kafka-consumer-groups --delete --group test-oversized 2>/dev/null || true
```

---

### 💡 Key Takeaway

**Consumer Group Sizing Rules:**
- **Undersized:** Lag builds, system falls behind
- **Perfect size:** One consumer per partition
- **Oversized:** Wasted resources, some consumers idle
- **Rule of thumb:** Start with partition count, scale up partitions if needed

---

## 🎓 Lab 3: Broker Health Monitoring

### Key Concepts

**Critical Broker Metrics:**

1. **Under-Replicated Partitions**
   - Alert: Any value > 0 for more than 5 minutes
   - Means: Some partitions don't have full replicas in sync
   - Risk: If another broker fails, data loss possible

2. **Disk Usage**
   - Warning: > 75%
   - Critical: > 85%
   - Action: Add disk space or reduce retention

3. **CPU Usage**
   - Warning: > 75% sustained
   - Critical: > 90%
   - Action: Scale horizontally or tune configs

4. **Request Queue Size**
   - Warning: > 100 requests queued
   - Means: Brokers can't keep up with requests
   - Action: Increase network threads or add brokers

**Key Insight:** Under-replicated partitions is the **most critical** metric - indicates imminent data loss risk.

---

### Exercise 3.1: Monitor Healthy Cluster

**Task:** Check your cluster's baseline health.

```bash
# Check for under-replicated partitions
kafka-topics --describe --under-replicated-partitions
```

**Expected Output:** (empty if healthy)

If you see output, something is wrong! All partitions should be fully replicated.

```bash
# List all topics and check their health
kafka-topics --list | while read topic; do
  echo "=== $topic ==="
  kafka-topics --describe --topic $topic | grep -E "Leader|Isr"
done
```

**Questions to Answer:**
1. Do all partitions have the expected number of replicas?
2. Are all replicas in the ISR list?
3. Are leaders evenly distributed across brokers?

---

### Exercise 3.2: Simulate Broker Failure

**Task:** See how the cluster responds to a broker failure.

Covered by [`src/mod-02/lab3-simulate-failure.sh`](src/mod-02/lab3-simulate-failure.sh) (creates test topic, produces data, stops broker-2, shows under-replicated partitions).

**Expected Output:**
You should see partitions that had Broker 2 as a replica showing as under-replicated.

**Questions to Answer:**
1. How many partitions are under-replicated?
2. Can you still produce/consume messages?
3. What's the risk if another broker fails now?

---

### Exercise 3.3: Observe Self-Healing

**Task:** Watch the cluster automatically recover.

Covered by [`src/mod-02/lab3-observe-recovery.sh`](src/mod-02/lab3-observe-recovery.sh) (restarts broker-2, verifies under-replicated partitions are gone, checks ISR health).

**Questions to Answer:**
1. How long did self-healing take?
2. Are all partitions back in sync?
3. Did you lose any data during the failure?

---

### Exercise 3.4: Set Up Monitoring Alerts

**Task:** Define alert thresholds for your system.

Create a file called `kafka-alerts.yml` using the rules at [`src/mod-02/kafka-alerts.yml`](src/mod-02/kafka-alerts.yml).

**Questions to Answer:**
1. What's your threshold for alerting on consumer lag?
2. How long should you wait before alerting on under-replicated partitions?
3. What's the difference between a "warning" and "critical" alert?

---

### 💡 Key Takeaway

**Monitoring Priority:**
1. **Under-replicated partitions:** Most critical - data loss risk
2. **Consumer lag:** Application health indicator
3. **Disk usage:** Operational sustainability
4. **CPU/Memory:** Capacity planning

---

## 🧹 Lab Cleanup

After completing all exercises:

```bash
bash src/mod-02/cleanup.sh
```

---

## 📝 Module 2 Quiz

Test your understanding:

1. **A consumer group has 50 messages of lag. What does this mean?**
   - A) The consumer is 50 seconds behind
   - B) The consumer has 50 messages left to process
   - C) There are 50 consumers in the group
   - D) The producer sent 50 messages

2. **You have 20 partitions. What's the maximum number of active consumers in one group?**
   - A) 10
   - B) 20
   - C) 40
   - D) Unlimited

3. **Your consumer lag is growing from 100 → 500 → 1200 messages. What should you do?**
   - A) Restart the brokers
   - B) Add more partitions
   - C) Scale up the consumer group
   - D) Reduce producer throughput

4. **Under-replicated partitions mean:**
   - A) Partitions with no leader
   - B) Partitions missing full replica count in ISR
   - C) Partitions with no consumers
   - D) Partitions that are too small

5. **Best practice for monitoring consumer lag threshold:**
   - A) Alert at 100,000 messages
   - B) Alert at 50% of total topic size
   - C) Alert when lag grows consistently over 5 minutes
   - D) Never alert on lag

**Answers:** 1-B, 2-B, 3-C, 4-B, 5-C

---

## 🔗 Additional Resources

### Official Documentation
- [Consumer Groups](https://kafka.apache.org/documentation/#intro_consumers)
- [Replication](https://kafka.apache.org/documentation/#replication)
- [Monitoring](https://kafka.apache.org/documentation/#monitoring)

### Real-World Case Studies
- [Walmart Inventory Lag Issues](https://www.confluent.io/blog/walmart-real-time-inventory-management-using-kafka/)
- [Uber Real-Time Data Platform](https://www.uber.com/blog/kafka/)
- [Confluent Consumer Lag Monitoring](https://docs.confluent.io/platform/current/monitor/monitor-consumer-lag.html)

### Tools
- [Kafka Manager](https://github.com/yahoo/CMAK) - UI for managing clusters
- [Burrow](https://github.com/linkedin/Burrow) - LinkedIn's lag monitoring
- [Prometheus + Grafana](https://prometheus.io/) - Metrics and dashboards

---

## ✅ Module 2 Completion Checklist

- [ ] Completed Lab 1: Understanding Consumer Lag
- [ ] Completed Lab 2: Consumer Group Sizing
- [ ] Completed Lab 3: Broker Health Monitoring
- [ ] Created alert thresholds document
- [ ] Passed Module 2 Quiz (4/5 correct minimum)
- [ ] Ready to move to Module 3: Performance Optimization

---

**Congratulations on completing Module 2! 🎉**

You now know how to monitor Kafka systems and identify bottlenecks before they impact production. In Module 3, you'll learn to optimize producer and consumer performance for maximum throughput.
