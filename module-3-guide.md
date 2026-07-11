# Module 3: Optimize Producer and Consumer Performance
## Student Lab Guide

---

## 📚 Module Overview

In this module, you'll learn to optimize Kafka performance through producer batching, compression, consumer fetch tuning, and broker-side configuration. You'll use performance testing tools to measure improvements and understand trade-offs between throughput and latency.

**Learning Objectives:**
- Optimize producer configurations for maximum throughput while meeting latency SLAs
- Compare compression algorithms and select the best for your use case
- Tune consumer fetch settings to prevent timeouts and rebalancing
- Understand broker-side threading and buffer configurations

**Estimated Time:** 90 minutes of hands-on practice

---

## 🔧 Lab Environment Setup

### Prerequisites
- Completed Modules 1 & 2
- Kafka cluster running (`docker-compose ps` shows all containers "Up")
- Familiarity with `kafka-producer-perf-test` tool

### Verify Your Environment

```bash
cd ~/kafka-labs
docker-compose up -d
sleep 15
docker exec broker-1 kafka-producer-perf-test --help | head -5
```

---

## 🎓 Lab 1: Producer Batching and Compression

### Key Concepts

**Producer Batching:**
- `batch.size`: Maximum bytes to batch before sending (default: 16KB)
- `linger.ms`: How long to wait for batch to fill (default: 0ms)
- **Trade-off:** Larger batches = higher throughput but higher latency

**Compression:**
- Reduces network bandwidth and disk usage
- Types: `none`, `gzip`, `snappy`, `lz4`, `zstd`
- **Trade-off:** Better compression = more CPU usage

**Performance Metrics:**
- **Throughput:** Records per second or MB per second
- **Latency:** Time from produce to acknowledgment
  - P50: Median latency
  - P95: 95th percentile (only 5% slower)
  - P99: 99th percentile (worst case for most requests)

**Key Insight:** Default configs prioritize latency. Production systems usually need to optimize for throughput.

---

### Exercise 1.1: Baseline Performance Test

**Task:** Measure default producer performance.

Covered by section "Baseline" in [`src/mod-03/lab1-producer-perf.sh`](src/mod-03/lab1-producer-perf.sh).

**Expected Output:**
```
1000000 records sent, 45123.45 records/sec (44.07 MB/sec),
12.34 ms avg latency, 28.56 ms max latency,
10 ms 50th, 25 ms 95th, 31 ms 99th, 35 ms 99.9th.
```

**Record Your Results:**
```
Baseline Performance:
Throughput: _____ records/sec
Average Latency: _____ ms
P95 Latency: _____ ms
P99 Latency: _____ ms
```

---

### Exercise 1.2: Optimize Batch Size

**Task:** Test larger batch sizes to improve throughput.

Covered by section "Optimize Batch Size" in [`src/mod-03/lab1-producer-perf.sh`](src/mod-03/lab1-producer-perf.sh).

**Record Your Results:**
```
Batch Size Comparison:
16KB  (baseline): _____ rec/sec, _____ ms P95
32KB: _____ rec/sec (+___%), _____ ms P95
64KB: _____ rec/sec (+___%), _____ ms P95
128KB: _____ rec/sec (+___%), _____ ms P95
```

**Questions to Answer:**
1. At what batch size did you see diminishing returns?
2. Did latency increase significantly with larger batches?
3. What batch size gives the best throughput/latency balance?

---

### Exercise 1.3: Add Linger Time

**Task:** Allow batches to fill by waiting a few milliseconds.

Covered by section "Add Linger Time" in [`src/mod-03/lab1-producer-perf.sh`](src/mod-03/lab1-producer-perf.sh).

**Questions to Answer:**
1. Did throughput improve compared to 128KB batch with 0ms linger?
2. How much did P95 latency increase?
3. Is the trade-off worth it for your use case?

---

### Exercise 1.4: Test Compression Algorithms

**Task:** Compare different compression types.

Covered by section "Test Compression Algorithms" in [`src/mod-03/lab1-producer-perf.sh`](src/mod-03/lab1-producer-perf.sh).

**Record Your Results:**
```
Compression Comparison (128KB batch, 10ms linger):
none:   _____ rec/sec, _____ ms P95
lz4:    _____ rec/sec, _____ ms P95
snappy: _____ rec/sec, _____ ms P95
zstd:   _____ rec/sec, _____ ms P95
gzip:   _____ rec/sec, _____ ms P95
```

**Questions to Answer:**
1. Which compression gave the best throughput?
2. Which had the lowest latency?
3. When would you choose gzip despite lower throughput?

---

### Exercise 1.5: Calculate Your Improvement

**Task:** Compare your best configuration to baseline.

```
Baseline (from Exercise 1.1):     _____ rec/sec
Best Optimized (your best test):  _____ rec/sec

Improvement: _____ % increase

Configuration Used:
- batch.size: _____
- linger.ms: _____
- compression.type: _____
```

**Typical Results:**
Most students see 3-5x improvement with proper tuning!

---

### 💡 Key Takeaway

**Production Recommendations:**
- **batch.size:** 128KB-256KB for high throughput
- **linger.ms:** 5-20ms (acceptable latency increase)
- **compression.type:** `lz4` for most workloads (best speed/ratio balance)
- **acks:** `all` for durability (despite performance cost)

---

## 🎓 Lab 2: Consumer Fetch Optimization

### Key Concepts

**Consumer Fetch Settings:**
- `fetch.min.bytes`: Minimum data to fetch (default: 1 byte)
- `fetch.max.wait.ms`: Max time to wait for `fetch.min.bytes` (default: 500ms)
- `max.poll.records`: Max records returned per poll (default: 500)

**The Rebalancing Problem:**
- Consumer must call `poll()` within `max.poll.interval.ms` (default: 5 min)
- If processing takes too long → timeout → rebalancing → chaos
- **Solution:** Size `max.poll.records` based on processing time

**Fetch Strategies:**
1. **Real-time:** Small fetch, frequent polls → low latency
2. **Batch:** Large fetch, infrequent polls → high throughput
3. **Heavy processing:** Small batches to avoid timeout

**Key Insight:** Configure fetch based on your processing pattern, not just throughput.

---

### Exercise 2.1: Calculate Safe max.poll.records

**Scenario:** Your consumer has these characteristics:
- Processing time: 2ms per record
- `max.poll.interval.ms`: 300,000ms (5 minutes)

**Your Task:** Calculate the maximum safe `max.poll.records`.

```
Step 1: Calculate processing time per 1000 records
        = 1000 records × 2ms = _____ ms

Step 2: How many batches can fit in 5 minutes?
        = 300,000ms ÷ _____ ms = _____ batches

Step 3: Add safety margin (use 20% of timeout)
        = 300,000ms × 0.20 = _____ ms available for one batch
        
Step 4: Max records per batch
        = _____ ms ÷ 2ms = _____ records

Safe max.poll.records: _____ (round down)
```

**Rule of Thumb:** Keep processing time under 20% of timeout.

---

### Exercise 2.2: Test Real-Time Configuration

**Task:** Configure consumer for low-latency, real-time processing.

Covered by section "Consume with real-time settings" in [`src/mod-03/lab2-consumer-fetch.sh`](src/mod-03/lab2-consumer-fetch.sh).

**Configuration Explanation:**
- `fetch.min.bytes=1`: Don't wait for data, return immediately
- `fetch.max.wait.ms=100`: Max 100ms wait (low latency)
- `max.poll.records=100`: Small batches, frequent polling

**Use Cases:** Fraud detection, real-time alerting, live dashboards

---

### Exercise 2.3: Test High-Throughput Configuration

**Task:** Configure consumer for batch processing, high efficiency.

Covered by section "Consume with high-throughput settings" in [`src/mod-03/lab2-consumer-fetch.sh`](src/mod-03/lab2-consumer-fetch.sh).

**Configuration Explanation:**
- `fetch.min.bytes=102400` (100KB): Wait for substantial data
- `fetch.max.wait.ms=500`: Max 500ms wait
- `max.poll.records=1000`: Large batches, fewer polls

**Use Cases:** ETL pipelines, analytics, data warehouse loading

---

### Exercise 2.4: Compare Strategies

**Task:** Understand the trade-offs.

| Strategy | fetch.min.bytes | fetch.max.wait.ms | max.poll.records | Best For |
|----------|-----------------|-------------------|------------------|----------|
| Real-time | 1 | 100 | 100-500 | Low latency, instant response |
| Balanced | 10KB | 500 | 500-1000 | Most workloads |
| High-throughput | 100KB | 500 | 1000-5000 | Batch processing, ETL |
| Heavy processing | 1 | 100 | 50-200 | ML inference, complex transforms |

**Questions to Answer:**
1. Which strategy would you use for a fraud detection system?
2. Which for loading data into a data warehouse?
3. What happens if you set `max.poll.records` too high?

---

### 💡 Key Takeaway

**Consumer Tuning Guidelines:**
- **Start with:** Balanced settings (middle row in table)
- **If lag builds:** Increase `max.poll.records` or scale consumers
- **If timeouts occur:** Decrease `max.poll.records`
- **If latency matters:** Decrease `fetch.max.wait.ms`
- **If throughput matters:** Increase `fetch.min.bytes`

---

## 🎓 Lab 3: Broker-Side Performance Tuning

### Key Concepts

**Broker Threading:**
- `num.network.threads`: Handle request parsing/response (match CPU cores)
- `num.io.threads`: Handle disk reads/writes (2× disk count)

**Socket Buffers:**
- `socket.send.buffer.bytes`: OS-level send buffer
- `socket.receive.buffer.bytes`: OS-level receive buffer
- Larger = fewer TCP stalls with high throughput

**Request Flow:**
```
1. Client sends request
2. Network thread picks up, parses
3. Hands to I/O thread
4. I/O thread reads/writes disk
5. Returns to network thread
6. Network thread sends response
```

**Bottleneck Detection:**
- High `RequestQueueSize` → Need more network threads
- High disk latency → Need more I/O threads or faster disks
- High CPU → Scale network threads

**Key Insight:** Broker tuning gives 10-20% improvement, not as dramatic as producer optimization.

---

### Exercise 3.1: Understand Current Broker Config

**Task:** Check your broker's current settings.

Covered by section "View current broker configuration" in [`src/mod-03/lab3-broker-tuning.sh`](src/mod-03/lab3-broker-tuning.sh).

**Default Values (typical):**
```
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400    (100KB)
socket.receive.buffer.bytes=102400 (100KB)
```

---

### Exercise 3.2: Calculate Optimal Thread Counts

**Task:** Determine ideal settings for your hardware.

**Your System:**
```bash
# Check CPU cores
docker exec broker-1 nproc
# Result: _____ cores
```

**Recommended Configuration:**
```
num.network.threads = Number of CPU cores
                    = _____ threads

num.io.threads = 2 × (number of disks)
               = 2 × 1 disk (Docker typically uses 1)
               = 2 threads (minimum)
               = 8-16 for production with multiple disks
```

**Socket Buffers:**
```
For high-throughput systems (>100 MB/sec):
socket.send.buffer.bytes = 1048576 (1MB)
socket.receive.buffer.bytes = 1048576 (1MB)

For normal systems:
socket.send.buffer.bytes = 102400 (100KB) [default]
socket.receive.buffer.bytes = 102400 (100KB) [default]
```

---

### Exercise 3.3: Test With Current Settings

**Task:** Establish baseline with current broker config.

Covered by section "Performance test with current settings" in [`src/mod-03/lab3-broker-tuning.sh`](src/mod-03/lab3-broker-tuning.sh).

**Record Your Results:**
```
Baseline (default broker settings): _____ records/sec
```

**Note:** Broker tuning requires restart, which we won't do in this lab. In production, you'd:
1. Apply new configs to `server.properties`
2. Rolling restart brokers
3. Re-run performance tests
4. Expect 10-20% improvement

---

### Exercise 3.4: Understand the Complete Optimization

**Task:** Calculate total improvement from all three labs.

```
Module 3 Complete Optimization:

Lab 1 - Producer Baseline:        _____ rec/sec
Lab 1 - Producer Optimized:       _____ rec/sec (+___% improvement)

Lab 2 - Consumer tuning:          Prevents timeouts, enables stable processing

Lab 3 - Broker tuning (estimated): +15% additional improvement
                                   = _____ rec/sec (estimated final)

Total Improvement: _____% (typical: 300-500%)
```

---

### 💡 Key Takeaway

**Complete Optimization Strategy:**
1. **Producer tuning (Lab 1):** 3-5x improvement (biggest impact)
2. **Consumer tuning (Lab 2):** Stability, prevents rebalancing
3. **Broker tuning (Lab 3):** 10-20% additional improvement

**Combined:** 4-6x total throughput improvement with same infrastructure!

---

## 🧹 Lab Cleanup

After completing all exercises:

```bash
bash src/mod-03/cleanup.sh
```

---

## 📝 Module 3 Quiz

Test your understanding:

1. **Which producer setting has the BIGGEST impact on throughput?**
   - A) acks=all
   - B) batch.size and linger.ms
   - C) compression.type
   - D) num.io.threads

2. **For most workloads, the recommended compression algorithm is:**
   - A) none (no compression)
   - B) gzip (highest compression)
   - C) lz4 (best speed/ratio balance)
   - D) snappy (fastest)

3. **If your consumer processes 100 records in 5 seconds and max.poll.interval.ms=300000, what's a safe max.poll.records?**
   - A) 100
   - B) 1000
   - C) 6000
   - D) 60000

4. **num.network.threads should be set to:**
   - A) 1 (single-threaded)
   - B) Number of CPU cores
   - C) 2 × number of disks
   - D) Number of partitions

5. **In production optimization, which gives the largest improvement?**
   - A) Broker threading (100x)
   - B) Consumer fetch settings (10x)
   - C) Producer batching (3-5x)
   - D) All equal (~20% each)

**Answers:** 1-B, 2-C, 3-B, 4-B, 5-C

---

## 🔗 Additional Resources

### Official Documentation
- [Producer Configurations](https://kafka.apache.org/documentation/#producerconfigs)
- [Consumer Configurations](https://kafka.apache.org/documentation/#consumerconfigs)
- [Broker Configurations](https://kafka.apache.org/documentation/#brokerconfigs)
- [Performance Tuning](https://kafka.apache.org/documentation/#hwandos)

### Real-World Optimization Stories
- [LinkedIn's Kafka at Scale](https://engineering.linkedin.com/kafka/running-kafka-scale)
- [Uber's Kafka Optimization](https://www.uber.com/blog/kafka/)
- [Confluent Performance Best Practices](https://www.confluent.io/blog/optimizing-apache-kafka-deployment/)

### Tools
- [kafka-producer-perf-test](https://kafka.apache.org/documentation/#producerperf)
- [kafka-consumer-perf-test](https://kafka.apache.org/documentation/#consumerperf)
- [JMX Monitoring](https://kafka.apache.org/documentation/#monitoring)

---

## ✅ Module 3 Completion Checklist

- [ ] Completed Lab 1: Producer Batching and Compression
- [ ] Completed Lab 2: Consumer Fetch Optimization
- [ ] Completed Lab 3: Broker-Side Tuning
- [ ] Achieved 3x+ throughput improvement
- [ ] Passed Module 3 Quiz (4/5 correct minimum)
- [ ] Ready for the final project

---

## 🎓 Final Project: Real-World Optimization

Now that you've completed all three modules, apply everything you've learned:

**Scenario:** You're the platform engineer at an e-commerce company. Build and optimize a Kafka cluster that:

1. **Module 1 Skills:** Configure topics for orders, inventory, and analytics with appropriate replication and partitions
2. **Module 2 Skills:** Set up monitoring, consumer groups, and alerting
3. **Module 3 Skills:** Optimize for 100,000 orders/day with <50ms P95 latency

**Deliverables:**
- Topic configurations
- Consumer group sizing calculations
- Performance test results showing improvement
- Monitoring/alerting configuration
- Documentation explaining decisions

---

**Congratulations on completing Module 3! 🎉**

You now have the skills to design, monitor, and optimize production Kafka systems. You've learned to balance throughput, latency, durability, and cost - the key trade-offs in distributed systems engineering.

**Next Steps:**
- Complete the final project
- Build your own Kafka application
- Earn your Kafka certification
- Join the Kafka community!
