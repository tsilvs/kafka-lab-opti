# Kafka Performance Optimization - Lab Guides

> Hands-On Labs for Apache Kafka Performance & Availability

---

## 📚 Overview

Three lab guides covering configuration, monitoring, and optimization of Apache Kafka clusters for production use.

**Total Time:** ~3 hours of hands-on practice  
**Prerequisites:** Basic familiarity with command-line terminals and Docker  
**Tools Required:** Docker Desktop (8GB+ RAM recommended)  

---

## 📋 Quick Access

### **[Command Reference](command-reference.md)** 🔥

Complete command-line reference for every exercise across all three modules, organized by module and topic — commands, purpose, and expected result.

**[→ View Command Reference](command-reference.md)**

---

## 🎯 Learning Path

### **[Module 1: Configure Topics for High Availability](module-1-guide.md)**

📊 **Difficulty:** Beginner

Configure Kafka topics with replication factors, partition counts, and durability settings for high availability and fault tolerance.

**Labs:** Replication Factors and Data Durability · Partition Strategy for Parallelism · Topic Configuration Best Practices

**[Start Module 1 →](module-1-guide.md)**

---

### **[Module 2: Monitor Performance and Identify Bottlenecks](module-2-guide.md)**

📊 **Difficulty:** Intermediate

Consumer lag monitoring, consumer group sizing, and broker health metrics to diagnose bottlenecks before they hit production.

**Labs:** Understanding Consumer Lag · Consumer Group Sizing and Parallelism · Broker Health Monitoring

**[Start Module 2 →](module-2-guide.md)**

---

### **[Module 3: Optimize Producer and Consumer Performance](module-3-guide.md)**

📊 **Difficulty:** Advanced

Producer batching, compression, consumer fetch tuning, and broker-side configuration to maximize throughput within latency SLAs.

**Labs:** Producer Batching and Compression · Consumer Fetch Optimization · Broker-Side Performance Tuning

**[Start Module 3 →](module-3-guide.md)**

---

## 🚀 Quick Start

### Prerequisites

1. **Install Docker Desktop**
   - [Mac](https://docs.docker.com/desktop/install/mac-install/)
   - [Windows](https://docs.docker.com/desktop/install/windows-install/)
   - [Linux](https://docs.docker.com/desktop/install/linux-install/)

2. **Allocate Resources to Docker**
   - Minimum: 8GB RAM, 4 CPUs
   - Recommended: 12GB RAM, 6 CPUs
   - Docker Desktop → Settings → Resources

### Set Up Your Lab Environment

```bash
bash src/mod-01/setup.sh
```

This creates `~/kafka-labs`, copies the cluster definition ([`src/mod-01/docker-compose.yml`](src/mod-01/docker-compose.yml)), starts the 3-broker cluster, and verifies it's reachable. Full walkthrough in [Module 1](module-1-guide.md#lab-environment-setup).

---

## 📖 How to Use These Guides

1. **Read "Key Concepts"** in the module guide
2. **Run the lab scripts** under `src/mod-0N/` for each exercise
3. **Answer the "Questions to Answer"** using the command output
4. **Use the [Command Reference](command-reference.md)** for quick copy-paste lookups
5. **Complete the quiz** at the end of each module

Each lab includes: 🎯 objectives, 💡 key concepts, ⚙️ hands-on exercises, ❓ questions to answer, ✅ key takeaways.

---

## 🛠️ What You'll Build

- ✅ 3-broker Kafka cluster with Docker
- ✅ Production-ready topics with replication and partitioning
- ✅ Consumer lag and broker health monitoring
- ✅ Correctly sized consumer groups
- ✅ 3-5x producer throughput via batching and compression
- ✅ Tuned consumer fetch settings
- ✅ Monitoring/alerting rules

**Final Project:** Optimize a complete Kafka system for an e-commerce platform processing 100,000 orders/day.

---

## 📊 Skills Progression

```
Module 1: Configure     →  Module 2: Monitor      →  Module 3: Optimize
├─ Replication          →  ├─ Consumer lag        →  ├─ Producer batching
├─ Partitions           →  ├─ Consumer groups     →  ├─ Compression
└─ Topic configs        →  └─ Broker health       →  └─ Fetch tuning

Beginner                   Intermediate              Advanced
```

---

## 💼 Real-World Applications

- **Netflix** - 700+ billion events/day using 36 Kafka clusters
- **LinkedIn** - 7+ trillion messages/day through 100+ clusters
- **Uber** - Real-time dispatch and surge pricing
- **Walmart** - 100 million SKUs/day inventory sync

---

## 🧪 Lab Environment

### What's Included

- **3-broker Kafka cluster** (Confluent Platform 7.5.0)
- **Zookeeper** for coordination
- **Command-line tools** for topics, producers, consumers
- **Performance testing tools** (`kafka-producer-perf-test`)

### Architecture

```
┌─────────────────────────────────────────┐
│         Your Computer (Docker)          │
├─────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐
│  │ Broker 1 │  │ Broker 2 │  │ Broker 3 │
│  │ :9092    │  │ :9093    │  │ :9094    │
│  └──────────┘  └──────────┘  └──────────┘
│       │             │             │        
│  ┌────────────────────────────────────┐  
│  │   KRaft controller quorum :29093   │  
│  │   (runs inside the brokers)        │  
│  └────────────────────────────────────┘  
└─────────────────────────────────────────┘
```

### Lab Scripts

All non-interactive setup commands live under [`src/`](src/), one directory per module:

- [`src/kafka-env.sh`](src/kafka-env.sh) - shared CLI wrapper functions
- [`src/mod-01/`](src/mod-01/) - cluster setup, replication, partitions, config patterns
- [`src/mod-02/`](src/mod-02/) - consumer lag, group sizing, broker health
- [`src/mod-03/`](src/mod-03/) - producer/consumer/broker performance tuning

Each module directory has its own `cleanup.sh`.

---

## 📚 Additional Resources

### Official Documentation

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Platform Documentation](https://docs.confluent.io/)

### Performance Guides

- [Kafka Performance Tuning](https://www.redpanda.com/guides/kafka-performance-kafka-performance-tuning)
- [Consumer Lag Monitoring](https://docs.confluent.io/platform/current/monitor/monitor-consumer-lag.html)
- [Partition Strategies](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/)

### Community

- [Kafka Users Mailing List](https://kafka.apache.org/contact)
- [Confluent Community Slack](https://launchpass.com/confluentcommunity)
- [Stack Overflow: apache-kafka](https://stackoverflow.com/questions/tagged/apache-kafka)

---

## 🤝 Contributing

Found an issue or have a suggestion?

- **Report bugs:** Open an issue describing the problem
- **Suggest improvements:** Open an issue with your idea
- **Fix typos:** Submit a pull request

---

## ⚖️ License

This educational material is provided as-is for learning purposes.

Apache Kafka and related trademarks are property of the Apache Software Foundation.

---

## ✅ Module Completion Checklist

- [ ] **Module 1:** Configured topics with replication and partitioning, passed quiz (4/5 correct)
- [ ] **Module 2:** Monitored consumer lag and sized consumer groups, passed quiz (4/5 correct)
- [ ] **Module 3:** Optimized producer and consumer performance, passed quiz (4/5 correct)
- [ ] **Final Project:** Built complete e-commerce Kafka system

---

## 🚀 What's Next?

1. **Apply to real projects** - Use Kafka in your applications
2. **Explore advanced topics** - Kafka Streams, ksqlDB, Schema Registry
3. **Get certified** - Confluent Certified Developer for Apache Kafka
4. **Join the community** - Contribute to open source, help others learn

---

**Happy Learning! 🎉**

*Last Updated: November 2024*
