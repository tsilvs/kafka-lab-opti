# Kafka Core Concepts — Diagrams

> Visual reference for topic/partition/replica architecture referenced across Module 1-3 guides.
> SVG shown below each diagram (works everywhere, width-capped). Mermaid source collapsed underneath — expand to edit / view in renderers with native Mermaid support (GitHub, VS Code).
> SVGs regenerated via `scripts/render-diagrams.py` (uses [mermaid.ink](https://mermaid.ink)).

---

## 1. Topic → Partitions → Broker Replication

A topic is split into partitions; each partition is replicated across N brokers (`replication.factor`). One replica is **leader** (serves all reads/writes), others are **followers** (ISR members).

<div align="center" style="background: white; max-width: 80vw; margin: 0 auto;">

![Diagram 1](assets/diagrams/diagram-01.svg)

</div>

<details>
<summary>Mermaid source</summary>

```mermaid
graph TB
    subgraph Topic["Topic: orders (3 partitions, RF=3)"]
        P0["Partition 0"]
        P1["Partition 1"]
        P2["Partition 2"]
    end

    subgraph B1["Broker 1"]
        P0L["P0 Leader"]
        P1F1["P1 Follower"]
        P2F1["P2 Follower"]
    end

    subgraph B2["Broker 2"]
        P0F1["P0 Follower"]
        P1L["P1 Leader"]
        P2F2["P2 Follower"]
    end

    subgraph B3["Broker 3"]
        P0F2["P0 Follower"]
        P1F2["P1 Follower"]
        P2L["P2 Leader"]
    end

    P0 -.-> P0L
    P1 -.-> P1L
    P2 -.-> P2L
```

</details>

---

## 2. Producer Partitioning (key → partition)

Producer hashes message key to pick a partition (or round-robins if key is null). All messages with same key land on same partition → preserves order per key.

<div align="center" style="background: white; max-width: 80vw; margin: 0 auto;">

![Diagram 2](assets/diagrams/diagram-02.svg)

</div>

<details>
<summary>Mermaid source</summary>

```mermaid
flowchart LR
    K1["key=user-42"] --> H{"hash(key) % partitions"}
    K2["key=user-17"] --> H
    K3["key=null"] --> RR["round-robin / sticky"]

    H --> P0["Partition 0"]
    H --> P1["Partition 1"]
    RR --> P2["Partition 2"]

    P0 --> B1["Broker (leader P0)"]
    P1 --> B2["Broker (leader P1)"]
    P2 --> B3["Broker (leader P2)"]
```

</details>

---

## 3. In-Sync Replica (ISR) Set & Acks

`acks=all` waits for all ISR members to ack before confirming write. `min.insync.replicas` sets minimum ISR size for write to succeed.

<div align="center" style="background: white; max-width: 80vw; margin: 0 auto;">

![Diagram 3](assets/diagrams/diagram-03.svg)

</div>

<details>
<summary>Mermaid source</summary>

```mermaid
sequenceDiagram
    participant Prod as Producer (acks=all)
    participant L as Leader (Broker 2)
    participant F1 as Follower (Broker 1, ISR)
    participant F2 as Follower (Broker 3, ISR)

    Prod->>L: Write record
    L->>F1: Replicate
    L->>F2: Replicate
    F1-->>L: Ack (fetch caught up)
    F2-->>L: Ack (fetch caught up)
    L-->>Prod: Ack (min.insync.replicas satisfied)
```

</details>

---

## 4. Consumer Group Partition Assignment

Each partition assigned to exactly one consumer within a group. More consumers than partitions → idle consumers. Fewer → some consumers own multiple partitions.

<div align="center" style="background: white; max-width: 80vw; margin: 0 auto;">

![Diagram 4](assets/diagrams/diagram-04.svg)

</div>

<details>
<summary>Mermaid source</summary>

```mermaid
graph LR
    subgraph Topic["Topic: orders (4 partitions)"]
        TP0["P0"]
        TP1["P1"]
        TP2["P2"]
        TP3["P3"]
    end

    subgraph CG["Consumer Group: order-processors"]
        C1["Consumer 1"]
        C2["Consumer 2"]
    end

    TP0 --> C1
    TP1 --> C1
    TP2 --> C2
    TP3 --> C2
```

</details>

---

## 5. Consumer Lag

Lag = latest offset (log end) − consumer's committed offset. Growing lag → consumer slower than producer.

<div align="center" style="background: white; max-width: 80vw; margin: 0 auto;">

![Diagram 5](assets/diagrams/diagram-05.svg)

</div>

<details>
<summary>Mermaid source</summary>

```mermaid
flowchart LR
    subgraph Partition0["Partition 0 log"]
        direction LR
        O0["offset 0"] --> O1["..."] --> O97["offset 97"] --> O98["offset 98\n(committed)"] --> O99["offset 99"] --> O100["offset 100\n(log end)"]
    end

    O98 -.->|"lag = 2"| O100
```

</details>

---

## Related guides

- [Module 1: Configure Topics for High Availability](module-1-guide.md) — replication factor, partitions
- [Module 2: Monitor Performance and Identify Bottlenecks](module-2-guide.md) — consumer lag, group sizing
- [Module 3: Optimize Producer and Consumer Performance](module-3-guide.md) — batching, ISR/acks tuning
  </content>
