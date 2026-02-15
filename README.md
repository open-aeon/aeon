[简体中文](./README_zh-CN.md)

# Aeon - A Kafka-compatible message queue in Rust

Aeon is a high-performance, Kafka-protocol compatible message queue built from scratch in asynchronous Rust. It aims to explore the core principles of modern distributed messaging systems, drawing inspiration from platforms like Apache Kafka and Redpanda.

**This project is currently at `v0.1.0`, focusing on a complete, single-node core engine.**

## 🌟 Highlights & Features

The `v0.1.0` release delivers a robust, single-node message broker with a focus on deep engineering solutions and protocol correctness.

*   **🚀 Kafka Protocol Compatibility via Automated Code Generation**:
    *   Instead of error-prone manual implementation, the entire Kafka protocol layer is **automatically generated**.
    *   A `build.rs` script parses Kafka's official JSON protocol specifications and uses procedural macros (`aeon-protocol-macro`) to generate all request/response structs, along with their version-aware serialization (`Encode`) and deserialization (`Decode`) logic.


*   **🔄 Complete Consumer Group & Rebalancing Implementation**:
    *   Features a fully functional **Group Coordinator**, implemented as a separate asynchronous Actor, to manage the entire consumer lifecycle. This is often the most complex part of a message broker.
    *   Correctly handles the complete Kafka consumer group protocol suite: `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, and `OffsetCommit/Fetch`.
    *   Supports dynamic partition rebalancing as consumers join or leave a group, ensuring no messages are lost and consumption is balanced.

*   **💾 High-Performance I/O & Storage Engine**:
    *   The storage engine is built on **memory-mapped files (`mmap`)** to achieve high-throughput writes and **zero-copy reads**, minimizing kernel/user-space context switching.
    *   Implements a **segmented log** design coupled with a **sparse in-memory index**, allowing for efficient offset lookups without consuming large amounts of memory.

*   **✅ End-to-End Tested with Standard Clients**:
    *   The broker's correctness and compatibility are validated by a comprehensive integration test suite.
    *   The test suite uses the standard `rdkafka` (librdkafka) Rust client to perform a full produce/consume cycle, proving that Aeon works seamlessly with the existing Kafka ecosystem.

## 🏛️ Architecture (v0.1.0)

![Aeon Architecture](./assets/architecture.png)

The single-node architecture of Aeon `v0.1.0` is composed of several key components:

1.  **TCP Server (`server`)**: The front door of the broker. It listens for incoming Kafka client connections, decodes raw byte streams into Kafka `Request` frames, and passes them to the handler.
2.  **Request Handler (`handler`)**: Decodes the request type and dispatches it to the appropriate logic within the `Broker`. It then encodes the `Response` and sends it back to the client.
3.  **Broker (`broker`)**: The central component that orchestrates all broker activities. It holds the metadata for topics, partitions, and consumer groups.
4.  **Group Coordinator (`coordinator`)**: Manages the lifecycle of consumer groups. Each group is handled by its own async task, processing events like joining members, heartbeats, and triggering rebalances.
5.  **Storage Engine (`storage`)**: The persistence layer. It manages the physical log segments on disk for each topic-partition.
    *   **Log (`log`)**: A logical representation of a topic-partition's ordered message sequence.
    *   **Segment (`segment`)**: A single log file on disk, backed by `mmap`. Contains the actual `RecordBatch` data.
    *   **Index**: A sparse index file for each segment, mapping message offsets to physical byte positions in the segment file.

## 🚀 Getting Started

Currently, the primary way to run and interact with Aeon is through the integration tests.

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/open-aeon/aeon.git
    cd aeon
    ```

2.  **Run the integration test**:
    This test will start a broker, create a topic, produce 10 messages, and consume them using a consumer group.
    ```bash
    cargo test --test consumer_group_test
    ```

## 🗺️ Roadmap

The future of Aeon is focused on evolving from a single-node broker into a fully-featured, distributed, and cloud-native messaging platform.

*   **v0.1.0**: Single-Node Core (✅ Completed)

*   **v0.2.0**: Feature Completeness & Performance Validation
    *   Implement full Kafka feature compatibility, including topic management, timestamps, and transactions.
    *   Conduct comprehensive performance benchmarking and tuning to establish a performance baseline.

*   **v0.3.0**: Distributed Architecture
    *   Evolve Aeon into a high-availability, multi-broker distributed cluster using a pure Raft-based architecture for metadata management.

*   **v0.4.0**: Enterprise-Grade Features
    *   **Security**: Implement TLS for encrypted transport.
    *   **Monitoring**: Add Prometheus metrics for deep observability.
    *   **Management**: Implement advanced management APIs like `DescribeCluster`, `DescribeConfigs`, and `AlterConfigs`.

*   **Future Exploration**:
    *   **Linux-Optimized I/O**: Explore cutting-edge technologies like `io_uring` and `kTLS` for superior performance in Linux environments.
    *   **Cloud-Native Features**: Introduce tiered storage for cost-effective long-term data retention.
    *   **Extensibility**: Add WASM (WebAssembly) support to enable simplified, in-broker stream processing.
