# Raft Module

This directory implements Aeon's Raft-based distributed features, covering both metadata and data logs.

## Directory Structure

raft/
  mod.rs
  types.rs
  manager.rs
  transport.rs
  README.md
  fsm/
    mod.rs
    meta_fsm.rs
    data_fsm.rs
  storage/
    mod.rs
    wal.rs
    hard_state.rs
    snapshot.rs

## File Descriptions

### `mod.rs`
- Entry point of the Raft module.
- Exposes submodules (`types`, `manager`, `transport`, `fsm`, `storage`).
- Contains module-level exports and initialization.

### `types.rs`
- Defines common types and data structures for the Raft domain.
- Examples: `NodeId`, `GroupId`, `Term`, `LogIndex`, command/event enums, error types, etc.
- Aims to minimize magic types and redundant definitions across files.

### `manager.rs`
- Raft Manager (acts as a unified scheduling layer for multiple Raft Groups).
- Responsible for group lifecycle (create, start, stop, query leader, propose/read routing).
- Provides a unified interface to the Broker (upwards) and interacts with Raft nodes/storage (downwards).

### `transport.rs`
- Abstraction for Raft node-to-node network communication.
- Defines interfaces for sending and receiving RPCs such as AppendEntries, Vote, and Snapshot.
- Shields upper layers from the details of specific network implementations (TCP, HTTP, QUIC, etc).

---

### `fsm/mod.rs`
- Entry point and trait definitions for the FSM (Finite State Machine) submodule.
- Defines unified behavior interfaces like `apply`, `snapshot`, and `restore`.

### `fsm/meta_fsm.rs`
- Metadata state machine.
- Handles control plane commands for topic/broker/partition assignments and member info.
- State is updated by committed logs from the `meta-raft group`.

### `fsm/data_fsm.rs`
- Data state machine.
- Handles data plane commands such as message appends and offset commits.
- Business data storage is updated by committed logs from the `data-raft group`.

---

### `storage/mod.rs`
- Entry point for the Raft storage submodule.
- Aggregates and exports `wal`, `hard_state`, and `snapshot` interfaces and implementations.

### `storage/wal.rs`
- Raft WAL (Write-Ahead Log) storage.
- Responsible for persisting Raft entries (append, truncate, read).
- Ensures that logs can be replayed for crash recovery.

### `storage/hard_state.rs`
- Persistent storage for Raft HardState.
- Typically includes `current_term`, `voted_for`, `commit_index`, etc.
- Guarantees correct protocol semantics after node restart.

### `storage/snapshot.rs`
- Logic for Raft snapshot read/write and installation.
- Supports state compression, fast recovery, and log compaction.

---

## Design Boundaries

- `raft/storage/*` is only responsible for **persistence required by the Raft protocol**, and does not directly handle Kafka business logic.
- Business data storage (topic/partition segment) still resides in `src/storage`, bridged via `data_fsm`.
- The `raft` module manages "order and consistency", while the `fsm` manages "business state changes".
