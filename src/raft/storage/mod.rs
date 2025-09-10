// Raft storage implementations
// 
// This module provides storage implementations for openraft, including:
// - RaftLogStorage: for storing and managing Raft log entries
// - RaftStateMachine: for applying committed log entries to the state machine
//
// Currently provides memory-based implementations for PoC, which can be
// replaced with persistent storage implementations later.

pub mod memory;

pub use memory::{MemoryStorage, MemoryStateMachine};