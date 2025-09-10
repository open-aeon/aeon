use crate::raft::generic::{
    RaftNode, RaftAdmin, RaftNodeStatus, RaftRequest, RaftResponse, 
    ReadConsistency, LogStats, RaftResult
};
use crate::raft::storage::memory::{MemoryStorage, MemoryStateMachine};
use crate::raft::types::NodeId;
use async_trait::async_trait;
use raft::{prelude::*, RawNode, Config, StateRole};
use bytes::Bytes;
use slog::{Logger, o, Discard};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Raft-rs adapter that implements our generic Raft node interface
/// 
/// This adapter wraps raft-rs's RawNode and provides a high-level async interface
/// that matches our GenericRaftNode trait.
#[derive(Clone)]
pub struct RaftRsAdapter {
    /// The underlying raft-rs node
    raft_node: Arc<Mutex<RawNode<MemoryStorage>>>,
    /// State machine for applying committed entries
    state_machine: Arc<MemoryStateMachine>,
    /// Node configuration
    node_id: NodeId,
    /// Peers in the cluster
    peers: HashMap<NodeId, String>,
    /// Channel for receiving Raft messages
    message_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Message>>>>,
    /// Channel for sending Raft messages  
    message_tx: mpsc::UnboundedSender<Message>,
}

impl RaftRsAdapter {
    /// Create a new RaftRsAdapter
    pub fn new(
        node_id: NodeId,
        peers: Vec<(NodeId, String)>,
        storage: MemoryStorage,
    ) -> Result<Self, Box<dyn Error>> {
        // Create Raft configuration
        let config = Config {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            applied: 0,
            ..Default::default()
        };

        // Create logger (using a simple discard logger for now)
        let logger = Logger::root(Discard, o!());
        
        // Create RawNode
        let mut raft_node = RawNode::new(&config, storage, &logger)?;
        
        // For single node cluster, bootstrap immediately
        if peers.len() == 1 && peers[0].0 == node_id {
            log::info!("Bootstrapping single-node cluster for node {}", node_id);
            
            // Bootstrap single node cluster
            let mut conf_state = raft::eraftpb::ConfState::default();
            conf_state.voters.push(node_id);
            
            let mut snapshot_metadata = raft::eraftpb::SnapshotMetadata::default();
            snapshot_metadata.conf_state = protobuf::SingularPtrField::some(conf_state);
            snapshot_metadata.index = 0;
            snapshot_metadata.term = 0;
            
            let mut snapshot = raft::eraftpb::Snapshot::default();
            snapshot.metadata = protobuf::SingularPtrField::some(snapshot_metadata);
            snapshot.data = Bytes::new();
            
            // Apply the bootstrap snapshot
            raft_node.raft.restore(snapshot);
        }
        
        // Create message channel
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        // Convert peers to HashMap
        let peers_map: HashMap<NodeId, String> = peers.into_iter().collect();

        let adapter = Self {
            raft_node: Arc::new(Mutex::new(raft_node)),
            state_machine: Arc::new(MemoryStateMachine::new()),
            node_id,
            peers: peers_map,
            message_rx: Arc::new(Mutex::new(Some(message_rx))),
            message_tx,
        };
        
        // Start the Raft event loop
        let adapter_clone = adapter.clone();
        tokio::spawn(async move {
            if let Err(e) = adapter_clone.run_event_loop().await {
                log::error!("Raft event loop error: {}", e);
            }
        });
        
        Ok(adapter)
    }

    /// Start the Raft node event loop
    async fn run_event_loop(&self) -> Result<(), Box<dyn Error>> {
        let mut message_rx = {
            let mut rx_opt = self.message_rx.lock().unwrap();
            rx_opt.take().ok_or("Message receiver already taken")?
        };

        loop {
            // Check for incoming messages
            if let Ok(msg) = message_rx.try_recv() {
                let mut node = self.raft_node.lock().unwrap();
                node.step(msg)?;
            }

            // Drive the Raft state machine
            {
                let mut node = self.raft_node.lock().unwrap();
                
                // Check if we have ready state to process
                if node.has_ready() {
                    let ready = node.ready();

                    // Apply committed entries
                    if !ready.committed_entries().is_empty() {
                        for entry in ready.committed_entries() {
                            if !entry.data.is_empty() {
                                self.state_machine.apply(entry)?;
                            }
                        }
                    }

                    // Send out messages
                    for msg in ready.messages() {
                        // In a real implementation, we would send these to the appropriate peers
                        // For now, we'll just log them
                        log::debug!("Would send message to node {}: {:?}", msg.to, msg.msg_type);
                    }

                    // Persist hard state and entries if needed
                    if let Some(hs) = ready.hs() {
                        // In a real implementation, we would persist this
                        log::debug!("Would persist hard state: {:?}", hs);
                    }

                    if !ready.entries().is_empty() {
                        // In a real implementation, we would persist these entries
                        log::debug!("Would persist {} entries", ready.entries().len());
                    }

                    // Advance the Raft state machine
                    let light_rd = node.advance(ready);
                    
                    // Handle any additional committed entries
                    let committed_entries = light_rd.committed_entries();
                    if !committed_entries.is_empty() {
                        for entry in committed_entries {
                            if !entry.data.is_empty() {
                                self.state_machine.apply(entry)?;
                            }
                        }
                    }

                    node.advance_apply();
                }

                            // Tick the Raft node
            node.tick();
            
            // For single node, campaign immediately if not leader
            if self.peers.len() == 1 && node.raft.state != StateRole::Leader {
                log::debug!("Single node campaigning for leadership");
                let _ = node.campaign();
            }
            
            // Process any ready state immediately after campaign
            if node.has_ready() {
                let ready = node.ready();
                // Apply any committed entries
                if !ready.committed_entries().is_empty() {
                    for entry in ready.committed_entries() {
                        if !entry.data.is_empty() {
                            let _ = self.state_machine.apply(entry);
                        }
                    }
                }
                let light_rd = node.advance(ready);
                let committed_entries = light_rd.committed_entries();
                if !committed_entries.is_empty() {
                    for entry in committed_entries {
                        if !entry.data.is_empty() {
                            let _ = self.state_machine.apply(entry);
                        }
                    }
                }
                node.advance_apply();
            }
            }

            // Small delay to prevent busy waiting
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Propose a new entry to the Raft log
    fn propose_entry(&self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let mut node = self.raft_node.lock().unwrap();
        node.propose(vec![], data)?;
        Ok(())
    }

    /// Check if this node is the current leader
    fn is_leader(&self) -> bool {
        let node = self.raft_node.lock().unwrap();
        node.raft.state == StateRole::Leader
    }

    /// Get current Raft status
    fn get_status(&self) -> RaftNodeStatus {
        let node = self.raft_node.lock().unwrap();
        let raft_status = node.status();
        
        RaftNodeStatus {
            node_id: self.node_id,
            is_leader: node.raft.state == StateRole::Leader,
            term: raft_status.hs.term,
            commit_index: raft_status.hs.commit,
            applied_index: node.raft.raft_log.applied,
            last_log_index: node.raft.raft_log.last_index(),
            follower_count: if node.raft.state == StateRole::Leader {
                let vote_count = node.raft.prs().votes().len();
                Some(if vote_count > 0 { vote_count as u32 - 1 } else { 0 }) // Exclude self
            } else {
                None
            },
        }
    }
}

#[async_trait]
impl RaftNode for RaftRsAdapter {
    async fn propose(&self, request: RaftRequest) -> Result<RaftResponse, Box<dyn Error>> {
        if !self.is_leader() {
            return Ok(RaftResponse::error("Not the leader".to_string()));
        }

        // Serialize the request
        let data = bincode::serialize(&request)?;
        
        // Propose to Raft
        self.propose_entry(data)?;

        // For now, return a simple response
        // In a real implementation, we would wait for the entry to be committed
        // and applied before returning the actual response
        let response = RaftResponse::success(b"Proposal submitted".to_vec());
        Ok(if let Some(req_id) = request.request_id {
            response.with_request_id(req_id)
        } else {
            response
        })
    }

    async fn query(
        &self,
        request: RaftRequest,
        consistency: ReadConsistency,
    ) -> Result<RaftResponse, Box<dyn Error>> {
        match consistency {
            ReadConsistency::Linearizable => {
                if !self.is_leader() {
                    return Ok(RaftResponse::error("Not the leader for linearizable read".to_string()));
                }
                // TODO: Implement linearizable read with ReadIndex
            }
            ReadConsistency::LeaderLease => {
                if !self.is_leader() {
                    return Ok(RaftResponse::error("Not the leader for leader lease read".to_string()));
                }
                // TODO: Check leader lease validity
            }
            ReadConsistency::Eventual => {
                // Can read from any node
            }
        }

        // For read requests, query the state machine directly
        let result = format!("Query processed for {} bytes", request.data.len()).into_bytes();
        let response = RaftResponse::success(result);
        
        Ok(if let Some(req_id) = request.request_id {
            response.with_request_id(req_id)
        } else {
            response
        })
    }

    async fn status(&self) -> RaftNodeStatus {
        self.get_status()
    }

    async fn wait_for_leadership(&self, timeout: Option<std::time::Duration>) -> Result<(), Box<dyn Error>> {
        let start = std::time::Instant::now();
        
        loop {
            if self.is_leader() {
                return Ok(());
            }
            
            if let Some(timeout) = timeout {
                if start.elapsed() > timeout {
                    return Err("Timeout waiting for leadership".into());
                }
            }
            
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    async fn step_down(&self) -> Result<(), Box<dyn Error>> {
        if !self.is_leader() {
            return Ok(()); // Already not a leader
        }
        
        // TODO: Implement step down logic
        // This would typically involve stopping to send heartbeats
        // and allowing the election timeout to trigger a new election
        Err("Step down not implemented yet".into())
    }

    async fn get_cluster_members(&self) -> Result<Vec<NodeId>, Box<dyn Error>> {
        // TODO: Extract cluster members from Raft configuration
        let peers: Vec<NodeId> = self.peers.keys().cloned().collect();
        let mut members = peers;
        members.push(self.node_id);
        Ok(members)
    }
}

#[async_trait]
impl RaftAdmin for RaftRsAdapter {
    async fn add_node(&self, node_id: NodeId, address: String) -> Result<(), Box<dyn Error>> {
        if !self.is_leader() {
            return Err("Not the leader, cannot add node".into());
        }
        
        // TODO: Implement configuration change to add node
        // This would involve:
        // 1. Creating a ConfChange entry
        // 2. Proposing it through Raft
        // 3. Waiting for it to be applied
        log::info!("Would add node {} at {}", node_id, address);
        Err("Add node not implemented yet".into())
    }

    async fn remove_node(&self, node_id: NodeId) -> Result<(), Box<dyn Error>> {
        if !self.is_leader() {
            return Err("Not the leader, cannot remove node".into());
        }
        
        // TODO: Implement configuration change to remove node
        log::info!("Would remove node {}", node_id);
        Err("Remove node not implemented yet".into())
    }

    async fn create_snapshot(&self) -> Result<(), Box<dyn Error>> {
        // TODO: Trigger snapshot creation
        log::info!("Would create snapshot");
        Err("Create snapshot not implemented yet".into())
    }

    async fn get_log_stats(&self) -> Result<LogStats, Box<dyn Error>> {
        let node = self.raft_node.lock().unwrap();
        let raft = &node.raft;
        
        Ok(LogStats {
            first_index: 1, // TODO: Get from storage
            last_index: raft.raft_log.last_index(),
            committed_index: raft.raft_log.committed,
            applied_index: raft.raft_log.applied,
            entry_count: raft.raft_log.last_index(),
            log_size_bytes: 0, // TODO: Calculate actual size
        })
    }

    async fn compact_log(&self, retain_index: u64) -> Result<(), Box<dyn Error>> {
        // TODO: Implement log compaction
        log::info!("Would compact log to index {}", retain_index);
        Err("Log compaction not implemented yet".into())
    }
}