use std::cmp::min;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{watch, Mutex};

use crate::raft::manager::RaftManager;
use crate::raft::rpc::pb;
use crate::raft::storage::hard_state::{DynHardStateStore, FileHardStateStore, MemHardStateStore};
use crate::raft::storage::snapshot::{DynSnapshotStore, FileSnapshotStore, MemSnapshotStore};
use crate::raft::storage::wal::{DynRaftLogStore, FileRaftLogStore, MemRaftLogStore};
use crate::raft::transport::RaftTransport;
use crate::raft::types::{
    GroupId, GroupSpec, HardState, LogIndex, NodeId, RaftCommand, RaftEntry, RaftError, RaftResult,
};

use super::transport::RaftRpcHandler;

const DEFAULT_HEARTBEAT_MS: u64 = 120;
const DEFAULT_ELECTION_MIN_MS: u64 = 300;
const DEFAULT_ELECTION_MAX_MS: u64 = 600;
const TICK_INTERVAL_MS: u64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

struct GroupState {
    role: Role,
    hard_state: HardState,
    leader_id: Option<NodeId>,
    log: Vec<RaftEntry>,
    last_applied: LogIndex,
    election_deadline: Instant,
    last_heartbeat: Instant,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
}

impl GroupState {
    fn new(hard_state: HardState, log: Vec<RaftEntry>, election_deadline: Instant) -> Self {
        Self {
            role: Role::Follower,
            hard_state,
            leader_id: None,
            log,
            last_applied: 0,
            election_deadline,
            last_heartbeat: Instant::now(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    fn last_log_index(&self) -> LogIndex {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn term_at(&self, index: LogIndex) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log.iter().find(|e| e.index == index).map(|e| e.term)
    }
}

struct GroupRuntime {
    spec: GroupSpec,
    state: Mutex<GroupState>,
    stop_tx: watch::Sender<bool>,
}

impl GroupRuntime {
    fn majority(&self) -> usize {
        self.spec.members.len() / 2 + 1
    }
}

pub struct CoreRaftManager {
    node_id: NodeId,
    transport: Arc<dyn RaftTransport>,
    log_store: DynRaftLogStore,
    hard_state_store: DynHardStateStore,
    snapshot_store: DynSnapshotStore,
    heartbeat_interval: Duration,
    election_min: Duration,
    election_max: Duration,
    groups: DashMap<GroupId, Arc<GroupRuntime>>,
}

impl CoreRaftManager {
    pub fn new(
        node_id: NodeId,
        transport: Arc<dyn RaftTransport>,
        log_store: DynRaftLogStore,
        hard_state_store: DynHardStateStore,
        snapshot_store: DynSnapshotStore,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            transport,
            log_store,
            hard_state_store,
            snapshot_store,
            heartbeat_interval: Duration::from_millis(DEFAULT_HEARTBEAT_MS),
            election_min: Duration::from_millis(DEFAULT_ELECTION_MIN_MS),
            election_max: Duration::from_millis(DEFAULT_ELECTION_MAX_MS),
            groups: DashMap::new(),
        })
    }

    pub fn with_memory(node_id: NodeId, transport: Arc<dyn RaftTransport>) -> Arc<Self> {
        Self::new(
            node_id,
            transport,
            MemRaftLogStore::new(),
            MemHardStateStore::new(),
            MemSnapshotStore::new(),
        )
    }

    pub fn with_durable_dir(
        node_id: NodeId,
        transport: Arc<dyn RaftTransport>,
        base_dir: impl AsRef<Path>,
    ) -> RaftResult<Arc<Self>> {
        let base = base_dir.as_ref();
        let wal = FileRaftLogStore::new(base.join("wal"))?;
        let hard_state = FileHardStateStore::new(base.join("hard_state"))?;
        let snapshot = FileSnapshotStore::new(base.join("snapshot"))?;
        Ok(Self::new(node_id, transport, wal, hard_state, snapshot))
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn force_tick(&self, group_id: GroupId) -> RaftResult<()> {
        let Some(group) = self.groups.get(&group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(group_id));
        };
        self.tick_group(group).await
    }

    pub async fn committed_index(&self, group_id: GroupId) -> RaftResult<LogIndex> {
        let Some(group) = self.groups.get(&group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(group_id));
        };
        let state = group.state.lock().await;
        Ok(state.hard_state.commit_index)
    }

    fn random_election_deadline(&self) -> Instant {
        let min_ms = self.election_min.as_millis() as u64;
        let max_ms = self.election_max.as_millis() as u64;
        let span = max_ms.saturating_sub(min_ms).max(1);
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        let offset = (now_ns ^ self.node_id) % span;
        Instant::now() + Duration::from_millis(min_ms + offset)
    }

    async fn tick_group(&self, group: Arc<GroupRuntime>) -> RaftResult<()> {
        let (role, election_deadline, last_heartbeat) = {
            let state = group.state.lock().await;
            (state.role, state.election_deadline, state.last_heartbeat)
        };

        match role {
            Role::Leader => {
                if last_heartbeat.elapsed() >= self.heartbeat_interval {
                    self.replicate_heartbeat(group.clone()).await?;
                    let mut state = group.state.lock().await;
                    state.last_heartbeat = Instant::now();
                }
            }
            Role::Follower | Role::Candidate => {
                if Instant::now() >= election_deadline {
                    self.start_election(group.clone()).await?;
                }
            }
        }
        Ok(())
    }

    fn spawn_group_ticker(self: Arc<Self>, group_id: GroupId, group: Arc<GroupRuntime>) {
        let group_for_loop = group.clone();
        let mut stop_rx = group.stop_tx.subscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(TICK_INTERVAL_MS));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if self.tick_group(group_for_loop.clone()).await.is_err() {
                            continue;
                        }
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
            let _ = group_id;
        });
    }

    async fn start_election(&self, group: Arc<GroupRuntime>) -> RaftResult<()> {
        let (group_id, peers, req, term, majority, hs_to_persist) = {
            let mut state = group.state.lock().await;
            state.role = Role::Candidate;
            state.hard_state.current_term += 1;
            state.hard_state.voted_for = Some(self.node_id);
            state.leader_id = None;
            state.election_deadline = self.random_election_deadline();
            let term = state.hard_state.current_term;
            let req = pb::VoteRequest {
                group_id: group.spec.group_id,
                term,
                candidate_id: self.node_id,
                last_log_index: state.last_log_index(),
                last_log_term: state.last_log_term(),
            };
            let peers = group
                .spec
                .members
                .iter()
                .copied()
                .filter(|n| *n != self.node_id)
                .collect::<Vec<_>>();
            (
                group.spec.group_id,
                peers,
                req,
                term,
                group.majority(),
                state.hard_state.clone(),
            )
        };
        self.hard_state_store.save(group_id, &hs_to_persist).await?;

        let mut grants = 1usize;
        let mut highest_term = term;
        for peer in peers {
            let resp = self.transport.send_vote(peer, req.clone()).await;
            if let Ok(resp) = resp {
                if resp.term > highest_term {
                    highest_term = resp.term;
                }
                if resp.vote_granted {
                    grants += 1;
                }
            }
        }

        let mut state = group.state.lock().await;
        if highest_term > state.hard_state.current_term {
            state.role = Role::Follower;
            state.hard_state.current_term = highest_term;
            state.hard_state.voted_for = None;
            state.election_deadline = self.random_election_deadline();
            let hs = state.hard_state.clone();
            drop(state);
            self.hard_state_store.save(group_id, &hs).await?;
            return Ok(());
        }
        if state.hard_state.current_term != term || state.role != Role::Candidate {
            return Ok(());
        }

        if grants >= majority {
            state.role = Role::Leader;
            state.leader_id = Some(self.node_id);
            let next = state.last_log_index() + 1;
            state.next_index = group
                .spec
                .members
                .iter()
                .copied()
                .filter(|n| *n != self.node_id)
                .map(|n| (n, next))
                .collect();
            state.match_index = group
                .spec
                .members
                .iter()
                .copied()
                .filter(|n| *n != self.node_id)
                .map(|n| (n, 0))
                .collect();
            state.last_heartbeat = Instant::now();
        } else {
            state.role = Role::Follower;
            state.election_deadline = self.random_election_deadline();
        }
        drop(state);

        if grants >= majority {
            self.replicate_heartbeat(group).await?;
        }
        Ok(())
    }

    async fn replicate_heartbeat(&self, group: Arc<GroupRuntime>) -> RaftResult<()> {
        let (peers, req, term) = {
            let state = group.state.lock().await;
            if state.role != Role::Leader {
                return Ok(());
            }
            let req = pb::AppendEntriesRequest {
                group_id: group.spec.group_id,
                term: state.hard_state.current_term,
                leader_id: self.node_id,
                prev_log_index: state.last_log_index(),
                prev_log_term: state.last_log_term(),
                entries: Vec::new(),
                leader_commit: state.hard_state.commit_index,
            };
            let peers = group
                .spec
                .members
                .iter()
                .copied()
                .filter(|n| *n != self.node_id)
                .collect::<Vec<_>>();
            (peers, req, state.hard_state.current_term)
        };

        for peer in peers {
            if let Ok(resp) = self.transport.send_append_entries(peer, req.clone()).await {
                if resp.term > term {
                    let mut save_hs: Option<HardState> = None;
                    let mut state = group.state.lock().await;
                    if resp.term > state.hard_state.current_term {
                        state.role = Role::Follower;
                        state.hard_state.current_term = resp.term;
                        state.hard_state.voted_for = None;
                        state.leader_id = Some(peer);
                        state.election_deadline = self.random_election_deadline();
                        save_hs = Some(state.hard_state.clone());
                    }
                    drop(state);
                    if let Some(hs) = save_hs {
                        self.hard_state_store.save(group.spec.group_id, &hs).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn replicate_entry(&self, group: Arc<GroupRuntime>, entry_index: LogIndex) -> RaftResult<usize> {
        let peers = group
            .spec
            .members
            .iter()
            .copied()
            .filter(|n| *n != self.node_id)
            .collect::<Vec<_>>();

        let mut success = 1usize;
        for peer in peers {
            match self.replicate_to_peer(group.clone(), peer, entry_index).await {
                Ok(true) => success += 1,
                Ok(false) => {}
                Err(RaftError::Transport(_)) => {}
                Err(RaftError::GroupNotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(success)
    }

    async fn replicate_to_peer(
        &self,
        group: Arc<GroupRuntime>,
        peer: NodeId,
        target_index: LogIndex,
    ) -> RaftResult<bool> {
        loop {
            let (req, current_term) = {
                let state = group.state.lock().await;
                if state.role != Role::Leader {
                    return Ok(false);
                }
                let next_index = *state.next_index.get(&peer).unwrap_or(&(state.last_log_index() + 1));
                let prev_log_index = next_index.saturating_sub(1);
                let prev_log_term = state.term_at(prev_log_index).unwrap_or(0);
                let entries = state
                    .log
                    .iter()
                    .filter(|e| e.index >= next_index && e.index <= target_index)
                    .map(|e| pb::Entry {
                        term: e.term,
                        index: e.index,
                        payload: e.payload.clone(),
                    })
                    .collect::<Vec<_>>();
                (
                    pb::AppendEntriesRequest {
                        group_id: group.spec.group_id,
                        term: state.hard_state.current_term,
                        leader_id: self.node_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: state.hard_state.commit_index,
                    },
                    state.hard_state.current_term,
                )
            };

            let resp = self.transport.send_append_entries(peer, req.clone()).await?;
            if resp.term > current_term {
                let mut save_hs: Option<HardState> = None;
                let mut state = group.state.lock().await;
                if resp.term > state.hard_state.current_term {
                    state.role = Role::Follower;
                    state.hard_state.current_term = resp.term;
                    state.hard_state.voted_for = None;
                    state.leader_id = Some(peer);
                    state.election_deadline = self.random_election_deadline();
                    save_hs = Some(state.hard_state.clone());
                }
                drop(state);
                if let Some(hs) = save_hs {
                    self.hard_state_store.save(group.spec.group_id, &hs).await?;
                }
                return Ok(false);
            }
            if resp.success {
                let mut state = group.state.lock().await;
                state.match_index.insert(peer, resp.match_index);
                state.next_index.insert(peer, resp.match_index + 1);
                return Ok(resp.match_index >= target_index);
            }

            let mut state = group.state.lock().await;
            let next = state.next_index.get(&peer).copied().unwrap_or(1);
            if next <= 1 {
                return Ok(false);
            }
            state.next_index.insert(peer, next - 1);
        }
    }

    async fn apply_committed(&self, group: Arc<GroupRuntime>) -> RaftResult<()> {
        let group_id = group.spec.group_id;
        let (entries_to_apply, commit_index) = {
            let mut state = group.state.lock().await;
            if state.last_applied >= state.hard_state.commit_index {
                return Ok(());
            }
            let start = state.last_applied + 1;
            let end = state.hard_state.commit_index;
            let entries = state
                .log
                .iter()
                .filter(|e| e.index >= start && e.index <= end)
                .cloned()
                .collect::<Vec<_>>();
            state.last_applied = end;
            (entries, end)
        };
        let _ = entries_to_apply;

        // Snapshot persistence hook, currently no-op except keeping latest commit watermark.
        let hs = {
            let state = group.state.lock().await;
            state.hard_state.clone()
        };
        self.hard_state_store.save(group_id, &hs).await?;

        // Keep clippy happy about future extension with snapshot state.
        let _ = self.snapshot_store.load_latest(group_id).await?;
        let _ = commit_index;
        Ok(())
    }

    async fn append_local_entry(&self, group: Arc<GroupRuntime>, cmd: RaftCommand) -> RaftResult<RaftEntry> {
        let (entry, hs) = {
            let mut state = group.state.lock().await;
            if state.role != Role::Leader {
                return Err(RaftError::NotLeader(group.spec.group_id));
            }
            let next_index = state.last_log_index() + 1;
            let payload = bincode::serialize(&cmd)
                .map_err(|e| RaftError::Internal(format!("serialize raft command failed: {e}")))?;
            let entry = RaftEntry {
                term: state.hard_state.current_term,
                index: next_index,
                payload,
            };
            state.log.push(entry.clone());
            (entry, state.hard_state.clone())
        };
        self.log_store.append(group.spec.group_id, &[entry.clone()]).await?;
        self.hard_state_store.save(group.spec.group_id, &hs).await?;
        Ok(entry)
    }

    async fn load_group_log(&self, group_id: GroupId) -> RaftResult<Vec<RaftEntry>> {
        let last = self.log_store.last_index(group_id).await?;
        if last == 0 {
            return Ok(Vec::new());
        }
        self.log_store.entries(group_id, 1, last + 1).await
    }
}

#[async_trait]
impl RaftManager for Arc<CoreRaftManager> {
    async fn create_group(&self, spec: GroupSpec) -> RaftResult<()> {
        if self.groups.contains_key(&spec.group_id) {
            return Ok(());
        }
        if !spec.members.contains(&self.node_id) {
            return Err(RaftError::Internal(format!(
                "local node {} is not in group {} members",
                self.node_id, spec.group_id
            )));
        }

        let hard_state = self
            .hard_state_store
            .load(spec.group_id)
            .await?
            .unwrap_or_default();
        let log = self.load_group_log(spec.group_id).await?;
        let (stop_tx, _stop_rx) = watch::channel(false);
        let runtime = Arc::new(GroupRuntime {
            spec: spec.clone(),
            state: Mutex::new(GroupState::new(hard_state, log, self.random_election_deadline())),
            stop_tx,
        });
        if spec.members.len() == 1 && spec.members[0] == self.node_id {
            let mut state = runtime.state.lock().await;
            state.role = Role::Leader;
            state.leader_id = Some(self.node_id);
            state.last_heartbeat = Instant::now();
        }
        self.groups.insert(spec.group_id, runtime.clone());
        self.clone().spawn_group_ticker(spec.group_id, runtime);
        Ok(())
    }

    async fn remove_group(&self, group_id: GroupId) -> RaftResult<()> {
        if let Some((_, group)) = self.groups.remove(&group_id) {
            let _ = group.stop_tx.send(true);
        }
        Ok(())
    }

    async fn propose(&self, group_id: GroupId, cmd: RaftCommand) -> RaftResult<LogIndex> {
        let Some(group) = self.groups.get(&group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(group_id));
        };
        self.tick_group(group.clone()).await?;
        {
            let state = group.state.lock().await;
            if state.role != Role::Leader {
                return Err(RaftError::NotLeader(group_id));
            }
        }

        let entry = self.append_local_entry(group.clone(), cmd).await?;
        let replicated = self.replicate_entry(group.clone(), entry.index).await?;
        if replicated < group.majority() {
            return Err(RaftError::Internal(format!(
                "replication quorum not reached for group {} entry {}",
                group_id, entry.index
            )));
        }

        let hs_to_persist = {
            let mut state = group.state.lock().await;
            state.hard_state.commit_index = state.hard_state.commit_index.max(entry.index);
            state.hard_state.clone()
        };
        self.hard_state_store.save(group_id, &hs_to_persist).await?;
        self.apply_committed(group).await?;
        let group = self
            .groups
            .get(&group_id)
            .map(|g| g.clone())
            .ok_or(RaftError::GroupNotFound(group_id))?;
        self.replicate_heartbeat(group).await?;
        Ok(entry.index)
    }

    async fn read_index(&self, group_id: GroupId) -> RaftResult<LogIndex> {
        let Some(group) = self.groups.get(&group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(group_id));
        };
        self.tick_group(group.clone()).await?;
        let state = group.state.lock().await;
        if state.role != Role::Leader {
            return Err(RaftError::NotLeader(group_id));
        }
        Ok(state.hard_state.commit_index)
    }

    async fn leader(&self, group_id: GroupId) -> RaftResult<Option<NodeId>> {
        let Some(group) = self.groups.get(&group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(group_id));
        };
        self.tick_group(group.clone()).await?;
        let state = group.state.lock().await;
        Ok(state.leader_id)
    }

    async fn members(&self, group_id: GroupId) -> RaftResult<Vec<NodeId>> {
        let Some(group) = self.groups.get(&group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(group_id));
        };
        Ok(group.spec.members.clone())
    }
}

#[async_trait]
impl RaftRpcHandler for CoreRaftManager {
    async fn handle_append_entries(
        &self,
        req: pb::AppendEntriesRequest,
    ) -> RaftResult<pb::AppendEntriesResponse> {
        let Some(group) = self.groups.get(&req.group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(req.group_id));
        };
        let mut should_apply = false;
        let mut response = pb::AppendEntriesResponse {
            term: 0,
            success: false,
            match_index: 0,
        };

        let (truncate_from, to_append, hs_to_persist) = {
            let mut truncate_from: Option<LogIndex> = None;
            let mut to_append: Vec<RaftEntry> = Vec::new();
            let mut state = group.state.lock().await;
            if req.term < state.hard_state.current_term {
                response.term = state.hard_state.current_term;
                response.match_index = state.last_log_index();
                return Ok(response);
            }

            if req.term > state.hard_state.current_term {
                state.hard_state.current_term = req.term;
                state.hard_state.voted_for = None;
            }
            state.role = Role::Follower;
            state.leader_id = Some(req.leader_id);
            state.election_deadline = self.random_election_deadline();

            if req.prev_log_index > 0 {
                let Some(term) = state.term_at(req.prev_log_index) else {
                    response.term = state.hard_state.current_term;
                    response.match_index = state.last_log_index();
                    return Ok(response);
                };
                if term != req.prev_log_term {
                    response.term = state.hard_state.current_term;
                    response.match_index = state.last_log_index();
                    return Ok(response);
                }
            }

            let incoming = req
                .entries
                .into_iter()
                .map(|e| RaftEntry {
                    term: e.term,
                    index: e.index,
                    payload: e.payload,
                })
                .collect::<Vec<_>>();
            if !incoming.is_empty() {
                for entry in &incoming {
                    if let Some(local) = state.log.iter().find(|x| x.index == entry.index) {
                        if local.term != entry.term {
                            truncate_from = Some(entry.index);
                            break;
                        }
                    }
                }
                if let Some(from) = truncate_from {
                    state.log.retain(|e| e.index < from);
                }

                for entry in incoming {
                    if state.log.iter().any(|e| e.index == entry.index) {
                        continue;
                    }
                    to_append.push(entry.clone());
                    state.log.push(entry);
                }
            }

            if req.leader_commit > state.hard_state.commit_index {
                state.hard_state.commit_index = min(req.leader_commit, state.last_log_index());
                should_apply = true;
            }
            let hs_to_persist = state.hard_state.clone();
            response.term = state.hard_state.current_term;
            response.success = true;
            response.match_index = state.last_log_index();
            (truncate_from, to_append, hs_to_persist)
        };

        if let Some(from) = truncate_from {
            self.log_store.truncate_suffix(req.group_id, from).await?;
        }
        if !to_append.is_empty() {
            self.log_store.append(req.group_id, &to_append).await?;
        }
        self.hard_state_store.save(req.group_id, &hs_to_persist).await?;

        if should_apply {
            self.apply_committed(group).await?;
        }
        Ok(response)
    }

    async fn handle_vote(&self, req: pb::VoteRequest) -> RaftResult<pb::VoteResponse> {
        let Some(group) = self.groups.get(&req.group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(req.group_id));
        };
        let mut hs_to_persist: Option<HardState> = None;
        let mut state = group.state.lock().await;
        if req.term < state.hard_state.current_term {
            return Ok(pb::VoteResponse {
                term: state.hard_state.current_term,
                vote_granted: false,
            });
        }

        if req.term > state.hard_state.current_term {
            state.hard_state.current_term = req.term;
            state.hard_state.voted_for = None;
            state.role = Role::Follower;
            state.leader_id = None;
        }

        let up_to_date = req.last_log_term > state.last_log_term()
            || (req.last_log_term == state.last_log_term()
                && req.last_log_index >= state.last_log_index());

        let can_vote = state.hard_state.voted_for.is_none()
            || state.hard_state.voted_for == Some(req.candidate_id);
        let granted = can_vote && up_to_date;

        if granted {
            state.hard_state.voted_for = Some(req.candidate_id);
            state.election_deadline = self.random_election_deadline();
            hs_to_persist = Some(state.hard_state.clone());
        }
        let response = pb::VoteResponse {
            term: state.hard_state.current_term,
            vote_granted: granted,
        };
        drop(state);
        if let Some(hs) = hs_to_persist {
            self.hard_state_store.save(req.group_id, &hs).await?;
        }
        Ok(response)
    }

    async fn handle_install_snapshot(
        &self,
        req: pb::InstallSnapshotRequest,
    ) -> RaftResult<pb::InstallSnapshotResponse> {
        let Some(group) = self.groups.get(&req.group_id).map(|g| g.clone()) else {
            return Err(RaftError::GroupNotFound(req.group_id));
        };
        let snap = req.snapshot.ok_or_else(|| {
            RaftError::Internal(format!("snapshot payload is missing for group {}", req.group_id))
        })?;

        let snapshot = crate::raft::types::SnapshotData {
            meta: crate::raft::types::SnapshotMeta {
                last_included_index: snap.meta.as_ref().map(|m| m.last_included_index).unwrap_or(0),
                last_included_term: snap.meta.as_ref().map(|m| m.last_included_term).unwrap_or(0),
            },
            data: snap.data,
        };
        self.snapshot_store.save_latest(req.group_id, &snapshot).await?;

        let mut state = group.state.lock().await;
        if req.term >= state.hard_state.current_term {
            state.hard_state.current_term = req.term;
            state.role = Role::Follower;
            state.leader_id = Some(req.leader_id);
            state.election_deadline = self.random_election_deadline();
            let hs_to_persist = state.hard_state.clone();
            let response = pb::InstallSnapshotResponse {
                term: state.hard_state.current_term,
                accepted: true,
            };
            drop(state);
            self.hard_state_store.save(req.group_id, &hs_to_persist).await?;
            return Ok(response);
        }
        Ok(pb::InstallSnapshotResponse {
            term: state.hard_state.current_term,
            accepted: false,
        })
    }
}
