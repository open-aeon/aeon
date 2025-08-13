use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Sleep};
use futures::future::pending;

use crate::common::metadata::TopicPartition;
use crate::error::consumer::ConsumerGroupError;
use crate::broker::consumer_group::{ConsumerGroup, ConsumerMember, GroupState, JoinGroupResult, LeaveGroupResult, HeartbeatResult, SyncGroupResult};

pub struct JoinGroupRequest {
    pub group_id: String,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub session_timeout: u64,
    pub rebalance_timeout: u64,
    pub topics: Vec<String>,
    pub supported_protocols: Vec<(String, Vec<u8>)>
}

pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

pub struct HeartbeatRequest {
    pub group_id: String,
    pub member_id: String,
}

pub struct CommitOffsetRequest {
    pub group_id: String,
    pub tp: TopicPartition,
    pub offset: i64,
}

pub struct FetchOffsetRequest {
    pub group_id: String,
    pub tp: TopicPartition,
}

pub struct SyncGroupRequest {
    pub group_id: String,
    pub member_id: String,
    pub generation_id: u32,
    pub assignment: HashMap<String, Vec<(String, Vec<u32>)>>,
}

pub enum CoordinatorCommand {
    JoinGroup{
        request: JoinGroupRequest,
        response_tx: oneshot::Sender<Result<JoinGroupResult, ConsumerGroupError>>,
    },
    LeaveGroup {
        request: LeaveGroupRequest,
        response_tx: oneshot::Sender<Result<LeaveGroupResult, ConsumerGroupError>>,
    },
    Heartbeat {
        request: HeartbeatRequest,
        response_tx: oneshot::Sender<Result<HeartbeatResult, ConsumerGroupError>>,
    },
    CommitOffset {
        request: CommitOffsetRequest,
    },
    FetchOffset {
        request: FetchOffsetRequest,
        response_tx: oneshot::Sender<Result<Option<i64>, ConsumerGroupError>>,
    },
    SyncGroup {
        request: SyncGroupRequest,
        response_tx: oneshot::Sender<Result<SyncGroupResult, ConsumerGroupError>>,
    },
}

pub struct GroupCoordinator {
    /// consumer_group state machine
    group: ConsumerGroup,
    
    /// channel to receive commands from the broker
    command_rx : mpsc::Receiver<CoordinatorCommand>,

    /// timer for rebalance
    rebalance_timer: Option<Pin<Box<Sleep>>>,

    /// store the join group request that is not yet processed
    pending_join_responders: HashMap<String, oneshot::Sender<Result<JoinGroupResult, ConsumerGroupError>>>,

    /// store the sync group request that is not yet processed
    pending_sync_responders: HashMap<String, oneshot::Sender<Result<SyncGroupResult, ConsumerGroupError>>>,

    /// 预期到齐的标识集合（按 instanceId 或 memberId）
    expected_ids: Option<HashSet<String>>,
    /// 是否按 instanceId 判断到齐（true 按 instanceId；false 按 memberId）
    expect_by_instance: bool,
    /// 本轮 join 已到达的标识集合
    pending_arrived_ids: HashSet<String>,
}

impl GroupCoordinator {
    pub fn new(group_id: String) -> mpsc::Sender<CoordinatorCommand> {
        let (tx, rx) = mpsc::channel(100);
        let coordinator = GroupCoordinator {
            group: ConsumerGroup::new(group_id),
            command_rx: rx,
            rebalance_timer: None,
            pending_join_responders: HashMap::new(),
            pending_sync_responders: HashMap::new(),
            expected_ids: None,
            expect_by_instance: false,
            pending_arrived_ids: HashSet::new(),
        };

        tokio::spawn(coordinator.run());
        tx
    }

    pub fn with_state(group: ConsumerGroup) -> mpsc::Sender<CoordinatorCommand> {
        let (tx, rx) = mpsc::channel(100);
        let coordinator = GroupCoordinator {
            group,
            command_rx: rx,
            rebalance_timer: None,
            pending_join_responders: HashMap::new(),
            pending_sync_responders: HashMap::new(),
            expected_ids: None,
            expect_by_instance: false,
            pending_arrived_ids: HashSet::new(),
        };

        tokio::spawn(coordinator.run());
        tx
    }

    async fn run(mut self) {
        println!("[Coordinator] Starting coordinator for group: {}", self.group.name);
        let mut heartbeat_check_timer = tokio::time::interval(Duration::from_millis(1000));

        loop {
            tokio::select! {
                Some(command) = self.command_rx.recv() => {
                    match command {
                        CoordinatorCommand::JoinGroup { request, response_tx } => {
                            self.handle_join_group(request, response_tx);
                        }
                        CoordinatorCommand::LeaveGroup { request, response_tx } => {
                            self.handle_leave_group(request, response_tx);
                        }
                        CoordinatorCommand::Heartbeat { request, response_tx } => {
                            self.handle_heartbeat(request, response_tx);
                        }
                        CoordinatorCommand::CommitOffset { request } => {
                            self.handle_commit_offset(request);
                        }
                        CoordinatorCommand::FetchOffset { request, response_tx } => {
                            self.handle_fetch_offset(request, response_tx);
                        }
                        CoordinatorCommand::SyncGroup { request, response_tx } => {
                            if self.handle_sync_group(request, response_tx) {
                                self.on_sync_complete();
                            }
                        }
                    }
                }

                _ = async { 
                    match self.rebalance_timer.as_mut() {
                        Some(t) => t.as_mut().await,
                        None => pending::<()>().await,
                    }
                 } => {
                    self.on_rebalance_timeout().await;
                }

                _ = heartbeat_check_timer.tick() => {
                    self.check_session_timeouts();
                }

                else => {
                    println!("[Coordinator] Shutting down for group '{}'.", self.group.name);
                    break;
                }
            }
        }
    }

    fn handle_join_group(&mut self, request: JoinGroupRequest, response_tx: oneshot::Sender<Result<JoinGroupResult, ConsumerGroupError>>) {
        let member_id = request.member_id.clone();
        let member = ConsumerMember {
            id: member_id.clone(),
            group_instance_id: request.group_instance_id.clone(),
            session_timeout: Duration::from_millis(request.session_timeout),
            rebalance_timeout: Duration::from_millis(request.rebalance_timeout),
            topics: request.topics,
            supported_protocols: request.supported_protocols,
            last_heartbeat: Instant::now(),
            assignment: vec![],
        };
        // 若尚未进入 PreparingRebalance，则初始化预期集合（优先使用静态成员 instanceId，否则用 memberId）
        if self.group.state != GroupState::PreparingRebalance {
            let prev_static: HashSet<String> = self.group.get_members().values()
                .filter_map(|m| m.group_instance_id.clone())
                .collect();
            if !prev_static.is_empty() {
                self.expect_by_instance = true;
                self.expected_ids = Some(prev_static);
            } else {
                self.expect_by_instance = false;
                let prev_members: HashSet<String> = self.group.get_members().keys().cloned().collect();
                self.expected_ids = if prev_members.is_empty() { None } else { Some(prev_members) };
            }
            self.pending_arrived_ids.clear();
            self.group.transition_to(GroupState::PreparingRebalance);
        }

        self.group.add_member(member);
        self.pending_join_responders.insert(member_id.clone(), response_tx);

        // 启动或缩短一个很小的去抖窗口（不超过客户端给的上限）
        let debounce = Duration::from_millis(200);
        let max_timeout = Duration::from_millis(request.rebalance_timeout);
        let timeout = if debounce < max_timeout { debounce } else { max_timeout };
        if self.rebalance_timer.is_none() {
            self.rebalance_timer = Some(Box::pin(sleep(timeout)));
            println!("[Coordinator] Group '{}' rebalance started. Timeout: {:?}", self.group.name, timeout);
        }

        // 记录本次到达的标识
        let arrived_id = if self.expect_by_instance {
            request.group_instance_id.clone().unwrap_or_default()
        } else {
            // 使用 pending_join_responders 中的键而非已移动的 member_id
            // 此时 member_id 已被移动进 map，改用请求中的 id 字符串
            member_id.clone()
        };
        if !arrived_id.is_empty() {
            self.pending_arrived_ids.insert(arrived_id);
        }

        // 预期集合已全部到达则提前完成
        if let Some(expected) = &self.expected_ids {
            if expected.is_subset(&self.pending_arrived_ids) {
                self.rebalance_timer = Some(Box::pin(sleep(Duration::from_millis(0))));
            }
        }
    }

    fn handle_leave_group(&mut self, request: LeaveGroupRequest, response_tx: oneshot::Sender<Result<LeaveGroupResult, ConsumerGroupError>>) {
        println!("[Coordinator] Member '{}' left group '{}'.", self.group.name, self.group.name);
        self.group.remove_member(&request.member_id);
        let _ = response_tx.send(Ok(LeaveGroupResult {}));

        self.trigger_rebalance_if_needed();
    }

    fn handle_heartbeat(&mut self, request: HeartbeatRequest, response_tx: oneshot::Sender<Result<HeartbeatResult, ConsumerGroupError>>) {
        if self.group.heartbeat(&request.member_id) {
            let _ = response_tx.send(Ok(HeartbeatResult {}));
        } else {
            let _ = response_tx.send(Err(ConsumerGroupError::MemberNotFound(request.member_id)));
        }
    }

    fn handle_commit_offset(&mut self, request: CommitOffsetRequest) {
        self.group.commit_offset(request.tp, request.offset);
    }

    fn handle_fetch_offset(&mut self, request: FetchOffsetRequest, response_tx: oneshot::Sender<Result<Option<i64>, ConsumerGroupError>>) {
        let offset = self.group.fetch_offset(&request.tp);
        let _ = response_tx.send(Ok(offset));
    }

    fn handle_sync_group(&mut self, request: SyncGroupRequest, response_tx: oneshot::Sender<Result<SyncGroupResult, ConsumerGroupError>>) -> bool {
        self.pending_sync_responders.insert(request.member_id.clone(), response_tx);

        if !request.assignment.is_empty() {
            if let Err(e) = self.group.apply_assignment(request.member_id, request.generation_id, request.assignment) {
                for(_, responder) in self.pending_sync_responders.drain() {
                    let _ = responder.send(Err(e.clone()));
                }
                return false;
            }
        }

        self.group.all_members_synced(&self.pending_sync_responders)
    }

    async fn on_rebalance_timeout(&mut self) {
        println!("[Coordinator] Rebalance started for group '{}'.", self.group.name);

        let results = self.group.complete_join_phase();

        match results {
            Ok(results_map) => {
                for(member_id, responder) in self.pending_join_responders.drain() {
                    if let Some(result) = results_map.get(&member_id) {
                        let _ = responder.send(Ok(result.clone()));
                    } else {
                        let _ = responder.send(Err(ConsumerGroupError::MemberNotFound(member_id)));
                    }
                }
            }
            Err(e) => {
                for(_, responder) in self.pending_join_responders.drain() {
                    let _ = responder.send(Err(e.clone()));
                }
            }
        }

        self.rebalance_timer = None;
        println!("[Coordinator] Rebalance completed for group '{}'.", self.group.name);
    }

    fn on_sync_complete(&mut self) {
        println!("[Coordinator] Sync completed for group '{}'.", self.group.name);
        let assignments = self.group.get_all_assignments();

        for (member_id, responder) in self.pending_sync_responders.drain() {
            let member_assignment = assignments.get(&member_id).cloned().unwrap_or_default();
            let result = SyncGroupResult {
                assignment: member_assignment,
            };
            let _ = responder.send(Ok(result));
        }

        self.group.transition_to(GroupState::Stable);
    }

    fn check_session_timeouts(&mut self) {
        let now = Instant::now();
        
        let timeout_member_ids: Vec<String> = self.group.get_members().values()
            .filter(|m| now.duration_since(m.last_heartbeat) > m.session_timeout)
            .map(|m| m.id.clone()).collect();

        if timeout_member_ids.is_empty() {
            return;
        }

        println!("[Coordinator] Session timeout for members: {:?}", timeout_member_ids);

        for member_id in timeout_member_ids {
            self.group.remove_member(&member_id);
        }

        self.trigger_rebalance_if_needed();
    }

    fn trigger_rebalance_if_needed(&mut self) {
        if self.group.state == GroupState::CompletingRebalance && self.rebalance_timer.is_none() {
            self.fail_all_pending_responders(ConsumerGroupError::RebalanceInProgress);

            let timeout = Duration::from_millis(3000);
            self.rebalance_timer = Some(Box::pin(sleep(timeout)));
            println!("[Coordinator] Group '{}' rebalance started. Timeout: {:?}", self.group.name, timeout);
        }
    }

    fn fail_all_pending_responders(&mut self, error: ConsumerGroupError) {
        for(_, responder) in self.pending_join_responders.drain() {
            let _ = responder.send(Err(error.clone()));
        }
        for(_, responder) in self.pending_sync_responders.drain() {
            let _ = responder.send(Err(error.clone()));
        }
    }
}