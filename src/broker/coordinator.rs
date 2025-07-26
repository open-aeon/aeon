use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Sleep};

use crate::error::consumer::ConsumerGroupError;
use crate::protocol::response::JoinGroupResult;
use crate::broker::consumer_group::{ConsumerGroup, ConsumerMember};

pub struct JoinGroupRequest {
    pub group_id: String,
    pub member_id: String,
    pub session_timeout: u64,
    pub rebalance_timeout: u64,
    pub topics: Vec<String>,
    pub supported_protocols: Vec<(String, Vec<u8>)>
}

pub enum CorrdinatorCommand {
    JoinGroup{
        request: JoinGroupRequest,
        response_tx: oneshot::Sender<Result<JoinGroupResult, ConsumerGroupError>>,
    },
}

pub struct GroupCoordinator {
    /// consumer_group state machine
    group: ConsumerGroup,
    
    /// channel to receive commands from the broker
    command_rx : mpsc::Receiver<CorrdinatorCommand>,

    /// timer for rebalance
    rebalance_timer: Option<Pin<Box<Sleep>>>,

    /// store the join group request that is not yet processed
    pending_join_responder: HashMap<String, oneshot::Sender<Result<JoinGroupResult, ConsumerGroupError>>>,
}

impl GroupCoordinator {
    pub fn new(group_id: String) -> mpsc::Sender<CorrdinatorCommand> {
        let (tx, rx) = mpsc::channel(100);
        let coordinator = GroupCoordinator {
            group: ConsumerGroup::new(group_id),
            command_rx: rx,
            rebalance_timer: None,
            pending_join_responder: HashMap::new(),
        };

        tokio::spawn(coordinator.run());
        tx
    }

    pub fn with_state(group: ConsumerGroup) -> mpsc::Sender<CorrdinatorCommand> {
        let (tx, rx) = mpsc::channel(100);
        let coordinator = GroupCoordinator {
            group,
            command_rx: rx,
            rebalance_timer: None,
            pending_join_responder: HashMap::new(),
        };

        tokio::spawn(coordinator.run());
        tx
    }

    async fn run(mut self) {
        println!("[Coordinator] Starting coordinator for group: {}", self.group.name);

        loop {
            tokio::select! {
                Some(command) = self.command_rx.recv() => {
                    match command {
                        CorrdinatorCommand::JoinGroup { request, response_tx } => {
                            self.handle_join_group(request, response_tx);
                        }
                    }
                }

                _ = async { self.rebalance_timer.as_mut().unwrap().await } => {
                    self.on_rebalance_timeout().await;
                }

                else => {
                    println!("[Coordinator] Shutting down for group '{}'.", self.group.name);
                    break;
                }
            }
        }
    }

    fn handle_join_group(&mut self, request: JoinGroupRequest, response_tx: oneshot::Sender<Result<JoinGroupResult, ConsumerGroupError>>) {
        let member_id = request.member_id;
        let member = ConsumerMember {
            id: member_id.clone(),
            session_timeout: Duration::from_millis(request.session_timeout),
            rebalance_timeout: Duration::from_millis(request.rebalance_timeout),
            topics: request.topics,
            supported_protocols: request.supported_protocols,
            last_heartbeat: Instant::now(),
            assignment: vec![],
        };
        self.group.add_member(member);
        self.pending_join_responder.insert(member_id, response_tx);

        if self.rebalance_timer.is_none() {
            let timeout = Duration::from_millis(request.rebalance_timeout);
            self.rebalance_timer = Some(Box::pin(sleep(timeout)));
            println!("[Coordinator] Group '{}' rebalance started. Timeout: {:?}", self.group.name, timeout);
        }
    }

    async fn on_rebalance_timeout(&mut self) {
        println!("[Coordinator] Rebalance started for group '{}'.", self.group.name);

        let results = self.group.complete_join_phase();

        match results {
            Ok(results_map) => {
                for(member_id, responder) in self.pending_join_responder.drain() {
                    if let Some(result) = results_map.get(&member_id) {
                        let _ = responder.send(Ok(result.clone()));
                    } else {
                        let _ = responder.send(Err(ConsumerGroupError::MemberNotFound(member_id)));
                    }
                }
            }
            Err(e) => {
                for(_, responder) in self.pending_join_responder.drain() {
                    let _ = responder.send(Err(e.clone()));
                }
            }
        }

        self.rebalance_timer = None;
        println!("[Coordinator] Rebalance completed for group '{}'.", self.group.name);
    }
}