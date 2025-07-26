use std::{collections::{HashMap, HashSet}, time::{Duration, Instant}};
use anyhow::Result;

use crate::{common::metadata::{TopicMetadata, TopicPartition}};
use crate::error::consumer::ConsumerGroupError;
use crate::protocol::response::{JoinGroupResult, MemberInfo};

#[derive(Debug, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

#[derive(Debug, Clone)]
pub struct ConsumerMember {
    pub id: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration, // todo: 为什么要这个字段？
    pub topics: Vec<String>,
    pub supported_protocols: Vec<(String,Vec<u8>)>,
    pub last_heartbeat: Instant,
    pub assignment: Vec<TopicPartition>,
}

#[derive(Debug)]
pub struct ConsumerGroup {
    pub name: String,
    pub state: GroupState,
    members: HashMap<String, ConsumerMember>,
    offsets: HashMap<TopicPartition, i64>, //todo: 是否保留？
    protocol: Option<String>,
    generation_id: u32,
    leader_id: Option<String>,
}

impl ConsumerGroup {
    pub fn new(name: String) -> Self {
        Self {
            name,
            state: GroupState::Empty,
            members: HashMap::new(),
            offsets: HashMap::new(),
            protocol: None,
            generation_id: 0,
            leader_id: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn next_generation(&self) -> u32{
        self.generation_id + 1
    }

    pub fn commit_offset(&mut self, tp: TopicPartition, offset: i64) {
        self.offsets.insert(tp, offset);
    }

    pub fn fetch_offset(&self, tp: &TopicPartition) -> Option<i64> {
        self.offsets.get(tp).copied()
    }

    pub fn add_member(&mut self, member: ConsumerMember) {
        self.members.insert(member.id.clone(), member);
    }

    pub fn remove_member(&mut self, member_id: &str) -> Option<ConsumerMember> {
        let member = self.members.remove(member_id);
        if member.is_some() {
            self.state = GroupState::PreparingRebalance;
        }
        if self.members.is_empty() {
            self.state = GroupState::Empty;
        }
        member
    }

    pub fn get_member(&self, member_id: &str) -> Option<&ConsumerMember> {
        self.members.get(member_id)
    }

    pub fn get_members(&self) -> &HashMap<String, ConsumerMember> {
        &self.members
    }

    pub fn heartbeat(&mut self, member_id: &str) -> Result<()> {
        if let Some(member) = self.members.get_mut(member_id) {
            member.last_heartbeat = Instant::now();
        }
        Ok(())
    }

    pub fn complete_join_phase(&mut self) -> Result<HashMap<String, JoinGroupResult>, ConsumerGroupError> {
        if self.state != GroupState::PreparingRebalance {
            return Err(ConsumerGroupError::InvalidState);
        }

        if self.is_empty() {
            self.state = GroupState::Empty;
            self.leader_id = None;
            self.protocol = None;
            return Ok(HashMap::new());
        }

        self.elect_leader()?;

        let leader_id = self.leader_id.as_ref().unwrap().clone();
        let protocol = self.protocol.as_ref().unwrap().clone();

        let leader_member_info : Vec<MemberInfo> = self.members.values()
            .map(|m| {
                let metadata = m.supported_protocols.iter()
                    .find(|(p, _)| p == &protocol)
                    .map(|(_, v)| v.clone()).unwrap_or_default();

                MemberInfo {
                    id: m.id.clone(),
                    metadata,
                }
            })
            .collect();

        let mut results = HashMap::new();
        
        for member_id in self.members.keys() {
            let is_leader = member_id == &leader_id;

            let result = JoinGroupResult {
                members: if is_leader { leader_member_info.clone() } else { vec![] },
                member_id: member_id.clone(),
                generation_id: self.generation_id,
                protocol: Some(protocol.clone()),
                leader_id: leader_id.clone(),
            };
            results.insert(member_id.clone(), result);
        }

        self.state = GroupState::CompletingRebalance;

        Ok(results)
    }

    fn elect_leader(&mut self) -> Result<(), ConsumerGroupError> {
        let leader_id = match self.members.keys().next() {
            Some(id) => id.clone(),
            None => return Ok(()),
        };

        self.leader_id = Some(leader_id.clone());

        let leader = self.members.get(&leader_id).unwrap();
        let mut protocol_chosen = None;

        for(protocol_name, _) in &leader.supported_protocols {
            let all_members_support = self.members.values().all(|m| {
                m.supported_protocols.iter().any(|(p, _)| p == protocol_name)
            });

            if all_members_support {
                protocol_chosen = Some(protocol_name.clone());
                break;
            }
        }

        if let Some(protocol) = protocol_chosen {
            self.protocol = Some(protocol);
            Ok(())
        } else {
            self.leader_id = None;
            Err(ConsumerGroupError::NoCommonProtocol)
        }
    }
}