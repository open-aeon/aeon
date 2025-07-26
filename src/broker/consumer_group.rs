use std::{collections::{HashMap, HashSet}, time::{Duration, Instant}};
use anyhow::Result;

use crate::{common::metadata::{TopicMetadata, TopicPartition}};
use crate::error::protocol::ProtocolError;
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

    pub fn add_member(&mut self, member: ConsumerMember) -> bool {
        let is_new_member = !self.members.contains_key(&member.id);
        self.members.insert(member.id.clone(), member);
        is_new_member
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
}