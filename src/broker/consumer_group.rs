use std::{collections::HashMap, time::{Duration, Instant}};
use anyhow::Result;

use crate::common::metadata::TopicPartition;

#[derive(Debug)]
enum GroupState {
    Pending,
    Rebalancing,
    Stable,
}

#[derive(Debug)]
pub struct ConsumerMember {
    pub id: String,
    pub session_timeout: Duration,
    pub last_heartbeat: Instant,
    pub assignment: Vec<TopicPartition>,
}

#[derive(Debug)]
pub struct ConsumerGroup {
    pub name: String,
    pub members: HashMap<String, ConsumerMember>,
    pub offsets: HashMap<TopicPartition, i64>,
    pub state: GroupState,
    pub leader_id: Option<String>,
    pub generation_id: u32,
}

impl ConsumerGroup {
    pub fn new(name: String) -> Self {
        Self {
            name,
            members: HashMap::new(),
            offsets: HashMap::new(),
            state: GroupState::Pending,
            leader_id: None,
            generation_id: 0,
        }
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
        self.members.remove(member_id)
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

    pub fn leader_election(&mut self) -> Result<String> {
        todo!()
    }

    pub fn rebalance(&mut self) -> Result<u32> {
        todo!()
    }
}