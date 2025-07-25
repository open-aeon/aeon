use std::{collections::{HashMap, HashSet}, time::{Duration, Instant}};
use anyhow::Result;

use crate::common::metadata::{TopicMetadata, TopicPartition};
use crate::broker::assignment::{AssignmentContext, AssignmentResult, PartitionAssignor};

#[derive(Debug, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    Rebalancing,
    Stable,
}

#[derive(Debug, Clone)]
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
    pub topics: HashSet<String>,
    pub offsets: HashMap<TopicPartition, i64>,
    pub state: GroupState,
    pub generation_id: u32,
    pub strategy: String,
}

impl ConsumerGroup {
    pub fn new(name: String) -> Self {
        Self {
            name,
            members: HashMap::new(),
            topics: HashSet::new(),
            offsets: HashMap::new(),
            state: GroupState::Empty,
            generation_id: 0,
            strategy: "roundrobin".to_string(),
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

    pub fn rebalance(&mut self) {
        if !self.members.is_empty() {
            self.state = GroupState::PreparingRebalance;
        } else {
            self.state = GroupState::Empty;
        }

        self.generation_id += 1;
    }

    pub fn assign(&mut self, assignor: &dyn PartitionAssignor, topic_metadata: &HashMap<String, TopicMetadata>) {
        println!(
            "Performing assignment for group '{}' with strategy '{}'", 
            self.name, assignor.name()
        );

        let context = AssignmentContext {
            members: &self.members,
            topics: topic_metadata,
        };

        let result = assignor.assign(&context);

        for member in self.members.values_mut() {
            member.assignment.clear();
        }

        for (member_id, partitions) in result {
            if let Some(member) = self.members.get_mut(&member_id) {
                member.assignment = partitions;
            }
        }

        self.state = GroupState::Stable;
    }
}