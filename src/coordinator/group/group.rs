use std::collections::HashMap;
use std::time::Instant;

use log::{debug, info};
use tokio::sync::oneshot;

use crate::common::metadata::TopicPartition;
use crate::error::consumer::ConsumerGroupError;

use super::{ConsumerMember, GroupState, JoinGroupResult, MemberInfo, SyncGroupResult};

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
        if let Some(ref instance_id) = member.group_instance_id {
            if let Some((old_id, _old_member)) = self.members.iter()
                .find(|(_, m)| m.group_instance_id.as_ref() == Some(instance_id))
                .map(|(k, v)| (k.clone(), v.clone())) {
                let preserved_assignment = _old_member.assignment.clone();
                self.members.remove(&old_id);
                let mut new_member = member;
                if new_member.assignment.is_empty() {
                    new_member.assignment = preserved_assignment;
                }
                self.members.insert(new_member.id.clone(), new_member);
                return;
            }
        }
        info!("[ConsumerGroup] Adding member '{}' to group '{}'.", member.id, self.name);
        self.members.insert(member.id.clone(), member);
        debug!("[ConsumerGroup] Members: {:?}", self.members);
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

    pub fn heartbeat(&mut self, member_id: &str) -> bool {
        if let Some(member) = self.members.get_mut(member_id) {
            member.last_heartbeat = Instant::now();
            true
        } else {
            false
        }
    }

    pub fn complete_join_phase(&mut self) -> std::result::Result<HashMap<String, JoinGroupResult>, ConsumerGroupError> {
        if self.state != GroupState::PreparingRebalance {
            return Err(ConsumerGroupError::InvalidState);
        }

        if self.is_empty() {
            self.state = GroupState::Empty;
            self.leader_id = None;
            self.protocol = None;
            return Ok(HashMap::new());
        }

        self.generation_id = self.generation_id + 1;

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
                generation_id: self.generation_id,
                member_id: member_id.clone(),
                members: if is_leader { leader_member_info.clone() } else { vec![] },
                protocol: self.protocol.clone(),
                leader_id: leader_id.clone(),
            };
            results.insert(member_id.clone(), result);
        }
        self.state = GroupState::CompletingRebalance;

        Ok(results)
    }

    pub fn elect_leader(&mut self) -> std::result::Result<(), ConsumerGroupError> {
        // Step 1: Select leader (first member)
        let leader_id = match self.members.keys().next() {
            Some(id) => id.clone(),
            None => return Err(ConsumerGroupError::InvalidState),
        };

        // Step 2: Select common protocol
        let leader = self.members.get(&leader_id).unwrap();
        for (protocol, _) in &leader.supported_protocols {
            let all_members_support = self.members.values().all(|m| {
                m.supported_protocols.iter().any(|(p, _)| p == protocol)
            });
            if all_members_support {
                self.protocol = Some(protocol.clone());
                break;
            }
        }

        if self.protocol.is_none() {
            return Err(ConsumerGroupError::NoCommonProtocol);
        }

        self.leader_id = Some(leader_id);
        Ok(())
    }

    pub fn assignment_snapshot(&self) -> HashMap<String, Vec<(String, Vec<u32>)>> {
        self.members.iter().map(|(m_id, m)| (m_id.clone(), m.assignment.clone())).collect()
    }

    pub fn apply_assignment(
        &mut self,
        leader_id: String,
        generation_id: u32,
        assignments: HashMap<String, Vec<(String, Vec<u32>)>>,
    ) -> Result<(), ConsumerGroupError> {
        if generation_id != self.generation_id {
            return Err(ConsumerGroupError::InvalidGeneration(generation_id, self.generation_id));
        }

        if self.leader_id.as_ref() != Some(&leader_id) {
            return Err(ConsumerGroupError::NotLeader(leader_id));
        }

        for (member_id, assignment) in assignments {
            if let Some(member) = self.members.get_mut(&member_id) {
                member.assignment = assignment;
            }
        }

        Ok(())
    }

    pub fn get_all_assignments(&self) -> HashMap<String, Vec<(String, Vec<u32>)>> {
        self.members
            .iter()
            .map(|(m_id, m)| (m_id.clone(), m.assignment.clone()))
            .collect()
    }

    pub fn transition_to(&mut self, state: GroupState) {
        self.state = state;
    }

    pub fn all_members_synced(&self, pending_sync_responders: &HashMap<String, oneshot::Sender<Result<SyncGroupResult, ConsumerGroupError>>>) -> bool {
        self.members.len() == pending_sync_responders.len()
    }
}
