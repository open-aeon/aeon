mod round_robin;

use std::collections::HashMap;

use crate::common::metadata::{TopicPartition, TopicMetadata};
use crate::broker::consumer_group::ConsumerMember;

pub struct AssignmentContext<'a> {
    pub members: &'a HashMap<String, ConsumerMember>,
    pub topics: &'a HashMap<String, TopicMetadata>,
}

pub type AssignmentResult = HashMap<String, Vec<TopicPartition>>;

pub trait PartitionAssignor {
    fn assign(&self, context: &AssignmentContext) -> AssignmentResult;
    fn name(&self) -> &str;
}