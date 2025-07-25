use super::{AssignmentResult, AssignmentContext, PartitionAssignor};
use crate::common::metadata::TopicPartition;
use std::collections::HashMap;

#[derive(Default)]
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &str {
        "roundrobin"
    }

    fn assign(&self, context: &AssignmentContext) -> AssignmentResult {
        let mut assignment: AssignmentResult = HashMap::new();
        if context.members.is_empty() {
            return assignment;
        }

        // 1. 获取并排序所有成员 ID，以保证确定性
        let mut members: Vec<_> = context.members.keys().collect();
        members.sort();
        for member_id in &members {
            assignment.insert(member_id.to_string(), Vec::new());
        }

        // 2. 获取并排序所有分区，以保证确定性
        let mut all_partitions = Vec::new();
        let mut sorted_topics: Vec<_> = context.topics.keys().collect();
        sorted_topics.sort();

        for topic_name in sorted_topics {
            if let Some(metadata) = context.topics.get(topic_name) {
                for i in 0..metadata.partition_count { // 假设 TopicMetadata 有 partition_count
                    all_partitions.push(TopicPartition {
                        topic: topic_name.clone(),
                        partition: i,
                    });
                }
            }
        }
        
        // 3. 轮询分配
        let mut member_iter = members.iter().cycle();
        for partition in all_partitions {
            if let Some(member_id) = member_iter.next() {
                if let Some(partitions) = assignment.get_mut(*member_id) {
                    partitions.push(partition);
                }
            }
        }
        
        assignment
    }
}
