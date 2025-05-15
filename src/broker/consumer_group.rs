use std::{
    collections::HashMap,
    sync::Arc,
    path::PathBuf,
};
use tokio::sync::RwLock;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMetadata {
    pub group_id: String,
    pub offsets: HashMap<String, HashMap<i32, u64>>, // topic -> partition -> offset
    pub consumers: HashMap<String, Vec<String>>, // group_id -> consumer_ids
    pub assignments: HashMap<String, Vec<i32>>, // consumer_id -> partitions
}

#[derive(Clone)]
pub struct ConsumerGroupManager {
    groups: Arc<RwLock<HashMap<String, ConsumerGroupMetadata>>>,
    data_dir: PathBuf,
}

impl ConsumerGroupManager {
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        let groups = Arc::new(RwLock::new(HashMap::new()));
        Ok(Self { groups, data_dir })
    }

    pub async fn get_offset(&self, group_id: &str, topic: &str, partition: i32) -> Result<u64> {
        let groups = self.groups.read().await;
        if let Some(group) = groups.get(group_id) {
            if let Some(topic_offsets) = group.offsets.get(topic) {
                if let Some(&offset) = topic_offsets.get(&partition) {
                    return Ok(offset);
                }
            }
        }
        Ok(0) // 如果没有找到，返回0
    }

    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: u64,
        consumer_id: &str,
    ) -> Result<()> {
        let mut groups = self.groups.write().await;
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroupMetadata {
                group_id: group_id.to_string(),
                offsets: HashMap::new(),
                consumers: HashMap::new(),
                assignments: HashMap::new(),
            });

        // 更新偏移量
        let topic_offsets = group
            .offsets
            .entry(topic.to_string())
            .or_insert_with(HashMap::new);
        topic_offsets.insert(partition, offset);

        // 更新消费者列表
        let consumers = group
            .consumers
            .entry(group_id.to_string())
            .or_insert_with(Vec::new);
        if !consumers.contains(&consumer_id.to_string()) {
            consumers.push(consumer_id.to_string());
        }

        // 保存到文件
        self.save_metadata(group_id)?;

        Ok(())
    }

    fn save_metadata(&self, group_id: &str) -> Result<()> {
        let groups = self.groups.blocking_read();
        if let Some(group) = groups.get(group_id) {
            let file_path = self.data_dir.join(format!("{}.json", group_id));
            let json = serde_json::to_string_pretty(group)?;
            fs::write(file_path, json)?;
        }
        Ok(())
    }

    pub async fn load_metadata(&self) -> Result<()> {
        let mut groups = self.groups.write().await;
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
                let file_content = fs::read_to_string(entry.path())?;
                let group: ConsumerGroupMetadata = serde_json::from_str(&file_content)?;
                groups.insert(group.group_id.clone(), group);
            }
        }
        Ok(())
    }

    pub async fn assign_partitions(
        &self,
        group_id: &str,
        topic: &str,
        consumer_id: &str,
        partition_count: i32,
    ) -> Result<Vec<i32>> {
        let mut groups = self.groups.write().await;
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroupMetadata {
                group_id: group_id.to_string(),
                offsets: HashMap::new(),
                consumers: HashMap::new(),
                assignments: HashMap::new(),
            });

        // 获取消费者组中的所有消费者
        let consumers = group
            .consumers
            .entry(group_id.to_string())
            .or_insert_with(Vec::new);
        
        if !consumers.contains(&consumer_id.to_string()) {
            consumers.push(consumer_id.to_string());
        }

        // 计算每个消费者应该分配的分区数
        let consumer_count = consumers.len();
        let partitions_per_consumer = partition_count / consumer_count as i32;
        let extra_partitions = partition_count % consumer_count as i32;

        // 找到当前消费者在列表中的位置
        let consumer_index = consumers
            .iter()
            .position(|id| id == consumer_id)
            .unwrap_or(0);

        // 计算分配给当前消费者的分区
        let mut assigned_partitions = Vec::new();
        let mut current_partition = 0;

        for i in 0..consumer_count {
            let partitions_to_assign = partitions_per_consumer + if i < extra_partitions as usize { 1 } else { 0 };
            
            if i == consumer_index {
                for _ in 0..partitions_to_assign {
                    assigned_partitions.push(current_partition);
                    current_partition += 1;
                }
            } else {
                current_partition += partitions_to_assign;
            }
        }

        // 更新分配信息
        let topic_assignments = group
            .assignments
            .entry(consumer_id.to_string())
            .or_insert_with(Vec::new);
        
        // 移除该主题的旧分配
        topic_assignments.retain(|&p| p >= partition_count);
        // 添加新的分配
        topic_assignments.extend(assigned_partitions.clone());
        
        // 保存到文件
        self.save_metadata(group_id)?;

        println!("消费者组 {} 中消费者 {} 被分配主题 {} 的分区: {:?}", 
            group_id, consumer_id, topic, assigned_partitions);

        Ok(assigned_partitions)
    }

    pub async fn get_assigned_partitions(
        &self,
        group_id: &str,
        consumer_id: &str,
    ) -> Result<Vec<i32>> {
        let groups = self.groups.read().await;
        if let Some(group) = groups.get(group_id) {
            if let Some(partitions) = group.assignments.get(consumer_id) {
                return Ok(partitions.clone());
            }
        }
        Ok(Vec::new())
    }

    pub async fn rebalance_group(&self, group_id: &str) -> Result<()> {
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            // 获取所有活跃的消费者
            let consumers = group.consumers.get(group_id).cloned().unwrap_or_default();
            if consumers.is_empty() {
                return Ok(());
            }

            // 获取所有主题的分区数
            let mut topic_partitions = HashMap::new();
            for (topic, offsets) in &group.offsets {
                let partition_count = offsets.len() as i32;
                topic_partitions.insert(topic.clone(), partition_count);
            }

            // 为每个主题重新分配分区
            for (topic, partition_count) in topic_partitions {
                let consumer_count = consumers.len();
                let partitions_per_consumer = partition_count / consumer_count as i32;
                let extra_partitions = partition_count % consumer_count as i32;

                let mut current_partition = 0;
                for (i, consumer_id) in consumers.iter().enumerate() {
                    let partitions_to_assign = partitions_per_consumer + if i < extra_partitions as usize { 1 } else { 0 };
                    
                    // 计算分配给当前消费者的分区
                    let mut assigned_partitions = Vec::new();
                    for _ in 0..partitions_to_assign {
                        assigned_partitions.push(current_partition);
                        current_partition += 1;
                    }

                    // 更新分配信息
                    let topic_assignments = group
                        .assignments
                        .entry(consumer_id.clone())
                        .or_insert_with(Vec::new);
                    
                    // 移除该主题的旧分配
                    topic_assignments.retain(|&p| p >= partition_count);
                    // 添加新的分配
                    topic_assignments.extend(assigned_partitions.clone());

                    println!("重新分配: 消费者组 {} 中消费者 {} 被分配主题 {} 的分区: {:?}", 
                        group_id, consumer_id, topic, assigned_partitions);
                }
            }

            // 保存到文件
            self.save_metadata(group_id)?;
        }
        Ok(())
    }

    pub async fn remove_consumer(&self, group_id: &str, consumer_id: &str) -> Result<()> {
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            // 从消费者列表中移除
            if let Some(consumers) = group.consumers.get_mut(group_id) {
                consumers.retain(|id| id != consumer_id);
            }

            // 从分配信息中移除
            group.assignments.remove(consumer_id);

            // 触发重新分配
            self.rebalance_group(group_id).await?;
        }
        Ok(())
    }

    pub async fn add_consumer(&self, group_id: &str, consumer_id: &str) -> Result<()> {
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            // 添加到消费者列表
            let consumers = group
                .consumers
                .entry(group_id.to_string())
                .or_insert_with(Vec::new);
            
            if !consumers.contains(&consumer_id.to_string()) {
                consumers.push(consumer_id.to_string());
            }

            // 触发重新分配
            self.rebalance_group(group_id).await?;
        }
        Ok(())
    }
} 