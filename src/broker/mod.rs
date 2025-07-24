pub mod partition;
pub mod topic;
pub mod consumer_group;

use anyhow::{Context, Result};
use uuid::Uuid;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

use crate::broker::topic::Topic;
use crate::common::metadata::{TopicMetadata, TopicPartition};
use crate::config::broker::BrokerConfig;
use crate::config::storage::StorageConfig;
use crate::broker::consumer_group::{ConsumerGroup, ConsumerMember};
use crate::common::metadata::OffsetCommitMetadata;

pub struct Broker {
    pub config: Arc<BrokerConfig>,
    pub storage_config: Arc<StorageConfig>,
    topics: Arc<RwLock<HashMap<String, Topic>>>,
    data_dir: PathBuf,
    consumer_groups: Arc<RwLock<HashMap<String, Arc<RwLock<ConsumerGroup>>>>>,
}

impl Broker {
    pub fn new(config: BrokerConfig, storage_config: StorageConfig) -> Self {
        let data_dir = config.data_dir.clone();
        let topics = Arc::new(RwLock::new(HashMap::new()));
        let consumer_groups = Arc::new(RwLock::new(HashMap::new()));
        Self { config: Arc::new(config),storage_config: Arc::new(storage_config), topics, data_dir, consumer_groups }
    }

    pub async fn start(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)
        .with_context(|| format!("Failed to create data directory at {:?}", &self.data_dir))?;

        println!("Starting broker, data directory at: {:?}", &self.data_dir);

        let mut local_topics = HashMap::new();

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(topic_name_os) = path.file_name() {
                    if let Some(topic_name) = topic_name_os.to_str() {
                        println!("Loading topic: {}", topic_name);

                        let engine = self.storage_config.engine;
                        match Topic::load(topic_name.to_string(), path.clone(), engine, &self.storage_config) {
                            Ok(topic) => {
                                local_topics.insert(topic_name.to_string(), topic);
                            }
                            Err(e) => {
                                eprintln!("Failed to load topic {}: {}", topic_name, e);
                            }
                        }
                    }
                }
            }
        }

        if !local_topics.is_empty() {
            self.topics.write().await.extend(local_topics);
        }

        if !self.topics.read().await.contains_key(&self.config.internal_topic_name) {
            self.create_topic(self.config.internal_topic_name.clone(), self.config.internal_topic_partitions).await?;
            println!("Created internal topic: {}", self.config.internal_topic_name);
        }

        println!("Loading consumer offsets...");
        let groups = self.load_offsets().await?;
        println!("Loaded {} consumer groups", groups.len());

        self.consumer_groups.write().await.extend(groups);

        self.start_background_tasks();

        Ok(())
    }

    fn start_background_tasks(&self) {
        self.start_flush_task();
        self.start_cleanup_task();
        self.start_session_timeout_check_task();
    }

    fn start_flush_task(&self) {
        if let Some(flush_interval) = self.config.flush_interval_ms {
            let topics = self.topics.clone();
            let interval_duration = tokio::time::Duration::from_millis(flush_interval);

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_duration);

                loop {
                    interval.tick().await;

                    let topics = topics.read().await;
                    for topic in topics.values() {
                        if let Err(e) = topic.flush_all().await {
                            eprintln!("Failed to flush topic {}: {}", topic.name, e);
                        }
                    }
                }
            });
            println!("Spawned periodic flush task with interval: {:?}", interval_duration);
        }
    }

    fn start_cleanup_task(&self) {
        if let Some(cleanup_interval) = self.config.cleanup_interval_ms {
            let topics = self.topics.clone();
            let interval_duration = tokio::time::Duration::from_millis(cleanup_interval);

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_duration);
                
                loop {
                    interval.tick().await;

                    let topics = topics.read().await;
                    for topic in topics.values() {
                        if let Err(e) = topic.cleanup().await {
                            eprintln!("Failed to cleanup topic {}: {}", topic.name, e);
                        }
                    }
                }
            });
            println!("Spawned periodic cleanup task with interval: {:?}", interval_duration);
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        println!("Shutting down broker");
        let topics = self.topics.read().await;
        for (name, topic) in topics.iter() {
            println!("Flushing topic: {}", name);
            topic.flush_all().await?
        }
        println!("Broker shutdown complete");
        Ok(())
    }

    pub async fn create_topic(&self, name: String, p_num: u32) -> Result<()> {
        if self.topics.read().await.contains_key(&name) {
            return Err(anyhow::anyhow!("Topic already exists: {}", name));
        }

        let engine = self.storage_config.engine;
        let path = self.data_dir.join(&name);
        // todo: potential race condition here, to be fixed
        let topic = Topic::create(name.clone(), path, p_num, engine, &self.storage_config)?;
        self.topics.write().await.insert(name, topic);
        Ok(())
    }

    pub async fn delete_topic(&self, name: String) -> Result<()> {
        let mut topics = self.topics.write().await;
        if let Some(topic) = topics.remove(&name) {
            drop(topics);
            topic.delete().await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", name))
        }
    }

    pub async fn append(&self, tp: &TopicPartition, data: &[u8]) -> Result<u64> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            topic.append(p_id, data).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
        }
    }

    pub async fn append_batch(&self, tp: &TopicPartition, data: &[Vec<u8>]) -> Result<u64> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            topic.append_batch(p_id, data).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
        }
    }

    pub async fn read(&self, tp: &TopicPartition, offset: u64) -> Result<Vec<u8>> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            topic.read(p_id, offset).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
        }
    }

    pub async fn get_topics_meta(&self) -> Result<HashMap<String, TopicMetadata>> {
        let topics = self.topics.read().await;
        let mut meta = HashMap::new();
        for (name, topic) in topics.iter() {
            meta.insert(name.clone(), topic.meta().await);
        }
        Ok(meta)
    }

    pub  async fn join_group(&self, group_id: String, member_id: String, session_timeout: u64) -> Result<(String, u32, String)> {
        let mut groups = self.consumer_groups.write().await;
        
        let group = groups.entry(group_id.clone()).or_insert_with(|| Arc::new(RwLock::new(ConsumerGroup::new(group_id))));
        let mut group_lock = group.write().await;

        let new_member_id = if member_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            member_id
        };

        let member = ConsumerMember {
            id: new_member_id.clone(),
            session_timeout: Duration::from_millis(session_timeout),
            last_heartbeat: Instant::now(),
            assignment: Vec::new(),
        };

        let is_new_member = group_lock.add_member(member);
        let leader_id = group_lock.leader_election()?;
        if is_new_member {
            group_lock.rebalance()?;
        }
        let generation_id = group_lock.generation_id;

        Ok((new_member_id, generation_id, leader_id))
    }

    pub async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let groups = self.consumer_groups.read().await;
        if let Some(group) = groups.get(group_id) {
            let mut group_lock = group.write().await;
            let existed = group_lock.remove_member(member_id);

            if existed.is_some() {
                if let Some(leader) = &group_lock.leader_id {
                    if leader == member_id {
                        group_lock.leader_election()?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn heartbeat(&self, group_id: &str, member_id: &str) -> Result<()> {
        let groups = self.consumer_groups.read().await;
        if let Some(group) = groups.get(group_id) {
            let mut group_lock = group.write().await;
            group_lock.heartbeat(member_id)?;
        }
        Ok(())
    }

    pub async fn commit_offset(&self, group_id: String, tp: TopicPartition, offset: i64) -> Result<()> {
        let message = OffsetCommitMetadata {
            group_id: group_id,
            topic: tp.topic.clone(),
            partition: tp.partition,
            offset,
            timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        };
        self.persist_offset(&message).await?;

        let group_arc : Arc<RwLock<ConsumerGroup>>;

        {
            let mut groups = self.consumer_groups.write().await;

            let group = groups.entry(message.group_id.clone()).or_insert_with(|| Arc::new(RwLock::new(ConsumerGroup::new(message.group_id.clone()))));
            group_arc = Arc::clone(group);
        }
        
        let mut group_lock = group_arc.write().await;
        group_lock.commit_offset(tp, offset);

        Ok(())
    }

    pub async fn fetch_offset(&self, group_id: &str, tp: &TopicPartition) -> Result<Option<i64>> {
        let groups = self.consumer_groups.read().await;
        if let Some(group) = groups.get(group_id) {
            let group_lock = group.read().await;
            Ok(group_lock.fetch_offset(tp))
        } else {
            Ok(None)
        }
    }

    async fn persist_offset(&self, metadata: &OffsetCommitMetadata) -> Result<()> {
        let data = bincode::serialize(metadata)?;

        let partition = 0;
        let tp = TopicPartition {
            topic: self.config.internal_topic_name.clone(),
            partition,
        };

        self.append(&tp, &data).await?;

        Ok(())
    }

    async fn load_offsets(&self) -> Result<HashMap<String, Arc<RwLock<ConsumerGroup>>>> {
        let mut consumer_group: HashMap<String, ConsumerGroup> = HashMap::new();

        for p in 0..self.config.internal_topic_partitions {
            let tp = TopicPartition {
                topic: self.config.internal_topic_name.clone(),
                partition: p,
            };

            let mut current_offset = 0;
            loop {
                match self.read(&tp, current_offset).await {
                    Ok(data) => {
                        if let Ok(message) = bincode::deserialize::<OffsetCommitMetadata>(&data) {
                            let group = consumer_group.entry(message.group_id.clone()).or_insert_with(||ConsumerGroup::new(message.group_id.clone()));

                            let offset_tp = TopicPartition {
                                topic: message.topic,
                                partition: message.partition,
                            };

                            group.commit_offset(offset_tp, message.offset);
                        }
                        current_offset += 1;
                    }
                    Err(e) => {
                        if let Some(err) = e.downcast_ref::<crate::error::StorageError>() {
                            if matches!(err, crate::error::StorageError::InvalidOffset) {
                                break;
                            }
                        }
                        return Err(e);
                    }
                }
            }
        }

        let groups = consumer_group.into_iter().map(|(k, v)| (k, Arc::new(RwLock::new(v)))).collect();
        Ok(groups)
    }

    fn start_session_timeout_check_task(&self) {
        if let Some(heartbeat_interval) = self.config.heartbeat_interval_ms {
            let interval_duration = tokio::time::Duration::from_millis(heartbeat_interval);

            let consumer_groups = self.consumer_groups.clone();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_duration);
    
                loop {
                    interval.tick().await;
    
                    // 1. 获取所有消费组的ID列表
                    // 我们在循环的开始获取一次读锁，拿到所有 group 的 Arc<RwLock> 的克隆。
                    // 然后立即释放读锁，避免长时间持有。
                    let groups_to_check: Vec<Arc<RwLock<ConsumerGroup>>> = {
                        let groups_map = consumer_groups.read().await;
                        groups_map.values().cloned().collect()
                    }; // <-- 读锁在这里被释放
    
                    // 2. 遍历并检查每个组
                    for group_arc in groups_to_check {
                        let mut group_lock = group_arc.write().await; // 锁定单个组进行检查和修改
    
                        // 3. 找出所有超时的成员
                        let mut dead_members = Vec::new();
                        for member in group_lock.members.values() {
                            if member.last_heartbeat.elapsed() > member.session_timeout {
                                println!(
                                    "Member {} in group {} timed out. Last heartbeat: {:?}, Session timeout: {:?}", 
                                    member.id, group_lock.name, member.last_heartbeat, member.session_timeout
                                );
                                dead_members.push(member.id.clone());
                            }
                        }
    
                        // 4. 如果有成员超时，就将其移除并触发重平衡
                        if !dead_members.is_empty() {
                            println!("Group {} is rebalancing due to members timing out: {:?}", group_lock.name, dead_members);
                            for member_id in dead_members {
                                group_lock.remove_member(&member_id);
                            }
                            
                            // 成员变化后，重新选举 Leader 并触发重平衡
                            let _ = group_lock.leader_election();
                            let _ = group_lock.rebalance();
                        }
                    }
                }
            });
    
            println!("Spawned session timeout checker task with interval: {:?}", interval_duration);
        }
    }
}