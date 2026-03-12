use aeon::broker::Broker;
use aeon::config::broker::BrokerConfig;
use aeon::config::storage::StorageConfig;
use aeon::storage::factory::StorageEngine;

#[tokio::test]
async fn create_topic_goes_through_meta_raft_path() {
    let temp = tempfile::tempdir().unwrap();
    let broker_cfg = BrokerConfig {
        id: 1,
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: 19092,
        data_dir: temp.path().join("data"),
        flush_interval_ms: Some(1000),
        cleanup_interval_ms: Some(60_000),
        internal_topic_name: "__aeon_offsets".to_string(),
        internal_topic_partitions: 1,
        heartbeat_interval_ms: Some(3000),
    };
    let storage_cfg = StorageConfig {
        engine: StorageEngine::Mmap,
        segment_size: 1024 * 1024,
        index_interval_bytes: 4096,
        retention_policy_ms: None,
        retention_policy_bytes: None,
        preallocate: false,
    };

    let broker = Broker::new(broker_cfg, storage_cfg);
    broker.start().await.unwrap();
    broker.create_topic("meta_topic".to_string(), 2).await.unwrap();

    let all = broker.get_all_topics_metadata().await.unwrap();
    assert!(all.contains_key("meta_topic"));
}
