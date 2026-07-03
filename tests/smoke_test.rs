use aeon::broker::Broker;
use aeon::config::{BrokerConfig, ServerConfig, StorageConfig};
use aeon::server::Server;
use aeon::storage::factory::StorageEngine;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::timeout;

fn smoke_command() -> Vec<String> {
    std::env::var("AEON_REAL_CLIENT_SMOKE_CMD")
        .expect("set AEON_REAL_CLIENT_SMOKE_CMD to a real Kafka client smoke command")
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn smoke_timeout() -> Duration {
    std::env::var("AEON_REAL_CLIENT_SMOKE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(60))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external Kafka-compatible client command"]
async fn real_client_smoke_command_runs_against_local_broker() {
    let command = smoke_command();
    assert!(
        !command.is_empty(),
        "AEON_REAL_CLIENT_SMOKE_CMD cannot be empty"
    );

    let temp_dir = TempDir::new().expect("temp data dir");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local listener addr");

    let broker_config = BrokerConfig {
        id: 0,
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: addr.port(),
        data_dir: temp_dir.path().join("data"),
        flush_interval_ms: None,
        cleanup_interval_ms: None,
        internal_topic_name: "__aeon_offsets".to_string(),
        internal_topic_partitions: 1,
        heartbeat_interval_ms: Some(3000),
    };

    let storage_config = StorageConfig {
        engine: StorageEngine::Mmap,
        segment_size: 1024 * 1024,
        index_interval_bytes: 4096,
        retention_policy_ms: None,
        retention_policy_bytes: None,
        preallocate: false,
    };

    let broker = Arc::new(Broker::new(broker_config, storage_config));
    broker.start().await.expect("start broker");

    let server_config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: addr.port(),
        max_connections: 16,
        connection_idle_timeout: Duration::from_secs(30),
    };
    let server = Server::new(broker.clone(), server_config);
    let server_task = tokio::spawn(async move { server.start(listener).await });

    let log_dir = temp_dir.path().join("client-logs");
    std::fs::create_dir_all(&log_dir).expect("client log dir");
    let output = timeout(
        smoke_timeout(),
        Command::new(&command[0])
            .args(&command[1..])
            .env("AEON_BOOTSTRAP_SERVERS", addr.to_string())
            .env("AEON_SMOKE_TOPIC", "aeon-smoke-topic")
            .env("AEON_SMOKE_GROUP", "aeon-smoke-group")
            .env("AEON_LOG_DIR", &log_dir)
            .output(),
    )
    .await
    .expect("external Kafka client smoke command timed out")
    .expect("run external Kafka client smoke command");

    std::fs::write(log_dir.join("stdout.log"), &output.stdout).expect("write stdout log");
    std::fs::write(log_dir.join("stderr.log"), &output.stderr).expect("write stderr log");

    broker.initiate_shutdown().await;
    server_task.abort();

    assert!(
        output.status.success(),
        "real client smoke command failed with status {:?}; logs: {}",
        output.status.code(),
        log_dir.display()
    );
}
