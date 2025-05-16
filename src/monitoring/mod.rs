mod metrics;
mod tracing;

pub use metrics::Metrics;
pub use tracing::Tracing;

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Monitoring {
    metrics: Arc<RwLock<Metrics>>,
    tracing: Arc<RwLock<Tracing>>,
}

impl Monitoring {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(Metrics::new())),
            tracing: Arc::new(RwLock::new(Tracing::new())),
        }
    }

    pub async fn record_request(&self, request_type: &str) {
        let mut metrics = self.metrics.write().await;
        metrics.record_request(request_type);
    }

    pub async fn record_response(&self, response_type: &str) {
        let mut metrics = self.metrics.write().await;
        metrics.record_response(response_type);
    }

    pub async fn record_latency(&self, operation: &str, latency: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.record_latency(operation, latency);
    }
} 