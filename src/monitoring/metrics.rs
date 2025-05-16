use std::collections::HashMap;

pub struct Metrics {
    request_counts: HashMap<String, u64>,
    response_counts: HashMap<String, u64>,
    latencies: HashMap<String, Vec<u64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            request_counts: HashMap::new(),
            response_counts: HashMap::new(),
            latencies: HashMap::new(),
        }
    }

    pub fn record_request(&mut self, request_type: &str) {
        *self.request_counts.entry(request_type.to_string()).or_insert(0) += 1;
    }

    pub fn record_response(&mut self, response_type: &str) {
        *self.response_counts.entry(response_type.to_string()).or_insert(0) += 1;
    }

    pub fn record_latency(&mut self, operation: &str, latency: u64) {
        self.latencies
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(latency);
    }

    pub fn get_request_count(&self, request_type: &str) -> u64 {
        *self.request_counts.get(request_type).unwrap_or(&0)
    }

    pub fn get_response_count(&self, response_type: &str) -> u64 {
        *self.response_counts.get(response_type).unwrap_or(&0)
    }

    pub fn get_average_latency(&self, operation: &str) -> Option<f64> {
        self.latencies.get(operation).map(|latencies| {
            let sum: u64 = latencies.iter().sum();
            sum as f64 / latencies.len() as f64
        })
    }
} 