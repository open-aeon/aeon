use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct Tracing {
    traces: HashMap<String, Vec<Trace>>,
}

struct Trace {
    operation: String,
    start_time: Instant,
    end_time: Option<Instant>,
    metadata: HashMap<String, String>,
}

impl Tracing {
    pub fn new() -> Self {
        Self {
            traces: HashMap::new(),
        }
    }

    pub fn start_trace(&mut self, trace_id: &str, operation: &str) {
        let trace = Trace {
            operation: operation.to_string(),
            start_time: Instant::now(),
            end_time: None,
            metadata: HashMap::new(),
        };
        
        self.traces
            .entry(trace_id.to_string())
            .or_insert_with(Vec::new)
            .push(trace);
    }

    pub fn end_trace(&mut self, trace_id: &str) {
        if let Some(traces) = self.traces.get_mut(trace_id) {
            if let Some(trace) = traces.last_mut() {
                trace.end_time = Some(Instant::now());
            }
        }
    }

    pub fn add_metadata(&mut self, trace_id: &str, key: &str, value: &str) {
        if let Some(traces) = self.traces.get_mut(trace_id) {
            if let Some(trace) = traces.last_mut() {
                trace.metadata.insert(key.to_string(), value.to_string());
            }
        }
    }

    pub fn get_trace_duration(&self, trace_id: &str) -> Option<Duration> {
        self.traces.get(trace_id).and_then(|traces| {
            traces.last().and_then(|trace| {
                trace.end_time.map(|end| end.duration_since(trace.start_time))
            })
        })
    }
} 