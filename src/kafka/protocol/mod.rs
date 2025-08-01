use crate::kafka::codec::{Decode, Encode};

// This file is a placeholder and will be populated by `build.rs`.
// It includes the code generated from the Kafka protocol JSON definitions.

include!(concat!(env!("OUT_DIR"), "/kafka_protocol.rs")); 