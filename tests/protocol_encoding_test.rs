//! tests/protocol_encoding_test.rs

use aeon::kafka::codec::{Decode, Encode};
use aeon::kafka::protocol::*;
use bytes::BytesMut;

/// A round-trip test for a flexible version of ApiVersionsResponse.
///
/// This test verifies that we can correctly encode a struct instance into bytes
/// and then decode it back into an identical instance, for a flexible API version.
/// This is a critical end-to-end test for our code generation pipeline, especially
/// for the logic handling tagged fields and compact types in flexible versions.
#[test]
fn test_api_versions_v3_roundtrip() {
    // We are testing with api_version 3, which is a flexible version.
    let api_version = 3;

    // 1. Create an instance of the struct with test data.
    // We populate both regular fields and tagged fields.
    let original = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: 0, // Produce
                min_version: 0,
                max_version: 9,
            },
            ApiVersion {
                api_key: 18, // ApiVersions
                min_version: 0,
                max_version: 3,
            },
        ],
        throttle_time_ms: 42,
        // Tagged fields below (for v3+)
        supported_features: vec![
            SupportedFeatureKey {
                name: "feature_A".to_string(),
                min_version: 1,
                max_version: 2,
            }
        ],
        finalized_features_epoch: 123456789,
        finalized_features: vec![],
        zk_migration_ready: false,
    };

    // 2. Encode the struct into a byte buffer.
    let mut buf = BytesMut::new();
    original.encode(&mut buf, api_version).unwrap();
    let encoded_bytes = buf.freeze();

    // 3. Decode the bytes back into a new struct instance.
    let mut read_buf = encoded_bytes.clone();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, api_version).unwrap();

    // 4. Assert that the original and decoded structs are identical.
    // This confirms our Encode and Decode implementations are symmetric.
    assert_eq!(original, decoded);
}
