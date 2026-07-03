use aeon::kafka::codec::Encode;
use aeon::kafka::protocol::*;
use aeon::kafka::{RequestHeader, Response, ResponseHeader, ResponseType};
use bytes::{BufMut, BytesMut};

fn decode_hex(input: &str) -> Vec<u8> {
    let hex = input.trim();
    assert_eq!(hex.len() % 2, 0, "hex fixture length must be even");

    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("valid hex byte"))
        .collect()
}

fn response_frame(response: Response) -> Vec<u8> {
    let mut body = BytesMut::new();
    response.encode(&mut body, response.api_version).unwrap();

    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    frame.to_vec()
}

fn request_header_frame(header: RequestHeader) -> Vec<u8> {
    let mut body = BytesMut::new();
    header.encode(&mut body, header.api_version).unwrap();

    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    frame.to_vec()
}

fn api_versions_response(api_version: i16) -> Response {
    let api_keys = vec![
        ApiVersion {
            api_key: 0,
            min_version: 3,
            max_version: 9,
        },
        ApiVersion {
            api_key: 18,
            min_version: 0,
            max_version: 3,
        },
    ];

    let response = if api_version >= 3 {
        ApiVersionsResponse {
            error_code: 0,
            api_keys,
            throttle_time_ms: 0,
            supported_features: vec![],
            finalized_features_epoch: -1,
            finalized_features: vec![],
            zk_migration_ready: false,
        }
    } else {
        ApiVersionsResponse {
            error_code: 0,
            api_keys,
            ..Default::default()
        }
    };

    Response {
        header: ResponseHeader { correlation_id: 7 },
        response_type: ResponseType::ApiVersions(response),
        api_key: 18,
        api_version,
    }
}

#[test]
fn api_versions_v0_response_matches_golden_frame() {
    let expected = decode_hex(include_str!(
        "fixtures/kafka_protocol/api_versions_v0_response.hex"
    ));
    assert_eq!(response_frame(api_versions_response(0)), expected);
}

#[test]
fn api_versions_v3_response_matches_golden_frame() {
    let expected = decode_hex(include_str!(
        "fixtures/kafka_protocol/api_versions_v3_response.hex"
    ));
    assert_eq!(response_frame(api_versions_response(3)), expected);
}

#[test]
fn metadata_v0_request_header_matches_golden_frame() {
    let expected = decode_hex(include_str!(
        "fixtures/kafka_protocol/metadata_v0_request_header_frame.hex"
    ));
    let frame = request_header_frame(RequestHeader {
        api_key: 3,
        api_version: 0,
        correlation_id: 9,
        client_id: Some("golden".to_string()),
    });

    assert_eq!(frame, expected);
}

#[test]
fn metadata_v9_request_header_matches_golden_frame() {
    let expected = decode_hex(include_str!(
        "fixtures/kafka_protocol/metadata_v9_request_header_frame.hex"
    ));
    let frame = request_header_frame(RequestHeader {
        api_key: 3,
        api_version: 9,
        correlation_id: 9,
        client_id: Some("golden".to_string()),
    });

    assert_eq!(frame, expected);
}
