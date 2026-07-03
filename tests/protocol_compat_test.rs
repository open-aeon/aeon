use aeon::kafka::codec::Encode;
use aeon::kafka::compat;
use aeon::kafka::RequestHeader;
use aeon::server::ServerCodec;
use bytes::{BufMut, BytesMut};
use tokio_util::codec::Decoder;

fn request_frame(api_key: i16, api_version: i16) -> BytesMut {
    let header = RequestHeader {
        api_key,
        api_version,
        correlation_id: 42,
        client_id: Some("compat-test".to_string()),
    };
    let mut body = BytesMut::new();
    header.encode(&mut body, api_version).unwrap();

    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    frame
}

#[test]
fn registry_generates_current_api_versions_response_entries() {
    let versions = compat::api_versions();
    let entries = versions
        .iter()
        .map(|api| (api.api_key, api.min_version, api.max_version))
        .collect::<Vec<_>>();

    assert_eq!(
        entries,
        vec![
            (0, 3, 9),
            (1, 4, 12),
            (2, 1, 10),
            (3, 0, 12),
            (8, 2, 10),
            (9, 1, 10),
            (10, 0, 4),
            (11, 0, 9),
            (12, 0, 4),
            (13, 0, 5),
            (14, 0, 5),
            (18, 0, 3),
            (19, 2, 7),
        ]
    );
}

#[test]
fn registry_defines_flexible_version_boundaries() {
    assert!(!compat::is_flexible_version(0, 8));
    assert!(compat::is_flexible_version(0, 9));

    assert!(!compat::is_flexible_version(3, 8));
    assert!(compat::is_flexible_version(3, 9));

    assert!(!compat::is_flexible_version(18, 2));
    assert!(compat::is_flexible_version(18, 3));

    assert!(!compat::is_flexible_version(999, 99));
}

#[test]
fn codec_rejects_known_api_with_unsupported_version_before_body_decode() {
    let mut codec = ServerCodec::default();
    let mut frame = request_frame(0, 2);
    let err = codec
        .decode(&mut frame)
        .expect_err("unsupported version should fail");

    assert!(
        err.to_string()
            .contains("Unsupported API version 2 for API key 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn codec_rejects_unknown_api_key_before_body_decode() {
    let mut codec = ServerCodec::default();
    let mut frame = request_frame(999, 0);
    let err = codec
        .decode(&mut frame)
        .expect_err("unknown api key should fail");

    assert!(
        err.to_string().contains("Unknown API key: 999"),
        "unexpected error: {err}"
    );
}
