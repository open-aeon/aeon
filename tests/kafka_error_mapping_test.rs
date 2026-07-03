use aeon::kafka::errors;

#[test]
fn priority_error_codes_match_kafka_contract() {
    assert_eq!(errors::NONE, 0);
    assert_eq!(errors::OFFSET_OUT_OF_RANGE, 1);
    assert_eq!(errors::CORRUPT_MESSAGE, 2);
    assert_eq!(errors::UNKNOWN_TOPIC_OR_PARTITION, 3);
    assert_eq!(errors::COORDINATOR_LOAD_IN_PROGRESS, 14);
    assert_eq!(errors::COORDINATOR_NOT_AVAILABLE, 15);
    assert_eq!(errors::NOT_COORDINATOR, 16);
    assert_eq!(errors::INVALID_RECORD, 17);
    assert_eq!(errors::ILLEGAL_GENERATION, 22);
    assert_eq!(errors::UNKNOWN_MEMBER_ID, 25);
    assert_eq!(errors::REBALANCE_IN_PROGRESS, 27);
    assert_eq!(errors::UNSUPPORTED_VERSION, 35);
    assert_eq!(errors::TOPIC_ALREADY_EXISTS, 36);
    assert_eq!(errors::INVALID_PARTITIONS, 37);
    assert_eq!(errors::INVALID_REQUEST, 42);
    assert_eq!(errors::OFFSET_NOT_AVAILABLE, 78);
    assert_eq!(errors::FENCED_INSTANCE_ID, 82);
}
