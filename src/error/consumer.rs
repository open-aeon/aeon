use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum ConsumerGroupError {
    #[error("The group is not in a valid state for this operation.")]
    InvalidState,

    #[error("Member '{0}' not found in the group.")]
    MemberNotFound(String),

    #[error("No common protocol found among all members.")]
    NoCommonProtocol,

    #[error("Inconsistent protocol support among members.")]
    InconsistentProtocols,

    #[error("Invalid generation ID. Expected {0}, got {1}.")]
    InvalidGeneration(u32, u32),

    #[error("Member '{0}' is not the leader.")]
    NotLeader(String),
}