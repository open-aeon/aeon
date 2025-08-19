pub mod generic;
pub mod server;
pub mod adapter;
pub mod types;
pub mod storage;

pub mod raft_service {
    tonic::include_proto!("raft");
}
