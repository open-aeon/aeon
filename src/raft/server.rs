use tonic::{Request, Response, Status};
use crate::raft::raft_service::{
    raft_service_server::RaftService, AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse, InstallSnapshotRequest, InstallSnapshotResponse
};

#[derive(Debug, Default)]
pub struct RaftServer {}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        println!("[Raft] Received AppendEntries request: {:?}", request);

        let response = AppendEntriesResponse {
            term: 0, 
            success: true,
            conflict_opt: 0,
        };
        Ok(Response::new(response))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        println!("[Raft] Received RequestVote request: {:?}", request);

        let response = RequestVoteResponse {
            term: 0,
            vote_granted: true,
        };
        Ok(Response::new(response))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        println!("[Raft] Received InstallSnapshot request: {:?}", request);

        let response = InstallSnapshotResponse {
            term: 0,
        };
        Ok(Response::new(response))
    }
}
