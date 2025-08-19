use openraft::{declare_raft_types, BasicNode, TokioRuntime};
use std::io::Cursor;

// Raft type configuration: unified definition of associated types used within the cluster.
// - D / R use Vec<u8> for flexible business payload serialization by upper layers
// - NodeId uses u64
// - Node uses BasicNode, which contains basic node metadata (such as address)
// - SnapshotData uses in-memory Cursor<Vec<u8>> for now, can be replaced with a persistent implementation later
// - Responder uses openraft's OneshotResponder
// - AsyncRuntime uses TokioRuntime
declare_raft_types!(
    pub TypeConfig:
        D            = Vec<u8>,
        R            = Vec<u8>,
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder    = openraft::impls::OneshotResponder<TypeConfig>,
        AsyncRuntime = TokioRuntime,
);


