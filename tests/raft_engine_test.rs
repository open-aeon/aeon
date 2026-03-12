use std::sync::Arc;
use std::time::Duration;

use aeon::raft::core::{CoreRaftManager, LocalRaftTransport};
use aeon::raft::manager::RaftManager;
use aeon::raft::types::{GroupKind, GroupSpec, RaftCommand, RaftError};

async fn wait_for_leader(
    managers: &[Arc<CoreRaftManager>],
    group_id: u64,
    max_rounds: usize,
) -> Option<u64> {
    for _ in 0..max_rounds {
        for mgr in managers {
            let _ = mgr.force_tick(group_id).await;
        }
        for mgr in managers {
            if let Ok(Some(leader)) = mgr.leader(group_id).await {
                return Some(leader);
            }
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    None
}

#[tokio::test]
async fn raft_three_nodes_elect_and_replicate() {
    let transport = LocalRaftTransport::new();
    let m1 = CoreRaftManager::with_memory(1, transport.clone());
    let m2 = CoreRaftManager::with_memory(2, transport.clone());
    let m3 = CoreRaftManager::with_memory(3, transport.clone());
    transport.register_handler(1, m1.clone());
    transport.register_handler(2, m2.clone());
    transport.register_handler(3, m3.clone());

    let spec = GroupSpec {
        group_id: 10,
        kind: GroupKind::Meta,
        members: vec![1, 2, 3],
    };
    m1.create_group(spec.clone()).await.unwrap();
    m2.create_group(spec.clone()).await.unwrap();
    m3.create_group(spec).await.unwrap();

    let managers = vec![m1.clone(), m2.clone(), m3.clone()];
    let leader_id = wait_for_leader(&managers, 10, 20)
        .await
        .expect("leader should be elected");
    let leader = managers
        .iter()
        .find(|m| m.node_id() == leader_id)
        .expect("leader manager");

    let idx = leader
        .propose(10, RaftCommand::Meta(vec![1, 2, 3]))
        .await
        .expect("propose should succeed");
    assert_eq!(idx, 1);

    for _ in 0..6 {
        for mgr in &managers {
            let _ = mgr.force_tick(10).await;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }

    for mgr in &managers {
        let committed = mgr.committed_index(10).await.unwrap();
        assert!(committed >= 1, "node {} has committed={committed}", mgr.node_id());
    }
}

#[tokio::test]
async fn raft_re_elect_after_leader_removed() {
    let transport = LocalRaftTransport::new();
    let m1 = CoreRaftManager::with_memory(1, transport.clone());
    let m2 = CoreRaftManager::with_memory(2, transport.clone());
    let m3 = CoreRaftManager::with_memory(3, transport.clone());
    transport.register_handler(1, m1.clone());
    transport.register_handler(2, m2.clone());
    transport.register_handler(3, m3.clone());

    let spec = GroupSpec {
        group_id: 11,
        kind: GroupKind::Meta,
        members: vec![1, 2, 3],
    };
    m1.create_group(spec.clone()).await.unwrap();
    m2.create_group(spec.clone()).await.unwrap();
    m3.create_group(spec).await.unwrap();

    let managers = vec![m1.clone(), m2.clone(), m3.clone()];
    let old_leader = wait_for_leader(&managers, 11, 20)
        .await
        .expect("leader should be elected");

    let leader_mgr = managers
        .iter()
        .find(|m| m.node_id() == old_leader)
        .unwrap();
    leader_mgr.remove_group(11).await.unwrap();
    transport.remove_handler(old_leader);

    let survivors = managers
        .into_iter()
        .filter(|m| m.node_id() != old_leader)
        .collect::<Vec<_>>();
    let mut new_leader: Option<u64> = None;
    let mut idx = None;
    for _ in 0..30 {
        for mgr in &survivors {
            let _ = mgr.force_tick(11).await;
        }
        for mgr in &survivors {
            match mgr.propose(11, RaftCommand::Meta(vec![7])).await {
                Ok(i) => {
                    new_leader = Some(mgr.node_id());
                    idx = Some(i);
                    break;
                }
                Err(RaftError::NotLeader(_)) => {}
                Err(e) => panic!("unexpected propose error during re-election: {e}"),
            }
        }
        if new_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let new_leader = new_leader.expect("new leader should be elected");
    assert_ne!(new_leader, old_leader);
    let idx = idx.expect("leader propose should succeed");
    assert_eq!(idx, 1);
}

#[tokio::test]
async fn raft_durable_state_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let transport = LocalRaftTransport::new();
    let first = CoreRaftManager::with_durable_dir(1, transport.clone(), temp.path()).unwrap();
    transport.register_handler(1, first.clone());

    let spec = GroupSpec {
        group_id: 12,
        kind: GroupKind::Meta,
        members: vec![1],
    };
    first.create_group(spec.clone()).await.unwrap();
    let idx = first
        .propose(12, RaftCommand::Meta(vec![9, 9]))
        .await
        .unwrap();
    assert_eq!(idx, 1);
    drop(first);
    transport.remove_handler(1);

    let transport2 = LocalRaftTransport::new();
    let second = CoreRaftManager::with_durable_dir(1, transport2.clone(), temp.path()).unwrap();
    transport2.register_handler(1, second.clone());
    second.create_group(spec).await.unwrap();

    let committed = second.committed_index(12).await.unwrap();
    assert_eq!(committed, 1);
    let idx2 = second
        .propose(12, RaftCommand::Meta(vec![8, 8]))
        .await
        .unwrap();
    assert_eq!(idx2, 2);
}

#[tokio::test]
async fn non_leader_propose_is_rejected() {
    let transport = LocalRaftTransport::new();
    let m1 = CoreRaftManager::with_memory(1, transport.clone());
    let m2 = CoreRaftManager::with_memory(2, transport.clone());
    let m3 = CoreRaftManager::with_memory(3, transport.clone());
    transport.register_handler(1, m1.clone());
    transport.register_handler(2, m2.clone());
    transport.register_handler(3, m3.clone());

    let spec = GroupSpec {
        group_id: 13,
        kind: GroupKind::Meta,
        members: vec![1, 2, 3],
    };
    m1.create_group(spec.clone()).await.unwrap();
    m2.create_group(spec.clone()).await.unwrap();
    m3.create_group(spec).await.unwrap();

    let managers = vec![m1.clone(), m2.clone(), m3.clone()];
    let leader = wait_for_leader(&managers, 13, 20).await.unwrap();
    let follower = managers
        .iter()
        .find(|m| m.node_id() != leader)
        .cloned()
        .unwrap();

    let err = follower
        .propose(13, RaftCommand::Meta(vec![1]))
        .await
        .expect_err("follower propose should fail");
    assert!(matches!(err, RaftError::NotLeader(13)));
}
