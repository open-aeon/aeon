use bifrost::raft::{
    manager::{RaftGroupManager, RaftGroupId},
    generic::{RaftNode, RaftRequest, ReadConsistency},
};
use std::time::Duration;
use tokio::time::sleep;

/// 基础 Raft 引擎测试
#[tokio::test]
async fn test_single_node_basic_operations() {
    println!("🧪 测试：单节点 Raft Group 基本操作");
    
    // 创建单节点 Raft Group Manager
    let manager = RaftGroupManager::new(1);
    
    // 创建一个测试主题的 Raft Group
    let group_id = RaftGroupId::new("test-topic".to_string(), 0);
    let replica_nodes = vec![1]; // 单节点
    
    let result = manager.create_group(group_id.clone(), replica_nodes).await;
    assert!(result.is_ok(), "创建 Raft Group 失败: {:?}", result.err());
    
    // 获取 Group 句柄
    let group = manager.get_group(&group_id).await;
    assert!(group.is_some(), "无法获取创建的 Raft Group");
    
    let group = group.unwrap();
    
    // 等待一段时间让节点初始化和选举 Leader
    sleep(Duration::from_millis(1000)).await;
    
    // 测试节点状态
    let status = group.status().await;
    println!("📊 节点状态: node_id={}, is_leader={}, term={}", 
             status.node_id, status.is_leader, status.term);
    assert_eq!(status.node_id, 1, "节点 ID 不匹配");
    
    // 测试写入操作（propose）
    let request = RaftRequest::new(b"test-data".to_vec())
        .with_type("write".to_string())
        .with_id("req-001".to_string());
    
    let response = group.propose(request).await;
    println!("✍️ 写入响应: success={:?}", response.as_ref().map(|r| r.success));
    assert!(response.is_ok(), "写入操作失败: {:?}", response.err());
    
    let response = response.unwrap();
    assert!(response.success, "写入操作未成功");
    assert_eq!(response.request_id, Some("req-001".to_string()));
    
    // 测试读取操作（query）
    let query_request = RaftRequest::new(b"query-data".to_vec())
        .with_type("read".to_string())
        .with_id("req-002".to_string());
    
    let query_response = group.query(query_request, ReadConsistency::Linearizable).await;
    println!("🔍 读取响应: success={:?}", query_response.as_ref().map(|r| r.success));
    assert!(query_response.is_ok(), "读取操作失败: {:?}", query_response.err());
    
    let query_response = query_response.unwrap();
    assert!(query_response.success, "读取操作未成功");
    assert_eq!(query_response.request_id, Some("req-002".to_string()));
    
    println!("✅ 单节点 Raft Group 测试通过");
}

#[tokio::test]
async fn test_multi_raft_groups() {
    println!("🧪 测试：多 Raft Group 管理");
    
    let manager = RaftGroupManager::new(1);
    
    // 创建多个 Raft Group
    let groups = vec![
        RaftGroupId::new("topic-a".to_string(), 0),
        RaftGroupId::new("topic-a".to_string(), 1),
        RaftGroupId::new("topic-b".to_string(), 0),
        RaftGroupId::metadata(), // 元数据组
    ];
    
    // 创建所有 Group
    for group_id in &groups {
        let result = manager.create_group(group_id.clone(), vec![1]).await;
        assert!(result.is_ok(), "创建 Raft Group {} 失败: {:?}", group_id, result.err());
        println!("📦 创建 Raft Group: {}", group_id);
    }
    
    // 验证所有 Group 都存在
    for group_id in &groups {
        let group = manager.get_group(group_id).await;
        assert!(group.is_some(), "无法找到 Raft Group: {}", group_id);
    }
    
    // 获取所有状态
    let all_statuses = manager.get_all_statuses().await;
    assert_eq!(all_statuses.len(), groups.len(), "状态数量不匹配");
    
    for (group_id, status) in &all_statuses {
        println!("📊 Group {} 状态: 节点={}, Leader={}", 
                 group_id, status.node_id, status.is_leader);
        assert_eq!(status.node_id, 1, "节点 ID 不匹配");
    }
    
    println!("✅ 多 Raft Group 管理测试通过");
}

#[tokio::test]
async fn test_different_read_consistency_levels() {
    println!("🧪 测试：不同读取一致性级别");
    
    let manager = RaftGroupManager::new(1);
    let group_id = RaftGroupId::new("consistency-test".to_string(), 0);
    
    manager.create_group(group_id.clone(), vec![1]).await.unwrap();
    let group = manager.get_group(&group_id).await.unwrap();
    
    // 先写入一些数据
    let write_request = RaftRequest::new(b"consistency-data".to_vec())
        .with_type("write".to_string());
    
    group.propose(write_request).await.unwrap();
    
    // 测试不同的一致性级别
    let consistency_levels = vec![
        ReadConsistency::Linearizable,
        ReadConsistency::LeaderLease,
        ReadConsistency::Eventual,
    ];
    
    for consistency in consistency_levels {
        let read_request = RaftRequest::new(b"read-test".to_vec())
            .with_type("read".to_string())
            .with_id(format!("consistency-{:?}", consistency));
        
        let response = group.query(read_request, consistency.clone()).await;
        assert!(response.is_ok(), "一致性级别 {:?} 读取失败: {:?}", consistency, response.err());
        
        let response = response.unwrap();
        println!("🔍 一致性级别 {:?}: 成功={}", consistency, response.success);
    }
    
    println!("✅ 读取一致性级别测试通过");
}

#[tokio::test]
async fn test_raft_group_lifecycle() {
    println!("🧪 测试：Raft Group 生命周期管理");
    
    let manager = RaftGroupManager::new(1);
    
    // 创建 Group
    let group_id = RaftGroupId::new("lifecycle-test".to_string(), 0);
    let create_result = manager.create_group(group_id.clone(), vec![1]).await;
    assert!(create_result.is_ok(), "创建 Group 失败");
    println!("📦 创建 Group: {}", group_id);
    
    // 验证 Group 存在
    let group = manager.get_group(&group_id).await;
    assert!(group.is_some(), "Group 不存在");
    
    // 写入一些数据
    let group = group.unwrap();
    let request = RaftRequest::new(b"lifecycle-data".to_vec());
    let response = group.propose(request).await;
    assert!(response.is_ok(), "写入数据失败");
    println!("✍️ 写入数据成功");
    
    // 删除 Group
    let remove_result = manager.remove_group(&group_id).await;
    assert!(remove_result.is_ok(), "删除 Group 失败");
    println!("🗑️ 删除 Group: {}", group_id);
    
    // 验证 Group 不存在
    let group_after_remove = manager.get_group(&group_id).await;
    assert!(group_after_remove.is_none(), "Group 删除后仍然存在");
    
    println!("✅ Raft Group 生命周期测试通过");
}

#[tokio::test] 
async fn test_cluster_membership() {
    println!("🧪 测试：集群成员管理");
    
    let manager = RaftGroupManager::new(1);
    
    // 添加集群节点
    manager.add_peer(2, "127.0.0.1:8002".to_string());
    manager.add_peer(3, "127.0.0.1:8003".to_string());
    
    // 创建多节点 Raft Group
    let group_id = RaftGroupId::new("cluster-test".to_string(), 0);
    let replica_nodes = vec![1, 2, 3];
    
    let result = manager.create_group(group_id.clone(), replica_nodes.clone()).await;
    assert!(result.is_ok(), "创建多节点 Group 失败: {:?}", result.err());
    
    let group = manager.get_group(&group_id).await.unwrap();
    
    // 获取集群成员
    let members = group.get_cluster_members().await;
    assert!(members.is_ok(), "获取集群成员失败: {:?}", members.err());
    
    let members = members.unwrap();
    println!("👥 集群成员: {:?}", members);
    
    // 验证成员包含我们添加的节点
    for &node_id in &replica_nodes {
        assert!(members.contains(&node_id), "节点 {} 不在集群成员中", node_id);
    }
    
    println!("✅ 集群成员管理测试通过");
}
