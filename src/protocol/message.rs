use serde::{Deserialize, Serialize};

/// 协议消息结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub content: Vec<u8>,
}

#[allow(dead_code)]
impl Message {
    /// 从客户端消息创建协议消息
    pub fn new(content: Vec<u8>) -> Self {
        Self {
            content,
        }
    }
}