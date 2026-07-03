use crate::kafka::protocol::ApiVersion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiStatus {
    Supported,
    Partial,
    Experimental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiSupport {
    pub key: i16,
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
    pub flexible_min_version: Option<i16>,
    pub status: ApiStatus,
}

pub const API_SUPPORT: &[ApiSupport] = &[
    ApiSupport {
        key: 0,
        name: "Produce",
        min_version: 3,
        max_version: 9,
        flexible_min_version: Some(9),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 1,
        name: "Fetch",
        min_version: 4,
        max_version: 12,
        flexible_min_version: Some(12),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 2,
        name: "ListOffsets",
        min_version: 1,
        max_version: 10,
        flexible_min_version: Some(6),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 3,
        name: "Metadata",
        min_version: 0,
        max_version: 12,
        flexible_min_version: Some(9),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 8,
        name: "OffsetCommit",
        min_version: 2,
        max_version: 10,
        flexible_min_version: Some(8),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 9,
        name: "OffsetFetch",
        min_version: 1,
        max_version: 10,
        flexible_min_version: Some(6),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 10,
        name: "FindCoordinator",
        min_version: 0,
        max_version: 4,
        flexible_min_version: Some(3),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 11,
        name: "JoinGroup",
        min_version: 0,
        max_version: 9,
        flexible_min_version: Some(6),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 12,
        name: "Heartbeat",
        min_version: 0,
        max_version: 4,
        flexible_min_version: Some(4),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 13,
        name: "LeaveGroup",
        min_version: 0,
        max_version: 5,
        flexible_min_version: Some(4),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 14,
        name: "SyncGroup",
        min_version: 0,
        max_version: 5,
        flexible_min_version: Some(4),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 18,
        name: "ApiVersions",
        min_version: 0,
        max_version: 3,
        flexible_min_version: Some(3),
        status: ApiStatus::Partial,
    },
    ApiSupport {
        key: 19,
        name: "CreateTopics",
        min_version: 2,
        max_version: 7,
        flexible_min_version: Some(5),
        status: ApiStatus::Partial,
    },
];

pub fn supported_apis() -> &'static [ApiSupport] {
    API_SUPPORT
}

pub fn lookup_api(key: i16) -> Option<&'static ApiSupport> {
    API_SUPPORT.iter().find(|api| api.key == key)
}

pub fn is_supported_version(key: i16, version: i16) -> bool {
    lookup_api(key)
        .map(|api| version >= api.min_version && version <= api.max_version)
        .unwrap_or(false)
}

pub fn is_flexible_version(key: i16, version: i16) -> bool {
    lookup_api(key)
        .and_then(|api| api.flexible_min_version)
        .map(|min_version| version >= min_version)
        .unwrap_or(false)
}

pub fn api_versions() -> Vec<ApiVersion> {
    API_SUPPORT
        .iter()
        .map(|api| ApiVersion {
            api_key: api.key,
            min_version: api.min_version,
            max_version: api.max_version,
            ..Default::default()
        })
        .collect()
}
