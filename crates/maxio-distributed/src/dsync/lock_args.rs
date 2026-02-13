use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockArgs {
    pub uid: String,
    pub resources: Vec<String>,
    pub owner: String,
    pub source: String,
    pub quorum: usize,
}

impl LockArgs {
    pub fn new(
        uid: String,
        resources: Vec<String>,
        owner: String,
        source: String,
        quorum: usize,
    ) -> Self {
        Self {
            uid,
            resources,
            owner,
            source,
            quorum,
        }
    }
}
