use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

pub trait CredentialProvider: Send + Sync {
    fn lookup(&self, access_key: &str) -> Option<Credentials>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticCredentialProvider {
    root: Option<Credentials>,
}

impl StaticCredentialProvider {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        let access_key = access_key.into();
        let secret_key = secret_key.into();
        let root = if access_key.is_empty() || secret_key.is_empty() {
            None
        } else {
            Some(Credentials {
                access_key,
                secret_key,
            })
        };
        Self { root }
    }

    pub fn disabled() -> Self {
        Self { root: None }
    }
}

impl CredentialProvider for StaticCredentialProvider {
    fn lookup(&self, access_key: &str) -> Option<Credentials> {
        self.root
            .as_ref()
            .filter(|cred| cred.access_key == access_key)
            .cloned()
    }
}

impl CredentialProvider for Arc<dyn CredentialProvider> {
    fn lookup(&self, access_key: &str) -> Option<Credentials> {
        self.as_ref().lookup(access_key)
    }
}
