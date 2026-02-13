use std::sync::Arc;

use maxio_iam::IAMSys;

#[derive(Clone, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

pub trait CredentialProvider: Send + Sync {
    fn lookup(&self, access_key: &str) -> Option<Credentials>;

    fn is_root_access_key(&self, _access_key: &str) -> bool {
        false
    }

    fn is_allowed(&self, access_key: &str, action: &str, resource: &str) -> bool {
        self.is_root_access_key(access_key)
            || self.lookup(access_key).is_some_and(|_| {
                let _ = (action, resource);
                true
            })
    }
}

#[derive(Clone, Debug, Default)]
pub struct StaticCredentialProvider {
    root: Option<Credentials>,
    iam: Option<Arc<IAMSys>>,
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
        Self { root, iam: None }
    }

    pub fn disabled() -> Self {
        Self {
            root: None,
            iam: None,
        }
    }

    pub fn with_iam(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        iam: Arc<IAMSys>,
    ) -> Self {
        let mut provider = Self::new(access_key, secret_key);
        provider.iam = Some(iam);
        provider
    }
}

impl CredentialProvider for StaticCredentialProvider {
    fn lookup(&self, access_key: &str) -> Option<Credentials> {
        let root = self
            .root
            .as_ref()
            .filter(|cred| cred.access_key == access_key)
            .cloned();

        if root.is_some() {
            return root;
        }

        self.iam
            .as_ref()
            .and_then(|iam| iam.user_secret_key(access_key))
            .map(|secret_key| Credentials {
                access_key: access_key.to_string(),
                secret_key,
            })
    }

    fn is_root_access_key(&self, access_key: &str) -> bool {
        self.root
            .as_ref()
            .is_some_and(|cred| cred.access_key == access_key)
    }

    fn is_allowed(&self, access_key: &str, action: &str, resource: &str) -> bool {
        if self.is_root_access_key(access_key) {
            return true;
        }

        self.iam
            .as_ref()
            .is_some_and(|iam| iam.check_permission(access_key, action, resource))
    }
}

impl CredentialProvider for Arc<dyn CredentialProvider> {
    fn lookup(&self, access_key: &str) -> Option<Credentials> {
        self.as_ref().lookup(access_key)
    }

    fn is_root_access_key(&self, access_key: &str) -> bool {
        self.as_ref().is_root_access_key(access_key)
    }

    fn is_allowed(&self, access_key: &str, action: &str, resource: &str) -> bool {
        self.as_ref().is_allowed(access_key, action, resource)
    }
}
