use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

use chrono::Utc;
use maxio_common::error::{MaxioError, Result};

use crate::{
    policy::evaluate_policy,
    store::IamStore,
    types::{Effect, Policy, PolicyStatement, User},
};

#[derive(Debug, Clone)]
pub struct IAMSys {
    store: IamStore,
    users: Arc<RwLock<HashMap<String, User>>>,
    policies: Arc<RwLock<HashMap<String, Policy>>>,
}

impl IAMSys {
    pub async fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        let store = IamStore::new(data_dir).await?;

        let mut policies = HashMap::new();
        for policy in store.list_policies().await? {
            policies.insert(policy.name.clone(), policy);
        }

        let mut users = HashMap::new();
        for user in store.list_users().await? {
            users.insert(user.access_key.clone(), user);
        }

        let sys = Self {
            store,
            users: Arc::new(RwLock::new(users)),
            policies: Arc::new(RwLock::new(policies)),
        };

        sys.ensure_builtin_policies().await?;
        Ok(sys)
    }

    pub async fn create_user(&self, access_key: &str, secret_key: &str) -> Result<User> {
        if access_key.is_empty() || secret_key.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "access key and secret key are required".to_string(),
            ));
        }

        {
            let users = self.users_read()?;
            if users.contains_key(access_key) {
                return Err(MaxioError::InvalidArgument(format!(
                    "user already exists: {access_key}"
                )));
            }
        }

        let user = User {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            policy_names: Vec::new(),
            created_at: Utc::now(),
        };

        self.store.save_user(&user).await?;
        self.users_write()?
            .insert(user.access_key.clone(), user.clone());
        Ok(user)
    }

    pub async fn delete_user(&self, access_key: &str) -> Result<()> {
        self.store.delete_user(access_key).await?;
        self.users_write()?.remove(access_key);
        Ok(())
    }

    pub async fn list_users(&self) -> Result<Vec<User>> {
        let mut users: Vec<User> = self.users_read()?.values().cloned().collect();
        users.sort_by(|a, b| a.access_key.cmp(&b.access_key));
        Ok(users)
    }

    pub async fn get_user(&self, access_key: &str) -> Result<Option<User>> {
        Ok(self.users_read()?.get(access_key).cloned())
    }

    pub async fn create_policy(&self, policy: Policy) -> Result<()> {
        if policy.name.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "policy name is required".to_string(),
            ));
        }
        if policy.statements.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "policy must include at least one statement".to_string(),
            ));
        }

        self.store.save_policy(&policy).await?;
        self.policies_write()?.insert(policy.name.clone(), policy);
        Ok(())
    }

    pub async fn delete_policy(&self, name: &str) -> Result<()> {
        self.store.delete_policy(name).await?;
        self.policies_write()?.remove(name);

        let mut updated_users = Vec::new();
        {
            let mut users = self.users_write()?;
            for user in users.values_mut() {
                if user
                    .policy_names
                    .iter()
                    .any(|policy_name| policy_name == name)
                {
                    user.policy_names.retain(|policy_name| policy_name != name);
                    updated_users.push(user.clone());
                }
            }
        }

        for user in &updated_users {
            self.store.save_user(user).await?;
        }

        Ok(())
    }

    pub async fn attach_policy(&self, access_key: &str, policy_name: &str) -> Result<()> {
        if !self.policies_read()?.contains_key(policy_name) {
            return Err(MaxioError::InvalidArgument(format!(
                "policy not found: {policy_name}"
            )));
        }

        let updated_user = {
            let mut users = self.users_write()?;
            let user = users.get_mut(access_key).ok_or_else(|| {
                MaxioError::InvalidArgument(format!("user not found: {access_key}"))
            })?;

            if !user.policy_names.iter().any(|name| name == policy_name) {
                user.policy_names.push(policy_name.to_string());
                Some(user.clone())
            } else {
                None
            }
        };

        if let Some(user) = updated_user {
            self.store.save_user(&user).await?;
        }

        Ok(())
    }

    pub async fn detach_policy(&self, access_key: &str, policy_name: &str) -> Result<()> {
        let updated_user = {
            let mut users = self.users_write()?;
            let user = users.get_mut(access_key).ok_or_else(|| {
                MaxioError::InvalidArgument(format!("user not found: {access_key}"))
            })?;

            let before = user.policy_names.len();
            user.policy_names.retain(|name| name != policy_name);
            if user.policy_names.len() != before {
                Some(user.clone())
            } else {
                None
            }
        };

        if let Some(user) = updated_user {
            self.store.save_user(&user).await?;
        }

        Ok(())
    }

    pub fn check_permission(&self, access_key: &str, action: &str, resource: &str) -> bool {
        let users = match self.users_read() {
            Ok(users) => users,
            Err(_) => return false,
        };
        let Some(user) = users.get(access_key) else {
            return false;
        };

        let policies_map = match self.policies_read() {
            Ok(policies) => policies,
            Err(_) => return false,
        };
        let policies = user
            .policy_names
            .iter()
            .filter_map(|name| policies_map.get(name).cloned())
            .collect::<Vec<_>>();

        evaluate_policy(&policies, action, resource)
    }

    pub fn user_secret_key(&self, access_key: &str) -> Option<String> {
        self.users_read()
            .ok()
            .and_then(|users| users.get(access_key).map(|user| user.secret_key.clone()))
    }

    async fn ensure_builtin_policies(&self) -> Result<()> {
        let builtins = [builtin_readwrite_policy(), builtin_readonly_policy()];

        for policy in builtins {
            let should_create = {
                let policies = self.policies_read()?;
                !policies.contains_key(&policy.name)
            };

            if should_create {
                self.create_policy(policy).await?;
            }
        }

        Ok(())
    }

    fn users_read(&self) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, User>>> {
        self.users
            .read()
            .map_err(|_| MaxioError::InternalError("iam users lock poisoned".to_string()))
    }

    fn users_write(&self) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, User>>> {
        self.users
            .write()
            .map_err(|_| MaxioError::InternalError("iam users lock poisoned".to_string()))
    }

    fn policies_read(&self) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, Policy>>> {
        self.policies
            .read()
            .map_err(|_| MaxioError::InternalError("iam policies lock poisoned".to_string()))
    }

    fn policies_write(&self) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, Policy>>> {
        self.policies
            .write()
            .map_err(|_| MaxioError::InternalError("iam policies lock poisoned".to_string()))
    }
}

fn builtin_readwrite_policy() -> Policy {
    Policy {
        name: "readwrite".to_string(),
        version: "2012-10-17".to_string(),
        statements: vec![PolicyStatement {
            effect: Effect::Allow,
            actions: vec!["s3:*".to_string()],
            resources: vec!["arn:aws:s3:::*".to_string(), "arn:aws:s3:::*/*".to_string()],
        }],
    }
}

fn builtin_readonly_policy() -> Policy {
    Policy {
        name: "readonly".to_string(),
        version: "2012-10-17".to_string(),
        statements: vec![PolicyStatement {
            effect: Effect::Allow,
            actions: vec!["s3:Get*".to_string(), "s3:List*".to_string()],
            resources: vec!["arn:aws:s3:::*".to_string(), "arn:aws:s3:::*/*".to_string()],
        }],
    }
}
