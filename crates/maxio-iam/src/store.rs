use std::path::{Path, PathBuf};

use maxio_common::error::{MaxioError, Result};
use tokio::fs;

use crate::types::{Policy, User};

#[derive(Debug, Clone)]
pub struct IamStore {
    users_dir: PathBuf,
    policies_dir: PathBuf,
}

impl IamStore {
    pub async fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        let base = data_dir.as_ref().join(".iam");
        let users_dir = base.join("users");
        let policies_dir = base.join("policies");
        fs::create_dir_all(&users_dir).await?;
        fs::create_dir_all(&policies_dir).await?;

        Ok(Self {
            users_dir,
            policies_dir,
        })
    }

    pub async fn save_user(&self, user: &User) -> Result<()> {
        let path = self.user_path(&user.access_key);
        let data = serde_json::to_vec_pretty(user).map_err(|err| {
            MaxioError::InternalError(format!(
                "failed to serialize user {}: {err}",
                user.access_key
            ))
        })?;
        fs::write(path, data).await?;
        Ok(())
    }

    pub async fn get_user(&self, access_key: &str) -> Result<Option<User>> {
        self.read_json_if_exists(self.user_path(access_key)).await
    }

    pub async fn delete_user(&self, access_key: &str) -> Result<()> {
        self.delete_if_exists(self.user_path(access_key)).await
    }

    pub async fn list_users(&self) -> Result<Vec<User>> {
        self.read_all_json::<User>(&self.users_dir).await
    }

    pub async fn save_policy(&self, policy: &Policy) -> Result<()> {
        let path = self.policy_path(&policy.name);
        let data = serde_json::to_vec_pretty(policy).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize policy {}: {err}", policy.name))
        })?;
        fs::write(path, data).await?;
        Ok(())
    }

    pub async fn get_policy(&self, name: &str) -> Result<Option<Policy>> {
        self.read_json_if_exists(self.policy_path(name)).await
    }

    pub async fn delete_policy(&self, name: &str) -> Result<()> {
        self.delete_if_exists(self.policy_path(name)).await
    }

    pub async fn list_policies(&self) -> Result<Vec<Policy>> {
        self.read_all_json::<Policy>(&self.policies_dir).await
    }

    fn user_path(&self, access_key: &str) -> PathBuf {
        self.users_dir.join(format!("{access_key}.json"))
    }

    fn policy_path(&self, name: &str) -> PathBuf {
        self.policies_dir.join(format!("{name}.json"))
    }

    async fn read_json_if_exists<T: serde::de::DeserializeOwned>(
        &self,
        path: PathBuf,
    ) -> Result<Option<T>> {
        match fs::read(path).await {
            Ok(bytes) => {
                let value = serde_json::from_slice(&bytes).map_err(|err| {
                    MaxioError::InternalError(format!("failed to deserialize iam json: {err}"))
                })?;
                Ok(Some(value))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn delete_if_exists(&self, path: PathBuf) -> Result<()> {
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn read_all_json<T: serde::de::DeserializeOwned>(&self, dir: &Path) -> Result<Vec<T>> {
        let mut values = Vec::new();
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let is_json = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
            if !is_json {
                continue;
            }

            let bytes = fs::read(&path).await?;
            let value = serde_json::from_slice::<T>(&bytes).map_err(|err| {
                MaxioError::InternalError(format!("failed to deserialize {:?}: {err}", path))
            })?;
            values.push(value);
        }
        Ok(values)
    }
}
