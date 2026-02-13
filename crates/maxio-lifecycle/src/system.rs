use std::{collections::HashMap, path::PathBuf, sync::Arc};

use chrono::Utc;
use maxio_common::{
    error::{MaxioError, Result},
    types::ObjectInfo,
};
use maxio_storage::traits::{ObjectLayer, ObjectVersion};
use tracing::warn;

use crate::{
    store::LifecycleStore,
    types::{LifecycleConfiguration, LifecycleRule, RuleStatus},
};

pub struct LifecycleSys {
    store: LifecycleStore,
    data_dir: PathBuf,
}

impl LifecycleSys {
    pub fn new(store: LifecycleStore, data_dir: PathBuf) -> Self {
        Self { store, data_dir }
    }

    pub async fn get_config(&self, bucket: &str) -> Result<Option<LifecycleConfiguration>> {
        self.store.get_config(bucket).await
    }

    pub async fn set_config(&self, bucket: &str, config: LifecycleConfiguration) -> Result<()> {
        validate_config(&config)?;
        self.store.set_config(bucket, &config).await
    }

    pub async fn delete_config(&self, bucket: &str) -> Result<()> {
        self.store.delete_config(bucket).await
    }

    pub async fn run_lifecycle_scan(&self, object_layer: Arc<dyn ObjectLayer>) -> Result<()> {
        let buckets = self.store.list_configured_buckets().await?;
        for bucket in buckets {
            let Some(config) = self.store.get_config(&bucket).await? else {
                continue;
            };

            if let Err(err) = self
                .scan_bucket_rules(object_layer.as_ref(), &bucket, &config)
                .await
            {
                warn!(
                    bucket = %bucket,
                    root = %self.data_dir.display(),
                    error = %err,
                    "lifecycle scan failed for bucket"
                );
            }
        }
        Ok(())
    }

    async fn scan_bucket_rules(
        &self,
        object_layer: &dyn ObjectLayer,
        bucket: &str,
        config: &LifecycleConfiguration,
    ) -> Result<()> {
        let mut by_prefix: HashMap<String, Vec<&LifecycleRule>> = HashMap::new();
        for rule in &config.rules {
            if rule.status != RuleStatus::Enabled {
                continue;
            }
            let prefix = rule
                .filter
                .as_ref()
                .and_then(|filter| filter.prefix.clone())
                .unwrap_or_default();
            by_prefix.entry(prefix).or_default().push(rule);
        }

        for (prefix, rules) in by_prefix {
            if rules.is_empty() {
                continue;
            }
            self.apply_current_version_rules(object_layer, bucket, &prefix, &rules)
                .await;
            self.apply_noncurrent_version_rules(object_layer, bucket, &prefix, &rules)
                .await;
        }

        Ok(())
    }

    async fn apply_current_version_rules(
        &self,
        object_layer: &dyn ObjectLayer,
        bucket: &str,
        prefix: &str,
        rules: &[&LifecycleRule],
    ) {
        let mut marker = String::new();
        loop {
            let page = match object_layer
                .list_objects(bucket, prefix, &marker, "", 1000)
                .await
            {
                Ok(page) => page,
                Err(err) => {
                    warn!(bucket = %bucket, prefix = %prefix, error = %err, "failed to list objects for lifecycle scan");
                    return;
                }
            };

            for object in page.objects {
                if rules.iter().any(|rule| is_expired(&object, rule)) {
                    if let Err(err) = object_layer.delete_object(bucket, &object.key).await {
                        warn!(bucket = %bucket, key = %object.key, error = %err, "failed to delete expired object");
                    }
                }
            }

            if !page.is_truncated {
                break;
            }
            marker = match page.next_marker {
                Some(next) => next,
                None => break,
            };
        }
    }

    async fn apply_noncurrent_version_rules(
        &self,
        object_layer: &dyn ObjectLayer,
        bucket: &str,
        prefix: &str,
        rules: &[&LifecycleRule],
    ) {
        let version_rules: Vec<&LifecycleRule> = rules
            .iter()
            .copied()
            .filter(|rule| rule.noncurrent_version_expiration.is_some())
            .collect();

        if version_rules.is_empty() {
            return;
        }

        let versions = match object_layer.list_object_versions(bucket, prefix, i32::MAX).await {
            Ok(versions) => versions,
            Err(err) => {
                warn!(bucket = %bucket, prefix = %prefix, error = %err, "failed to list object versions for lifecycle scan");
                return;
            }
        };

        for version in versions {
            if should_expire_noncurrent_version(&version, &version_rules) {
                if let Err(err) = object_layer
                    .delete_object_version(bucket, &version.key, &version.version_id)
                    .await
                {
                    warn!(
                        bucket = %bucket,
                        key = %version.key,
                        version_id = %version.version_id,
                        error = %err,
                        "failed to delete expired noncurrent object version"
                    );
                }
            }
        }
    }
}

fn validate_config(config: &LifecycleConfiguration) -> Result<()> {
    if config.rules.is_empty() {
        return Err(MaxioError::InvalidArgument(
            "lifecycle configuration must include at least one rule".to_string(),
        ));
    }

    for rule in &config.rules {
        let has_expiration = rule.expiration.is_some();
        let has_noncurrent_expiration = rule.noncurrent_version_expiration.is_some();
        if !has_expiration && !has_noncurrent_expiration {
            return Err(MaxioError::InvalidArgument(format!(
                "lifecycle rule {} must include expiration action",
                rule.id
            )));
        }

        if let Some(exp) = &rule.expiration {
            if exp.days.is_some() && exp.date.is_some() {
                return Err(MaxioError::InvalidArgument(format!(
                    "lifecycle rule {} expiration cannot include both days and date",
                    rule.id
                )));
            }
            if exp
                .days
                .is_some_and(|days| days < 0)
            {
                return Err(MaxioError::InvalidArgument(format!(
                    "lifecycle rule {} expiration days must be non-negative",
                    rule.id
                )));
            }
        }

        if let Some(noncurrent) = &rule.noncurrent_version_expiration {
            if noncurrent.noncurrent_days < 0 {
                return Err(MaxioError::InvalidArgument(format!(
                    "lifecycle rule {} noncurrent days must be non-negative",
                    rule.id
                )));
            }
        }
    }

    Ok(())
}

fn should_expire_noncurrent_version(version: &ObjectVersion, rules: &[&LifecycleRule]) -> bool {
    if version.is_latest {
        return false;
    }

    let age_days = (Utc::now() - version.last_modified).num_days();
    rules.iter().any(|rule| {
        rule.noncurrent_version_expiration
            .as_ref()
            .is_some_and(|policy| age_days >= i64::from(policy.noncurrent_days))
    })
}

pub fn is_expired(object: &ObjectInfo, rule: &LifecycleRule) -> bool {
    if let Some(exp) = &rule.expiration {
        if let Some(days) = exp.days {
            let age = Utc::now() - object.last_modified;
            return age.num_days() >= i64::from(days);
        }
        if let Some(date) = exp.date {
            return Utc::now() >= date;
        }
    }
    false
}
