use std::{collections::HashMap, path::PathBuf, sync::Arc};

use chrono::Utc;
use maxio_common::{
    error::{MaxioError, Result},
    types::ObjectInfo,
};
use maxio_storage::traits::{ObjectLayer, ObjectVersion};
use tracing::{info, warn};

use crate::{
    store::LifecycleStore,
    types::{
        Action, LifecycleConfiguration, LifecycleRule, ObjectOpts, RuleStatus,
    },
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
            let prefix = rule.get_prefix();
            by_prefix.entry(prefix).or_default().push(rule);
        }

        for (prefix, rules) in by_prefix {
            if rules.is_empty() {
                continue;
            }
            self.apply_current_version_rules(object_layer, bucket, &prefix, config)
                .await;
            self.apply_noncurrent_version_rules(object_layer, bucket, &prefix, config)
                .await;
        }

        Ok(())
    }

    async fn apply_current_version_rules(
        &self,
        object_layer: &dyn ObjectLayer,
        bucket: &str,
        prefix: &str,
        config: &LifecycleConfiguration,
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
                let obj_opts = object_info_to_opts(&object, true);
                let event = config.eval(&obj_opts);

                match event.action {
                    Action::None => {}
                    Action::Delete | Action::DeleteRestored => {
                        info!(
                            bucket = %bucket,
                            key = %object.key,
                            rule_id = %event.rule_id,
                            "deleting expired object"
                        );
                        if let Err(err) = object_layer.delete_object(bucket, &object.key).await {
                            warn!(bucket = %bucket, key = %object.key, error = %err, "failed to delete expired object");
                        }
                    }
                    Action::DeleteAllVersions => {
                        info!(
                            bucket = %bucket,
                            key = %object.key,
                            rule_id = %event.rule_id,
                            "deleting all versions of expired object"
                        );
                        if let Err(err) = self.delete_all_versions(object_layer, bucket, &object.key).await {
                            warn!(bucket = %bucket, key = %object.key, error = %err, "failed to delete all versions");
                        }
                    }
                    Action::Transition => {
                        info!(
                            bucket = %bucket,
                            key = %object.key,
                            rule_id = %event.rule_id,
                            storage_class = %event.storage_class,
                            "transitioning object (not implemented)"
                        );
                        // Transition to different storage class would be implemented here
                    }
                    _ => {}
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
        config: &LifecycleConfiguration,
    ) {
        if !config.rules.iter().any(|r| {
            r.status == RuleStatus::Enabled
                && (r.noncurrent_version_expiration.is_some()
                    || r.noncurrent_version_transition.is_some()
                    || r.del_marker_expiration.is_some())
        }) {
            return;
        }

        let versions = match object_layer
            .list_object_versions(bucket, prefix, i32::MAX)
            .await
        {
            Ok(versions) => versions,
            Err(err) => {
                warn!(bucket = %bucket, prefix = %prefix, error = %err, "failed to list object versions for lifecycle scan");
                return;
            }
        };

        // Group versions by key to track remaining versions
        let mut versions_by_key: HashMap<String, Vec<&ObjectVersion>> = HashMap::new();
        for version in &versions {
            versions_by_key
                .entry(version.key.clone())
                .or_default()
                .push(version);
        }

        for (key, key_versions) in versions_by_key {
            let num_versions = key_versions.len() as i32;
            let mut remaining_versions = num_versions;

            for (idx, version) in key_versions.iter().enumerate() {
                let successor_mod_time = if idx > 0 {
                    Some(key_versions[idx - 1].last_modified)
                } else {
                    None
                };

                let obj_opts = ObjectOpts {
                    name: version.key.clone(),
                    mod_time: Some(version.last_modified),
                    size: version.size,
                    version_id: version.version_id.clone(),
                    is_latest: version.is_latest,
                    delete_marker: version.is_delete_marker,
                    num_versions,
                    successor_mod_time,
                    ..Default::default()
                };

                let event = config.eval_at(&obj_opts, Utc::now(), remaining_versions);

                match event.action {
                    Action::None => {}
                    Action::DeleteVersion | Action::DeleteRestoredVersion => {
                        info!(
                            bucket = %bucket,
                            key = %key,
                            version_id = %version.version_id,
                            rule_id = %event.rule_id,
                            "deleting expired noncurrent version"
                        );
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
                        } else {
                            remaining_versions -= 1;
                        }
                    }
                    Action::DelMarkerDeleteAllVersions => {
                        info!(
                            bucket = %bucket,
                            key = %key,
                            rule_id = %event.rule_id,
                            "deleting all versions due to delete marker expiration"
                        );
                        if let Err(err) = self.delete_all_versions(object_layer, bucket, &key).await {
                            warn!(bucket = %bucket, key = %key, error = %err, "failed to delete all versions");
                        }
                        break; // All versions deleted, move to next key
                    }
                    Action::TransitionVersion => {
                        info!(
                            bucket = %bucket,
                            key = %key,
                            version_id = %version.version_id,
                            rule_id = %event.rule_id,
                            storage_class = %event.storage_class,
                            "transitioning noncurrent version (not implemented)"
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    async fn delete_all_versions(
        &self,
        object_layer: &dyn ObjectLayer,
        bucket: &str,
        key: &str,
    ) -> Result<()> {
        let versions = object_layer
            .list_object_versions(bucket, key, i32::MAX)
            .await?;

        for version in versions {
            if version.key == key {
                object_layer
                    .delete_object_version(bucket, &version.key, &version.version_id)
                    .await?;
            }
        }
        Ok(())
    }
}

fn validate_config(config: &LifecycleConfiguration) -> Result<()> {
    if config.rules.is_empty() {
        return Err(MaxioError::InvalidArgument(
            "lifecycle configuration must include at least one rule".to_string(),
        ));
    }

    if config.rules.len() > 1000 {
        return Err(MaxioError::InvalidArgument(
            "lifecycle configuration allows a maximum of 1000 rules".to_string(),
        ));
    }

    // Check for duplicate rule IDs
    let mut seen_ids = std::collections::HashSet::new();
    for rule in &config.rules {
        if !rule.id.is_empty() && !seen_ids.insert(&rule.id) {
            return Err(MaxioError::InvalidArgument(format!(
                "duplicate rule ID: {}",
                rule.id
            )));
        }
    }

    for rule in &config.rules {
        let has_expiration = rule.expiration.is_some();
        let has_transition = rule.transition.is_some();
        let has_noncurrent_expiration = rule.noncurrent_version_expiration.is_some();
        let has_noncurrent_transition = rule.noncurrent_version_transition.is_some();
        let has_del_marker_expiration = rule.del_marker_expiration.is_some();

        if !has_expiration
            && !has_transition
            && !has_noncurrent_expiration
            && !has_noncurrent_transition
            && !has_del_marker_expiration
        {
            return Err(MaxioError::InvalidArgument(format!(
                "lifecycle rule {} must include at least one action",
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
            if exp.days.is_some_and(|days| days < 0) {
                return Err(MaxioError::InvalidArgument(format!(
                    "lifecycle rule {} expiration days must be non-negative",
                    rule.id
                )));
            }
        }

        if let Some(trans) = &rule.transition {
            if trans.days.is_some() && trans.date.is_some() {
                return Err(MaxioError::InvalidArgument(format!(
                    "lifecycle rule {} transition cannot include both days and date",
                    rule.id
                )));
            }
            if trans.storage_class.is_empty() {
                return Err(MaxioError::InvalidArgument(format!(
                    "lifecycle rule {} transition must specify storage class",
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

        if let Some(noncurrent_trans) = &rule.noncurrent_version_transition {
            if noncurrent_trans.storage_class.is_empty() {
                return Err(MaxioError::InvalidArgument(format!(
                    "lifecycle rule {} noncurrent version transition must specify storage class",
                    rule.id
                )));
            }
        }
    }

    Ok(())
}

fn object_info_to_opts(object: &ObjectInfo, is_latest: bool) -> ObjectOpts {
    // Extract user tags from metadata if present
    let user_tags = object
        .metadata
        .iter()
        .filter(|(k, _)| k.starts_with("x-amz-tagging"))
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");

    ObjectOpts {
        name: object.key.clone(),
        user_tags,
        mod_time: Some(object.last_modified),
        size: object.size,
        version_id: object.version_id.clone().unwrap_or_default(),
        is_latest,
        delete_marker: false, // ObjectInfo doesn't track delete markers
        num_versions: 1,
        ..Default::default()
    }
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
