use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use maxio_common::{
    error::{MaxioError, Result},
    types::ObjectInfo,
};
use maxio_storage::traits::ObjectLayer;
use serde::{Deserialize, Serialize};
use tokio::fs::{self, OpenOptions};
use tracing::{debug, warn};

use crate::{
    system::is_expired,
    types::{LifecycleConfiguration, RuleStatus},
    LifecycleSys,
};

const SCANNER_STATE_FILE: &str = ".scanner-state.json";
const SCANNER_LOCK_FILE: &str = ".scanner-leader.lock";
const SMALL_BRANCH_OBJECT_THRESHOLD: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScanMode {
    Normal,
    Deep,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerCycle {
    pub next: u64,
    pub current: u64,
    pub started: Option<DateTime<Utc>>,
    pub cycle_completed: Option<DateTime<Utc>>,
}

impl Default for ScannerCycle {
    fn default() -> Self {
        Self {
            next: 1,
            current: 0,
            started: None,
            cycle_completed: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerItem {
    pub path: String,
    pub bucket: String,
    pub object_name: String,
    pub lifecycle_config: Option<LifecycleConfiguration>,
    pub lifecycle_actionable: bool,
    pub heal_eligible: bool,
    pub heal_selected: bool,
    pub heal_verified: bool,
}

#[derive(Debug, Clone)]
pub struct ScannerConfig {
    pub interval: Duration,
    pub deep_scan_cycle_interval: u64,
    pub heal_check_sample_rate: u64,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30 * 60),
            deep_scan_cycle_interval: 24,
            heal_check_sample_rate: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScannerObjectCache {
    pub etag: String,
    pub size: i64,
    pub last_modified_unix_nanos: i64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct PersistedScannerState {
    cycle: ScannerCycle,
    data_usage_cache: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct FolderScanner {
    pub root: PathBuf,
    pub old_cache: HashMap<String, ScannerObjectCache>,
    pub new_cache: HashMap<String, ScannerObjectCache>,
    pub update_cache: HashMap<String, ScannerItem>,
    pub mode: ScanMode,
    pub cycle: ScannerCycle,
    pub data_usage_cache: HashMap<String, u64>,
    state_path: PathBuf,
    lock_path: PathBuf,
}

impl FolderScanner {
    pub fn new(root: PathBuf, mode: ScanMode) -> Self {
        Self {
            state_path: root.join(SCANNER_STATE_FILE),
            lock_path: root.join(SCANNER_LOCK_FILE),
            root,
            old_cache: HashMap::new(),
            new_cache: HashMap::new(),
            update_cache: HashMap::new(),
            mode,
            cycle: ScannerCycle::default(),
            data_usage_cache: HashMap::new(),
        }
    }

    pub fn set_scan_mode(&mut self, mode: ScanMode) {
        self.mode = mode;
    }

    pub async fn run_loop(
        &mut self,
        object_layer: Arc<dyn ObjectLayer>,
        lifecycle: Arc<LifecycleSys>,
        config: ScannerConfig,
    ) -> Result<()> {
        let mut ticker = tokio::time::interval(config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            if let Err(err) = self
                .run_cycle(Arc::clone(&object_layer), Arc::clone(&lifecycle), &config)
                .await
            {
                warn!(error = %err, "background scanner cycle failed");
            }
        }
    }

    pub async fn run_cycle(
        &mut self,
        object_layer: Arc<dyn ObjectLayer>,
        lifecycle: Arc<LifecycleSys>,
        config: &ScannerConfig,
    ) -> Result<ScannerCycle> {
        fs::create_dir_all(&self.root).await?;

        let Some(lock_guard) = self.acquire_leader_lock().await? else {
            debug!("scanner leader lock already held, skipping cycle");
            return Ok(self.cycle.clone());
        };

        let persisted = self.load_state().await?;
        self.cycle = persisted.cycle;
        self.data_usage_cache = persisted.data_usage_cache;

        self.cycle.current = self.cycle.next;
        self.cycle.next = self.cycle.current.saturating_add(1);
        self.cycle.started = Some(Utc::now());
        self.cycle.cycle_completed = None;

        let effective_mode = self.effective_mode(config.deep_scan_cycle_interval);
        self.rotate_caches();

        let buckets = object_layer.list_buckets().await?;
        let mut in_progress_usage = HashMap::with_capacity(buckets.len());

        for bucket in buckets {
            let bucket_name = bucket.name;
            let lifecycle_config = match lifecycle.get_config(&bucket_name).await {
                Ok(config) => config,
                Err(err) => {
                    warn!(bucket = %bucket_name, error = %err, "failed to load bucket lifecycle config during scan");
                    None
                }
            };

            let object_count = self
                .scan_bucket(
                    object_layer.as_ref(),
                    &bucket_name,
                    lifecycle_config,
                    effective_mode,
                    config,
                )
                .await?;
            in_progress_usage.insert(bucket_name, object_count);
            self.data_usage_cache = in_progress_usage.clone();
            self.persist_state().await?;
        }

        self.compact_updates();
        self.data_usage_cache = in_progress_usage;

        if let Err(err) = lifecycle.run_lifecycle_scan(object_layer).await {
            warn!(error = %err, "lifecycle evaluation failed during scanner cycle");
        }

        self.cycle.cycle_completed = Some(Utc::now());
        self.persist_state().await?;
        lock_guard.release().await;

        Ok(self.cycle.clone())
    }

    fn rotate_caches(&mut self) {
        self.old_cache = std::mem::take(&mut self.new_cache);
        self.update_cache.clear();
    }

    fn effective_mode(&self, deep_scan_cycle_interval: u64) -> ScanMode {
        if self.mode == ScanMode::Deep {
            return ScanMode::Deep;
        }

        if deep_scan_cycle_interval == 0 {
            return ScanMode::Normal;
        }

        if self.cycle.current % deep_scan_cycle_interval == 0 {
            ScanMode::Deep
        } else {
            ScanMode::Normal
        }
    }

    async fn scan_bucket(
        &mut self,
        object_layer: &dyn ObjectLayer,
        bucket: &str,
        lifecycle_config: Option<LifecycleConfiguration>,
        mode: ScanMode,
        config: &ScannerConfig,
    ) -> Result<u64> {
        let mut marker = String::new();
        let mut scanned_count = 0_u64;

        loop {
            let page = object_layer.list_objects(bucket, "", &marker, "", 1000).await?;
            for object in page.objects {
                scanned_count = scanned_count.saturating_add(1);
                self.process_object(
                    object_layer,
                    bucket,
                    object,
                    lifecycle_config.as_ref(),
                    mode,
                    config,
                )
                .await;
            }

            if !page.is_truncated {
                break;
            }

            marker = match page.next_marker {
                Some(next_marker) => next_marker,
                None => break,
            };
        }

        Ok(scanned_count)
    }

    async fn process_object(
        &mut self,
        object_layer: &dyn ObjectLayer,
        bucket: &str,
        object: ObjectInfo,
        lifecycle_config: Option<&LifecycleConfiguration>,
        mode: ScanMode,
        config: &ScannerConfig,
    ) {
        let cache_key = format!("{bucket}/{}", object.key);
        let cache_value = ScannerObjectCache {
            etag: object.etag.clone(),
            size: object.size,
            last_modified_unix_nanos: object.last_modified.timestamp_nanos_opt().unwrap_or_default(),
        };

        let changed = self.old_cache.get(&cache_key) != Some(&cache_value);
        self.new_cache.insert(cache_key.clone(), cache_value);
        if !changed {
            return;
        }

        let lifecycle_actionable = lifecycle_config
            .map(|config| {
                config
                    .rules
                    .iter()
                    .filter(|rule| rule.status == RuleStatus::Enabled)
                    .any(|rule| is_expired(&object, rule))
            })
            .unwrap_or(false);

        let mut item = ScannerItem {
            path: cache_key.clone(),
            bucket: bucket.to_string(),
            object_name: object.key.clone(),
            lifecycle_config: lifecycle_config.cloned(),
            lifecycle_actionable,
            heal_eligible: mode == ScanMode::Deep,
            heal_selected: false,
            heal_verified: false,
        };

        if self.should_trigger_heal_check(
            mode,
            bucket,
            &object.key,
            self.cycle.current,
            config.heal_check_sample_rate,
        ) {
            item.heal_selected = true;
            item.heal_verified = self.verify_integrity(object_layer, bucket, &object.key).await;
        }

        self.update_cache.insert(cache_key, item);
    }

    async fn verify_integrity(&self, object_layer: &dyn ObjectLayer, bucket: &str, key: &str) -> bool {
        match object_layer.get_object(bucket, key, None).await {
            Ok(_) => true,
            Err(err) => {
                warn!(bucket = %bucket, key = %key, error = %err, "integrity verification failed during deep scan");
                false
            }
        }
    }

    fn should_trigger_heal_check(
        &self,
        mode: ScanMode,
        bucket: &str,
        object_name: &str,
        cycle: u64,
        sample_rate: u64,
    ) -> bool {
        if mode != ScanMode::Deep || sample_rate == 0 {
            return false;
        }

        let mut hasher = DefaultHasher::new();
        cycle.hash(&mut hasher);
        bucket.hash(&mut hasher);
        object_name.hash(&mut hasher);
        hasher.finish() % sample_rate == 0
    }

    fn compact_updates(&mut self) {
        let mut branch_counters: HashMap<(String, String), usize> = HashMap::new();
        for item in self.update_cache.values() {
            let branch = item
                .object_name
                .split('/')
                .next()
                .unwrap_or_default()
                .to_string();
            let key = (item.bucket.clone(), branch);
            let next_count = branch_counters.get(&key).copied().unwrap_or(0).saturating_add(1);
            branch_counters.insert(key, next_count);
        }

        let mut compacted: HashMap<String, ScannerItem> = HashMap::new();
        let mut branch_added: HashMap<(String, String), bool> = HashMap::new();

        for item in self.update_cache.values() {
            let branch = item
                .object_name
                .split('/')
                .next()
                .unwrap_or_default()
                .to_string();
            let branch_key = (item.bucket.clone(), branch.clone());
            let count = branch_counters.get(&branch_key).copied().unwrap_or(0);

            if count < SMALL_BRANCH_OBJECT_THRESHOLD {
                if branch_added.get(&branch_key).copied().unwrap_or(false) {
                    continue;
                }

                let collapsed_key = format!("{}/{branch}/*", item.bucket);
                compacted.insert(
                    collapsed_key.clone(),
                    ScannerItem {
                        path: collapsed_key,
                        bucket: item.bucket.clone(),
                        object_name: format!("{branch}/*"),
                        lifecycle_config: item.lifecycle_config.clone(),
                        lifecycle_actionable: item.lifecycle_actionable,
                        heal_eligible: item.heal_eligible,
                        heal_selected: item.heal_selected,
                        heal_verified: item.heal_verified,
                    },
                );
                branch_added.insert(branch_key, true);
            } else {
                compacted.insert(item.path.clone(), item.clone());
            }
        }

        self.update_cache = compacted;
    }

    async fn load_state(&self) -> Result<PersistedScannerState> {
        match fs::read(&self.state_path).await {
            Ok(bytes) => serde_json::from_slice::<PersistedScannerState>(&bytes).map_err(|err| {
                MaxioError::InternalError(format!(
                    "failed to parse scanner state {}: {err}",
                    self.state_path.display()
                ))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Ok(PersistedScannerState::default())
            }
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    async fn persist_state(&self) -> Result<()> {
        let payload = PersistedScannerState {
            cycle: self.cycle.clone(),
            data_usage_cache: self.data_usage_cache.clone(),
        };
        let state_bytes = serde_json::to_vec_pretty(&payload).map_err(|err| {
            MaxioError::InternalError(format!(
                "failed to serialize scanner state {}: {err}",
                self.state_path.display()
            ))
        })?;
        fs::write(&self.state_path, state_bytes).await?;
        Ok(())
    }

    async fn acquire_leader_lock(&self) -> Result<Option<LeaderLockGuard>> {
        let lock_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.lock_path)
            .await;

        match lock_file {
            Ok(_) => {
                let lock_payload = format!(
                    "{{\"created\":\"{}\"}}",
                    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                );
                fs::write(&self.lock_path, lock_payload).await?;
                Ok(Some(LeaderLockGuard::new(self.lock_path.clone())))
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
            Err(err) => Err(MaxioError::Io(err)),
        }
    }
}

#[derive(Debug)]
struct LeaderLockGuard {
    path: PathBuf,
}

impl LeaderLockGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    async fn release(self) {
        if let Err(err) = try_remove_file(&self.path).await {
            warn!(path = %self.path.display(), error = %err, "failed to release scanner leader lock");
        }
    }
}

async fn try_remove_file(path: &Path) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(MaxioError::Io(err)),
    }
}
