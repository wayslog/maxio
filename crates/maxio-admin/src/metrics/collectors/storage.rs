use std::sync::Arc;

use maxio_common::error::Result;

use crate::metrics::registry::{GaugeMetric, MetricsRegistry};

pub struct StorageMetrics {
    disk_used_bytes: Arc<GaugeMetric>,
    disk_free_bytes: Arc<GaugeMetric>,
    disk_total_bytes: Arc<GaugeMetric>,
    objects_count: Arc<GaugeMetric>,
    buckets_count: Arc<GaugeMetric>,
}

impl StorageMetrics {
    pub fn register(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            disk_used_bytes: registry.register_gauge(
                "disk_used_bytes",
                "Used disk space in bytes",
                &[],
            )?,
            disk_free_bytes: registry.register_gauge(
                "disk_free_bytes",
                "Free disk space in bytes",
                &[],
            )?,
            disk_total_bytes: registry.register_gauge(
                "disk_total_bytes",
                "Total disk capacity in bytes",
                &[],
            )?,
            objects_count: registry.register_gauge(
                "objects_count",
                "Total number of stored objects",
                &[],
            )?,
            buckets_count: registry.register_gauge(
                "buckets_count",
                "Total number of buckets",
                &[],
            )?,
        })
    }

    pub fn update_snapshot(
        &self,
        disk_used_bytes: u64,
        disk_free_bytes: u64,
        disk_total_bytes: u64,
        objects_count: u64,
        buckets_count: u64,
    ) {
        self.disk_used_bytes
            .set(&[], saturating_i64_from_u64(disk_used_bytes));
        self.disk_free_bytes
            .set(&[], saturating_i64_from_u64(disk_free_bytes));
        self.disk_total_bytes
            .set(&[], saturating_i64_from_u64(disk_total_bytes));
        self.objects_count
            .set(&[], saturating_i64_from_u64(objects_count));
        self.buckets_count
            .set(&[], saturating_i64_from_u64(buckets_count));
    }
}

fn saturating_i64_from_u64(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value as i64
    }
}
