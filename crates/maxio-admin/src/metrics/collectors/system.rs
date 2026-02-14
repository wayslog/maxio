use std::{sync::Arc, time::Instant};

use maxio_common::error::Result;

use crate::metrics::registry::{GaugeMetric, MetricsRegistry};

pub struct SystemMetrics {
    start_time: Instant,
    uptime_seconds: Arc<GaugeMetric>,
    process_resident_memory_bytes: Arc<GaugeMetric>,
    threads_count: Arc<GaugeMetric>,
}

impl SystemMetrics {
    pub fn register(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            start_time: Instant::now(),
            uptime_seconds: registry.register_gauge(
                "uptime_seconds",
                "Process uptime in seconds",
                &[],
            )?,
            process_resident_memory_bytes: registry.register_gauge(
                "process_resident_memory_bytes",
                "Resident memory size of the process in bytes",
                &[],
            )?,
            threads_count: registry.register_gauge(
                "threads_count",
                "Number of worker threads (goroutine equivalent)",
                &[],
            )?,
        })
    }

    pub fn refresh(&self) {
        self.uptime_seconds
            .set(&[], self.start_time.elapsed().as_secs() as i64);

        if let Some(resident_bytes) = read_resident_memory_bytes() {
            self.process_resident_memory_bytes
                .set(&[], saturating_i64_from_u64(resident_bytes));
        }

        if let Ok(parallelism) = std::thread::available_parallelism() {
            self.threads_count.set(&[], parallelism.get() as i64);
        }
    }
}

fn saturating_i64_from_u64(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value as i64
    }
}

#[cfg(target_os = "linux")]
fn read_resident_memory_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())?;
            return kb.checked_mul(1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn read_resident_memory_bytes() -> Option<u64> {
    None
}
