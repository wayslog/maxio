use std::{sync::Arc, time::Duration};

use maxio_common::error::Result;

use crate::metrics::registry::{CounterMetric, HistogramMetric, MetricsRegistry};

pub struct ApiMetrics {
    requests_total: Arc<CounterMetric>,
    request_duration_seconds: Arc<HistogramMetric>,
    errors_total: Arc<CounterMetric>,
}

impl ApiMetrics {
    pub fn register(registry: &MetricsRegistry) -> Result<Self> {
        let requests_total = registry.register_counter(
            "requests_total",
            "Total number of admin API requests",
            &["method", "status"],
        )?;

        let request_duration_seconds = registry.register_histogram(
            "request_duration_seconds",
            "Duration of admin API requests in seconds",
            &["method", "status"],
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )?;

        let errors_total = registry.register_counter(
            "errors_total",
            "Total number of admin API errors",
            &["method", "status"],
        )?;

        Ok(Self {
            requests_total,
            request_duration_seconds,
            errors_total,
        })
    }

    pub fn record_request(&self, method: &str, status: u16, duration: Duration) {
        let status_value = status.to_string();
        self.requests_total.inc_one(&[method, &status_value]);
        self.request_duration_seconds
            .observe(&[method, &status_value], duration.as_secs_f64());

        if status >= 500 {
            self.errors_total.inc_one(&[method, &status_value]);
        }
    }

    pub fn record_error(&self, method: &str, status: u16) {
        let status_value = status.to_string();
        self.errors_total.inc_one(&[method, &status_value]);
    }
}
