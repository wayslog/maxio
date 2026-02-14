use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use maxio_common::error::{MaxioError, Result};

use crate::metrics::types::{CollectedMetric, MetricDescriptor, MetricSample, MetricType, MetricValue};

type LabelValues = Vec<String>;

trait RegisteredMetric: Send + Sync {
    fn descriptor(&self) -> MetricDescriptor;
    fn collect(&self) -> Vec<MetricSample>;
}

pub struct MetricsRegistry {
    metrics: RwLock<HashMap<String, Arc<dyn RegisteredMetric>>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_counter(
        &self,
        name: &str,
        help: &str,
        variable_labels: &[&str],
    ) -> Result<Arc<CounterMetric>> {
        let metric = Arc::new(CounterMetric::new(name, help, variable_labels));
        self.register(metric.clone())?;
        Ok(metric)
    }

    pub fn register_gauge(
        &self,
        name: &str,
        help: &str,
        variable_labels: &[&str],
    ) -> Result<Arc<GaugeMetric>> {
        let metric = Arc::new(GaugeMetric::new(name, help, variable_labels));
        self.register(metric.clone())?;
        Ok(metric)
    }

    pub fn register_histogram(
        &self,
        name: &str,
        help: &str,
        variable_labels: &[&str],
        buckets: &[f64],
    ) -> Result<Arc<HistogramMetric>> {
        let metric = Arc::new(HistogramMetric::new(name, help, variable_labels, buckets));
        self.register(metric.clone())?;
        Ok(metric)
    }

    pub fn collect_all(&self) -> Vec<CollectedMetric> {
        let metrics = match self.metrics.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };

        let mut collected = metrics
            .values()
            .map(|metric| CollectedMetric {
                descriptor: metric.descriptor(),
                samples: metric.collect(),
            })
            .collect::<Vec<_>>();

        collected.sort_by(|left, right| left.descriptor.name.cmp(&right.descriptor.name));
        collected
    }

    pub fn render_prometheus(&self) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_millis());

        let metrics = self.collect_all();
        let mut output = String::new();

        for metric in metrics {
            output.push_str("# HELP ");
            output.push_str(&metric.descriptor.name);
            output.push(' ');
            output.push_str(&escape_help(&metric.descriptor.help));
            output.push('\n');

            output.push_str("# TYPE ");
            output.push_str(&metric.descriptor.name);
            output.push(' ');
            output.push_str(metric.descriptor.metric_type.as_prometheus_type());
            output.push('\n');

            for sample in metric.samples {
                match sample.value {
                    MetricValue::Counter(value) | MetricValue::Gauge(value) => {
                        output.push_str(&render_sample_line(
                            &metric.descriptor.name,
                            &sample.labels,
                            value,
                            timestamp,
                        ));
                    }
                    MetricValue::Histogram {
                        buckets,
                        count,
                        sum,
                    } => {
                        let mut cumulative = 0_u64;
                        for (bound, bucket_count) in buckets {
                            cumulative = cumulative.saturating_add(bucket_count);
                            let mut labels = sample.labels.clone();
                            labels.push(("le".to_string(), format_bucket_bound(bound)));
                            output.push_str(&render_sample_line(
                                &format!("{}_bucket", metric.descriptor.name),
                                &labels,
                                cumulative as f64,
                                timestamp,
                            ));
                        }

                        output.push_str(&render_sample_line(
                            &format!("{}_sum", metric.descriptor.name),
                            &sample.labels,
                            sum,
                            timestamp,
                        ));
                        output.push_str(&render_sample_line(
                            &format!("{}_count", metric.descriptor.name),
                            &sample.labels,
                            count as f64,
                            timestamp,
                        ));
                    }
                }
            }
        }

        output
    }

    fn register<M: RegisteredMetric + 'static>(&self, metric: Arc<M>) -> Result<()> {
        let descriptor = metric.descriptor();
        let name = descriptor.name.clone();
        let mut metrics = self.metrics.write().map_err(|_| {
            MaxioError::InternalError("failed to acquire metrics registry lock".to_string())
        })?;

        if metrics.contains_key(&name) {
            return Err(MaxioError::InvalidArgument(format!(
                "metric already registered: {name}"
            )));
        }

        metrics.insert(name, metric);
        Ok(())
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CounterMetric {
    descriptor: MetricDescriptor,
    series: RwLock<HashMap<LabelValues, Arc<AtomicU64>>>,
}

impl CounterMetric {
    fn new(name: &str, help: &str, variable_labels: &[&str]) -> Self {
        Self {
            descriptor: MetricDescriptor {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Counter,
                variable_labels: variable_labels.iter().map(|label| (*label).to_string()).collect(),
            },
            series: RwLock::new(HashMap::new()),
        }
    }

    pub fn inc(&self, labels: &[&str], value: u64) {
        let series = self.get_or_create_series(labels);
        series.fetch_add(value, Ordering::Relaxed);
    }

    pub fn inc_one(&self, labels: &[&str]) {
        self.inc(labels, 1);
    }

    fn get_or_create_series(&self, labels: &[&str]) -> Arc<AtomicU64> {
        let label_values = normalize_labels(&self.descriptor, labels);
        if let Ok(guard) = self.series.read()
            && let Some(existing) = guard.get(&label_values)
        {
            return existing.clone();
        }

        match self.series.write() {
            Ok(mut guard) => guard
                .entry(label_values)
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .clone(),
            Err(_) => Arc::new(AtomicU64::new(0)),
        }
    }
}

impl RegisteredMetric for CounterMetric {
    fn descriptor(&self) -> MetricDescriptor {
        self.descriptor.clone()
    }

    fn collect(&self) -> Vec<MetricSample> {
        let series = match self.series.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };

        series
            .iter()
            .map(|(label_values, value)| MetricSample {
                labels: materialize_labels(&self.descriptor, label_values),
                value: MetricValue::Counter(value.load(Ordering::Relaxed) as f64),
            })
            .collect()
    }
}

pub struct GaugeMetric {
    descriptor: MetricDescriptor,
    series: RwLock<HashMap<LabelValues, Arc<AtomicI64>>>,
}

impl GaugeMetric {
    fn new(name: &str, help: &str, variable_labels: &[&str]) -> Self {
        Self {
            descriptor: MetricDescriptor {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Gauge,
                variable_labels: variable_labels.iter().map(|label| (*label).to_string()).collect(),
            },
            series: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, labels: &[&str], value: i64) {
        let series = self.get_or_create_series(labels);
        series.store(value, Ordering::Relaxed);
    }

    pub fn inc(&self, labels: &[&str], value: i64) {
        let series = self.get_or_create_series(labels);
        series.fetch_add(value, Ordering::Relaxed);
    }

    pub fn dec(&self, labels: &[&str], value: i64) {
        self.inc(labels, -value);
    }

    fn get_or_create_series(&self, labels: &[&str]) -> Arc<AtomicI64> {
        let label_values = normalize_labels(&self.descriptor, labels);
        if let Ok(guard) = self.series.read()
            && let Some(existing) = guard.get(&label_values)
        {
            return existing.clone();
        }

        match self.series.write() {
            Ok(mut guard) => guard
                .entry(label_values)
                .or_insert_with(|| Arc::new(AtomicI64::new(0)))
                .clone(),
            Err(_) => Arc::new(AtomicI64::new(0)),
        }
    }
}

impl RegisteredMetric for GaugeMetric {
    fn descriptor(&self) -> MetricDescriptor {
        self.descriptor.clone()
    }

    fn collect(&self) -> Vec<MetricSample> {
        let series = match self.series.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };

        series
            .iter()
            .map(|(label_values, value)| MetricSample {
                labels: materialize_labels(&self.descriptor, label_values),
                value: MetricValue::Gauge(value.load(Ordering::Relaxed) as f64),
            })
            .collect()
    }
}

pub struct HistogramMetric {
    descriptor: MetricDescriptor,
    buckets: Vec<f64>,
    series: RwLock<HashMap<LabelValues, Arc<HistogramSeries>>>,
}

struct HistogramSeries {
    bucket_counts: Vec<AtomicU64>,
    count: AtomicU64,
    sum: Mutex<f64>,
}

impl HistogramMetric {
    fn new(name: &str, help: &str, variable_labels: &[&str], buckets: &[f64]) -> Self {
        let mut sorted_buckets = buckets.to_vec();
        sorted_buckets.sort_by(|left, right| left.total_cmp(right));

        Self {
            descriptor: MetricDescriptor {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Histogram,
                variable_labels: variable_labels.iter().map(|label| (*label).to_string()).collect(),
            },
            buckets: sorted_buckets,
            series: RwLock::new(HashMap::new()),
        }
    }

    pub fn observe(&self, labels: &[&str], value: f64) {
        let series = self.get_or_create_series(labels);

        let bucket_index = self
            .buckets
            .iter()
            .position(|bucket| value <= *bucket)
            .unwrap_or(self.buckets.len());

        if let Some(bucket) = series.bucket_counts.get(bucket_index) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }

        series.count.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut sum) = series.sum.lock() {
            *sum += value;
        }
    }

    fn get_or_create_series(&self, labels: &[&str]) -> Arc<HistogramSeries> {
        let label_values = normalize_labels(&self.descriptor, labels);
        if let Ok(guard) = self.series.read()
            && let Some(existing) = guard.get(&label_values)
        {
            return existing.clone();
        }

        match self.series.write() {
            Ok(mut guard) => guard
                .entry(label_values)
                .or_insert_with(|| {
                    Arc::new(HistogramSeries {
                        bucket_counts: (0..self.buckets.len() + 1)
                            .map(|_| AtomicU64::new(0))
                            .collect(),
                        count: AtomicU64::new(0),
                        sum: Mutex::new(0.0),
                    })
                })
                .clone(),
            Err(_) => Arc::new(HistogramSeries {
                bucket_counts: (0..self.buckets.len() + 1)
                    .map(|_| AtomicU64::new(0))
                    .collect(),
                count: AtomicU64::new(0),
                sum: Mutex::new(0.0),
            }),
        }
    }
}

impl RegisteredMetric for HistogramMetric {
    fn descriptor(&self) -> MetricDescriptor {
        self.descriptor.clone()
    }

    fn collect(&self) -> Vec<MetricSample> {
        let series = match self.series.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };

        series
            .iter()
            .map(|(label_values, entry)| {
                let mut buckets = self
                    .buckets
                    .iter()
                    .enumerate()
                    .map(|(index, bound)| {
                        (*bound, entry.bucket_counts[index].load(Ordering::Relaxed))
                    })
                    .collect::<Vec<_>>();

                let inf_count = entry.bucket_counts[self.buckets.len()].load(Ordering::Relaxed);
                buckets.push((f64::INFINITY, inf_count));

                let sum = match entry.sum.lock() {
                    Ok(value) => *value,
                    Err(_) => 0.0,
                };

                MetricSample {
                    labels: materialize_labels(&self.descriptor, label_values),
                    value: MetricValue::Histogram {
                        buckets,
                        count: entry.count.load(Ordering::Relaxed),
                        sum,
                    },
                }
            })
            .collect()
    }
}

fn normalize_labels(descriptor: &MetricDescriptor, labels: &[&str]) -> LabelValues {
    let expected = descriptor.variable_labels.len();
    (0..expected)
        .map(|index| labels.get(index).copied().unwrap_or_default().to_string())
        .collect()
}

fn materialize_labels(descriptor: &MetricDescriptor, values: &[String]) -> Vec<(String, String)> {
    descriptor
        .variable_labels
        .iter()
        .zip(values.iter())
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn render_sample_line(
    name: &str,
    labels: &[(String, String)],
    value: f64,
    timestamp: Option<u128>,
) -> String {
    let mut rendered = String::new();
    rendered.push_str(name);

    if !labels.is_empty() {
        rendered.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                rendered.push(',');
            }
            rendered.push_str(key);
            rendered.push_str("=\"");
            rendered.push_str(&escape_label_value(value));
            rendered.push('"');
        }
        rendered.push('}');
    }

    rendered.push(' ');
    rendered.push_str(&format_metric_value(value));

    if let Some(ts) = timestamp {
        rendered.push(' ');
        rendered.push_str(&ts.to_string());
    }

    rendered.push('\n');
    rendered
}

fn format_metric_value(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn format_bucket_bound(value: f64) -> String {
    if value.is_infinite() {
        "+Inf".to_string()
    } else {
        value.to_string()
    }
}

fn escape_help(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
