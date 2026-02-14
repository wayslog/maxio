#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl MetricType {
    pub fn as_prometheus_type(&self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricDescriptor {
    pub name: String,
    pub help: String,
    pub metric_type: MetricType,
    pub variable_labels: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Histogram {
        buckets: Vec<(f64, u64)>,
        count: u64,
        sum: f64,
    },
}

#[derive(Debug, Clone)]
pub struct MetricSample {
    pub labels: Vec<(String, String)>,
    pub value: MetricValue,
}

#[derive(Debug, Clone)]
pub struct CollectedMetric {
    pub descriptor: MetricDescriptor,
    pub samples: Vec<MetricSample>,
}
