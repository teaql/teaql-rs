use async_trait::async_trait;

/// Metric value types supported by Prometheus.
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// A monotonically increasing counter.
    Counter(f64),
    /// A value that can go up and down.
    Gauge(f64),
    /// A distribution of values with configurable buckets.
    Histogram {
        sum: f64,
        count: u64,
        buckets: Vec<(f64, u64)>,
    },
}

/// A single metric with name, help text, labels, and value.
#[derive(Debug, Clone)]
pub struct Metric {
    /// Metric name (e.g. "teaql_http_requests_total")
    pub name: String,
    /// Help text describing the metric
    pub help: String,
    /// Label key-value pairs
    pub labels: Vec<(String, String)>,
    /// The metric value
    pub value: MetricValue,
}

impl Metric {
    /// Create a counter metric.
    pub fn counter(name: impl Into<String>, help: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            help: help.into(),
            labels: Vec::new(),
            value: MetricValue::Counter(value),
        }
    }

    /// Create a gauge metric.
    pub fn gauge(name: impl Into<String>, help: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            help: help.into(),
            labels: Vec::new(),
            value: MetricValue::Gauge(value),
        }
    }

    /// Add a label to this metric.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }
}

/// Metrics collector trait.
///
/// Each component can contribute its own metrics:
/// - Database: connection pool size, active connections, query latency
/// - HTTP: request count, latency distribution
/// - Nacos: heartbeat success/failure count
#[async_trait]
pub trait MetricsCollector: Send + Sync {
    /// Collect a snapshot of current metrics.
    async fn collect(&self) -> Vec<Metric>;
}

/// Format metrics as Prometheus exposition format.
///
/// Output conforms to the [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/).
pub fn to_prometheus_exposition(metrics: &[Metric]) -> String {
    let mut output = String::new();
    for metric in metrics {
        // # HELP line
        output.push_str(&format!("# HELP {} {}\n", metric.name, metric.help));
        // # TYPE line
        let type_str = match &metric.value {
            MetricValue::Counter(_) => "counter",
            MetricValue::Gauge(_) => "gauge",
            MetricValue::Histogram { .. } => "histogram",
        };
        output.push_str(&format!("# TYPE {} {}\n", metric.name, type_str));

        // Labels string
        let labels_str = format_labels(&metric.labels);

        // Value lines
        match &metric.value {
            MetricValue::Counter(v) | MetricValue::Gauge(v) => {
                output.push_str(&format!("{}{} {}\n", metric.name, labels_str, v));
            }
            MetricValue::Histogram {
                sum,
                count,
                buckets,
            } => {
                for (le, c) in buckets {
                    let mut bucket_labels = metric.labels.clone();
                    bucket_labels.push(("le".to_string(), format!("{le}")));
                    let bucket_labels_str = format_labels(&bucket_labels);
                    output.push_str(&format!(
                        "{}_bucket{} {}\n",
                        metric.name, bucket_labels_str, c
                    ));
                }
                // +Inf bucket
                let mut inf_labels = metric.labels.clone();
                inf_labels.push(("le".to_string(), "+Inf".to_string()));
                let inf_labels_str = format_labels(&inf_labels);
                output.push_str(&format!(
                    "{}_bucket{} {}\n",
                    metric.name, inf_labels_str, count
                ));

                output.push_str(&format!("{}_sum{} {}\n", metric.name, labels_str, sum));
                output.push_str(&format!("{}_count{} {}\n", metric.name, labels_str, count));
            }
        }
    }
    output
}

fn format_labels(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        String::new()
    } else {
        let pairs: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
        format!("{{{}}}", pairs.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_metric() {
        let metric = Metric::counter("http_requests_total", "Total HTTP requests", 1234.0);
        assert_eq!(metric.name, "http_requests_total");
        assert!(metric.labels.is_empty());
        assert!(matches!(metric.value, MetricValue::Counter(v) if v == 1234.0));
    }

    #[test]
    fn test_gauge_metric_with_labels() {
        let metric = Metric::gauge("db_pool_active", "Active connections", 5.0)
            .with_label("pool", "primary");
        assert_eq!(metric.labels.len(), 1);
        assert_eq!(
            metric.labels[0],
            ("pool".to_string(), "primary".to_string())
        );
    }

    #[test]
    fn test_prometheus_counter_format() {
        let metrics = vec![Metric::counter(
            "teaql_requests_total",
            "Total requests",
            42.0,
        )];
        let output = to_prometheus_exposition(&metrics);
        assert!(output.contains("# HELP teaql_requests_total Total requests\n"));
        assert!(output.contains("# TYPE teaql_requests_total counter\n"));
        assert!(output.contains("teaql_requests_total 42\n"));
    }

    #[test]
    fn test_prometheus_gauge_with_labels() {
        let metrics = vec![
            Metric::gauge("db_connections", "DB connections", 10.0).with_label("pool", "main"),
        ];
        let output = to_prometheus_exposition(&metrics);
        assert!(output.contains("# TYPE db_connections gauge\n"));
        assert!(output.contains("db_connections{pool=\"main\"} 10\n"));
    }

    #[test]
    fn test_prometheus_histogram_format() {
        let metrics = vec![Metric {
            name: "query_duration_seconds".to_string(),
            help: "Query duration".to_string(),
            labels: Vec::new(),
            value: MetricValue::Histogram {
                sum: 12.5,
                count: 155,
                buckets: vec![(0.01, 100), (0.1, 150), (1.0, 155)],
            },
        }];
        let output = to_prometheus_exposition(&metrics);
        assert!(output.contains("# TYPE query_duration_seconds histogram\n"));
        assert!(output.contains("query_duration_seconds_bucket{le=\"0.01\"} 100\n"));
        assert!(output.contains("query_duration_seconds_bucket{le=\"0.1\"} 150\n"));
        assert!(output.contains("query_duration_seconds_bucket{le=\"1\"} 155\n"));
        assert!(output.contains("query_duration_seconds_bucket{le=\"+Inf\"} 155\n"));
        assert!(output.contains("query_duration_seconds_sum 12.5\n"));
        assert!(output.contains("query_duration_seconds_count 155\n"));
    }

    #[test]
    fn test_prometheus_empty_metrics() {
        let output = to_prometheus_exposition(&[]);
        assert!(output.is_empty());
    }

    #[test]
    fn test_format_labels_empty() {
        assert_eq!(format_labels(&[]), "");
    }

    #[test]
    fn test_format_labels_multiple() {
        let labels = vec![
            ("method".to_string(), "GET".to_string()),
            ("path".to_string(), "/api".to_string()),
        ];
        assert_eq!(format_labels(&labels), "{method=\"GET\",path=\"/api\"}");
    }

    struct MockCollector;

    #[async_trait]
    impl MetricsCollector for MockCollector {
        async fn collect(&self) -> Vec<Metric> {
            vec![Metric::counter("test_total", "Test counter", 1.0)]
        }
    }

    #[tokio::test]
    async fn test_metrics_collector_trait() {
        let collector = MockCollector;
        let metrics = collector.collect().await;
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "test_total");
    }
}
