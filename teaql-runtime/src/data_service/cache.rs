use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use teaql_core::Record;

#[derive(Debug, Default)]
pub struct InMemoryAggregationCache {
    namespace: String,
    entries: Mutex<HashMap<String, AggregationCacheEntry>>,
}

pub trait AggregationCacheBackend: Send + Sync {
    fn namespace(&self) -> &str;
    fn get(&self, key: &str, max_age_millis: u64) -> Option<Vec<Record>>;
    fn put(&self, key: String, rows: Vec<Record>);
    fn invalidate_namespace(&self, namespace: &str);
}

#[derive(Debug, Clone)]
struct AggregationCacheEntry {
    stored_at: Instant,
    rows: Vec<Record>,
}

impl InMemoryAggregationCache {
    pub fn with_namespace(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl AggregationCacheBackend for InMemoryAggregationCache {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn get(&self, key: &str, max_age_millis: u64) -> Option<Vec<Record>> {
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(key)?;
        (max_age_millis == 0 || entry.stored_at.elapsed() <= Duration::from_millis(max_age_millis))
            .then(|| entry.rows.clone())
    }

    fn put(&self, key: String, rows: Vec<Record>) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(
                key,
                AggregationCacheEntry {
                    stored_at: Instant::now(),
                    rows,
                },
            );
        }
    }

    fn invalidate_namespace(&self, namespace: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            let prefix = format!("{namespace}::");
            entries.retain(|key, _| !key.starts_with(&prefix));
        }
    }
}

impl InMemoryAggregationCache {
    pub fn get(&self, key: &str, max_age_millis: u64) -> Option<Vec<Record>> {
        AggregationCacheBackend::get(self, key, max_age_millis)
    }

    pub fn put(&self, key: String, rows: Vec<Record>) {
        AggregationCacheBackend::put(self, key, rows);
    }

    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }

    pub fn invalidate_namespace(&self, namespace: &str) {
        AggregationCacheBackend::invalidate_namespace(self, namespace);
    }
}
