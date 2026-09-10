use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use teaql_core::Value;

use super::UserContext;

#[derive(Debug, Clone, PartialEq)]
pub struct ContinuousPageCursor {
    pub cursor_id: String,
    pub query_key: String,
    pub entity: String,
    pub direction: teaql_core::SortDirection,
    pub boundary: Value,
    pub page_size: u64,
    pub next_offset: u64,
    pub expires_at: SystemTime,
}

#[async_trait::async_trait]
pub trait ContinuousPageCursorStore: Send + Sync + 'static {
    async fn get(
        &self,
        query_key: &str,
        target_offset: u64,
    ) -> Result<Option<ContinuousPageCursor>, String>;
    async fn put(&self, cursor: ContinuousPageCursor) -> Result<(), String>;
    async fn invalidate(&self, query_key: &str) -> Result<(), String>;
}

pub struct InMemoryContinuousPageCursorStore {
    cursors: Mutex<HashMap<String, ContinuousPageCursor>>,
    max_entries: usize,
}

impl Default for InMemoryContinuousPageCursorStore {
    fn default() -> Self {
        Self {
            cursors: Mutex::new(HashMap::new()),
            max_entries: 4096,
        }
    }
}

#[async_trait::async_trait]
impl ContinuousPageCursorStore for InMemoryContinuousPageCursorStore {
    async fn get(
        &self,
        query_key: &str,
        target_offset: u64,
    ) -> Result<Option<ContinuousPageCursor>, String> {
        let key = format!("{query_key}:{target_offset}");
        let mut cursors = self.cursors.lock().map_err(|error| error.to_string())?;
        if cursors
            .get(&key)
            .is_some_and(|cursor| cursor.expires_at <= SystemTime::now())
        {
            cursors.remove(&key);
        }
        Ok(cursors.get(&key).cloned())
    }

    async fn put(&self, cursor: ContinuousPageCursor) -> Result<(), String> {
        let key = format!("{}:{}", cursor.query_key, cursor.next_offset);
        let mut cursors = self.cursors.lock().map_err(|error| error.to_string())?;
        if cursors.len() >= self.max_entries {
            if let Some(oldest) = cursors
                .iter()
                .min_by_key(|(_, value)| value.expires_at)
                .map(|(key, _)| key.clone())
            {
                cursors.remove(&oldest);
            }
        }
        cursors.insert(key, cursor);
        Ok(())
    }

    async fn invalidate(&self, query_key: &str) -> Result<(), String> {
        let prefix = format!("{query_key}:");
        self.cursors
            .lock()
            .map_err(|error| error.to_string())?
            .retain(|key, _| !key.starts_with(&prefix));
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RetainedIdSet {
    pub query_key: String,
    pub ids: Arc<Vec<u64>>,
    pub expires_at: SystemTime,
}

#[async_trait::async_trait]
pub trait IdSetStore: Send + Sync + 'static {
    async fn get(&self, query_key: &str) -> Result<Option<RetainedIdSet>, String>;
    async fn put(&self, id_set: RetainedIdSet) -> Result<(), String>;
    async fn invalidate(&self, query_key: &str) -> Result<(), String>;
}

pub struct InMemoryIdSetStore {
    sets: Mutex<HashMap<String, RetainedIdSet>>,
    max_entries: usize,
    max_bytes: usize,
}

impl Default for InMemoryIdSetStore {
    fn default() -> Self {
        Self {
            sets: Mutex::new(HashMap::new()),
            max_entries: 64,
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

impl InMemoryIdSetStore {
    fn retained_bytes(sets: &HashMap<String, RetainedIdSet>) -> usize {
        sets.values()
            .map(|value| value.ids.len().saturating_mul(std::mem::size_of::<u64>()))
            .sum()
    }
}

#[async_trait::async_trait]
impl IdSetStore for InMemoryIdSetStore {
    async fn get(&self, query_key: &str) -> Result<Option<RetainedIdSet>, String> {
        let mut sets = self.sets.lock().map_err(|error| error.to_string())?;
        if sets
            .get(query_key)
            .is_some_and(|value| value.expires_at <= SystemTime::now())
        {
            sets.remove(query_key);
        }
        Ok(sets.get(query_key).cloned())
    }

    async fn put(&self, id_set: RetainedIdSet) -> Result<(), String> {
        let incoming_bytes = id_set.ids.len().saturating_mul(std::mem::size_of::<u64>());
        if incoming_bytes > self.max_bytes {
            return Err("ID set exceeds the process-local store memory ceiling".to_owned());
        }
        let mut sets = self.sets.lock().map_err(|error| error.to_string())?;
        sets.retain(|_, value| value.expires_at > SystemTime::now());
        while sets.len() >= self.max_entries
            || Self::retained_bytes(&sets).saturating_add(incoming_bytes) > self.max_bytes
        {
            let Some(oldest) = sets
                .iter()
                .min_by_key(|(_, value)| value.expires_at)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            sets.remove(&oldest);
        }
        sets.insert(id_set.query_key.clone(), id_set);
        Ok(())
    }

    async fn invalidate(&self, query_key: &str) -> Result<(), String> {
        self.sets
            .lock()
            .map_err(|error| error.to_string())?
            .remove(query_key);
        Ok(())
    }
}

pub(super) fn id_set_build_lock(query_key: &str) -> Arc<futures_util::lock::Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, std::sync::Weak<futures_util::lock::Mutex<()>>>>> =
        OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("ID set build lock registry poisoned");
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(query_key).and_then(std::sync::Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(futures_util::lock::Mutex::new(()));
    locks.insert(query_key.to_owned(), Arc::downgrade(&lock));
    lock
}

impl UserContext {
    pub fn set_continuous_page_cursor_store(&mut self, store: Arc<dyn ContinuousPageCursorStore>) {
        self.continuous_page_cursor_store = store;
    }

    pub fn continuous_page_plan(&self) -> Option<String> {
        self.continuous_page_observation
            .lock()
            .ok()
            .map(|value| value.0.clone())
    }

    pub fn continuous_page_cursor_id(&self) -> Option<String> {
        self.continuous_page_observation
            .lock()
            .ok()
            .and_then(|value| value.1.clone())
    }

    pub(crate) fn observe_continuous_page(
        &self,
        plan: impl Into<String>,
        cursor_id: Option<String>,
    ) {
        if let Ok(mut observation) = self.continuous_page_observation.lock() {
            *observation = (plan.into(), cursor_id);
        }
    }

    pub(crate) fn continuous_page_cursor_store(&self) -> &dyn ContinuousPageCursorStore {
        self.continuous_page_cursor_store.as_ref()
    }

    pub fn set_id_set_store(&mut self, store: Arc<dyn IdSetStore>) {
        self.id_set_store = store;
    }

    pub fn id_set_plan(&self) -> Option<String> {
        self.id_set_observation
            .lock()
            .ok()
            .map(|observation| observation.0.clone())
    }

    pub fn id_set_count(&self) -> Option<u64> {
        self.id_set_observation
            .lock()
            .ok()
            .and_then(|observation| observation.1)
    }

    pub(crate) fn observe_id_set(&self, plan: impl Into<String>, count: Option<u64>) {
        if let Ok(mut observation) = self.id_set_observation.lock() {
            *observation = (plan.into(), count);
        }
    }

    pub(crate) fn id_set_store(&self) -> &dyn IdSetStore {
        self.id_set_store.as_ref()
    }

    pub(crate) fn id_set_build_lock(&self, query_key: &str) -> Arc<futures_util::lock::Mutex<()>> {
        id_set_build_lock(query_key)
    }
}
