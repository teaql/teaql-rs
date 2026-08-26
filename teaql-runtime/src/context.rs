use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use teaql_core::{EntityDescriptor, Value};
use teaql_sql::{CompiledQuery, DatabaseKind};

use crate::EntityRuntimeState;
use crate::{
    CheckObjectStatus, CheckResult, CheckResults, CheckerRegistry, ContextError,
    EntityDataServiceBehavior, EntityDataServiceBehaviorRegistry, EntityGraphBuilder,
    EntityRegistry, GraphNode, InMemoryEntityGraphDecoderRegistry, InternalIdGenerator, Language,
    MetadataStore, ObjectLocation, RawAuditEvent, RawAuditEventSink, RequestPolicy, RuntimeError,
    local_id_generator,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextEntityRef {
    pub entity_type: String,
    pub id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRootError {
    pub expected_entity_type: String,
    pub actual_root: Option<ContextEntityRef>,
}

impl std::fmt::Display for ContextRootError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.actual_root {
            None => write!(
                formatter,
                "active root {} is missing from UserContext",
                self.expected_entity_type
            ),
            Some(actual) => write!(
                formatter,
                "active root type is {}, expected {}",
                actual.entity_type, self.expected_entity_type
            ),
        }
    }
}

impl std::error::Error for ContextRootError {}

#[cfg(test)]
mod active_root_tests {
    use super::UserContext;

    #[test]
    fn active_root_is_typed_and_fails_closed() {
        let context = UserContext::new().with_active_root("Tenant", 42);
        assert_eq!(context.require_active_root("Tenant").unwrap().id, 42);
        assert!(context.require_active_root("Organization").is_err());
        assert!(UserContext::new().require_active_root("Tenant").is_err());
    }
}

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

fn id_set_build_lock(query_key: &str) -> Arc<futures_util::lock::Mutex<()>> {
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
        let mut cursors = self.cursors.lock().map_err(|e| e.to_string())?;
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
        let mut cursors = self.cursors.lock().map_err(|e| e.to_string())?;
        if cursors.len() >= self.max_entries {
            if let Some(expired_or_oldest) = cursors
                .iter()
                .min_by_key(|(_, value)| value.expires_at)
                .map(|(key, _)| key.clone())
            {
                cursors.remove(&expired_or_oldest);
            }
        }
        cursors.insert(key, cursor);
        Ok(())
    }

    async fn invalidate(&self, query_key: &str) -> Result<(), String> {
        let prefix = format!("{query_key}:");
        self.cursors
            .lock()
            .map_err(|e| e.to_string())?
            .retain(|key, _| !key.starts_with(&prefix));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlLogOperation {
    Select,
    Insert,
    Update,
    Delete,
    Recover,
}

impl SqlLogOperation {
    pub fn is_select(self) -> bool {
        matches!(self, Self::Select)
    }

    pub fn is_mutation(self) -> bool {
        !self.is_select()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlLogOptions {
    pub select: bool,
    pub mutation: bool,
}

impl SqlLogOptions {
    pub fn disabled() -> Self {
        Self {
            select: false,
            mutation: false,
        }
    }

    pub fn select_only() -> Self {
        Self {
            select: true,
            mutation: false,
        }
    }

    pub fn mutation_only() -> Self {
        Self {
            select: false,
            mutation: true,
        }
    }

    pub fn all() -> Self {
        Self {
            select: true,
            mutation: true,
        }
    }

    pub fn enabled_for(self, operation: SqlLogOperation) -> bool {
        match operation.is_select() {
            true => self.select,
            false => self.mutation,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlLogEntry {
    pub operation: SqlLogOperation,
    pub sql: String,
    pub params: Vec<Value>,
    pub debug_sql: String,
    pub pretty_sql: String,
    pub started_at: SystemTime,
    pub ended_at: SystemTime,
    pub elapsed: Duration,
    pub result_count: Option<usize>,
    pub result_type: Option<String>,
    pub affected_rows: Option<u64>,
    pub result_summary: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedLogEntry {
    pub timestamp: SystemTime,
    pub user_identifier: Option<String>,
    pub trace_chain: Vec<teaql_core::TraceNode>,
    pub payload: LogPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogPayload {
    Sql(SqlLogEntry),
    Info(InfoLogEntry),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InfoLogEntry {
    pub message: String,
}

#[derive(Clone, Default)]
pub struct UnifiedLogBuffer {
    pub entries: std::sync::Arc<Mutex<Vec<UnifiedLogEntry>>>,
}

/// Context-owned proof required by the provider SPI. Its private field prevents
/// application crates from invoking a schema provider directly.
///
/// ```compile_fail
/// let _ = teaql_runtime::SchemaInvocation { _context_owned: () };
/// ```
pub struct SchemaInvocation {
    _context_owned: (),
}

pub trait SchemaProvider: Send + Sync {
    fn ensure_schema<'a>(
        &'a self,
        context: &'a UserContext,
        invocation: &'a SchemaInvocation,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>>;
}

pub struct UserContext {
    active_root: Option<ContextEntityRef>,
    pub(crate) metadata: Option<Box<dyn MetadataStore>>,
    pub(crate) entity_registry: Option<Box<dyn EntityRegistry>>,
    pub(crate) entity_graph_decoders: InMemoryEntityGraphDecoderRegistry,
    pub(crate) entity_data_service_behavior_registry:
        Option<Box<dyn EntityDataServiceBehaviorRegistry>>,
    pub(crate) request_policy: Option<Box<dyn RequestPolicy>>,
    pub(crate) checker_registry: Option<Box<dyn CheckerRegistry>>,
    pub(crate) event_sink: Option<Box<dyn RawAuditEventSink>>,
    pub(crate) custom_event_sink: Option<Box<dyn crate::SafeAuditEventSink>>,
    pub(crate) internal_id_generator: Option<Box<dyn InternalIdGenerator>>,
    schema_provider: Option<Box<dyn SchemaProvider>>,
    language: Language,
    i18n_catalog: Arc<crate::I18nCatalog>,
    typed_resources: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    named_resources: BTreeMap<String, Box<dyn Any + Send + Sync>>,
    locals: BTreeMap<String, Value>,
    pub(crate) initial_graphs: Vec<GraphNode>,
    pub(crate) root_graphs: Vec<GraphNode>,
    entity_runtime_state: EntityRuntimeState,
    sql_log_options: SqlLogOptions,
    sql_log_entries: Mutex<Vec<SqlLogEntry>>,
    user_identifier: Option<String>,
    timezone: Option<String>,
    trace_id: String,
    continuous_page_cursor_store: std::sync::Arc<dyn ContinuousPageCursorStore>,
    continuous_page_observation: Mutex<(String, Option<String>)>,
    id_set_store: Arc<dyn IdSetStore>,
    id_set_observation: Mutex<(String, Option<u64>)>,
    local_lock_owner: u64,
    remote_lock_owner: String,
    runtime_telemetry: Arc<dyn crate::RuntimeTelemetry>,
}

#[derive(Clone, Copy)]
struct LocalLockEntry {
    owner: u64,
    expires_at: Option<Instant>,
}

#[derive(Default)]
struct ProcessLocalLocks {
    entries: Mutex<HashMap<String, LocalLockEntry>>,
    changed: Condvar,
}

static PROCESS_LOCAL_LOCKS: OnceLock<ProcessLocalLocks> = OnceLock::new();
static NEXT_LOCAL_LOCK_OWNER: AtomicU64 = AtomicU64::new(1);

impl Default for UserContext {
    fn default() -> Self {
        let pid = std::process::id();
        let thread_id_str = format!("{:?}", std::thread::current().id());
        let numeric_thread_id = thread_id_str
            .strip_prefix("ThreadId(")
            .and_then(|s| s.strip_suffix(")"))
            .unwrap_or(&thread_id_str);
        let os_user = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "main".to_owned());
        let user_id = format!("{os_user}@pid-{pid}.tid-{numeric_thread_id}");
        let owner_sequence = NEXT_LOCAL_LOCK_OWNER.fetch_add(1, Ordering::Relaxed);
        Self {
            active_root: None,
            metadata: None,
            entity_registry: None,
            entity_graph_decoders: InMemoryEntityGraphDecoderRegistry::default(),
            entity_data_service_behavior_registry: None,
            request_policy: None,
            checker_registry: None,
            event_sink: None,
            custom_event_sink: None,
            internal_id_generator: None,
            schema_provider: None,
            language: Language::default(),
            i18n_catalog: crate::I18nCatalog::builtin().clone(),
            typed_resources: HashMap::new(),
            named_resources: BTreeMap::new(),
            locals: BTreeMap::new(),
            initial_graphs: Vec::new(),
            root_graphs: Vec::new(),
            entity_runtime_state: EntityRuntimeState::default(),
            sql_log_options: SqlLogOptions::all(),
            sql_log_entries: Mutex::new(Vec::new()),
            user_identifier: Some(user_id),
            timezone: Some("UTC".to_owned()),
            trace_id: format!(
                "req-{pid}-{numeric_thread_id}-{:x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros()
            ),
            continuous_page_cursor_store: std::sync::Arc::new(
                InMemoryContinuousPageCursorStore::default(),
            ),
            continuous_page_observation: Mutex::new(("DISABLED".to_owned(), None)),
            id_set_store: Arc::new(InMemoryIdSetStore::default()),
            id_set_observation: Mutex::new(("ID_SET_DISABLED".to_owned(), None)),
            local_lock_owner: owner_sequence,
            remote_lock_owner: format!(
                "teaql:{pid}:{owner_sequence}:{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ),
            runtime_telemetry: Arc::new(crate::NoopRuntimeTelemetry),
        }
    }
}

#[async_trait::async_trait]
pub trait DataStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Option<Value>;
    async fn put(&self, key: &str, value: Value, timeout_seconds: Option<u64>);
    async fn remove(&self, key: &str);
}

/// Provider-neutral distributed lock boundary.
///
/// Implementations must associate an acquired lock with `owner_token` and
/// release it only while that token still owns the key. A zero timeout is one
/// non-blocking attempt; a zero expiry means no automatic lease expiry.
#[async_trait::async_trait]
pub trait RemoteLockProvider: Send + Sync + 'static {
    async fn try_remote_lock(
        &self,
        key: &str,
        owner_token: &str,
        timeout_millis: u64,
        expire_millis: u64,
    ) -> bool;

    async fn unlock_remote(&self, key: &str, owner_token: &str) -> bool;
}

#[derive(Default)]
pub struct InMemoryDataStore {
    cache: std::sync::RwLock<HashMap<String, (Value, Option<std::time::Instant>)>>,
}

#[async_trait::async_trait]
impl DataStore for InMemoryDataStore {
    async fn get(&self, key: &str) -> Option<Value> {
        let lock = self.cache.read().unwrap();
        if let Some((val, expires_at)) = lock.get(key) {
            if let Some(exp) = expires_at {
                if std::time::Instant::now() > *exp {
                    return None;
                }
            }
            return Some(val.clone());
        }
        None
    }

    async fn put(&self, key: &str, value: Value, timeout_seconds: Option<u64>) {
        let mut lock = self.cache.write().unwrap();
        let expires_at = timeout_seconds
            .map(|secs| std::time::Instant::now() + std::time::Duration::from_secs(secs));
        lock.insert(key.to_string(), (value, expires_at));
    }

    async fn remove(&self, key: &str) {
        let mut lock = self.cache.write().unwrap();
        lock.remove(key);
    }
}

impl UserContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_active_root(mut self, entity_type: impl Into<String>, id: u64) -> Self {
        let entity_type = entity_type.into();
        assert!(
            !entity_type.trim().is_empty(),
            "active root entity type is required"
        );
        assert!(id > 0, "active root id must be positive");
        self.active_root = Some(ContextEntityRef { entity_type, id });
        self
    }

    pub fn require_active_root(
        &self,
        expected_entity_type: &str,
    ) -> Result<&ContextEntityRef, ContextRootError> {
        match &self.active_root {
            Some(root) if root.entity_type == expected_entity_type => Ok(root),
            actual_root => Err(ContextRootError {
                expected_entity_type: expected_entity_type.to_owned(),
                actual_root: actual_root.clone(),
            }),
        }
    }

    pub(crate) fn active_root_ref(&self) -> Option<&ContextEntityRef> {
        self.active_root.as_ref()
    }

    pub fn with_runtime_telemetry(mut self, telemetry: Arc<dyn crate::RuntimeTelemetry>) -> Self {
        self.runtime_telemetry = telemetry;
        self
    }

    pub fn set_runtime_telemetry(&mut self, telemetry: Arc<dyn crate::RuntimeTelemetry>) {
        self.runtime_telemetry = telemetry;
    }

    pub fn runtime_telemetry(&self) -> &Arc<dyn crate::RuntimeTelemetry> {
        &self.runtime_telemetry
    }

    pub(crate) fn runtime_telemetry_is_noop(&self) -> bool {
        self.runtime_telemetry.is_noop()
    }

    pub fn start_runtime_operation(
        &self,
        operation: crate::RuntimeOperation,
    ) -> crate::FailOpenRuntimeTelemetryScope {
        crate::start_runtime_operation(&self.runtime_telemetry, operation)
    }

    pub fn try_local_lock(&self, key: &str, timeout_millis: u64, expire_millis: u64) -> bool {
        let locks = PROCESS_LOCAL_LOCKS.get_or_init(ProcessLocalLocks::default);
        let deadline = Instant::now() + Duration::from_millis(timeout_millis);
        let mut entries = locks.entries.lock().expect("local lock state poisoned");
        loop {
            let now = Instant::now();
            match entries.get(key).copied() {
                None => {
                    entries.insert(
                        key.to_owned(),
                        LocalLockEntry {
                            owner: self.local_lock_owner,
                            expires_at: (expire_millis > 0)
                                .then(|| now + Duration::from_millis(expire_millis)),
                        },
                    );
                    return true;
                }
                Some(current)
                    if current.owner == self.local_lock_owner
                        || current.expires_at.is_some_and(|expiry| now >= expiry) =>
                {
                    entries.insert(
                        key.to_owned(),
                        LocalLockEntry {
                            owner: self.local_lock_owner,
                            expires_at: (expire_millis > 0)
                                .then(|| now + Duration::from_millis(expire_millis)),
                        },
                    );
                    return true;
                }
                Some(current) => {
                    if timeout_millis == 0 || now >= deadline {
                        return false;
                    }
                    let wake_after = current
                        .expires_at
                        .map(|expiry| expiry.saturating_duration_since(now))
                        .unwrap_or_else(|| deadline.saturating_duration_since(now))
                        .min(deadline.saturating_duration_since(now));
                    let waited = locks
                        .changed
                        .wait_timeout(entries, wake_after)
                        .expect("local lock state poisoned");
                    entries = waited.0;
                }
            }
        }
    }

    pub fn unlock_local(&self, key: &str) {
        let locks = PROCESS_LOCAL_LOCKS.get_or_init(ProcessLocalLocks::default);
        let mut entries = locks.entries.lock().expect("local lock state poisoned");
        if entries
            .get(key)
            .is_some_and(|entry| entry.owner == self.local_lock_owner)
        {
            entries.remove(key);
            locks.changed.notify_all();
        }
    }

    /// Attempts to acquire a provider-backed distributed lock.
    ///
    /// A missing provider remains a no-op success, matching the optional
    /// Remote Lock boundary in the other TeaQL runtimes. Install an
    /// `Arc<dyn RemoteLockProvider>` resource to enable distributed exclusion.
    pub async fn try_remote_lock(
        &self,
        key: &str,
        timeout_millis: u64,
        expire_millis: u64,
    ) -> bool {
        match self.get_resource::<Arc<dyn RemoteLockProvider>>() {
            Some(provider) => {
                provider
                    .try_remote_lock(key, &self.remote_lock_owner, timeout_millis, expire_millis)
                    .await
            }
            None => true,
        }
    }

    /// Releases a distributed lock only when this context still owns it.
    pub async fn unlock_remote(&self, key: &str) -> bool {
        match self.get_resource::<Arc<dyn RemoteLockProvider>>() {
            Some(provider) => provider.unlock_remote(key, &self.remote_lock_owner).await,
            None => true,
        }
    }

    pub fn user_identifier(&self) -> Option<&str> {
        self.user_identifier.as_deref()
    }

    pub fn set_user_identifier(&mut self, user_identifier: impl Into<String>) {
        self.user_identifier = Some(user_identifier.into());
    }

    pub fn set_continuous_page_cursor_store(
        &mut self,
        store: std::sync::Arc<dyn ContinuousPageCursorStore>,
    ) {
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

    pub fn with_user_identifier(mut self, user_identifier: impl Into<String>) -> Self {
        self.user_identifier = Some(user_identifier.into());
        self
    }

    pub fn set_user_identifier_option(&mut self, user_identifier: Option<String>) {
        self.user_identifier = user_identifier;
    }

    pub fn with_user_identifier_option(mut self, user_identifier: Option<String>) -> Self {
        self.user_identifier = user_identifier;
        self
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn set_timezone(&mut self, timezone: impl Into<String>) {
        self.timezone = Some(timezone.into());
    }

    pub fn with_timezone(mut self, timezone: impl Into<String>) -> Self {
        self.timezone = Some(timezone.into());
        self
    }

    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    pub fn set_trace_id(&mut self, trace_id: impl Into<String>) {
        self.trace_id = trace_id.into();
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = trace_id.into();
        self
    }

    pub fn with_module(mut self, module: crate::RuntimeModule) -> Self {
        module.apply_to(&mut self);
        self
    }

    pub fn entity_runtime_state(&self) -> EntityRuntimeState {
        // UserContext owns only the immutable identity-graph anchor. Every query/new-entity
        // operation receives fresh mutation state, even when the same context is reused.
        EntityRuntimeState::fresh_with_shared_graph(&self.entity_runtime_state)
    }

    pub fn initial_graphs(&self) -> &[GraphNode] {
        &self.initial_graphs
    }

    pub fn set_initial_graphs(&mut self, graphs: Vec<GraphNode>) {
        self.initial_graphs = graphs;
    }

    pub fn root_graphs(&self) -> &[GraphNode] {
        &self.root_graphs
    }

    pub fn set_root_graphs(&mut self, graphs: Vec<GraphNode>) {
        self.root_graphs = graphs;
    }

    pub fn with_metadata(mut self, metadata: impl MetadataStore + 'static) -> Self {
        self.metadata = Some(Box::new(metadata));
        self
    }

    pub fn set_metadata(&mut self, metadata: impl MetadataStore + 'static) {
        self.metadata = Some(Box::new(metadata));
    }

    pub fn with_entity_registry(mut self, registry: impl EntityRegistry + 'static) -> Self {
        self.entity_registry = Some(Box::new(registry));
        self
    }

    pub fn set_entity_registry(&mut self, registry: impl EntityRegistry + 'static) {
        self.entity_registry = Some(Box::new(registry));
    }

    pub fn set_entity_graph_decoder_registry(
        &mut self,
        registry: InMemoryEntityGraphDecoderRegistry,
    ) {
        self.entity_graph_decoders = registry;
    }

    pub(crate) fn has_entity_graph_decoder(&self, entity: &str) -> bool {
        self.entity_graph_decoders.contains(entity)
    }

    pub(crate) fn decode_compact_entity_into_graph(
        &self,
        entity: &str,
        row: teaql_core::CompactRow,
        root: &EntityRuntimeState,
        graph: &mut EntityGraphBuilder,
    ) -> Result<(), teaql_core::EntityError> {
        self.entity_graph_decoders
            .decode_compact(entity, row, root, graph)
    }

    pub(crate) fn decode_compact_entity_list_into_graph(
        &self,
        entity: &str,
        rows: Vec<teaql_core::CompactRow>,
        root: &EntityRuntimeState,
        graph: &mut EntityGraphBuilder,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> Result<(), teaql_core::EntityError> {
        self.entity_graph_decoders.decode_compact_list(
            entity,
            rows,
            root,
            graph,
            owner_entity,
            owner_id,
            relation,
        )
    }

    pub(crate) fn decode_compact_entity_batch_into_graph(
        &self,
        entity: &str,
        rows: Vec<teaql_core::CompactRow>,
        root: &EntityRuntimeState,
        graph: &mut EntityGraphBuilder,
    ) -> Result<(), teaql_core::EntityError> {
        self.entity_graph_decoders
            .decode_compact_batch(entity, rows, root, graph)
    }

    pub(crate) fn decode_compact_entity_option_into_graph(
        &self,
        entity: &str,
        rows: Vec<teaql_core::CompactRow>,
        root: &EntityRuntimeState,
        graph: &mut EntityGraphBuilder,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> Result<(), teaql_core::EntityError> {
        self.entity_graph_decoders.decode_compact_option(
            entity,
            rows,
            root,
            graph,
            owner_entity,
            owner_id,
            relation,
        )
    }

    pub fn with_entity_data_service_behavior_registry(
        mut self,
        registry: impl EntityDataServiceBehaviorRegistry + 'static,
    ) -> Self {
        self.entity_data_service_behavior_registry = Some(Box::new(registry));
        self
    }

    pub fn set_entity_data_service_behavior_registry(
        &mut self,
        registry: impl EntityDataServiceBehaviorRegistry + 'static,
    ) {
        self.entity_data_service_behavior_registry = Some(Box::new(registry));
    }

    pub fn with_request_policy(mut self, policy: impl RequestPolicy + 'static) -> Self {
        self.request_policy = Some(Box::new(policy));
        self
    }

    pub fn set_request_policy(&mut self, policy: impl RequestPolicy + 'static) {
        self.request_policy = Some(Box::new(policy));
    }

    pub fn clear_request_policy(&mut self) {
        self.request_policy = None;
    }

    pub fn with_checker_registry(mut self, registry: impl CheckerRegistry + 'static) -> Self {
        self.checker_registry = Some(Box::new(registry));
        self
    }

    pub fn set_checker_registry(&mut self, registry: impl CheckerRegistry + 'static) {
        self.checker_registry = Some(Box::new(registry));
    }

    pub(crate) fn with_event_sink(mut self, sink: impl RawAuditEventSink + 'static) -> Self {
        self.event_sink = Some(Box::new(sink));
        self
    }

    pub(crate) fn set_event_sink(&mut self, sink: impl RawAuditEventSink + 'static) {
        self.event_sink = Some(Box::new(sink));
    }

    pub fn with_custom_event_sink(
        mut self,
        sink: impl crate::SafeAuditEventSink + 'static,
    ) -> Self {
        self.custom_event_sink = Some(Box::new(sink));
        self
    }

    pub fn set_custom_event_sink(&mut self, sink: impl crate::SafeAuditEventSink + 'static) {
        self.custom_event_sink = Some(Box::new(sink));
    }

    pub fn with_internal_id_generator(
        mut self,
        generator: impl InternalIdGenerator + 'static,
    ) -> Self {
        self.internal_id_generator = Some(Box::new(generator));
        self
    }

    pub fn set_internal_id_generator(&mut self, generator: impl InternalIdGenerator + 'static) {
        self.internal_id_generator = Some(Box::new(generator));
    }

    pub fn with_schema_provider(mut self, provider: impl SchemaProvider + 'static) -> Self {
        self.schema_provider = Some(Box::new(provider));
        self
    }

    pub fn set_schema_provider(&mut self, provider: impl SchemaProvider + 'static) {
        self.schema_provider = Some(Box::new(provider));
    }

    pub async fn ensure_schema(&self) -> Result<(), RuntimeError> {
        let provider = self
            .schema_provider
            .as_ref()
            .ok_or_else(|| RuntimeError::Schema("missing schema provider".to_owned()))?;
        let invocation = SchemaInvocation { _context_owned: () };
        provider.ensure_schema(self, &invocation).await
    }

    pub fn with_language(mut self, language: Language) -> Self {
        self.language = language;
        self
    }

    pub fn set_language(&mut self, language: Language) {
        self.language = language;
    }

    pub fn with_i18n_catalog(mut self, catalog: Arc<crate::I18nCatalog>) -> Self {
        self.i18n_catalog = catalog;
        self
    }

    pub fn set_i18n_catalog(&mut self, catalog: Arc<crate::I18nCatalog>) {
        self.i18n_catalog = catalog;
    }

    pub fn with_sql_log_options(mut self, options: SqlLogOptions) -> Self {
        self.sql_log_options = options;
        self
    }

    pub fn set_sql_log_options(&mut self, options: SqlLogOptions) {
        self.sql_log_options = options;
    }

    pub fn enable_select_sql_log(&mut self) {
        self.sql_log_options.select = true;
    }

    pub fn enable_mutation_sql_log(&mut self) {
        self.sql_log_options.mutation = true;
    }

    pub fn enable_all_sql_log(&mut self) {
        self.sql_log_options = SqlLogOptions::all();
    }

    pub fn disable_sql_log(&mut self) {
        self.sql_log_options = SqlLogOptions::disabled();
        self.clear_sql_logs();
    }

    pub fn sql_log_options(&self) -> SqlLogOptions {
        self.sql_log_options
    }

    pub fn sql_logs(&self) -> Vec<SqlLogEntry> {
        self.sql_log_entries
            .lock()
            .map(|entries| entries.clone())
            .unwrap_or_default()
    }

    pub fn clear_sql_logs(&self) {
        if let Ok(mut entries) = self.sql_log_entries.lock() {
            entries.clear();
        }
    }

    pub(crate) fn record_sql_log(
        &self,
        operation: SqlLogOperation,
        query: &CompiledQuery,
        database_kind: DatabaseKind,
        started_at: SystemTime,
        ended_at: SystemTime,
        elapsed: Duration,
        result_count: Option<usize>,
        result_type: Option<String>,
        affected_rows: Option<u64>,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) {
        if !self.sql_log_options.enabled_for(operation) {
            return;
        }
        let debug_sql = query.debug_sql(database_kind);
        let result_summary = sql_result_summary(
            operation,
            result_count,
            result_type.as_deref(),
            affected_rows,
            &debug_sql,
        );

        let sql_log_entry = SqlLogEntry {
            operation,
            sql: query.sql.clone(),
            params: query.params.clone(),
            pretty_sql: pretty_sql(&debug_sql),
            debug_sql: debug_sql.clone(),
            started_at,
            ended_at,
            elapsed,
            result_summary: result_summary.clone(),
            result_count,
            result_type,
            affected_rows,
        };

        if let Ok(mut entries) = self.sql_log_entries.lock() {
            // Keep sql_log_entries backwards-compatible for now if needed,
            // wait, we modified SqlLogEntry. We can just push it directly since we removed comment.
            // Wait, we need to push a cloned SqlLogEntry since it doesn't have comment.
            entries.push(sql_log_entry.clone());
        }

        if let Some(buf) = self.get_resource::<UnifiedLogBuffer>() {
            if let Ok(mut entries) = buf.entries.lock() {
                entries.push(UnifiedLogEntry {
                    timestamp: started_at,
                    user_identifier: self.user_identifier.clone(),
                    trace_chain: trace_chain.clone(),
                    payload: LogPayload::Sql(sql_log_entry.clone()),
                });
            }
        }

        crate::log_formatter::LogManager::write_sql_log(&trace_chain, &sql_log_entry);
    }

    pub(crate) fn record_metadata_log(&self, metadata: &teaql_data_service::ExecutionMetadata) {
        let operation = match metadata.operation {
            teaql_data_service::DataServiceOperation::Query => SqlLogOperation::Select,
            teaql_data_service::DataServiceOperation::Insert => SqlLogOperation::Insert,
            teaql_data_service::DataServiceOperation::Update => SqlLogOperation::Update,
            teaql_data_service::DataServiceOperation::Delete => SqlLogOperation::Delete,
            teaql_data_service::DataServiceOperation::Recover => SqlLogOperation::Update,
            teaql_data_service::DataServiceOperation::Batch => SqlLogOperation::Update,
            teaql_data_service::DataServiceOperation::Schema => SqlLogOperation::Update,
        };
        if !self.sql_log_options.enabled_for(operation) {
            return;
        }
        if let Some(debug_sql) = &metadata.debug_query {
            let sql_log_entry = SqlLogEntry {
                operation,
                sql: metadata.parameterized_query.clone().unwrap_or_default(),
                params: metadata.params.clone(),
                pretty_sql: pretty_sql(debug_sql),
                debug_sql: debug_sql.clone(),
                started_at: metadata.started_at,
                ended_at: metadata.ended_at,
                elapsed: metadata
                    .ended_at
                    .duration_since(metadata.started_at)
                    .unwrap_or_default(),
                result_count: metadata.result_count,
                result_type: None, // Not directly available
                affected_rows: metadata.affected_rows,
                result_summary: String::new(), // We can synthesize this if needed, or leave it empty/basic
            };

            // synthesize a summary for the log
            let mut summary = String::new();
            if let Some(c) = metadata.result_count {
                summary = format!("{} rows returned", c);
            } else if let Some(a) = metadata.affected_rows {
                summary = format!("{} rows affected", a);
            }

            let mut final_entry = sql_log_entry;
            final_entry.result_summary = summary;

            if let Ok(mut entries) = self.sql_log_entries.lock() {
                entries.push(final_entry.clone());
            }

            if let Some(buf) = self.get_resource::<UnifiedLogBuffer>() {
                if let Ok(mut entries) = buf.entries.lock() {
                    entries.push(UnifiedLogEntry {
                        timestamp: metadata.started_at,
                        user_identifier: self.user_identifier.clone(),
                        trace_chain: metadata.trace_chain.clone(),
                        payload: LogPayload::Sql(final_entry.clone()),
                    });
                }
            }

            crate::log_formatter::LogManager::write_sql_log(&metadata.trace_chain, &final_entry);
        }
    }

    pub fn language(&self) -> Language {
        self.language
    }

    pub fn set_language_code(&mut self, code: &str) -> Result<(), RuntimeError> {
        let Some(language) = Language::from_code(code) else {
            return Err(RuntimeError::UnsupportedLocale(code.to_owned()));
        };
        self.language = language;
        Ok(())
    }

    pub fn set_locale_code(&mut self, code: &str) -> Result<(), RuntimeError> {
        self.set_language_code(code)
    }

    pub fn generate_id(&self, entity: &str) -> Result<Option<u64>, RuntimeError> {
        self.internal_id_generator
            .as_ref()
            .map(|generator| generator.generate_id(entity))
            .transpose()
    }

    pub fn next_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        match self.generate_id(entity)? {
            Some(id) => Ok(id),
            None => local_id_generator().generate_id(entity),
        }
    }

    pub fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.entity(name))
    }

    pub fn all_entities(&self) -> Vec<&EntityDescriptor> {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.all_entities())
            .unwrap_or_default()
    }

    pub fn require_entity(&self, name: &str) -> Result<&EntityDescriptor, RuntimeError> {
        self.entity(name)
            .ok_or_else(|| RuntimeError::MissingEntity(name.to_owned()))
    }

    pub fn insert_resource<T>(&mut self, resource: T)
    where
        T: Send + Sync + 'static,
    {
        self.typed_resources
            .insert(TypeId::of::<T>(), Box::new(resource));
    }

    pub fn get_resource<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.typed_resources
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn require_resource<T>(&self) -> Result<&T, ContextError>
    where
        T: Send + Sync + 'static,
    {
        self.get_resource::<T>()
            .ok_or(ContextError::MissingTypedResource(
                std::any::type_name::<T>(),
            ))
    }

    pub fn insert_named_resource<T>(&mut self, name: impl Into<String>, resource: T)
    where
        T: Send + Sync + 'static,
    {
        self.named_resources.insert(name.into(), Box::new(resource));
    }

    pub fn get_named_resource<T>(&self, name: &str) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.named_resources
            .get(name)
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn require_named_resource<T>(&self, name: &str) -> Result<&T, ContextError>
    where
        T: Send + Sync + 'static,
    {
        self.get_named_resource::<T>(name)
            .ok_or_else(|| ContextError::MissingResource(name.to_owned()))
    }

    pub fn put_local(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.locals.insert(key.into(), value.into());
    }

    pub fn local(&self, key: &str) -> Option<&Value> {
        self.locals.get(key)
    }

    pub fn remove_local(&mut self, key: &str) -> Option<Value> {
        self.locals.remove(key)
    }

    pub fn has_entity_data_service(&self, entity: &str) -> bool {
        let in_registry = self
            .entity_registry
            .as_ref()
            .map(|registry| registry.contains(entity))
            .unwrap_or(false);
        in_registry || self.entity(entity).is_some()
    }

    pub fn entity_data_service_behavior(
        &self,
        entity: &str,
    ) -> Option<std::sync::Arc<dyn EntityDataServiceBehavior>> {
        self.entity_data_service_behavior_registry
            .as_ref()
            .and_then(|registry| registry.behavior(entity))
    }

    pub fn has_checker(&self, entity: &str) -> bool {
        self.checker_registry
            .as_ref()
            .and_then(|registry| registry.checker(entity))
            .is_some()
    }

    pub fn check_and_fix_values(
        &self,
        entity: &str,
        values: &mut crate::EntityValues,
    ) -> Result<(), RuntimeError> {
        self.check_and_fix_values_at(entity, values, &ObjectLocation::root())
    }

    pub fn check_and_fix_values_at(
        &self,
        entity: &str,
        values: &mut crate::EntityValues,
        location: &ObjectLocation,
    ) -> Result<(), RuntimeError> {
        let status = CheckObjectStatus::from_values(values);
        let checker = self
            .checker_registry
            .as_ref()
            .and_then(|registry| registry.checker(entity));
        let mut results = CheckResults::new();
        if let Some(checker) = checker {
            checker.check_and_fix(self, values, location, &mut results);
        }

        // Keep runtime validation aligned with the schema generated from the
        // same metadata. Custom checkers get the first chance to supply or fix
        // a value; afterwards every NOT NULL property must be present on a
        // create, and an update must not explicitly clear one.
        if let Some(descriptor) = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.entity(entity))
        {
            for property in descriptor
                .properties
                .iter()
                // The optimistic-lock version is runtime-managed. Insert
                // preparation assigns its initial value after check/fix, so a
                // create caller must never be required to provide it.
                .filter(|property| !property.nullable && !property.is_version)
            {
                let missing = !values.contains_key(&property.name);
                let null = matches!(values.get(&property.name), Some(Value::Null));
                let property_location = location.clone().member(&property.name);
                let already_reported = results.iter().any(|result| {
                    result.rule == crate::CheckRule::Required
                        && result.location == property_location
                });
                if ((status.is_create() && missing) || null) && !already_reported {
                    results.push(CheckResult::required(property_location));
                }
            }
        }
        if results.is_empty() {
            return Ok(());
        }
        self.translate_check_results(&mut results);
        Err(RuntimeError::Check(results))
    }

    pub fn translate_check_results(&self, results: &mut CheckResults) {
        for result in results {
            if result.message.is_none() {
                result.message = Some(
                    self.i18n_catalog
                        .translate_check_result(self.language, result),
                );
            }
        }
    }

    pub fn send_event(&self, event: RawAuditEvent) -> Result<(), RuntimeError> {
        let scope = self.start_runtime_operation(
            crate::RuntimeOperation::new("audit", format!("{}.event", event.entity))
                .attribute("teaql.entity.type", event.entity.clone()),
        );
        let result = self.send_event_inner(event);
        match &result {
            Ok(()) => scope.success(std::collections::BTreeMap::new()),
            Err(_) => scope.failure("audit_error"),
        }
        result
    }

    fn send_event_inner(&self, event: RawAuditEvent) -> Result<(), RuntimeError> {
        if let Some(sink) = self.event_sink.as_ref() {
            sink.on_event(self, &event)?;
        }
        if let Some(sink) = self.custom_event_sink.as_ref() {
            let (mask_fields, max_len) = self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.entity(&event.entity))
                .map(|desc| (desc.audit_mask_fields.clone(), desc.audit_value_max_len))
                .unwrap_or_else(|| (vec![], None));

            let safe_event = event.build_safe_event(&mask_fields, max_len);
            sink.on_safe_event(self, &safe_event)?;
        }

        crate::log_formatter::LogManager::write_audit_log(&event);

        Ok(())
    }

    pub async fn get_in_store(&self, key: &str) -> Option<Value> {
        let store = self.get_resource::<Box<dyn DataStore>>()?;
        store.get(key).await
    }

    pub async fn put_in_store(
        &self,
        key: &str,
        value: impl Into<Value>,
        timeout_seconds: Option<u64>,
    ) {
        if let Some(store) = self.get_resource::<Box<dyn DataStore>>() {
            store.put(key, value.into(), timeout_seconds).await;
        }
    }

    pub async fn clear_in_store(&self, key: &str) {
        if let Some(store) = self.get_resource::<Box<dyn DataStore>>() {
            store.remove(key).await;
        }
    }
}

fn extract_id_from_sql(sql: &str) -> Option<String> {
    let sql_lower = sql.to_lowercase();
    let where_idx = sql_lower.find("where")?;
    let where_clause = &sql_lower[where_idx + 5..];

    let bytes = where_clause.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && &bytes[i..i + 2] == b"id" {
            // Check boundary before
            let prev_ok = i == 0 || {
                let prev_char = bytes[i - 1] as char;
                !prev_char.is_ascii_alphanumeric() && prev_char != '_' && prev_char != '.'
            };
            // Check boundary after
            let next_ok = i + 2 == bytes.len() || {
                let next_char = bytes[i + 2] as char;
                !next_char.is_ascii_alphanumeric() && next_char != '_'
            };

            if prev_ok && next_ok {
                // Found the standalone "id" word!
                // Now look for "=" after it
                let mut j = i + 2;
                while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b'=' {
                    j += 1;
                    while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                        j += 1;
                    }
                    // Now extract the value
                    let mut val_str = String::new();
                    if j < bytes.len() && bytes[j] == b'\'' {
                        j += 1; // consume single quote
                        while j < bytes.len() && bytes[j] != b'\'' {
                            val_str.push(bytes[j] as char);
                            j += 1;
                        }
                        return Some(val_str);
                    }
                    // No else needed — falls through to unquoted parsing
                    while j < bytes.len() {
                        let c = bytes[j] as char;
                        if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
                            break;
                        }
                        val_str.push(c);
                        j += 1;
                    }
                    if !val_str.is_empty() {
                        return Some(val_str);
                    }
                }
            }
        }
        i += 1;
    }
    None
}

fn sql_result_summary(
    operation: SqlLogOperation,
    result_count: Option<usize>,
    result_type: Option<&str>,
    affected_rows: Option<u64>,
    debug_sql: &str,
) -> String {
    match operation {
        SqlLogOperation::Select => {
            let count = result_count.unwrap_or(0);
            match count {
                0 => "MISS".to_owned(),
                1 => match result_type {
                    Some(result_type) => extract_id_from_sql(debug_sql)
                        .map(|id| format!("{result_type}({id})"))
                        .unwrap_or_else(|| result_type.to_owned()),
                    None => "row".to_owned(),
                },
                _ => match result_type {
                    Some(result_type) => format!("{count}*{result_type}"),
                    None => format!("{count}*rows"),
                },
            }
        }
        _ => {
            let affected = affected_rows.unwrap_or(0);
            format!("{affected} UPDATED")
        }
    }
}

fn pretty_sql(sql: &str) -> String {
    let mut pretty = sql.to_owned();
    for keyword in [
        " FROM ",
        " WHERE ",
        " GROUP BY ",
        " HAVING ",
        " ORDER BY ",
        " LIMIT ",
        " OFFSET ",
        " RETURNING ",
    ] {
        pretty = pretty.replace(keyword, &format!("\n{}", keyword.trim_start()));
    }
    pretty.replace(" AND ", "\n  AND ")
}

#[cfg(test)]
mod sql_log_option_tests {
    use super::*;

    #[test]
    fn disabled_sql_log_rejects_executor_metadata_before_recording() {
        let mut context = UserContext::default();
        context.disable_sql_log();
        let now = SystemTime::now();
        context.record_metadata_log(&teaql_data_service::ExecutionMetadata {
            backend: "sql".to_owned(),
            operation: teaql_data_service::DataServiceOperation::Query,
            started_at: now,
            ended_at: now,
            affected_rows: None,
            result_count: Some(1),
            trace_chain: Vec::new(),
            comment: Some("disabled log test".to_owned()),
            backend_request_id: None,
            parameterized_query: Some("SELECT id FROM sample WHERE id = $1".to_owned()),
            params: vec![Value::I64(1)],
            debug_query: Some("SELECT id FROM sample WHERE id = 1".to_owned()),
        });
        assert!(context.sql_logs().is_empty());
    }
}

#[cfg(test)]
mod entity_runtime_state_tests {
    use super::*;
    use crate::EntityKey;

    #[test]
    fn reused_user_context_returns_independent_mutation_ledgers() {
        let context = UserContext::default();
        let first = context.entity_runtime_state();
        let key = EntityKey::new("School", 1_u64);
        first.set(key.clone(), "name", "First");

        let second = context.entity_runtime_state();

        assert_eq!(first.changed_field_names(&key).len(), 1);
        assert!(second.changed_field_names(&key).is_empty());
    }
}
