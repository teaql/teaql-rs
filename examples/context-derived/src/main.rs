use context_fix_service_core::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering::SeqCst},
};
use teaql_core::{Entity, Value};
use teaql_data_service::{
    DataServiceExecutor, MutationExecutor, QueryExecutor, Transaction, TransactionExecutor,
};
use teaql_runtime::{
    CheckObjectStatus, CheckResults, ContextFixError, ContextFixInput, ContextFixResolver,
    FixedBusinessClock, InMemoryCheckerRegistry, ObjectLocation, RawAuditEventKind, RuntimeError,
    SafeAuditEvent, SafeAuditEventSink, TypedEntityChecker, UserContext,
};

#[derive(Default)]
struct State {
    phase: Mutex<String>,
    calls: AtomicUsize,
    checks: AtomicUsize,
    fail: AtomicUsize,
}
struct Resolver(Arc<State>);
impl ContextFixResolver for Resolver {
    fn resolve(
        &self,
        _: &UserContext,
        input: &ContextFixInput<'_>,
    ) -> Result<Value, ContextFixError> {
        self.0.calls.fetch_add(1, SeqCst);
        match input.function {
            "now" => Ok(Value::I64(input.now.0)),
            "uniStr" if self.0.fail.load(SeqCst) != 0 => Err(ContextFixError),
            "uniStr" => Ok(Value::Text(format!("token-{}", self.0.calls.load(SeqCst)))),
            function => Ok(Value::Text(format!(
                "{function}-{}",
                self.0.phase.lock().unwrap()
            ))),
        }
    }
}
struct Logic(Arc<State>);
impl EntryCheckerLogic for Logic {
    fn custom_context_fields(&self) -> &'static [&'static str] {
        &["source_ip", "last_ip", "opened_by", "changed_by"]
    }
    fn fix_entry(
        &self,
        context: &UserContext,
        entity: &mut Entry,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if status.is_create() {
            if let Some(v) = context.context_fix_value::<String>(
                "Entry",
                "source_ip",
                "ip",
                &[],
                status,
                location,
                results,
            ) {
                entity.update_source_ip(v);
            }
            if let Some(v) = context.context_fix_value::<String>(
                "Entry",
                "opened_by",
                "actor",
                &[],
                status,
                location,
                results,
            ) {
                entity.update_opened_by(v);
            }
        }
        if let Some(v) = context.context_fix_value::<String>(
            "Entry",
            "last_ip",
            "ip",
            &[],
            status,
            location,
            results,
        ) {
            entity.update_last_ip(v);
        }
        if let Some(v) = context.context_fix_value::<String>(
            "Entry",
            "changed_by",
            "actor",
            &[],
            status,
            location,
            results,
        ) {
            entity.update_changed_by(v);
        }
    }
    fn check_entry(
        &self,
        _: &UserContext,
        _: &Entry,
        _: CheckObjectStatus,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.0.checks.fetch_add(1, SeqCst);
    }
}
struct Audit(Arc<AtomicUsize>);
impl SafeAuditEventSink for Audit {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        if matches!(
            event.kind,
            RawAuditEventKind::Created | RawAuditEventKind::Updated | RawAuditEventKind::Deleted
        ) {
            self.0.fetch_add(1, SeqCst);
        }
        Ok(())
    }
}
// Observe the actual generated SQL executor. Counting the transaction boundary
// proves that a no-op never even begins a provider transaction.
struct Observed<T> {
    inner: T,
    calls: Arc<AtomicUsize>,
}
impl<T: DataServiceExecutor> DataServiceExecutor for Observed<T> {
    type Error = T::Error;
    fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
        self.inner.capabilities()
    }
}
impl<T: QueryExecutor + Sync> QueryExecutor for Observed<T> {
    fn dynamic_field_store(
        &self,
    ) -> Option<&dyn teaql_data_service::dynamic_fields::DynamicFieldStore> {
        self.inner.dynamic_field_store()
    }
    async fn query(
        &self,
        request: teaql_data_service::QueryRequest,
    ) -> Result<teaql_data_service::QueryResult, Self::Error> {
        self.calls.fetch_add(1, SeqCst);
        self.inner.query(request).await
    }
}
impl<T: MutationExecutor + Sync> MutationExecutor for Observed<T> {
    async fn mutate(
        &self,
        request: teaql_data_service::MutationRequest,
    ) -> Result<teaql_data_service::MutationResult, Self::Error> {
        self.calls.fetch_add(1, SeqCst);
        self.inner.mutate(request).await
    }
}
impl<T: TransactionExecutor + Sync> TransactionExecutor for Observed<T>
where
    for<'a> T::Tx<'a>: Send + Sync,
{
    type Tx<'a>
        = Observed<T::Tx<'a>>
    where
        Self: 'a;
    async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
        self.calls.fetch_add(1, SeqCst);
        Ok(Observed {
            inner: self.inner.begin().await?,
            calls: self.calls.clone(),
        })
    }
}
impl<T: Transaction + Send> Transaction for Observed<T> {
    type Error = T::Error;
    async fn commit(self) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, SeqCst);
        self.inner.commit().await
    }
    async fn rollback(self) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, SeqCst);
        self.inner.rollback().await
    }
}
fn new(context: &UserContext, catalog: u64, name: &str) -> Entry {
    let mut entry = Q::entries()
        .comment("Prepare a typed Context Fix subject")
        .purpose("Verify generated audited Save")
        .new_entity(context);
    entry.update_catalog_id(catalog).update_name(name);
    entry
}
async fn load(context: &UserContext, id: u64) -> Result<Entry, RuntimeError> {
    let mut rows = Q::entries()
        .with_id_is(id)
        .limit(1)
        .comment("Reload persisted values")
        .purpose("Verify generated E expressions")
        .execute_for_list(context)
        .await?;
    Ok(rows.data.remove(0))
}
fn rejection(error: RuntimeError, path: &str) {
    let RuntimeError::Check(results) = error else {
        panic!("expected Check, got {error:?}");
    };
    assert!(
        results.iter().any(|r| r.location.instance_path() == path),
        "{results:?}"
    );
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("TEAQL_CONTEXT_FIX_DB")?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(&path)?));
    let state = Arc::new(State::default());
    *state.phase.lock().unwrap() = "A".into();
    let audits = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let a = chrono::DateTime::from_timestamp_millis(1_961_662_530_000).unwrap();
    let b = a + chrono::Duration::days(1);
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_business_clock(FixedBusinessClock::new(a))
        .with_context_fix_resolver(Resolver(state.clone()));
    context.set_custom_event_sink(Audit(audits.clone()));
    let registry = InMemoryCheckerRegistry::new()
        .with_checker(TypedEntityChecker::<Catalog, _>::new(
            CatalogChecker::default(),
        ))
        .with_checker(TypedEntityChecker::<Entry, _>::new(EntryChecker::new(
            Logic(state.clone()),
        )));
    context.set_checker_registry(registry);
    context.ensure_schema().await?;
    let actual = context
        .require_resource::<ServiceRuntimeExecutor>()?
        .clone();
    context.register_executor(Observed {
        inner: actual,
        calls: calls.clone(),
    });
    let suffix = std::process::id();
    let reset = || {
        calls.store(0, SeqCst);
        audits.store(0, SeqCst);
        state.calls.store(0, SeqCst);
        state.checks.store(0, SeqCst);
    };
    let zero = || {
        assert_eq!(calls.load(SeqCst), 0, "provider calls");
        assert_eq!(audits.load(SeqCst), 0, "audit calls");
    };
    let entry = new(&context, 1, &format!("initial-{suffix}"));
    let entry = entry
        .audit_as("Create with Context defaults")
        .save(&context)
        .await?;
    let mut entry = load(&context, entry.id()).await?;
    assert_eq!(
        E::entry(&entry).get_born_at().eval().unwrap().0,
        a.timestamp_millis()
    );
    assert_eq!(
        E::entry(&entry).get_source_ip().eval().as_deref(),
        Some("ip-A")
    );
    let version = entry.version();
    let token = entry.guarded_token();
    *state.phase.lock().unwrap() = "B".into();
    context.set_business_clock(FixedBusinessClock::new(b));
    reset();
    for _ in 0..2 {
        entry = entry
            .audit_as("Save a clean entity under changed Context")
            .save(&context)
            .await?;
    }
    zero();
    assert_eq!(state.calls.load(SeqCst), 0);
    assert_eq!(entry.version(), version);
    entry.update_name(format!("updated-{suffix}"));
    reset();
    entry = entry
        .audit_as("Save an independent business edit")
        .save(&context)
        .await?;
    assert_eq!(audits.load(SeqCst), 1);
    assert_eq!(state.calls.load(SeqCst), 3, "Fix once, time/IP/actor only");
    entry = load(&context, entry.id()).await?;
    assert_eq!(entry.version(), version + 1);
    assert_eq!(entry.born_at().0, a.timestamp_millis());
    assert_eq!(entry.touched_at().0, b.timestamp_millis());
    assert_eq!(entry.source_ip(), "ip-A");
    assert_eq!(entry.last_ip(), "ip-B");
    assert_eq!(entry.opened_by(), "actor-A");
    assert_eq!(entry.changed_by(), "actor-B");
    assert_eq!(entry.guarded_token(), token);
    let name = entry.name();
    entry.update_name("x".repeat(300));
    *state.phase.lock().unwrap() = "C".into();
    reset();
    rejection(
        entry
            .clone()
            .audit_as("Reject an invalid business edit")
            .save(&context)
            .await
            .unwrap_err(),
        "/name",
    );
    zero();
    entry.update_name(name);
    reset();
    entry = entry
        .audit_as("Restore intent after a failed Fix")
        .save(&context)
        .await?;
    zero();
    assert_eq!(state.calls.load(SeqCst), 0);
    entry.update_name(format!("retry-{suffix}"));
    entry = entry.audit_as("Retry valid intent").save(&context).await?;
    entry = load(&context, entry.id()).await?;
    assert_eq!(entry.last_ip(), "ip-C");
    let failing = new(&context, 1, "provider-failure");
    state.fail.store(1, SeqCst);
    reset();
    rejection(
        failing
            .clone()
            .audit_as("Reject a failed typed Context provider")
            .save(&context)
            .await
            .unwrap_err(),
        "/guardedToken",
    );
    zero();
    state.fail.store(0, SeqCst);
    failing
        .audit_as("Retry a failed create")
        .save(&context)
        .await?;
    let mut parent = Q::catalogs()
        .comment("Prepare a parent")
        .purpose("Verify per-entity graph boundaries")
        .new_entity(&context);
    parent.update_name(format!("graph-{suffix}"));
    parent = parent.audit_as("Create graph owner").save(&context).await?;
    for name in ["first", "second"] {
        new(&context, parent.id(), name)
            .audit_as("Create graph children")
            .save(&context)
            .await?;
    }
    let parents = Q::catalogs()
        .with_id_is(parent.id())
        .select_entry_list_with(Q::entries().order_by_id_asc().limit(10))
        .limit(1)
        .comment("Load a complete two-child graph")
        .purpose("Validate indexed failures and graph rollback")
        .execute_for_list(&context)
        .await?;
    let parent = parents.data[0].clone();
    let pv = parent.version();
    let children = parent.entry_list();
    let children = children.value().unwrap();
    let mut first = children.data[0].clone();
    let mut second = children.data[1].clone();
    let first_name = first.name();
    let second_name = second.name();
    let sv = second.version();
    first.update_name("changed-first");
    second.update_name("x".repeat(300));
    *state.phase.lock().unwrap() = "D".into();
    reset();
    parent.include_pending_mutations_from(&first)?;
    parent.include_pending_mutations_from(&second)?;
    rejection(
        parent
            .clone()
            .audit_as("Reject the second child before any write")
            .save(&context)
            .await
            .unwrap_err(),
        "/entryList/1/name",
    );
    zero();
    first.update_name(first_name);
    second.update_name(second_name);
    reset();
    parent.include_pending_mutations_from(&first)?;
    parent.include_pending_mutations_from(&second)?;
    parent
        .clone()
        .audit_as("Undo graph intent after failed Check")
        .save(&context)
        .await?;
    zero();
    assert_eq!(state.calls.load(SeqCst), 0);
    first.update_name("accepted-first");
    parent.include_pending_mutations_from(&first)?;
    reset();
    parent
        .clone()
        .audit_as("Save only the changed child")
        .save(&context)
        .await?;
    assert_eq!(audits.load(SeqCst), 1);
    assert_eq!(state.calls.load(SeqCst), 3);
    let persisted = load(&context, first.id()).await?;
    assert_eq!(persisted.name(), "accepted-first");
    assert_eq!(persisted.last_ip(), "ip-D");
    let conn = pool.lock().unwrap();
    let actual: (String, String, i64) = conn.query_row(
        "SELECT name,last_ip,version FROM entry_data WHERE id=?",
        [i64::try_from(persisted.id())?],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(
        actual,
        (persisted.name(), persisted.last_ip(), persisted.version())
    );
    let parent_version: i64 = conn.query_row(
        "SELECT version FROM catalog_data WHERE id=?",
        [i64::try_from(parent.id())?],
        |r| r.get(0),
    )?;
    let sibling_version: i64 = conn.query_row(
        "SELECT version FROM entry_data WHERE id=?",
        [i64::try_from(second.id())?],
        |r| r.get(0),
    )?;
    assert_eq!(parent_version, pv);
    assert_eq!(sibling_version, sv);
    println!(
        "RUST_CONTEXT_FIX_SQLITE_PASS typed_q=true typed_e=true no_op_provider=0 failed_check_provider=0 graph_undo=true indexed_location=true fix_once=true"
    );
    Ok(())
}
