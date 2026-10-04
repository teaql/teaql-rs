//! teaql-rs #239: real lower-ledger allocation, not a preassigned token helper.
//! The runtime produces every trace. This observer delegates unchanged SQL.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use teaql_core::TeaqlEntity as _;
use teaql_core::{CompactRow, Entity, EntityDescriptor, RelationDescriptor, TraceKind, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, ExecutionObserver,
    MutationCommand, MutationExecutor, MutationRequest, MutationResult, QueryExecutor,
    QueryRequest, QueryResult, SchemaProvider, Transaction, TransactionExecutor,
};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::{
    SqliteDialect, SqliteIdSpaceGenerator, SqliteMutationExecutor, SqliteProviderExt,
};
use teaql_runtime::{
    EntityKey, InMemoryMetadataStore, InternalIdGenerator, LedgerEntity, RuntimeError,
    SafeAuditEvent, SafeAuditEventSink, UserContext, save_audited_ledger_entity,
};
use teaql_sql::SqlDataServiceExecutor;

#[teaql_entity]
#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "AllocationRoot")]
struct AllocationRoot {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}

#[teaql_entity]
#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "AllocationChild")]
struct AllocationChild {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    root_id: u64,
    name: String,
}

#[derive(Clone)]
struct Schema(Vec<Arc<EntityDescriptor>>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.iter().find(|entity| entity.name == name).cloned()
    }
}

#[derive(Clone, Default)]
struct Capture {
    commands: Arc<Mutex<Vec<MutationRequest>>>,
    metadata: Arc<Mutex<Vec<ExecutionMetadata>>>,
    audits: Arc<Mutex<Vec<SafeAuditEvent>>>,
}
impl SafeAuditEventSink for Capture {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.lock().unwrap().push(event.clone());
        Ok(())
    }
}

struct Observed<E>(E, Capture);
impl<E: DataServiceExecutor> DataServiceExecutor for Observed<E> {
    type Error = E::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        self.0.capabilities()
    }
}
impl<E: QueryExecutor + Send + Sync> QueryExecutor for Observed<E> {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        self.query_observed(request, None).await
    }
    async fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryResult, Self::Error> {
        let result = self
            .0
            .query_observed(request, Some(self.1.observe_failures(observer)))
            .await?;
        self.1
            .metadata
            .lock()
            .unwrap()
            .push(result.metadata.clone());
        Ok(result)
    }
}
impl<E: MutationExecutor + Send + Sync> MutationExecutor for Observed<E> {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        self.mutate_observed(request, None).await
    }
    async fn mutate_observed<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<MutationResult, Self::Error> {
        self.1.commands.lock().unwrap().push(request.clone());
        let result = self
            .0
            .mutate_observed(request, Some(self.1.observe_failures(observer)))
            .await?;
        self.1
            .metadata
            .lock()
            .unwrap()
            .push(result.metadata.clone());
        Ok(result)
    }
}

impl Capture {
    fn observe_failures<'a>(
        &self,
        upstream: Option<ExecutionObserver<'a>>,
    ) -> ExecutionObserver<'a> {
        let capture = self.clone();
        Arc::new(move |metadata| {
            capture.metadata.lock().unwrap().push(metadata.clone());
            if let Some(upstream) = &upstream {
                upstream(metadata);
            }
        })
    }
}
impl<E: TransactionExecutor + Send + Sync> TransactionExecutor for Observed<E>
where
    for<'tx> E::Tx<'tx>: Send + Sync,
{
    type Tx<'a>
        = Observed<E::Tx<'a>>
    where
        Self: 'a;
    async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
        Ok(Observed(self.0.begin().await?, self.1.clone()))
    }
}
impl<E: Transaction + Send> Transaction for Observed<E> {
    type Error = E::Error;
    async fn commit(self) -> Result<(), Self::Error> {
        self.0.commit().await
    }
    async fn rollback(self) -> Result<(), Self::Error> {
        self.0.rollback().await
    }
}

type Executor = Observed<SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>>;

async fn setup() -> (UserContext, Capture) {
    let (context, capture, _) = setup_with_transport().await;
    (context, capture)
}

async fn setup_with_transport() -> (UserContext, Capture, SqliteMutationExecutor) {
    setup_with_relations(true, false).await
}

async fn setup_with_relations(
    reverse: bool,
    forward: bool,
) -> (UserContext, Capture, SqliteMutationExecutor) {
    let mut root = AllocationRoot::entity_descriptor();
    if reverse {
        root = root.relation(
            RelationDescriptor::new("children", "AllocationChild")
                .many()
                .local_key("id")
                .foreign_key("root_id"),
        );
    }
    let mut child = AllocationChild::entity_descriptor().property(
        teaql_core::PropertyDescriptor::new("quantity", teaql_core::DataType::I64),
    );
    if forward {
        child = child.relation(
            RelationDescriptor::new("root", "AllocationRoot")
                .local_key("root_id")
                .foreign_key("id"),
        );
    }
    let capture = Capture::default();
    let mut context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new()
            .with_entity(root.clone())
            .with_entity(child.clone()),
    );
    context.set_custom_event_sink(capture.clone());
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    context.use_sqlite_provider(transport.clone());
    context.ensure_schema().await.unwrap();
    context.register_executor(Observed(
        SqlDataServiceExecutor::new(
            SqliteDialect,
            transport.clone(),
            Schema(vec![Arc::new(root), Arc::new(child)]),
        ),
        capture.clone(),
    ));
    (context, capture, transport)
}

fn new_root(id: u64, name: &str) -> AllocationRoot {
    let mut entity = AllocationRoot::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(id)),
        ("version".into(), Value::I64(0)),
        ("name".into(), Value::Text(name.into())),
    ])))
    .unwrap();
    entity.mark_as_new();
    entity
        .entity_runtime_state()
        .unwrap()
        .set(EntityKey::new("AllocationRoot", id), "name", name);
    entity
}

fn new_child(root_id: u64, name: &str) -> AllocationChild {
    let mut child = AllocationChild::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(0)),
        ("version".into(), Value::I64(0)),
        ("root_id".into(), Value::U64(root_id)),
        ("name".into(), Value::Text(name.into())),
    ])))
    .unwrap();
    child.mark_as_new();
    let state = child.entity_runtime_state().unwrap();
    state.set(EntityKey::new("AllocationChild", 0_u64), "root_id", root_id);
    state.set(EntityKey::new("AllocationChild", 0_u64), "name", name);
    child
}

/// Observe the real provider allocator, not a substitute counter. IDs are
/// allocated on the same physical SQLite connection while the save owns it.
#[derive(Clone)]
struct ObservedDatabaseIds {
    generator: SqliteIdSpaceGenerator,
    transport: SqliteMutationExecutor,
    capture: Capture,
    allocated: Arc<Mutex<Vec<(String, u64)>>>,
}

impl InternalIdGenerator for ObservedDatabaseIds {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        assert!(
            !self.transport.connection().lock().unwrap().is_autocommit(),
            "this probe must allocate inside the real SQLite transaction"
        );
        assert!(self.capture.audits.lock().unwrap().is_empty());
        let id = self.generator.generate_id(entity)?;
        self.allocated.lock().unwrap().push((entity.into(), id));
        Ok(id)
    }

    fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), RuntimeError> {
        InternalIdGenerator::ensure_floor(&self.generator, entity, floor)
    }
}

fn install_database_ids(
    context: &mut UserContext,
    capture: &Capture,
    transport: &SqliteMutationExecutor,
) -> ObservedDatabaseIds {
    let generator = SqliteIdSpaceGenerator::from_executor(transport.clone());
    generator.ensure_floor("AllocationRoot", 500).unwrap();
    generator.ensure_floor("AllocationChild", 800).unwrap();
    clear_observations(context, capture);
    let observed = ObservedDatabaseIds {
        generator,
        transport: transport.clone(),
        capture: capture.clone(),
        allocated: Default::default(),
    };
    context.set_internal_id_generator(observed.clone());
    observed
}

fn database_id_floor(transport: &SqliteMutationExecutor, entity: &str) -> i64 {
    transport
        .connection()
        .lock()
        .unwrap()
        .query_row(
            "SELECT current_level FROM teaql_id_space WHERE type_name = ?",
            [teaql_runtime::canonical_id_space_entity(entity)],
            |row| row.get(0),
        )
        .unwrap()
}

fn persisted_rows(transport: &SqliteMutationExecutor, table: &str) -> i64 {
    // Only fixture-owned table names reach this diagnostic; not a public API.
    transport
        .connection()
        .lock()
        .unwrap()
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

fn clear_observations(context: &UserContext, capture: &Capture) {
    capture.commands.lock().unwrap().clear();
    capture.metadata.lock().unwrap().clear();
    capture.audits.lock().unwrap().clear();
    context.clear_sql_logs();
}

fn assert_safe_insert_log(
    context: &UserContext,
    entity: &str,
    root_reason: &str,
    outcome: teaql_data_service::SqlExecutionOutcome,
) {
    let logs = context.sql_logs();
    let matching: Vec<_> = logs
        .iter()
        .filter(|log| {
            log.operation == teaql_runtime::SqlLogOperation::Insert
                && log
                    .trace_path
                    .iter()
                    .any(|node| node.kind == TraceKind::Entity && node.entity_type == entity)
        })
        .collect();
    assert_eq!(matching.len(), 1, "one actual safe INSERT log for {entity}");
    let log = matching[0];
    assert_eq!(log.audit_reason.as_deref(), Some(root_reason));
    assert_eq!(log.log_context.execution_outcome, Some(outcome));
    assert_eq!(
        log.trace_path.first().unwrap().entity_type,
        "AllocationRoot"
    );
    assert_eq!(log.trace_path.last().unwrap().kind, TraceKind::Sql);
    assert!(log.trace_path.iter().all(|node| !matches!(
        node.kind,
        TraceKind::Comment | TraceKind::Purpose | TraceKind::AuditReason
    )));
}

fn assert_assigned_chain(nodes: &[teaql_core::TraceNode], expected: &[(&str, u64, &str)]) {
    let actual: Vec<_> = nodes
        .iter()
        .filter(|node| node.kind == TraceKind::AuditReason)
        .map(|node| {
            (
                node.entity_type.as_str(),
                node.entity_id,
                node.comment.as_str(),
            )
        })
        .collect();
    let expected: Vec<_> = expected
        .iter()
        .map(|&(entity, id, reason)| (entity, Some(id), reason))
        .collect();
    assert_eq!(
        actual, expected,
        "allocated identity must survive to this consumer boundary"
    );
}

fn assert_insert(capture: &Capture, entity: &str, expected: &[(&str, u64, &str)]) -> u64 {
    let commands = capture.commands.lock().unwrap();
    fn find_insert<'a>(request: &'a MutationRequest, entity: &str) -> Option<&'a MutationRequest> {
        match &request.command {
            MutationCommand::Insert(command) if command.entity == entity => Some(request),
            MutationCommand::Batch(children) => {
                children.iter().find_map(|child| find_insert(child, entity))
            }
            _ => None,
        }
    }
    let request = commands
        .iter()
        .find_map(|request| find_insert(request, entity))
        .expect("actual lower-ledger insert request");
    let command = match &request.command {
        MutationCommand::Insert(command) => command,
        _ => unreachable!("find_insert returns only scalar INSERT requests"),
    };
    let assigned = command.values["id"].try_u64().unwrap();
    assert!(assigned > 0);
    assert_assigned_chain(request.trace_chain(), expected);
    let metadata = capture.metadata.lock().unwrap();
    let physical = metadata
        .iter()
        .flat_map(|record| {
            if record.statements.is_empty() {
                std::slice::from_ref(record)
            } else {
                record.statements.as_slice()
            }
        })
        .filter(|record| record.operation == teaql_data_service::DataServiceOperation::Insert)
        .find(|record| {
            record
                .trace_chain
                .iter()
                .any(|node| node.kind == TraceKind::Entity && node.entity_type == entity)
        })
        .expect("actual physical SQL metadata");
    assert_assigned_chain(&physical.trace_chain, expected);
    assert!(
        physical
            .trace_chain
            .iter()
            .any(|node| node.kind == TraceKind::Entity
                && node.entity_type == entity
                && node.entity_id == Some(assigned))
    );
    let events = capture.audits.lock().unwrap();
    let event = events
        .iter()
        .find(|event| {
            event.entity == entity && event.kind == teaql_runtime::RawAuditEventKind::Created
        })
        .expect("committed safe audit event");
    // Fixture names have undeclared log policy, so they are private. Trusted
    // command/physical facts above retain the exact reason; exported audit must
    // mask quoted names across every sibling, while keeping typed identities.
    let mut private_names = Vec::new();
    let mut pending: Vec<_> = commands.iter().collect();
    while let Some(request) = pending.pop() {
        match &request.command {
            MutationCommand::Batch(children) => pending.extend(children),
            MutationCommand::Insert(command) => {
                if let Some(Value::Text(name)) = command.values.get("name") {
                    private_names.push(name);
                }
            }
            _ => {}
        }
    }
    private_names.sort_by_key(|name| std::cmp::Reverse(name.len()));
    let safe_reasons: Vec<_> = expected
        .iter()
        .map(|(_, _, reason)| {
            private_names
                .iter()
                .fold((*reason).to_owned(), |text, name| {
                    text.replace(name.as_str(), "[REDACTED]")
                })
        })
        .collect();
    let safe_expected: Vec<_> = expected
        .iter()
        .zip(&safe_reasons)
        .map(|((entity, id, _), reason)| (*entity, *id, reason.as_str()))
        .collect();
    assert_assigned_chain(&event.trace_chain, &safe_expected);
    assert!(
        event.fields.iter().any(|field| field.name == "id"
            && field.value.as_deref() == Some(assigned.to_string().as_str()))
    );
    assigned
}

#[test]
fn root_allocated_during_ledger_planning_reaches_command_sql_and_committed_audit() {
    futures_executor::block_on(async {
        let (context, capture) = setup().await;
        let saved = save_audited_ledger_entity(
            new_root(0, "late root").audit_as("create late root"),
            &context,
        )
        .await
        .unwrap();
        assert!(saved.id > 0);
        assert_insert(
            &capture,
            "AllocationRoot",
            &[("AllocationRoot", saved.id, "create late root")],
        );
        let metadata = capture.metadata.lock().unwrap();
        let readback: Vec<_> = metadata
            .iter()
            .filter(|record| record.operation == teaql_data_service::DataServiceOperation::Query)
            .collect();
        assert!(
            !readback.is_empty(),
            "allocated root has an actual authoritative readback"
        );
        for record in readback {
            assert_assigned_chain(
                &record.trace_chain,
                &[("AllocationRoot", saved.id, "create late root")],
            );
        }
        let logs = context.sql_logs();
        let readbacks: Vec<_> = logs
            .iter()
            .filter(|entry| {
                entry.operation == teaql_runtime::SqlLogOperation::Select
                    && entry.comment.as_deref() == Some("create [REDACTED]")
            })
            .collect();
        assert!(!readbacks.is_empty(), "readback reaches the safe SQL sink");
        assert!(readbacks.iter().all(|entry| {
            entry.audit_reason.as_deref() == Some("create [REDACTED]")
                && entry
                    .purpose
                    .as_deref()
                    .is_some_and(|purpose| !purpose.trim().is_empty())
        }));
    });
}

#[test]
fn child_allocated_during_ledger_planning_retains_its_own_and_parent_identity() {
    futures_executor::block_on(async {
        let (context, capture) = setup().await;
        let mut root =
            save_audited_ledger_entity(new_root(42, "before").audit_as("seed root"), &context)
                .await
                .unwrap();
        capture.commands.lock().unwrap().clear();
        capture.metadata.lock().unwrap().clear();
        capture.audits.lock().unwrap().clear();
        context.clear_sql_logs();
        root.name = "after".into();
        root.entity_runtime_state().unwrap().set(
            EntityKey::new("AllocationRoot", 42_u64),
            "name",
            "after",
        );
        let mut child = AllocationChild::from_compact_row(CompactRow::from_map(BTreeMap::from([
            ("id".into(), Value::U64(0)),
            ("version".into(), Value::I64(0)),
            ("root_id".into(), Value::U64(42)),
            ("name".into(), Value::Text("late child".into())),
        ])))
        .unwrap();
        child.mark_as_new();
        let child_state = child.entity_runtime_state().unwrap();
        child_state.set(EntityKey::new("AllocationChild", 0_u64), "root_id", 42_u64);
        child_state.set(
            EntityKey::new("AllocationChild", 0_u64),
            "name",
            "late child",
        );
        child.set_comment("authorize late child");
        root.include_pending_mutations_from(&child).unwrap();
        save_audited_ledger_entity(root.audit_as("submit root"), &context)
            .await
            .unwrap();
        let rows = context
            .require_resource::<Executor>()
            .unwrap()
            .query(
                QueryRequest::from_query(
                    teaql_core::SelectQuery::new("AllocationChild")
                        .limit(10)
                        .comment("check allocated child")
                        .purpose("verify persistent identity"),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        let id = rows[0].get("id").unwrap().try_u64().unwrap();
        assert_eq!(rows[0].get("root_id").unwrap().try_u64(), Some(42));
        assert_insert(
            &capture,
            "AllocationChild",
            &[
                ("AllocationRoot", 42, "submit root"),
                ("AllocationChild", id, "authorize late child"),
            ],
        );
    });
}

#[test]
fn database_root_allocation_inside_transaction_reaches_all_trace_boundaries() {
    futures_executor::block_on(async {
        let (mut context, capture, transport) = setup_with_transport().await;
        let ids = install_database_ids(&mut context, &capture, &transport);
        let saved = save_audited_ledger_entity(
            new_root(0, "database root").audit_as("allocate database root"),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(saved.id, 501);
        assert_eq!(saved.version, 1);
        assert_eq!(
            *ids.allocated.lock().unwrap(),
            [("AllocationRoot".into(), 501)]
        );
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), 501);
        assert_eq!(persisted_rows(&transport, "allocation_root_data"), 1);
        assert_insert(
            &capture,
            "AllocationRoot",
            &[("AllocationRoot", 501, "allocate database root")],
        );
        for metadata in capture.metadata.lock().unwrap().iter().filter(|metadata| {
            metadata.operation == teaql_data_service::DataServiceOperation::Query
        }) {
            assert_assigned_chain(
                &metadata.trace_chain,
                &[("AllocationRoot", 501, "allocate database root")],
            );
        }
        assert!(transport.connection().lock().unwrap().is_autocommit());
    });
}

#[test]
fn database_child_allocation_preserves_existing_parent_and_local_reason() {
    futures_executor::block_on(async {
        let (mut context, capture, transport) = setup_with_transport().await;
        let root =
            save_audited_ledger_entity(new_root(42, "parent").audit_as("seed parent"), &context)
                .await
                .unwrap();
        clear_observations(&context, &capture);
        let ids = install_database_ids(&mut context, &capture, &transport);
        let mut child = new_child(42, "database child");
        child.set_comment("allocate database child");
        root.include_pending_mutations_from(&child).unwrap();
        let saved = save_audited_ledger_entity(root.audit_as("attach database child"), &context)
            .await
            .unwrap();
        assert_eq!(saved.id, 42);
        assert_eq!(
            *ids.allocated.lock().unwrap(),
            [("AllocationChild".into(), 801)]
        );
        assert_eq!(database_id_floor(&transport, "AllocationChild"), 801);
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), 500);
        assert_insert(
            &capture,
            "AllocationChild",
            &[
                ("AllocationRoot", 42, "attach database child"),
                ("AllocationChild", 801, "allocate database child"),
            ],
        );
        let fk: i64 = transport
            .connection()
            .lock()
            .unwrap()
            .query_row(
                "SELECT root_id FROM allocation_child_data WHERE id = 801",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(fk, 42);
    });
}

#[test]
fn database_allocated_root_and_child_form_one_persisted_graph() {
    futures_executor::block_on(assert_new_database_graph(true, false));
}

#[test]
fn database_allocated_graph_resolves_forward_only_relation_metadata() {
    futures_executor::block_on(assert_new_database_graph(false, true));
}

#[test]
fn database_allocated_graph_resolves_both_relation_directions_without_duplicate_audit() {
    futures_executor::block_on(assert_new_database_graph(true, true));
}

async fn assert_new_database_graph(reverse: bool, forward: bool) {
    let (mut context, capture, transport) = setup_with_relations(reverse, forward).await;
    let ids = install_database_ids(&mut context, &capture, &transport);
    let root = new_root(0, "new graph");
    let mut child = new_child(0, "new child");
    child.set_comment("allocate new graph child");
    child.entity_runtime_state().unwrap().set(
        EntityKey::new("AllocationChild", 0_u64),
        "quantity",
        0_i64,
    );
    root.include_pending_mutations_from(&child).unwrap();
    let saved = save_audited_ledger_entity(root.audit_as("allocate new graph"), &context)
        .await
        .unwrap();
    assert_eq!(saved.id, 501);
    assert_eq!(
        *ids.allocated.lock().unwrap(),
        [
            ("AllocationRoot".into(), 501),
            ("AllocationChild".into(), 801)
        ]
    );
    assert_eq!(database_id_floor(&transport, "AllocationRoot"), 501);
    assert_eq!(database_id_floor(&transport, "AllocationChild"), 801);
    assert_insert(
        &capture,
        "AllocationRoot",
        &[("AllocationRoot", 501, "allocate new graph")],
    );
    assert_insert(
        &capture,
        "AllocationChild",
        &[
            ("AllocationRoot", 501, "allocate new graph"),
            ("AllocationChild", 801, "allocate new graph child"),
        ],
    );
    let (fk, quantity): (i64, i64) = transport
        .connection()
        .lock()
        .unwrap()
        .query_row(
            "SELECT root_id, quantity FROM allocation_child_data WHERE id = 801",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(fk, 501);
    assert_eq!(quantity, 0, "ordinary numeric fields must not be rebound");
    assert_eq!(capture.audits.lock().unwrap().len(), 2);
    assert_safe_insert_log(
        &context,
        "AllocationChild",
        "allocate [REDACTED]",
        teaql_data_service::SqlExecutionOutcome::Success,
    );
}

#[test]
fn database_allocation_rolls_back_and_retry_uses_new_identity_and_intent() {
    futures_executor::block_on(async {
        let (mut context, capture, transport) = setup_with_transport().await;
        let ids = install_database_ids(&mut context, &capture, &transport);
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_allocation_root BEFORE INSERT ON allocation_root_data
             WHEN NEW.name = 'reject' BEGIN SELECT RAISE(ABORT, 'fixture rejects root'); END;",
            )
            .unwrap();
        let mut root = new_root(0, "reject");
        let error =
            save_audited_ledger_entity(root.clone().audit_as("rejected allocation"), &context)
                .await
                .unwrap_err();
        assert!(
            error.to_string().contains("fixture rejects root"),
            "{error}"
        );
        assert_eq!(
            *ids.allocated.lock().unwrap(),
            [("AllocationRoot".into(), 501)]
        );
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), 500);
        assert_eq!(persisted_rows(&transport, "allocation_root_data"), 0);
        assert!(capture.audits.lock().unwrap().is_empty());
        assert!(transport.connection().lock().unwrap().is_autocommit());
        let state = root.entity_runtime_state().unwrap();
        assert!(
            state
                .new_keys()
                .contains(&EntityKey::new("AllocationRoot", 0_u64))
        );
        assert!(!state.current_change_set().changes().is_empty());

        // A different operation can advance the sequence between attempts.
        ids.generator.ensure_floor("AllocationRoot", 700).unwrap();
        root.name = "accepted".into();
        state.set(EntityKey::new("AllocationRoot", 0_u64), "name", "accepted");
        clear_observations(&context, &capture);
        let saved =
            save_audited_ledger_entity(root.audit_as("retry reviewed allocation"), &context)
                .await
                .unwrap();
        assert_eq!(saved.id, 701);
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), 701);
        assert_insert(
            &capture,
            "AllocationRoot",
            &[("AllocationRoot", 701, "retry reviewed allocation")],
        );
        assert_eq!(persisted_rows(&transport, "allocation_root_data"), 1);
        assert!(state.current_change_set().changes().is_empty());
    });
}

#[test]
fn database_id_failure_cannot_fall_back_to_an_in_process_identity() {
    futures_executor::block_on(async {
        let (mut context, capture, transport) = setup_with_transport().await;
        let ids = install_database_ids(&mut context, &capture, &transport);
        ids.generator
            .ensure_floor("AllocationRoot", i64::MAX as u64)
            .unwrap();
        let error = save_audited_ledger_entity(
            new_root(0, "overflow").audit_as("reject exhausted ID space"),
            &context,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("overflow"), "{error}");
        assert!(ids.allocated.lock().unwrap().is_empty());
        assert!(capture.commands.lock().unwrap().is_empty());
        assert!(capture.audits.lock().unwrap().is_empty());
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), i64::MAX);
        assert_eq!(persisted_rows(&transport, "allocation_root_data"), 0);
        assert!(transport.connection().lock().unwrap().is_autocommit());
    });
}

#[test]
fn database_graph_failure_retains_assigned_trace_but_rolls_back_rows_and_retryable_ids() {
    futures_executor::block_on(async {
        let (mut context, capture, transport) = setup_with_transport().await;
        let ids = install_database_ids(&mut context, &capture, &transport);
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_allocated_child BEFORE INSERT ON allocation_child_data
             BEGIN SELECT RAISE(ABORT, 'fixture rejects child after root insert'); END;",
            )
            .unwrap();
        let root = new_root(0, "retryable graph");
        let mut child = new_child(0, "retryable child");
        child.set_comment("authorize retryable child");
        root.include_pending_mutations_from(&child).unwrap();
        let error =
            save_audited_ledger_entity(root.clone().audit_as("first graph attempt"), &context)
                .await
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("fixture rejects child after root insert"),
            "{error}"
        );
        assert_eq!(
            *ids.allocated.lock().unwrap(),
            [
                ("AllocationRoot".into(), 501),
                ("AllocationChild".into(), 801)
            ]
        );
        assert!(capture.audits.lock().unwrap().is_empty());
        assert_eq!(persisted_rows(&transport, "allocation_root_data"), 0);
        assert_eq!(persisted_rows(&transport, "allocation_child_data"), 0);
        assert_safe_insert_log(
            &context,
            "AllocationChild",
            // SQL exposes the request-owned reason; the private local child
            // reason remains in separately verified per-entity lineage.
            "first graph attempt",
            teaql_data_service::SqlExecutionOutcome::Failure,
        );
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), 500);
        assert_eq!(database_id_floor(&transport, "AllocationChild"), 800);
        {
            let metadata = capture.metadata.lock().unwrap();
            let statements: Vec<_> = metadata
                .iter()
                .flat_map(|record| {
                    if record.statements.is_empty() {
                        std::slice::from_ref(record)
                    } else {
                        record.statements.as_slice()
                    }
                })
                .filter(|record| {
                    record.operation == teaql_data_service::DataServiceOperation::Insert
                })
                .collect();
            assert_eq!(
                statements.len(),
                2,
                "actual successful root and rejected child SQL"
            );
            assert_eq!(
                statements[0].sql_log.execution_outcome,
                Some(teaql_data_service::SqlExecutionOutcome::Success)
            );
            assert_eq!(
                statements[1].sql_log.execution_outcome,
                Some(teaql_data_service::SqlExecutionOutcome::Failure)
            );
            assert_assigned_chain(
                &statements[0].trace_chain,
                &[("AllocationRoot", 501, "first graph attempt")],
            );
            assert_assigned_chain(
                &statements[1].trace_chain,
                &[
                    ("AllocationRoot", 501, "first graph attempt"),
                    ("AllocationChild", 801, "authorize retryable child"),
                ],
            );
        }
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_allocated_child")
            .unwrap();
        ids.generator.ensure_floor("AllocationRoot", 900).unwrap();
        ids.generator.ensure_floor("AllocationChild", 1200).unwrap();
        clear_observations(&context, &capture);
        let saved = save_audited_ledger_entity(root.audit_as("reviewed graph retry"), &context)
            .await
            .unwrap();
        assert_eq!(saved.id, 901);
        assert_insert(
            &capture,
            "AllocationRoot",
            &[("AllocationRoot", 901, "reviewed graph retry")],
        );
        assert_insert(
            &capture,
            "AllocationChild",
            &[
                ("AllocationRoot", 901, "reviewed graph retry"),
                ("AllocationChild", 1201, "authorize retryable child"),
            ],
        );
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), 901);
        assert_eq!(database_id_floor(&transport, "AllocationChild"), 1201);
        assert_eq!(persisted_rows(&transport, "allocation_root_data"), 1);
        assert_eq!(persisted_rows(&transport, "allocation_child_data"), 1);
    });
}

#[test]
fn database_allocated_explicit_transaction_delivers_audit_only_after_commit() {
    futures_executor::block_on(async {
        let (mut context, capture, transport) = setup_with_transport().await;
        let _ids = install_database_ids(&mut context, &capture, &transport);
        let capture_in_work = capture.clone();
        let saved = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    let saved = transaction
                        .save_audited(
                            new_root(0, "explicit database root")
                                .audit_as("explicit root allocation"),
                        )
                        .await?;
                    assert_eq!(saved.id, 501);
                    assert!(capture_in_work.audits.lock().unwrap().is_empty());
                    Ok(saved)
                })
            })
            .await
            .unwrap();
        assert_eq!(saved.id, 501);
        assert_insert(
            &capture,
            "AllocationRoot",
            &[("AllocationRoot", 501, "explicit root allocation")],
        );
        assert_eq!(database_id_floor(&transport, "AllocationRoot"), 501);
        assert_eq!(persisted_rows(&transport, "allocation_root_data"), 1);
    });
}

// TC-MUT-11: native ledger -> metadata-driven graph -> real SQLite. These
// fixtures do not stand in for generated relation/API acceptance.
struct BlankLocalCommitProbe {
    capture: Capture,
    independent: Mutex<rusqlite::Connection>,
}

impl SafeAuditEventSink for BlankLocalCommitProbe {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        let table = match event.entity.as_str() {
            "AllocationRoot" => "allocation_root_data",
            "Payment" => "payment_data",
            "PaymentAttempt" => "payment_attempt_data",
            "Shipment" => "shipment_data",
            other => panic!("unexpected audit target {other}"),
        };
        let connection = self.independent.lock().unwrap();
        assert!(connection.is_autocommit());
        let id = event.entity_id.expect("independent committed target ID");
        let row: (i64, i64) = connection
            .query_row(
                &format!("SELECT id, version FROM {table} WHERE id = ?"),
                [i64::try_from(id).unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("safe audit must see the row through a separate connection after commit");
        assert_eq!(row, (i64::try_from(id).unwrap(), 1));
        self.capture.audits.lock().unwrap().push(event.clone());
        Ok(())
    }
}

async fn blank_local_context(logging: bool) -> (UserContext, Capture, SqliteMutationExecutor) {
    use teaql_core::{DataType, PropertyDescriptor};
    let parent = |name: &str, foreign: &str| {
        EntityDescriptor::new(name)
            .table_name(match name {
                "Payment" => "payment_data",
                "PaymentAttempt" => "payment_attempt_data",
                "Shipment" => "shipment_data",
                _ => unreachable!(),
            })
            .property(PropertyDescriptor::new("id", DataType::U64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text))
            .property(PropertyDescriptor::new(foreign, DataType::U64))
    };
    let root = AllocationRoot::entity_descriptor()
        .table_name("allocation_root_data")
        .relation(
            RelationDescriptor::new("payments", "Payment")
                .many()
                .local_key("id")
                .foreign_key("customer_order"),
        )
        .relation(
            RelationDescriptor::new("shipments", "Shipment")
                .many()
                .local_key("id")
                .foreign_key("customer_order"),
        );
    let payment = parent("Payment", "customer_order").relation(
        RelationDescriptor::new("attempts", "PaymentAttempt")
            .many()
            .local_key("id")
            .foreign_key("payment"),
    );
    let entities = vec![
        root,
        payment,
        parent("PaymentAttempt", "payment"),
        parent("Shipment", "customer_order"),
    ];
    let directory = std::env::temp_dir().join(format!(
        "teaql-rust-blank-local-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let database = directory.join("trace.sqlite");
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open(&database).unwrap());
    let mut metadata = InMemoryMetadataStore::new();
    for entity in &entities {
        metadata = metadata.with_entity(entity.clone());
    }
    let mut context = UserContext::new().with_metadata(metadata);
    context.use_sqlite_provider(transport.clone());
    context.ensure_schema().await.unwrap();
    let capture = Capture::default();
    context.register_executor(Observed(
        SqlDataServiceExecutor::new(
            SqliteDialect,
            transport.clone(),
            Schema(entities.into_iter().map(Arc::new).collect()),
        ),
        capture.clone(),
    ));
    context.set_custom_event_sink(BlankLocalCommitProbe {
        capture: capture.clone(),
        independent: Mutex::new(rusqlite::Connection::open(&database).unwrap()),
    });
    if !logging {
        context.disable_sql_log();
    }
    clear_observations(&context, &capture);
    println!(
        "BLANK_LOCAL_DATABASE logging={logging} path={}",
        database.display()
    );
    (context, capture, transport)
}

async fn run_blank_local_execution(logging: bool, reasons: &[Option<&str>], inherit: bool) {
    let (context, capture, transport) = blank_local_context(logging).await;
    if inherit {
        for blank in reasons {
            // The existing fluent audited constructor rejects invalid input by
            // panic. Test that boundary separately from the structured request
            // error returned by native graph planning, not as a failed save.
            let rejected = std::panic::catch_unwind(|| {
                new_root(9999, "invalid root").audit_as(blank.unwrap_or_default())
            });
            let rejected = rejected
                .err()
                .expect("blank audited constructor must reject");
            assert_eq!(
                rejected
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| rejected.downcast_ref::<String>().map(String::as_str)),
                Some("audit comment must not be empty")
            );
            let mut invalid = teaql_runtime::GraphNode::new("AllocationRoot")
                .operation(teaql_runtime::GraphOperation::Create)
                .value("id", 9999_u64);
            invalid.comment = blank.map(str::to_owned);
            let executor = context.require_resource::<Executor>().unwrap();
            let error = teaql_runtime::EntityDataService::for_executor(
                &context,
                "AllocationRoot",
                executor,
            )
            .plan_graph(invalid)
            .await
            .unwrap_err();
            let teaql_runtime::DataServiceError::Runtime(RuntimeError::RequestIntent(error)) =
                error
            else {
                panic!("wrong intent failure {error:?}");
            };
            assert_eq!(error.code(), "REQUEST_COMMENT_REQUIRED");
            assert_eq!(error.field, "comment");
            assert_eq!(error.request_kind, teaql_core::RequestKind::Mutation);
            assert!(capture.commands.lock().unwrap().is_empty());
            assert!(capture.metadata.lock().unwrap().is_empty());
            assert!(capture.audits.lock().unwrap().is_empty());
            assert!(context.sql_logs().is_empty());
        }
    }
    let root = new_root(100, "native root");
    let ledger = root.entity_runtime_state().unwrap();
    let add = |entity: &str, id: u64, foreign: &str, parent: u64, reason: Option<&str>| {
        let key = EntityKey::new(entity, id);
        ledger.mark_as_new(key.clone());
        ledger.set(key.clone(), foreign, parent);
        ledger.set(key.clone(), "name", format!("native {entity} {id}"));
        if let Some(reason) = reason {
            ledger.set_entity_comment(key, reason);
        }
    };
    add(
        "Payment",
        201,
        "customer_order",
        100,
        Some("authorize payment"),
    );
    add(
        "Shipment",
        301,
        "customer_order",
        100,
        Some("prepare shipment"),
    );
    for (index, reason) in reasons.iter().enumerate() {
        // Original null/blank/control values reach runtime classification. No
        // test-side parent fallback or expected trace is installed on the ledger.
        add(
            "PaymentAttempt",
            400 + index as u64,
            "payment",
            201,
            *reason,
        );
    }
    let saved = save_audited_ledger_entity(root.audit_as("submit order"), &context)
        .await
        .unwrap();
    assert_eq!((saved.id, saved.version), (100, 1));
    let mut commands = Vec::new();
    fn scalar_requests<'a>(request: &'a MutationRequest, into: &mut Vec<&'a MutationRequest>) {
        if let MutationCommand::Batch(items) = &request.command {
            for item in items {
                scalar_requests(item, into);
            }
        } else {
            into.push(request);
        }
    }
    let captured = capture.commands.lock().unwrap();
    for request in captured.iter() {
        scalar_requests(request, &mut commands);
    }
    assert_eq!(commands.len(), reasons.len() + 3);
    let metadata = capture.metadata.lock().unwrap();
    let physical: Vec<_> = metadata
        .iter()
        .flat_map(|row| {
            if row.statements.is_empty() {
                std::slice::from_ref(row)
            } else {
                row.statements.as_slice()
            }
        })
        .collect();
    let audits = capture.audits.lock().unwrap();
    assert_eq!(audits.len(), commands.len());
    assert_eq!(
        physical
            .iter()
            .filter(|row| row.operation == teaql_data_service::DataServiceOperation::Insert)
            .count(),
        commands.len()
    );
    assert!(
        physical
            .iter()
            .any(|row| row.operation == teaql_data_service::DataServiceOperation::Query),
        "actual authoritative readback required"
    );
    for request in commands {
        let MutationCommand::Insert(command) = &request.command else {
            panic!("non-insert in create fixture");
        };
        let id = command.values["id"].try_u64().unwrap();
        let mut expected = vec![("AllocationRoot", 100, "submit order")];
        if command.entity == "Payment" || command.entity == "PaymentAttempt" {
            expected.push(("Payment", 201, "authorize payment"));
            if command.entity == "PaymentAttempt" && !inherit {
                expected.push(("PaymentAttempt", id, reasons[(id - 400) as usize].unwrap()));
            }
        } else if command.entity == "Shipment" {
            expected.push(("Shipment", 301, "prepare shipment"));
        }
        assert_eq!(request.comment(), "submit order");
        assert_assigned_chain(request.trace_chain(), &expected);
        let matching: Vec<_> = audits
            .iter()
            .filter(|event| event.entity == command.entity && event.entity_id == Some(id))
            .collect();
        assert_eq!(matching.len(), 1);
        assert_assigned_chain(&matching[0].trace_chain, &expected);
        let statements: Vec<_> = physical
            .iter()
            .filter(|row| {
                row.operation == teaql_data_service::DataServiceOperation::Insert
                    && row.trace_chain.iter().any(|node| {
                        node.kind == TraceKind::Entity
                            && node.entity_type == command.entity
                            && node.entity_id == Some(id)
                    })
            })
            .collect();
        assert_eq!(statements.len(), 1);
        assert_assigned_chain(&statements[0].trace_chain, &expected);
        assert_eq!(statements[0].comment.as_deref(), Some("submit order"));
        assert_eq!(statements[0].affected_rows, Some(1));
        assert_eq!(
            statements[0].sql_log.execution_outcome,
            Some(teaql_data_service::SqlExecutionOutcome::Success)
        );
    }
    let logs = context.sql_logs();
    if logging {
        assert_eq!(logs.len(), physical.len());
        for log in logs {
            assert_eq!(
                log.audit_reason.as_deref(),
                Some("submit order"),
                "owned request reason must not become a descendant's local reason"
            );
            assert_eq!(log.trace_path[0].entity_type, "AllocationRoot");
            assert_eq!(log.trace_path.last().unwrap().kind, TraceKind::Sql);
        }
    } else {
        assert!(logs.is_empty());
    }
    assert!(transport.connection().lock().unwrap().is_autocommit());
}

#[test]
fn blank_local_reasons_inherit_at_real_sinks_logging_on() {
    futures_executor::block_on(run_blank_local_execution(
        true,
        &[
            None,
            Some(""),
            Some(" \t\r\n"),
            Some("\u{85}"),
            Some("\u{a0}"),
            Some("\u{2003}"),
        ],
        true,
    ));
}
#[test]
fn blank_local_reasons_inherit_at_real_sinks_logging_off() {
    futures_executor::block_on(run_blank_local_execution(
        false,
        &[
            None,
            Some(""),
            Some(" \t\r\n"),
            Some("\u{85}"),
            Some("\u{a0}"),
            Some("\u{2003}"),
        ],
        true,
    ));
}
#[test]
fn non_white_space_local_reasons_survive_at_real_sinks_logging_on() {
    futures_executor::block_on(run_blank_local_execution(
        true,
        &[
            Some("\u{1c}"),
            Some("\u{1d}"),
            Some("\u{1e}"),
            Some("\u{1f}"),
        ],
        false,
    ));
}
#[test]
fn non_white_space_local_reasons_survive_at_real_sinks_logging_off() {
    futures_executor::block_on(run_blank_local_execution(
        false,
        &[
            Some("\u{1c}"),
            Some("\u{1d}"),
            Some("\u{1e}"),
            Some("\u{1f}"),
        ],
        false,
    ));
}
