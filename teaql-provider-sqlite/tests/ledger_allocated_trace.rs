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
    local_reason: &str,
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
    assert_eq!(log.audit_reason.as_deref(), Some(local_reason));
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
    assert_assigned_chain(&event.trace_chain, expected);
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
                    && entry.comment.as_deref() == Some("create late root")
            })
            .collect();
        assert!(!readbacks.is_empty(), "readback reaches the safe SQL sink");
        assert!(readbacks.iter().all(|entry| {
            entry.audit_reason.as_deref() == Some("create late root")
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
        "allocate new graph child",
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
            // Failed bind values are redacted even when they occur in prose.
            "authorize [REDACTED]",
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
