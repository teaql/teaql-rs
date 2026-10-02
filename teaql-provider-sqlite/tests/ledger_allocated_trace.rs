//! teaql-rs #239: real lower-ledger allocation, not a preassigned token helper.
//! The runtime produces every trace. This observer delegates unchanged SQL.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use teaql_core::TeaqlEntity as _;
use teaql_core::{CompactRow, Entity, EntityDescriptor, RelationDescriptor, TraceKind, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, MutationCommand,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
    SchemaProvider, Transaction, TransactionExecutor,
};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    EntityKey, InMemoryMetadataStore, LedgerEntity, RuntimeError, SafeAuditEvent,
    SafeAuditEventSink, UserContext, save_audited_ledger_entity,
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
        let result = self.0.query(request).await?;
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
        self.1.commands.lock().unwrap().push(request.clone());
        let result = self.0.mutate(request).await?;
        self.1
            .metadata
            .lock()
            .unwrap()
            .push(result.metadata.clone());
        Ok(result)
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
    let root = AllocationRoot::entity_descriptor().relation(
        RelationDescriptor::new("children", "AllocationChild")
            .many()
            .local_key("id")
            .foreign_key("root_id"),
    );
    let child = AllocationChild::entity_descriptor();
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
            transport,
            Schema(vec![Arc::new(root), Arc::new(child)]),
        ),
        capture.clone(),
    ));
    (context, capture)
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
