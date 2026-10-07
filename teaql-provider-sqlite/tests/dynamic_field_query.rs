//! Real SQLite root queries with a context-owned dynamic-field provider.
//! Root and durable extension rows use audited graph save, including cursor-local stream reads.
use std::collections::HashMap;
use std::sync::Arc;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldSelection, DynamicFieldState,
};
use teaql_core::{Entity, EntityDescriptor, TeaqlEntity as _, Value};
use teaql_data_service::SchemaProvider;
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::dynamic_fields::{DatabaseDynamicFieldsProvider, InMemoryDynamicFieldsProvider};
use teaql_runtime::{
    AuditedSaveExt, EntityKey, EntityRuntimeState, InMemoryMetadataStore, LedgerEntity,
    PurposedSelectQuery, UserContext,
};
use teaql_sql::SqlDataServiceExecutor;

#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "DynamicSchool", table = "dynamic_school", indexed_layout)]
struct DynamicSchool {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}
impl DynamicSchool {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "dynamic-school-sqlite-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
    ];

    fn update_native_name(&mut self, value: &str) {
        self.name = value.into();
        self.__teaql_runtime_state.set(
            EntityKey::new("DynamicSchool", self.id),
            "name",
            Value::Text(value.into()),
        );
    }

    fn fixture(id: u64) -> Self {
        let state = EntityRuntimeState::default();
        let key = EntityKey::new("DynamicSchool", id);
        state.mark_as_new(key.clone());
        state.set(key, "name", Value::Text(format!("school-{id}")));
        Self {
            id,
            version: 0,
            name: format!("school-{id}"),
            __load_state: teaql_core::eval::LoadState::FullyLoaded
                .into_indexed(Self::field_layout().unwrap().unwrap())
                .unwrap(),
            __teaql_runtime_state: state,
        }
    }
}
#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (name == "DynamicSchool").then(|| self.0.clone())
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

#[derive(Clone)]
struct AuditProbe(Arc<std::sync::Mutex<Vec<teaql_runtime::SafeAuditEvent>>>);
impl teaql_runtime::SafeAuditEventSink for AuditProbe {
    fn on_safe_event(
        &self,
        _: &UserContext,
        event: &teaql_runtime::SafeAuditEvent,
    ) -> Result<(), teaql_runtime::RuntimeError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
}
type AtomicFixture = (
    UserContext,
    SqliteMutationExecutor,
    Arc<teaql_core::dynamic_fields::DynamicFieldDefinitions>,
    Arc<std::sync::Mutex<Vec<teaql_runtime::SafeAuditEvent>>>,
);

fn dynamic_stream_request(chunk_size: usize) -> PurposedSelectQuery {
    let mut query = teaql_core::SelectQuery::new("DynamicSchool")
        .projects(["id", "version", "name"])
        .order_by(teaql_core::OrderBy::asc("id"))
        .limit(3)
        .select_dynamic_fields(
            DynamicFieldSelection::fields([("note".into(), teaql_core::DataType::Text)]).unwrap(),
        )
        .comment("stream durable extension values through the native cursor");
    query.stream_config = Some(teaql_core::StreamConfig { chunk_size });
    PurposedSelectQuery::new(
        query,
        "verify bounded cursor-local loading without recursive locks",
    )
}

async fn seed_stream_extensions(context: &UserContext) {
    for (index, mut row) in atomic_rows(context).await.into_iter().take(2).enumerate() {
        row.update_dynamic_field(
            "note",
            if index == 0 {
                "stream-value".into()
            } else {
                Value::Null
            },
        )
        .unwrap();
        row.update_dynamic_field("untouched", "unselected-private".into())
            .unwrap();
        row.audit_as("seed stream value and explicit null through audited mutation")
            .save(context)
            .await
            .unwrap();
    }
}

#[test]
fn dynamic_stream_uses_native_cursor_and_keeps_value_null_missing_and_tail() {
    use futures_util::StreamExt;
    futures_executor::block_on(async {
        let (context, transport, _, _) = atomic_fixture(true).await;
        seed_stream_extensions(&context).await;
        let service = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap();
        let mut stream = service
            .fetch_stream(&dynamic_stream_request(2))
            .await
            .unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(
            (first.chunk_index, first.rows.len(), first.is_last),
            (0, 2, false)
        );
        assert!(
            transport.connection().try_lock().is_err(),
            "cursor must retain the original connection"
        );
        let root = EntityRuntimeState::default();
        let mut entities = teaql_runtime::decode_compact_rows_with_read_metadata::<DynamicSchool>(
            first.rows, &root,
        )
        .unwrap();
        let state = entities[0].loaded_state_snapshot().unwrap();
        assert!(Arc::ptr_eq(
            &state,
            &entities[1].loaded_state_snapshot().unwrap()
        ));
        let tail = stream.next().await.unwrap().unwrap();
        assert_eq!(
            (tail.chunk_index, tail.rows.len(), tail.is_last),
            (1, 1, true)
        );
        entities.extend(
            teaql_runtime::decode_compact_rows_with_read_metadata::<DynamicSchool>(
                tail.rows, &root,
            )
            .unwrap(),
        );
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(transport.connection().try_lock().is_ok());
        for (index, row) in entities.iter().enumerate() {
            assert_eq!(row.id, index as u64 + 1);
            assert_eq!(row.name, format!("school-{}", row.id));
            let fields = row.dynamic_field_values().unwrap();
            assert_eq!(
                fields.field("note").unwrap().state(),
                match index {
                    0 => DynamicFieldState::Value,
                    1 => DynamicFieldState::Null,
                    _ => DynamicFieldState::NotLoaded,
                }
            );
            assert_eq!(
                fields.field("untouched").unwrap().state(),
                DynamicFieldState::NotLoaded
            );
            assert!(row.dirty_fields().is_none());
        }
        assert_eq!(
            entities[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"stream-value".into())
        );
        assert_eq!(
            atomic_rows(&context).await.len(),
            3,
            "completion releases transaction lease as well as the mutex"
        );
    });
}

#[test]
fn dynamic_stream_drop_releases_native_cursor_and_allows_audited_save() {
    use futures_util::StreamExt;
    futures_executor::block_on(async {
        let (context, transport, _, _) = atomic_fixture(true).await;
        seed_stream_extensions(&context).await;
        let service = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap();
        let mut stream = service
            .fetch_stream(&dynamic_stream_request(1))
            .await
            .unwrap();
        let first = stream.next().await.unwrap().unwrap();
        let root = EntityRuntimeState::default();
        let mut row = teaql_runtime::decode_compact_rows_with_read_metadata::<DynamicSchool>(
            first.rows, &root,
        )
        .unwrap()
        .remove(0);
        assert!(transport.connection().try_lock().is_err());
        drop(stream);
        assert!(transport.connection().try_lock().is_ok());
        row.update_dynamic_field("note", "after-drop".into())
            .unwrap();
        row.audit_as("save a held streamed entity after early cursor close")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(
            atomic_rows(&context).await[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"after-drop".into())
        );
    });
}

#[test]
fn dynamic_stream_invalid_stored_type_fails_the_batch_and_releases_locks() {
    use futures_util::StreamExt;
    futures_executor::block_on(async {
        let (context, transport, _, _) = atomic_fixture(true).await;
        seed_stream_extensions(&context).await;
        // Deliberately corrupt the provider's persisted metadata, not application data construction.
        transport.connection().lock().unwrap().execute(
            "UPDATE teaql_dynamic_field_storage_v1 SET data_type='I64',is_null=0,payload='PRIVATE-CANARY' WHERE owner_id='2' AND code='note'", []
        ).unwrap();
        let service = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap();
        let mut stream = service
            .fetch_stream(&dynamic_stream_request(2))
            .await
            .unwrap();
        let error = stream.next().await.unwrap().unwrap_err().to_string();
        assert!(error.contains("DYNAMIC_FIELD_DEFINITION_MISMATCH"));
        assert!(!error.contains("PRIVATE-CANARY"));
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(transport.connection().try_lock().is_ok());
        let rows = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap()
            .fetch_enhanced_entities::<DynamicSchool>(&PurposedSelectQuery::new(
                teaql_core::SelectQuery::new("DynamicSchool")
                    .limit(3)
                    .comment("read native rows after a failed extension stream"),
                "prove cursor failure releases the transaction lease",
            ))
            .await
            .unwrap();
        assert_eq!(rows.data.len(), 3);
    });
}

#[test]
fn dynamic_stream_covers_empty_exact_full_and_large_batch_limits() {
    use futures_util::StreamExt;
    futures_executor::block_on(async {
        let (context, transport, _, _) = atomic_fixture(true).await;
        seed_stream_extensions(&context).await;
        let service = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap();
        for size in [1, 2, 3, 73, 8_192] {
            let mut stream = service
                .fetch_stream(&dynamic_stream_request(size))
                .await
                .unwrap();
            let mut count = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.unwrap();
                assert!(chunk.rows.len() <= size);
                count += chunk.rows.len();
                let root = EntityRuntimeState::default();
                let decoded =
                    teaql_runtime::decode_compact_rows_with_read_metadata::<DynamicSchool>(
                        chunk.rows, &root,
                    )
                    .unwrap();
                assert!(
                    decoded
                        .iter()
                        .all(|row| row.dynamic_field_values().is_some())
                );
            }
            assert_eq!(count, 3);
            drop(stream);
            assert!(transport.connection().try_lock().is_ok());
        }
        let request = PurposedSelectQuery::new(
            teaql_core::SelectQuery::new("DynamicSchool")
                .limit(3)
                .and_filter(teaql_core::Expr::eq("id", 404_u64))
                .select_dynamic_fields(DynamicFieldSelection::All)
                .comment("stream an empty extension result"),
            "verify empty cursors release without an enhancement batch",
        );
        let mut stream = service.fetch_stream(&request).await.unwrap();
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(transport.connection().try_lock().is_ok());
        let error = match service.fetch_stream(&dynamic_stream_request(0)).await {
            Ok(_) => panic!("zero-sized extension stream must fail before opening a cursor"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("DYNAMIC_FIELD_STREAM_CHUNK_REQUIRED")
        );
        assert!(transport.connection().try_lock().is_ok());
    });
}

#[test]
fn dynamic_stream_plan_cannot_cross_executor_instances_even_on_the_same_connection() {
    use teaql_data_service::QueryExecutor;
    use teaql_data_service::dynamic_fields::DynamicFieldStreamPlan;
    use teaql_sql::{CompiledQuery, StreamingSqlTransport};
    futures_executor::block_on(async {
        let (context, transport, definitions, _) = atomic_fixture(true).await;
        let store = context
            .require_resource::<Executor>()
            .unwrap()
            .dynamic_field_store()
            .unwrap();
        let plan = DynamicFieldStreamPlan::new(
            "atomic".into(),
            definitions,
            DynamicFieldSelection::All,
            store,
            teaql_core::QueryIntent::new(
                "read extensions on a bound cursor",
                "reject mismatched native executors",
            )
            .unwrap(),
        )
        .unwrap();
        let other = SqliteMutationExecutor::new(transport.connection());
        let query = CompiledQuery {
            sql: "SELECT id FROM dynamic_school".into(),
            params: vec![],
            comment: Some(
                "what: cursor binding negative test; why: reject before executing".into(),
            ),
            log_context: Default::default(),
        };
        let error = match other.stream_sql_with_dynamic_fields(query, 2, plan) {
            Ok(_) => panic!("another executor cannot consume this plan"),
            Err(error) => error,
        };
        assert_eq!(error.code, "DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH");
        assert!(transport.connection().try_lock().is_ok());
    });
}

#[test]
fn dynamic_stream_plan_validates_complete_batches_before_exposing_any_sidecar() {
    use teaql_data_service::{QueryExecutor, dynamic_fields::DynamicFieldStreamPlan};
    futures_executor::block_on(async {
        let (context, _, definitions, _) = atomic_fixture(true).await;
        let store = context
            .require_resource::<Executor>()
            .unwrap()
            .dynamic_field_store()
            .unwrap();
        let plan = DynamicFieldStreamPlan::new(
            "atomic".into(),
            definitions,
            DynamicFieldSelection::fields([("note".into(), teaql_core::DataType::Text)]).unwrap(),
            store,
            teaql_core::QueryIntent::new(
                "read a selected extension batch",
                "validate complete batches and duplicate owner views",
            )
            .unwrap(),
        )
        .unwrap();
        let make = || {
            vec![
                teaql_core::CompactRow::from_map(std::collections::BTreeMap::from([(
                    "id".into(),
                    1_u64.into(),
                )])),
                teaql_core::CompactRow::from_map(std::collections::BTreeMap::from([(
                    "id".into(),
                    1_u64.into(),
                )])),
            ]
        };
        for (ids, stored, expected) in [
            (vec![1, 2], HashMap::new(), "DYNAMIC_FIELD_OWNER_MISMATCH"),
            (
                vec![1, 1],
                HashMap::new(),
                "DYNAMIC_FIELD_BATCH_OMITTED_OWNER",
            ),
            (
                vec![1, 1],
                HashMap::from([(2, HashMap::new())]),
                "DYNAMIC_FIELD_UNREQUESTED_OWNER",
            ),
            (
                vec![1, 1],
                HashMap::from([(
                    1,
                    HashMap::from([("untouched".into(), "PRIVATE-CANARY".into())]),
                )]),
                "DYNAMIC_FIELD_UNSELECTED_VALUE",
            ),
            (
                vec![1, 1],
                HashMap::from([(1, HashMap::from([("note".into(), Value::Bool(true))]))]),
                "DYNAMIC_FIELD_TYPE_MISMATCH",
            ),
        ] {
            let mut rows = make();
            let error = plan.merge(&mut rows, &ids, stored).unwrap_err();
            assert_eq!(error.code, expected);
            assert!(!error.to_string().contains("PRIVATE-CANARY"));
            assert!(
                rows.iter_mut()
                    .all(|row| row.take_loaded_dynamic_fields().is_none())
            );
        }
        let mut rows = make();
        plan.merge(
            &mut rows,
            &[1, 1],
            HashMap::from([(1, HashMap::from([("note".into(), "duplicate".into())]))]),
        )
        .unwrap();
        let first = rows[0].take_loaded_dynamic_fields().unwrap();
        let second = rows[1].take_loaded_dynamic_fields().unwrap();
        assert!(Arc::ptr_eq(first.definitions(), second.definitions()));
        assert!(Arc::ptr_eq(first.selected_codes(), second.selected_codes()));
        assert_eq!(
            first.field("note").unwrap().value(),
            Some(&"duplicate".into())
        );
        assert_eq!(
            second.field("note").unwrap().value(),
            Some(&"duplicate".into())
        );
    });
}

#[test]
fn held_dynamic_view_cannot_follow_a_changed_storage_profile() {
    futures_executor::block_on(async {
        let (mut context, _, definitions, audits) = atomic_fixture(true).await;
        audits.lock().unwrap().clear();
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_native_name("must-not-cross-profile");
        row.update_dynamic_field("note", "must-not-cross-profile".into())
            .unwrap();
        context.set_dynamic_fields_provider(Arc::new(
            DatabaseDynamicFieldsProvider::<Executor>::new("other-profile", [definitions.clone()])
                .unwrap(),
        ));
        let result = row
            .clone()
            .audit_as("reject a held view after storage profile switch")
            .save(&context)
            .await;
        assert!(
            result.is_err(),
            "held view was silently written to a different profile"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH")
        );
        assert_eq!(atomic_rows(&context).await[0].name, "school-1");
        assert!(audits.lock().unwrap().is_empty());
        context.set_dynamic_fields_provider(Arc::new(
            DatabaseDynamicFieldsProvider::<Executor>::new("atomic", [definitions]).unwrap(),
        ));
        row.audit_as("retry in the original storage profile")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(
            atomic_rows(&context).await[0].name,
            "must-not-cross-profile"
        );
    });
}
async fn atomic_fixture(seed: bool) -> AtomicFixture {
    let descriptor = DynamicSchool::entity_descriptor();
    let audits = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()))
        .with_custom_event_sink(AuditProbe(audits.clone()));
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    context.use_sqlite_provider(transport.clone());
    context.register_executor(Executor::new(
        SqliteDialect,
        transport.clone(),
        Schema(Arc::new(descriptor)),
    ));
    let definitions = DynamicFieldDefinitions::new(
        "DynamicSchool",
        "extension-v1",
        [
            ("note".into(), teaql_core::DataType::Text),
            ("untouched".into(), teaql_core::DataType::Text),
            ("amount".into(), teaql_core::DataType::F64),
        ],
    )
    .unwrap();
    context.set_dynamic_fields_provider(Arc::new(
        DatabaseDynamicFieldsProvider::<Executor>::new("atomic", [definitions.clone()]).unwrap(),
    ));
    context.ensure_schema().await.unwrap();
    audits.lock().unwrap().clear();
    if seed {
        for id in 1..=3 {
            DynamicSchool::fixture(id)
                .audit_as("seed ordinary root records")
                .save(&context)
                .await
                .unwrap();
        }
    }
    (context, transport, definitions, audits)
}

#[test]
fn held_dynamic_view_cannot_follow_a_different_executor_instance() {
    futures_executor::block_on(async {
        let (original, _, definitions, _) = atomic_fixture(true).await;
        let mut row = atomic_rows(&original).await.remove(0);
        row.update_native_name("must-not-cross-database");
        row.update_dynamic_field("note", "private original value".into())
            .unwrap();
        let (mut other, _, _, audits) = atomic_fixture(true).await;
        audits.lock().unwrap().clear();
        other.set_dynamic_fields_provider(Arc::new(
            DatabaseDynamicFieldsProvider::<Executor>::new("atomic", [definitions]).unwrap(),
        ));
        let error = row
            .clone()
            .audit_as("reject another physical executor with identical schema")
            .save(&other)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH")
        );
        assert_eq!(atomic_rows(&original).await[0].name, "school-1");
        assert_eq!(atomic_rows(&other).await[0].name, "school-1");
        assert!(audits.lock().unwrap().is_empty());
        row.audit_as("retry with original context and executor")
            .save(&original)
            .await
            .unwrap();
        assert_eq!(
            atomic_rows(&original).await[0].name,
            "must-not-cross-database"
        );
        assert_eq!(atomic_rows(&other).await[0].name, "school-1");
    });
}
async fn atomic_rows(context: &UserContext) -> Vec<DynamicSchool> {
    let query = PurposedSelectQuery::new(
        teaql_core::SelectQuery::new("DynamicSchool")
            .projects(["id", "version", "name"])
            .order_by(teaql_core::OrderBy::asc("id"))
            .limit(3)
            .select_dynamic_fields(
                DynamicFieldSelection::fields([("note".into(), teaql_core::DataType::Text)])
                    .unwrap(),
            )
            .comment("load typed atomic graph fixture"),
        "verify persistence boundaries",
    );
    context
        .entity_data_service::<Executor>("DynamicSchool")
        .unwrap()
        .fetch_enhanced_entities::<DynamicSchool>(&query)
        .await
        .unwrap()
        .data
}

#[test]
fn sqlite_child_enhancement_preserves_dynamic_union_through_native_save_and_readback() {
    futures_executor::block_on(async {
        let (context, _, _, _) = atomic_fixture(true).await;
        for (index, mut row) in atomic_rows(&context).await.into_iter().take(2).enumerate() {
            row.update_dynamic_field(
                "note",
                if index == 0 {
                    "enhanced-private".into()
                } else {
                    Value::Null
                },
            )
            .unwrap();
            row.update_dynamic_field(
                "untouched",
                if index == 0 {
                    "root-private".into()
                } else {
                    Value::Null
                },
            )
            .unwrap();
            row.audit_as("seed extension merge controls through audited mutation")
                .save(&context)
                .await
                .unwrap();
        }
        let child = teaql_core::SelectQuery::new("DynamicSchool")
            .projects(["id", "version", "name"])
            .limit(3)
            .comment("load selected dynamic enhancement")
            .select_dynamic_fields(
                DynamicFieldSelection::fields([("note".into(), teaql_core::DataType::Text)])
                    .unwrap(),
            );
        let request = PurposedSelectQuery::new(
            teaql_core::SelectQuery::new("DynamicSchool")
                .projects(["id", "version", "name"])
                .order_by(teaql_core::OrderBy::asc("id"))
                .limit(3)
                .comment("load root plus enhanced extension views")
                .child_enhancement(child)
                .select_dynamic_fields(
                    DynamicFieldSelection::fields([(
                        "untouched".into(),
                        teaql_core::DataType::Text,
                    )])
                    .unwrap(),
                ),
            "verify private fields and shared actual selection through graph save",
        );
        let mut rows = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap()
            .fetch_enhanced_entities::<DynamicSchool>(&request)
            .await
            .unwrap()
            .data;
        assert_eq!(rows.len(), 3);
        for (index, row) in rows.iter().enumerate() {
            for code in ["note", "untouched"] {
                assert_eq!(
                    row.dynamic_field_values()
                        .unwrap()
                        .field(code)
                        .unwrap()
                        .state(),
                    [
                        DynamicFieldState::Value,
                        DynamicFieldState::Null,
                        DynamicFieldState::NotLoaded
                    ][index]
                );
            }
            assert!(!row.has_pending_dynamic_mutations());
        }
        assert!(Arc::ptr_eq(
            &rows[0].loaded_state_snapshot().unwrap(),
            &rows[1].loaded_state_snapshot().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            &rows[0].loaded_state_snapshot().unwrap(),
            &rows[2].loaded_state_snapshot().unwrap()
        ));
        let mut changed = rows.remove(0);
        changed.update_native_name("enhanced-native-save");
        let saved = changed
            .audit_as("save only native name while retaining enhanced dynamic view")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(saved.name, "enhanced-native-save");
        assert_eq!(saved.version, 3);
        let json = saved.into_json();
        assert_eq!(json["#note"], "enhanced-private");
        assert_eq!(json["#untouched"], "root-private");
        assert_eq!(atomic_rows(&context).await[1].name, "school-2");
    });
}

#[test]
fn fluent_save_uses_the_same_atomic_ledger_path_and_retains_selected_extensions() {
    futures_executor::block_on(async {
        let (context, _, _, _) = atomic_fixture(true).await;
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_dynamic_field("note", "fluent".into()).unwrap();
        row.update_dynamic_field("untouched", "keep".into())
            .unwrap();
        let saved = row
            .audit_as("persist extensions through normal fluent save")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(saved.version, 2);
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("untouched")
                .unwrap()
                .value(),
            Some(&"keep".into())
        );
        // The following view intentionally does not select the stored sibling.
        let mut row = atomic_rows(&context).await.remove(0);
        assert_eq!(
            row.dynamic_field_values()
                .unwrap()
                .field("untouched")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        row.update_dynamic_field("note", "changed".into()).unwrap();
        let saved = row
            .audit_as("change only selected extension")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(saved.version, 3);
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("untouched")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        let unchanged = saved
            .audit_as("retain readonly selected extension view")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(
            unchanged
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"changed".into())
        );
        let mut fields = (*unchanged.dynamic_field_values().unwrap()).clone();
        // Verify durable unselected data using a bounded typed request, not INSERT/SELECT SQL.
        let query = PurposedSelectQuery::new(
            teaql_core::SelectQuery::new("DynamicSchool")
                .projects(["id", "version", "name"])
                .order_by(teaql_core::OrderBy::asc("id"))
                .limit(1)
                .select_dynamic_fields(DynamicFieldSelection::All)
                .comment("read preserved extension siblings"),
            "verify NotLoaded did not erase data",
        );
        let full = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap()
            .fetch_enhanced_entities::<DynamicSchool>(&query)
            .await
            .unwrap()
            .data;
        assert_eq!(
            full[0]
                .dynamic_field_values()
                .unwrap()
                .field("untouched")
                .unwrap()
                .value(),
            Some(&"keep".into())
        );
        fields.delete("note").unwrap();
        assert_eq!(
            unchanged
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"changed".into())
        );
    });
}

#[derive(Clone)]
struct AlternateSchema(Schema);
impl SchemaProvider for AlternateSchema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.get_entity(name)
    }
}
#[test]
fn configured_provider_on_separate_executor_cannot_borrow_native_transaction() {
    futures_executor::block_on(async {
        type Other = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, AlternateSchema>;
        let (mut context, transport, definitions, audits) = atomic_fixture(true).await;
        context.insert_resource(Other::new(
            SqliteDialect,
            SqliteMutationExecutor::new(transport.connection()),
            AlternateSchema(Schema(Arc::new(DynamicSchool::entity_descriptor()))),
        ));
        context.set_dynamic_fields_provider(Arc::new(
            DatabaseDynamicFieldsProvider::<Other>::new("atomic", [definitions]).unwrap(),
        ));
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_native_name("must-not-write");
        row.update_dynamic_field("note", "must-not-write".into())
            .unwrap();
        let error = row
            .clone()
            .audit_as("reject wrong operation binding")
            .save(&context)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED")
        );
        let loaded = atomic_rows(&context).await.remove(0);
        assert_eq!(loaded.name, "school-1");
        assert_eq!(loaded.version, 1);
        assert!(row.has_pending_dynamic_mutations());
        assert_eq!(audits.lock().unwrap().len(), 3);
    });
}

#[test]
fn invalid_dynamic_readback_rolls_back_both_sides_and_retry_retains_original_intent() {
    futures_executor::block_on(async {
        let (context, transport, _, audits) = atomic_fixture(true).await;
        transport.connection().lock().unwrap().execute_batch("CREATE TRIGGER corrupt_dynamic AFTER INSERT ON teaql_dynamic_field_storage_v1
            BEGIN UPDATE teaql_dynamic_field_storage_v1 SET payload='not-json' WHERE namespace=NEW.namespace AND owner_type=NEW.owner_type AND owner_id=NEW.owner_id AND code=NEW.code; END").unwrap();
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_native_name("must-roll-back");
        row.update_dynamic_field("note", "retry".into()).unwrap();
        let error = row
            .clone()
            .audit_as("reject malformed authoritative readback")
            .save(&context)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("DYNAMIC_FIELD_INVALID_STORAGE"));
        let loaded = atomic_rows(&context).await.remove(0);
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.name, "school-1");
        assert_eq!(
            loaded
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        assert_eq!(audits.lock().unwrap().len(), 3);
        assert!(row.has_pending_dynamic_mutations());
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER corrupt_dynamic")
            .unwrap();
        assert_eq!(
            row.audit_as("retry after correcting storage behavior")
                .save(&context)
                .await
                .unwrap()
                .version,
            2
        );
    });
}

#[test]
fn changed_definitions_and_nonfinite_values_reject_before_native_dml() {
    futures_executor::block_on(async {
        let (mut context, _, definitions, audits) = atomic_fixture(true).await;
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_native_name("must-not-write");
        row.update_dynamic_field("amount", Value::F64(f64::NAN))
            .unwrap();
        let error = row
            .clone()
            .audit_as("reject nonfinite extension batch before writes")
            .save(&context)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("DYNAMIC_FIELD_NONFINITE_NUMBER"));
        assert_eq!(atomic_rows(&context).await[0].name, "school-1");
        let newer = DynamicFieldDefinitions::new(
            "DynamicSchool",
            "extension-v2",
            definitions
                .fields()
                .iter()
                .map(|(code, data_type)| (code.clone(), *data_type)),
        )
        .unwrap();
        context.set_dynamic_fields_provider(Arc::new(
            DatabaseDynamicFieldsProvider::<Executor>::new("atomic", [newer]).unwrap(),
        ));
        let error = row
            .audit_as("reject stale extension definition view")
            .save(&context)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DYNAMIC_FIELD_DEFINITION_MISMATCH")
        );
        assert_eq!(atomic_rows(&context).await[0].version, 1);
        assert_eq!(audits.lock().unwrap().len(), 3);
    });
}

#[test]
fn explicitly_composed_dynamic_graph_rolls_back_together_without_adopting_unrelated_rows() {
    futures_executor::block_on(async {
        let (context, transport, _, audits) = atomic_fixture(true).await;
        let mut rows = atomic_rows(&context).await;
        let mut parent = rows.remove(0);
        let mut child = rows.remove(0);
        let mut unrelated = rows.remove(0);
        parent.update_native_name("parent-pending");
        parent
            .update_dynamic_field("note", "parent-extension".into())
            .unwrap();
        child.update_native_name("child-pending");
        child
            .update_dynamic_field("note", "child-extension".into())
            .unwrap();
        unrelated
            .update_dynamic_field("note", "unrelated".into())
            .unwrap();
        parent.include_pending_mutations_from(&child).unwrap();
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_second_dynamic BEFORE INSERT ON teaql_dynamic_field_storage_v1
            WHEN NEW.owner_id='2' BEGIN SELECT RAISE(ABORT,'reject second entity'); END",
            )
            .unwrap();
        assert!(
            parent
                .clone()
                .audit_as("rollback composed multi-entity graph")
                .save(&context)
                .await
                .is_err()
        );
        let loaded = atomic_rows(&context).await;
        assert!(loaded.iter().all(|row| {
            row.version == 1
                && row
                    .dynamic_field_values()
                    .unwrap()
                    .field("note")
                    .unwrap()
                    .state()
                    == DynamicFieldState::NotLoaded
        }));
        assert_eq!(loaded[0].name, "school-1");
        assert_eq!(loaded[1].name, "school-2");
        assert_eq!(audits.lock().unwrap().len(), 3);
        assert!(parent.has_pending_dynamic_mutations());
        assert!(unrelated.has_pending_dynamic_mutations());
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_second_dynamic")
            .unwrap();
        let saved = parent
            .audit_as("retry the explicit graph boundary only")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(saved.version, 2);
        let loaded = atomic_rows(&context).await;
        assert_eq!(loaded[0].version, 2);
        assert_eq!(loaded[1].version, 2);
        assert_eq!(loaded[2].version, 1);
        assert_eq!(
            loaded[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"parent-extension".into())
        );
        assert_eq!(
            loaded[1]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"child-extension".into())
        );
        assert_eq!(
            loaded[2]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        assert!(unrelated.has_pending_dynamic_mutations());
        assert_eq!(audits.lock().unwrap().len(), 5);
    });
}

struct RequireLoadedName;
impl teaql_runtime::TypedChecker<DynamicSchool> for RequireLoadedName {
    fn check_and_fix_typed(
        &self,
        _: &UserContext,
        entity: &mut DynamicSchool,
        _: teaql_runtime::CheckObjectStatus,
        location: &teaql_runtime::ObjectLocation,
        results: &mut teaql_runtime::CheckResults,
    ) {
        if !entity.is_field_loaded("name") {
            results.push(teaql_runtime::CheckResult::required(
                location.clone().member("name"),
            ));
        }
    }
}
#[test]
fn incomplete_native_projection_cannot_bypass_full_checker_via_dynamic_only_save() {
    futures_executor::block_on(async {
        let (mut context, _, _, audits) = atomic_fixture(true).await;
        context.set_checker_registry(teaql_runtime::InMemoryCheckerRegistry::new().with_checker(
            teaql_runtime::TypedEntityChecker::<DynamicSchool, _>::new(RequireLoadedName),
        ));
        let query = PurposedSelectQuery::new(
            teaql_core::SelectQuery::new("DynamicSchool")
                .projects(["id", "version"])
                .order_by(teaql_core::OrderBy::asc("id"))
                .limit(1)
                .select_dynamic_fields(
                    DynamicFieldSelection::fields([("note".into(), teaql_core::DataType::Text)])
                        .unwrap(),
                )
                .comment("load deliberately incomplete native projection"),
            "verify dynamic edits do not weaken checker",
        );
        let mut row = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap()
            .fetch_enhanced_entities::<DynamicSchool>(&query)
            .await
            .unwrap()
            .data
            .remove(0);
        row.update_dynamic_field("note", "blocked".into()).unwrap();
        let error = row
            .clone()
            .audit_as("reject edit without complete object")
            .save(&context)
            .await
            .unwrap_err();
        assert!(matches!(error, teaql_runtime::RuntimeError::Check(_)));
        assert!(row.has_pending_dynamic_mutations());
        assert_eq!(atomic_rows(&context).await[0].version, 1);
        assert_eq!(audits.lock().unwrap().len(), 3);
    });
}

#[test]
fn incompatible_dynamic_definition_composition_rejects_without_partial_import() {
    futures_executor::block_on(async {
        let (context, _, definitions, _) = atomic_fixture(true).await;
        let mut target = atomic_rows(&context).await.remove(0);
        let mut source = atomic_rows(&context).await.remove(0);
        target
            .update_dynamic_field("note", "target".into())
            .unwrap();
        let newer = DynamicFieldDefinitions::new(
            "DynamicSchool",
            "other-revision",
            definitions
                .fields()
                .iter()
                .map(|(code, data_type)| (code.clone(), *data_type)),
        )
        .unwrap();
        let fields =
            teaql_core::dynamic_fields::DynamicFieldValues::from_values(newer, HashMap::new())
                .unwrap();
        let state = teaql_core::LoadedSnapshot::with_dynamic_fields(
            &source.loaded_state_snapshot().unwrap(),
            &fields,
        )
        .unwrap();
        source.install_loaded_dynamic_fields(fields, state).unwrap();
        source.update_native_name("must-not-import");
        source
            .update_dynamic_field("note", "source".into())
            .unwrap();
        let before = target.entity_runtime_state().unwrap().current_change_set();
        assert!(matches!(
            target.include_pending_mutations_from(&source),
            Err(teaql_runtime::LedgerCompositionError::ConflictingDynamicDefinitions { .. })
        ));
        assert_eq!(
            target.entity_runtime_state().unwrap().current_change_set(),
            before
        );
    });
}

#[test]
fn ordinary_ledger_save_persists_dynamic_null_delete_and_version_with_masked_audit() {
    futures_executor::block_on(async {
        let (context, _, _, audits) = atomic_fixture(true).await;
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_native_name("native-updated");
        row.update_dynamic_field("note", "PRIVATE-DYNAMIC-CANARY".into())
            .unwrap();
        let saved = teaql_runtime::save_audited_ledger_entity(
            row.audit_as("save PRIVATE-DYNAMIC-CANARY graph"),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(saved.version, 2);
        assert_eq!(saved.name, "native-updated");
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"PRIVATE-DYNAMIC-CANARY".into())
        );
        assert!(
            saved
                .entity_runtime_state()
                .unwrap()
                .current_change_set()
                .is_empty()
        );
        assert_eq!(audits.lock().unwrap().len(), 4);
        assert!(!format!("{:?}", audits.lock().unwrap()).contains("PRIVATE-DYNAMIC-CANARY"));
        assert!(
            audits.lock().unwrap()[3]
                .fields
                .iter()
                .any(|field| field.name == "#note" && field.masked)
        );

        let mut row = saved;
        let selected = row.loaded_state_snapshot().unwrap();
        row.update_dynamic_field("note", Value::Null).unwrap();
        assert!(Arc::ptr_eq(
            &selected,
            &row.loaded_state_snapshot().unwrap()
        ));
        let saved = teaql_runtime::save_audited_ledger_entity(
            row.audit_as("clear prior PRIVATE-DYNAMIC-CANARY value"),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(saved.version, 3);
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::Null
        );
        assert!(!format!("{:?}", audits.lock().unwrap()).contains("PRIVATE-DYNAMIC-CANARY"));
        let mut row = saved;
        row.delete_dynamic_field("note").unwrap();
        let saved = teaql_runtime::save_audited_ledger_entity(
            row.audit_as("delete extension rather than persist NULL"),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(saved.version, 4);
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        assert_eq!(
            atomic_rows(&context).await[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
    });
}

#[test]
fn failed_dynamic_dml_rolls_back_native_and_preserves_intent_for_retry() {
    futures_executor::block_on(async {
        let (context, transport, _, audits) = atomic_fixture(true).await;
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_native_name("native-pending");
        row.update_dynamic_field("note", "private-pending".into())
            .unwrap();
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_dynamic_write BEFORE INSERT ON teaql_dynamic_field_storage_v1
            BEGIN SELECT RAISE(ABORT,'PRIVATE-DRIVER-CANARY'); END",
            )
            .unwrap();
        let error = teaql_runtime::save_audited_ledger_entity(
            row.clone().audit_as("rollback both parts"),
            &context,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("DYNAMIC_FIELD_STORAGE_FAILED"));
        assert!(!error.to_string().contains("PRIVATE-DRIVER-CANARY"));
        assert!(
            !row.entity_runtime_state()
                .unwrap()
                .current_change_set()
                .is_empty()
        );
        let loaded = atomic_rows(&context).await.remove(0);
        assert_eq!(loaded.name, "school-1");
        assert_eq!(loaded.version, 1);
        assert_eq!(
            loaded
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        assert_eq!(audits.lock().unwrap().len(), 3);
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_dynamic_write")
            .unwrap();
        let saved = teaql_runtime::save_audited_ledger_entity(
            row.audit_as("retry original intents"),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(saved.name, "native-pending");
        assert_eq!(saved.version, 2);
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"private-pending".into())
        );
        assert_eq!(audits.lock().unwrap().len(), 4);
    });
}

#[test]
fn dynamic_only_update_rejects_stale_version_and_preserves_unrelated_context_rows() {
    futures_executor::block_on(async {
        let (context, _, _, _) = atomic_fixture(true).await;
        let mut current = atomic_rows(&context).await.remove(0);
        let mut stale = atomic_rows(&context).await.remove(0);
        let mut unrelated = atomic_rows(&context).await.remove(1);
        unrelated
            .update_dynamic_field("note", "unrelated-pending".into())
            .unwrap();
        current
            .update_dynamic_field("note", "current".into())
            .unwrap();
        let current = teaql_runtime::save_audited_ledger_entity(
            current.audit_as("advance native optimistic version"),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(current.version, 2);
        stale.update_dynamic_field("note", "stale".into()).unwrap();
        assert!(
            teaql_runtime::save_audited_ledger_entity(
                stale.clone().audit_as("reject stale extension-only write"),
                &context
            )
            .await
            .is_err()
        );
        let rows = atomic_rows(&context).await;
        assert_eq!(
            rows[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"current".into())
        );
        assert_eq!(rows[1].version, 1);
        assert_eq!(
            rows[1]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        assert!(stale.has_pending_dynamic_mutations());
        assert!(unrelated.has_pending_dynamic_mutations());
    });
}

#[test]
fn unbound_new_entity_definitions_cannot_be_silently_assigned_to_current_storage() {
    futures_executor::block_on(async {
        let (context, _, definitions, audits) = atomic_fixture(false).await;
        let mut row = DynamicSchool::fixture(0);
        let fields = teaql_core::dynamic_fields::DynamicFieldValues::from_values(
            definitions,
            HashMap::new(),
        )
        .unwrap();
        let state = teaql_core::LoadedSnapshot::with_dynamic_fields(
            &row.loaded_state_snapshot().unwrap(),
            &fields,
        )
        .unwrap();
        row.install_loaded_dynamic_fields(fields, state).unwrap();
        row.update_dynamic_field("note", "unbound value".into())
            .unwrap();
        let rejected = row
            .clone()
            .audit_as("reject unbound definition source")
            .save(&context)
            .await
            .unwrap_err();
        assert!(
            rejected
                .to_string()
                .contains("DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH")
        );
        assert!(atomic_rows(&context).await.is_empty());
        assert!(row.has_pending_dynamic_mutations());
        assert!(audits.lock().unwrap().is_empty());
    });
}

#[test]
fn new_entity_dynamic_intent_survives_id_allocation_and_readback_is_authoritative() {
    futures_executor::block_on(async {
        let (context, transport, definitions, _) = atomic_fixture(false).await;
        let definitions = DatabaseDynamicFieldsProvider::<Executor>::new("atomic", [definitions])
            .unwrap()
            .definitions_for_context(&context, "DynamicSchool")
            .unwrap();
        transport.connection().lock().unwrap().execute_batch("CREATE TRIGGER normalize_dynamic AFTER INSERT ON teaql_dynamic_field_storage_v1
            BEGIN UPDATE teaql_dynamic_field_storage_v1 SET payload=UPPER(NEW.payload) WHERE namespace=NEW.namespace AND owner_type=NEW.owner_type AND owner_id=NEW.owner_id AND code=NEW.code; END").unwrap();
        let mut row = DynamicSchool::fixture(0);
        let fields = teaql_core::dynamic_fields::DynamicFieldValues::from_values(
            definitions,
            HashMap::new(),
        )
        .unwrap();
        let state = teaql_core::LoadedSnapshot::with_dynamic_fields(
            &row.loaded_state_snapshot().unwrap(),
            &fields,
        )
        .unwrap();
        row.install_loaded_dynamic_fields(fields, state).unwrap();
        row.update_dynamic_field("note", "canonicalize".into())
            .unwrap();
        let saved = teaql_runtime::save_audited_ledger_entity(
            row.audit_as("create typed entity and extensions together"),
            &context,
        )
        .await
        .unwrap();
        assert!(saved.id > 0);
        assert_eq!(saved.version, 1);
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&"CANONICALIZE".into())
        );
    });
}

#[test]
fn durable_extensions_survive_reopening_and_decode_into_shared_typed_views() {
    use teaql_core::dynamic_fields::DynamicFieldMutation;
    use teaql_data_service::dynamic_fields::DynamicFieldWrite;
    use teaql_data_service::{QueryExecutor, Transaction, TransactionExecutor};
    futures_executor::block_on(async {
        let path = std::env::temp_dir().join(format!(
            "teaql-dynamic-storage-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let descriptor = DynamicSchool::entity_descriptor();
        let definitions = DynamicFieldDefinitions::new(
            "DynamicSchool",
            "extension-v1",
            [("note".into(), teaql_core::DataType::Text)],
        )
        .unwrap();
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
        let transport =
            SqliteMutationExecutor::from_connection(rusqlite::Connection::open(&path).unwrap());
        context.use_sqlite_provider(transport.clone());
        context.register_executor(Executor::new(
            SqliteDialect,
            transport,
            Schema(Arc::new(descriptor.clone())),
        ));
        context.set_dynamic_fields_provider(Arc::new(
            DatabaseDynamicFieldsProvider::<Executor>::new("school-demo", [definitions.clone()])
                .unwrap(),
        ));
        context.ensure_schema().await.unwrap();
        context.ensure_schema().await.unwrap();
        for id in 1..=3 {
            DynamicSchool::fixture(id)
                .audit_as("seed root through ordinary audited graph save")
                .save(&context)
                .await
                .unwrap();
        }
        {
            let executor = context.require_resource::<Executor>().unwrap();
            let tx = executor.begin().await.unwrap();
            // Framework fixture initialization through the transaction-owned SPI.
            // This is not a second application-facing save API or graph-save proof.
            QueryExecutor::dynamic_field_store(&tx)
                .unwrap()
                .apply_writes(
                    &[
                        DynamicFieldWrite {
                            namespace: "school-demo".into(),
                            definitions: definitions.clone(),
                            owner_id: 1,
                            changes: [(
                                "note".into(),
                                DynamicFieldMutation::Set("stored-value".into()),
                            )]
                            .into_iter()
                            .collect(),
                        },
                        DynamicFieldWrite {
                            namespace: "school-demo".into(),
                            definitions: definitions.clone(),
                            owner_id: 2,
                            changes: [("note".into(), DynamicFieldMutation::Set(Value::Null))]
                                .into_iter()
                                .collect(),
                        },
                    ],
                    &teaql_core::MutationIntent::new("prepare persistent extension fixture")
                        .unwrap(),
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        drop(context);
        let mut reopened = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
        let transport =
            SqliteMutationExecutor::from_connection(rusqlite::Connection::open(&path).unwrap());
        reopened.use_sqlite_provider(transport.clone());
        reopened.register_executor(Executor::new(
            SqliteDialect,
            transport,
            Schema(Arc::new(descriptor)),
        ));
        reopened.set_dynamic_fields_provider(Arc::new(
            DatabaseDynamicFieldsProvider::<Executor>::new("school-demo", [definitions]).unwrap(),
        ));
        reopened.ensure_schema().await.unwrap();
        let query = PurposedSelectQuery::new(
            teaql_core::SelectQuery::new("DynamicSchool")
                .projects(["id", "version", "name"])
                .order_by(teaql_core::OrderBy::asc("id"))
                .limit(3)
                .select_dynamic_fields(
                    DynamicFieldSelection::fields([("note".into(), teaql_core::DataType::Text)])
                        .unwrap(),
                )
                .comment("read reopened database and stored extensions"),
            "verify durable values and shared load-state geometry",
        );
        let rows = reopened
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap()
            .fetch_enhanced_entities::<DynamicSchool>(&query)
            .await
            .unwrap()
            .data;
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&Value::Text("stored-value".into()))
        );
        assert_eq!(
            rows[1]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::Null
        );
        assert_eq!(
            rows[2]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        assert!(Arc::ptr_eq(
            &rows[0].loaded_state_snapshot().unwrap(),
            &rows[1].loaded_state_snapshot().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            &rows[1].loaded_state_snapshot().unwrap(),
            &rows[2].loaded_state_snapshot().unwrap()
        ));
        assert!(Arc::ptr_eq(
            rows[0].dynamic_field_values().unwrap().definitions(),
            rows[2].dynamic_field_values().unwrap().definitions()
        ));
        assert!(
            rows.iter()
                .all(|row| row.name == format!("school-{}", row.id) && row.version == 1)
        );
        drop(reopened);
        assert_eq!(
            rows[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&Value::Text("stored-value".into()))
        );
    });
}

#[test]
fn sqlite_audited_roots_load_row_owned_extensions_with_shared_masks() {
    futures_executor::block_on(async {
        let descriptor = DynamicSchool::entity_descriptor();
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
        let transport = SqliteMutationExecutor::from_connection(
            rusqlite::Connection::open_in_memory().unwrap(),
        );
        context.use_sqlite_provider(transport.clone());
        context.ensure_schema().await.unwrap();
        context.register_executor(Executor::new(
            SqliteDialect,
            transport,
            Schema(Arc::new(descriptor)),
        ));
        for id in 1..=3 {
            let saved = DynamicSchool::fixture(id)
                .audit_as("seed bounded dynamic-field query fixture")
                .save(&context)
                .await
                .unwrap();
            assert_eq!(saved.version, 1);
        }
        let definitions = DynamicFieldDefinitions::new(
            "DynamicSchool",
            "extension-v1",
            [("note".into(), teaql_core::DataType::Text)],
        )
        .unwrap();
        context.set_dynamic_fields_provider(Arc::new(
            InMemoryDynamicFieldsProvider::from_owners([(
                definitions,
                HashMap::from([
                    (
                        1,
                        HashMap::from([("note".into(), Value::Text("private-note".into()))]),
                    ),
                    (2, HashMap::from([("note".into(), Value::Null)])),
                ]),
            )])
            .unwrap(),
        ));
        let query = PurposedSelectQuery::new(
            teaql_core::SelectQuery::new("DynamicSchool")
                .projects(["id", "version", "name"])
                .order_by(teaql_core::OrderBy::asc("id"))
                .limit(3)
                .select_dynamic_fields(
                    DynamicFieldSelection::fields([("note".into(), teaql_core::DataType::Text)])
                        .unwrap(),
                )
                .comment("load stored roots and dynamic availability"),
            "verify SQLite query carrier and shared masks",
        );
        let rows = context
            .entity_data_service::<Executor>("DynamicSchool")
            .unwrap()
            .fetch_enhanced_entities::<DynamicSchool>(&query)
            .await
            .unwrap()
            .data;
        assert_eq!(rows.len(), 3);
        assert!(Arc::ptr_eq(
            &rows[0].loaded_state_snapshot().unwrap(),
            &rows[1].loaded_state_snapshot().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            &rows[1].loaded_state_snapshot().unwrap(),
            &rows[2].loaded_state_snapshot().unwrap()
        ));
        assert_eq!(
            rows[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&Value::Text("private-note".into()))
        );
        assert_eq!(
            rows[1]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::Null
        );
        assert_eq!(
            rows[2]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .state(),
            DynamicFieldState::NotLoaded
        );
        assert_eq!(rows[0].name, "school-1");
        assert!(
            rows.iter()
                .all(|row| row.version == 1 && row.dirty_fields().is_none())
        );
    });
}
