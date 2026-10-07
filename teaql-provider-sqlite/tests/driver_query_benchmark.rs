//! Opt-in matched-driver track; synthetic fixture setup is outside measurement.
#[path = "../../teaql-runtime/tests/support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use std::sync::{Arc, Mutex};
use teaql_core::{Entity, EntityDescriptor, Expr, OrderBy, SelectQuery, TeaqlEntity as _};
use teaql_data_service::SchemaProvider;
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::{CompiledQuery, SqlDataServiceExecutor, SqlDialect, SqlTransport};

#[teaql_entity]
#[derive(Debug, TeaqlEntity)]
#[teaql(entity = "DriverProbe", table = "driver_probe", indexed_layout)]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    note: Option<String>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}
impl Probe {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "driver-probe-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2), ("note", 3)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
        ("note", "note", "note"),
    ];
}
#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (name == "DriverProbe").then(|| self.0.clone())
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;
// Observe the real transport input in a separate correctness lane. Never use
// redacted diagnostic logs as driver-binding evidence, and keep observation
// locks/clones entirely outside the timed steady-state executor.
#[derive(Clone)]
struct RecordingTransport {
    inner: SqliteMutationExecutor,
    queries: Arc<Mutex<Vec<CompiledQuery>>>,
}
impl SqlTransport for RecordingTransport {
    type Error = <SqliteMutationExecutor as SqlTransport>::Error;

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<teaql_core::CompactRow>, Self::Error> {
        self.queries.lock().unwrap().push(query.clone());
        self.inner.fetch_all_compact_sql(query).await
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.inner.execute_sql(query).await
    }
}
type ObservedExecutor = SqlDataServiceExecutor<SqliteDialect, RecordingTransport, Schema>;
#[derive(Debug, PartialEq, Eq)]
struct NativeRow {
    id: u64,
    version: i64,
    name: String,
    note: Option<String>,
}

fn percentile(values: &[u128], numerator: usize) -> u128 {
    values[(values.len() * numerator).div_ceil(100).saturating_sub(1)]
}

fn native_compact(row: teaql_core::CompactRow) -> NativeRow {
    NativeRow {
        id: row.get("id").and_then(teaql_core::Value::try_u64).unwrap(),
        version: row
            .get("version")
            .and_then(teaql_core::Value::try_i64)
            .unwrap(),
        name: row
            .get("name")
            .and_then(teaql_core::Value::try_text)
            .unwrap()
            .to_owned(),
        note: row
            .get("note")
            .and_then(teaql_core::Value::try_text)
            .map(str::to_owned),
    }
}

fn allocation_layers(
    transport: &SqliteMutationExecutor,
    query: &CompiledQuery,
    full: bool,
    count: u64,
) {
    let (rows, calls, bytes, _) = measured(|| transport.fetch_all_compact(query).unwrap());
    assert_eq!(rows.len(), count as usize);
    println!("LAYER,compact_fetch,{full},{count},{calls},{bytes}");
    let expected = rows.iter().cloned().map(native_compact).collect::<Vec<_>>();
    let prepared = rows.clone();
    let (plain, calls, bytes, _) =
        measured(|| prepared.into_iter().map(native_compact).collect::<Vec<_>>());
    assert_eq!(plain, expected);
    println!("LAYER,plain_compact_decode,{full},{count},{calls},{bytes}");
    let prepared = rows;
    let root = teaql_runtime::EntityRuntimeState::default();
    let (typed, calls, bytes, _) = measured(|| {
        prepared
            .into_iter()
            .map(|row| Probe::from_compact_row_with_context(row, &root).unwrap())
            .collect::<Vec<_>>()
    });
    let snapshot = typed[0].loaded_state_snapshot().unwrap();
    for (native, entity) in expected.iter().zip(&typed) {
        assert_eq!(
            (&native.name, &native.note, native.id, native.version),
            (&entity.name, &entity.note, entity.id, entity.version)
        );
        assert_eq!(entity.is_field_loaded("note"), full);
        assert!(Arc::ptr_eq(
            &snapshot,
            &entity.loaded_state_snapshot().unwrap()
        ));
        assert!(entity.dirty_fields().is_none());
    }
    println!("LAYER,runtime_compact_decode,{full},{count},{calls},{bytes}");
    // Measure runtime-state cloning separately from SQL and typed-field cloning.
    // Preparing references performs no ledger access and is outside the counter.
    let states = typed
        .iter()
        .map(|entity| {
            entity
                .__teaql_runtime_state_any()
                .unwrap()
                .downcast_ref::<teaql_runtime::EntityRuntimeState>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    for phase in ["first_state_clone", "repeat_state_clone"] {
        let (cloned, calls, bytes, _) = measured(|| {
            states
                .iter()
                .map(|state| teaql_runtime::EntityRuntimeState::clone(state))
                .collect::<Vec<_>>()
        });
        // Retained baselines and empty mutation intent are correctness controls,
        // not part of the measured cloning workload.
        for (state, entity) in cloned.iter().zip(&typed) {
            let original = state.original_snapshot().unwrap();
            assert_eq!(
                original.get("id").and_then(teaql_core::Value::try_u64),
                Some(entity.id)
            );
            assert_eq!(
                original.get("version").and_then(teaql_core::Value::try_i64),
                Some(entity.version)
            );
            assert_eq!(
                original.get("name").and_then(teaql_core::Value::try_text),
                Some(entity.name.as_str())
            );
            assert_eq!(original.contains_key("note"), full);
            if full {
                assert_eq!(
                    original.get("note").and_then(teaql_core::Value::try_text),
                    entity.note.as_deref()
                );
            }
            assert!(state.current_change_set().is_empty());
        }
        assert!(typed.iter().all(|entity| entity.dirty_fields().is_none()));
        println!("LAYER,{phase},{full},{count},{calls},{bytes}");
    }
}

#[test]
#[ignore = "opt-in database/driver benchmark; run --release --ignored --nocapture"]
fn matched_sqlite_driver_queries() {
    let descriptor = Probe::entity_descriptor();
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
    context.use_sqlite_provider(transport.clone());
    context.register_executor(Executor::new(
        SqliteDialect,
        transport.clone(),
        Schema(Arc::new(descriptor.clone())),
    ));
    context.disable_sql_log();
    futures_executor::block_on(context.ensure_schema()).unwrap();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let mut observed_context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
    // Read-only qualification does not register a graph saver/transaction SPI.
    observed_context.insert_resource(ObservedExecutor::new(
        SqliteDialect,
        RecordingTransport {
            inner: transport.clone(),
            queries: queries.clone(),
        },
        Schema(Arc::new(descriptor.clone())),
    ));
    observed_context.disable_sql_log();
    // Deterministic benchmark input, not model bootstrap or a business save
    // alternative. Raw setup is excluded from every reported query sample.
    let connection = transport.connection();
    let engine: String = connection
        .lock()
        .unwrap()
        .query_row("SELECT sqlite_version()", [], |row| row.get(0))
        .unwrap();
    println!("ENGINE_VERSION,sqlite,{engine}");
    {
        let mut db = connection.lock().unwrap();
        let transaction = db.transaction().unwrap();
        let mut statement = transaction
            .prepare("INSERT INTO driver_probe(id,version,name,note) VALUES(?1,?2,?3,?4)")
            .unwrap();
        for id in 1_i64..=10_000 {
            statement
                .execute(rusqlite::params![
                    id,
                    1_i64,
                    format!("row-{id}"),
                    if id % 2 == 0 {
                        None
                    } else {
                        Some("nullable note")
                    }
                ])
                .unwrap();
        }
        drop(statement);
        transaction.commit().unwrap();
    }
    println!(
        "case,projection,rows,samples,p50_ns,p95_ns,queries_per_second,allocation_calls,requested_bytes"
    );
    for full in [false, true] {
        for count in [1_u64, 100, 10_000] {
            let names = if full {
                vec!["id", "version", "name", "note"]
            } else {
                vec!["id", "version", "name"]
            };
            let query = SelectQuery::new("DriverProbe")
                .projects(names)
                .filter(Expr::gt("version", 0_i64))
                .order_by(OrderBy::asc("id"))
                .limit(count)
                .comment("what: matched typed load-state driver probe");
            let request = PurposedSelectQuery::new(
                query.clone(),
                "why: isolate governed runtime query overhead",
            );
            let prepared = query.clone().prepare_for_list().unwrap();
            let compiled = SqliteDialect
                .compile_select(&descriptor, &prepared)
                .unwrap();
            let bindings: Vec<rusqlite::types::Value> = compiled
                .params
                .iter()
                .map(|value| match value {
                    teaql_core::Value::I64(value) => rusqlite::types::Value::Integer(*value),
                    teaql_core::Value::U64(value) => {
                        rusqlite::types::Value::Integer((*value).try_into().unwrap())
                    }
                    other => panic!("unexpected fixture binding {other:?}"),
                })
                .collect();
            let sql = compiled.sql_with_comment();
            let native = || {
                let db = connection.lock().unwrap();
                let mut statement = db.prepare_cached(&sql).unwrap();
                let mut cursor = statement
                    .query(rusqlite::params_from_iter(&bindings))
                    .unwrap();
                let mut result = Vec::new();
                while let Some(row) = cursor.next().unwrap() {
                    result.push(NativeRow {
                        id: row.get::<_, i64>(0).unwrap().try_into().unwrap(),
                        version: row.get(1).unwrap(),
                        name: row.get(2).unwrap(),
                        note: if full { row.get(3).unwrap() } else { None },
                    });
                }
                result
            };
            let runtime = |context: &UserContext| {
                futures_executor::block_on(
                    context
                        .entity_data_service::<Executor>("DriverProbe")
                        .unwrap()
                        .fetch_enhanced_entities::<Probe>(&request),
                )
                .unwrap()
                .data
            };
            let observed = futures_executor::block_on(
                observed_context
                    .entity_data_service::<ObservedExecutor>("DriverProbe")
                    .unwrap()
                    .fetch_enhanced_entities::<Probe>(&request),
            )
            .unwrap()
            .data;
            assert_eq!(observed.len(), count as usize);
            let captured = std::mem::take(&mut *queries.lock().unwrap());
            assert_eq!(captured.len(), 1, "one native statement per list query");
            let actual = &captured[0];
            assert_eq!(
                actual.sql_with_comment(),
                sql,
                "identical driver SQL including comment"
            );
            assert_eq!(actual.params, compiled.params, "identical driver bindings");
            println!("SQL_MATCH,{count},{full},{:?},{:?}", sql, actual.params);
            context.enable_select_sql_log();
            let cold = measured(|| runtime(&context));
            let actual = context
                .sql_logs()
                .pop()
                .expect("actual executed SQL evidence");
            assert_eq!(
                actual.sql, compiled.sql,
                "matched-driver track requires identical executed SQL"
            );
            assert_ne!(
                actual.params, compiled.params,
                "default diagnostics must stay masked"
            );
            assert_eq!(cold.0.len(), count as usize);
            // The capture lane already touched SQLite. This is the first query
            // on this runtime/shape, not a process/database cold-start result.
            println!("FIRST_RUNTIME_DEFAULT_LOG,{count},{full},{}", cold.3);
            context.disable_select_sql_log();
            for _ in 0..20 {
                std::hint::black_box(native());
                std::hint::black_box(runtime(&context));
            }
            // Matched transport and prepared-input hydration allocation stages.
            // No setup, clone preparation, snapshot assertions or elapsed-time claims.
            allocation_layers(&transport, &compiled, full, count);
            let mut raw_times = Vec::new();
            let mut runtime_times = Vec::new();
            let mut raw_allocations = Vec::new();
            let mut runtime_allocations = Vec::new();
            for sample in 0..31 {
                // Alternate order so one implementation is not always first.
                let (raw, raw_calls, raw_bytes, raw_time);
                let (typed, typed_calls, typed_bytes, typed_time);
                if sample % 2 == 0 {
                    (raw, raw_calls, raw_bytes, raw_time) = measured(native);
                    (typed, typed_calls, typed_bytes, typed_time) = measured(|| runtime(&context));
                } else {
                    (typed, typed_calls, typed_bytes, typed_time) = measured(|| runtime(&context));
                    (raw, raw_calls, raw_bytes, raw_time) = measured(native);
                }
                assert_eq!(raw.len(), count as usize);
                assert_eq!(typed.len(), raw.len());
                let state = typed[0].loaded_state_snapshot().unwrap();
                for (raw, typed) in raw.iter().zip(&typed) {
                    assert_eq!(
                        (raw.id, raw.version, &raw.name, &raw.note),
                        (typed.id, typed.version, &typed.name, &typed.note)
                    );
                    assert_eq!(typed.is_field_loaded("note"), full);
                    assert!(Arc::ptr_eq(&state, &typed.loaded_state_snapshot().unwrap()));
                }
                raw_times.push(raw_time);
                runtime_times.push(typed_time);
                raw_allocations.push((raw_calls, raw_bytes));
                runtime_allocations.push((typed_calls, typed_bytes));
            }
            for (label, mut times, mut allocations) in [
                ("rusqlite", raw_times, raw_allocations),
                ("teaql", runtime_times, runtime_allocations),
            ] {
                for (sample, (time, (calls, bytes))) in times.iter().zip(&allocations).enumerate() {
                    println!("SAMPLE,{label},{full},{count},{sample},{time},{calls},{bytes}");
                }
                times.sort_unstable();
                allocations.sort_unstable();
                let total: u128 = times.iter().sum();
                let (calls, bytes) = allocations[allocations.len() / 2];
                println!(
                    "{label},{},{count},31,{},{},{:.2},{calls},{bytes}",
                    if full { "full" } else { "sparse" },
                    percentile(&times, 50),
                    percentile(&times, 95),
                    31_000_000_000_f64 / total as f64
                );
            }
        }
    }
    println!("PASS matched typed driver results and shared immutable snapshots; log-off only");
}
