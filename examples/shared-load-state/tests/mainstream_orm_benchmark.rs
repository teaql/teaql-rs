//! Mainstream SQLite Diesel typed DSL; not a matched-SQL or generated-lib claim.
#[path = "../../../teaql-runtime/tests/support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use diesel::prelude::*;
use std::sync::Arc;
use teaql_core::{Entity, EntityDescriptor, Expr, OrderBy, SelectQuery, TeaqlEntity as _};
use teaql_data_service::SchemaProvider;
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::SqlDataServiceExecutor;

diesel::table! {
    driver_probe (id) {
        id -> BigInt,
        version -> BigInt,
        name -> Text,
        note -> Nullable<Text>,
    }
}
#[derive(Debug, diesel::Queryable, diesel::Selectable)]
#[diesel(table_name = driver_probe)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Full {
    id: i64,
    version: i64,
    name: String,
    note: Option<String>,
}
#[derive(Debug, diesel::Queryable, diesel::Selectable)]
#[diesel(table_name = driver_probe)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Sparse {
    id: i64,
    version: i64,
    name: String,
}
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
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "mainstream-probe-v1";
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

enum Rows {
    Full(Vec<Full>),
    Sparse(Vec<Sparse>),
}
fn diesel_query(connection: &mut SqliteConnection, full: bool, count: i64) -> Rows {
    use crate::driver_probe::dsl::*;
    let query = driver_probe
        .filter(version.gt(0))
        .filter(name.like("row-%"))
        .order(id.asc())
        .limit(count);
    if full {
        Rows::Full(query.select(Full::as_select()).load(connection).unwrap())
    } else {
        Rows::Sparse(query.select(Sparse::as_select()).load(connection).unwrap())
    }
}
fn teaql_query(context: &UserContext, request: &PurposedSelectQuery) -> Vec<Probe> {
    futures_executor::block_on(
        context
            .entity_data_service::<Executor>("DriverProbe")
            .unwrap()
            .fetch_enhanced_entities::<Probe>(request),
    )
    .unwrap()
    .data
}
fn validate(raw: &Rows, typed: &[Probe], full: bool, count: usize) {
    let length = match raw {
        Rows::Full(rows) => rows.len(),
        Rows::Sparse(rows) => rows.len(),
    };
    assert_eq!(length, count);
    assert_eq!(typed.len(), count);
    let snapshot = typed[0].loaded_state_snapshot().unwrap();
    for (index, row) in typed.iter().enumerate() {
        let (id, version, name, note) = match raw {
            Rows::Full(rows) => {
                let r = &rows[index];
                (r.id, r.version, r.name.as_str(), r.note.as_deref())
            }
            Rows::Sparse(rows) => {
                let r = &rows[index];
                (r.id, r.version, r.name.as_str(), None)
            }
        };
        assert_eq!(id, index as i64 + 3);
        assert_eq!(version, 1);
        assert_eq!(name, format!("row-{}", index + 3));
        assert_eq!(note, (full && id % 2 != 0).then_some("nullable note"));
        assert_eq!(
            (row.id, row.version, row.name.as_str(), row.note.as_deref()),
            (id as u64, version, name, note)
        );
        assert_eq!(row.is_field_loaded("note"), full);
        assert!(Arc::ptr_eq(
            &snapshot,
            &row.loaded_state_snapshot().unwrap()
        ));
        assert!(row.dirty_fields().is_none());
    }
}
fn report(lane: &str, full: bool, count: usize, samples: &[(u64, u64, u128)]) {
    for (index, (calls, bytes, nanos)) in samples.iter().enumerate() {
        println!("SAMPLE,{lane},{full},{count},{index},{nanos},{calls},{bytes}");
    }
    let mut times = samples.iter().map(|s| s.2).collect::<Vec<_>>();
    times.sort_unstable();
    let mut calls = samples.iter().map(|s| s.0).collect::<Vec<_>>();
    calls.sort_unstable();
    let mut bytes = samples.iter().map(|s| s.1).collect::<Vec<_>>();
    bytes.sort_unstable();
    let total: u128 = times.iter().sum();
    let n = samples.len();
    println!(
        "{lane},{},{count},{n},{},{},{:.2},{},{}",
        if full { "full" } else { "sparse" },
        times[n / 2],
        times[(n * 95).div_ceil(100) - 1],
        n as f64 * 1e9 / total as f64,
        calls[n / 2],
        bytes[n / 2]
    );
}

#[test]
#[ignore = "opt-in: --release --ignored --nocapture; compare bounded typed ORM APIs"]
fn mainstream_diesel_and_teaql_queries_agree() {
    let count_samples = std::env::var("TEAQL_ORM_SAMPLES")
        .map(|n| n.parse::<usize>().unwrap())
        .unwrap_or(31);
    assert!((3..=31).contains(&count_samples));
    let path = std::env::temp_dir().join(format!(
        "teaql-diesel-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    let database = path.join("probe.sqlite");
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open(&database).unwrap());
    let descriptor = Probe::entity_descriptor();
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
    context.use_sqlite_provider(transport.clone());
    context.register_executor(Executor::new(
        SqliteDialect,
        transport.clone(),
        Schema(Arc::new(descriptor)),
    ));
    context.disable_sql_log();
    futures_executor::block_on(context.ensure_schema()).unwrap();
    {
        let connection = transport.connection();
        let mut connection = connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        let mut insert = transaction
            .prepare("INSERT INTO driver_probe(id,version,name,note) VALUES(?,?,?,?)")
            .unwrap();
        for id in 1_i64..=10_002 {
            insert
                .execute(rusqlite::params![
                    id,
                    if id == 1 { -1 } else { 1 },
                    if id == 2 {
                        "not-selected".to_owned()
                    } else {
                        format!("row-{id}")
                    },
                    if id % 2 == 0 {
                        None
                    } else {
                        Some("nullable note")
                    }
                ])
                .unwrap();
        }
        drop(insert);
        transaction.commit().unwrap();
        let version: String = connection
            .query_row("SELECT sqlite_version()", [], |row| row.get(0))
            .unwrap();
        println!("ENGINE_VERSION,sqlite,{version}");
    }
    let mut connection = SqliteConnection::establish(database.to_str().unwrap()).unwrap();
    println!("FRAMEWORK,diesel,2.3.13");
    println!("ORM_LOG_MODE,off");
    println!("ORM_CONNECTION_POLICY,same-file-reused-connection-both-lanes");
    for full in [false, true] {
        for count in [1, 100, 10_000] {
            let mut query = SelectQuery::new("DriverProbe")
                .filter(Expr::and([
                    Expr::gt("version", 0_i64),
                    Expr::like("name", "row-%"),
                ]))
                .projects(["id", "version", "name"])
                .order_by(OrderBy::asc("id"))
                .limit(count as u64)
                .comment("what: bounded typed ORM retrieval");
            if full {
                query = query.project("note");
            }
            let request =
                PurposedSelectQuery::new(query, "why: compare mainstream SQLite typed retrieval");
            for _ in 0..20 {
                std::hint::black_box(diesel_query(&mut connection, full, count as i64));
                std::hint::black_box(teaql_query(&context, &request));
            }
            let mut native_samples = Vec::new();
            let mut teaql_samples = Vec::new();
            for index in 0..count_samples {
                let (raw, calls, bytes, nanos);
                let (typed, tcalls, tbytes, tnanos);
                if index % 2 == 0 {
                    (raw, calls, bytes, nanos) =
                        measured(|| diesel_query(&mut connection, full, count as i64));
                    (typed, tcalls, tbytes, tnanos) = measured(|| teaql_query(&context, &request));
                } else {
                    (typed, tcalls, tbytes, tnanos) = measured(|| teaql_query(&context, &request));
                    (raw, calls, bytes, nanos) =
                        measured(|| diesel_query(&mut connection, full, count as i64));
                }
                validate(&raw, &typed, full, count);
                native_samples.push((calls, bytes, nanos));
                teaql_samples.push((tcalls, tbytes, tnanos));
            }
            report("diesel", full, count, &native_samples);
            report("teaql", full, count, &teaql_samples);
        }
    }
    println!(
        "PASS mainstream Diesel and TeaQL bounded typed results with independent version and prefix filters"
    );
}
