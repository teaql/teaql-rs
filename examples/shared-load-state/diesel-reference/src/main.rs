//! Isolated, test-only Diesel DSL reference. No TeaQL runtime is linked.
#[path = "../../../../teaql-runtime/tests/support/allocation_counter.rs"]
mod allocation_counter;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};

diesel::table! {
    driver_probe (id) {
        id -> BigInt,
        version -> BigInt,
        name -> Text,
        note -> Nullable<Text>,
    }
}
#[derive(Debug, Serialize, diesel::Queryable, diesel::Selectable)]
#[diesel(table_name = driver_probe)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Full {
    id: i64,
    version: i64,
    name: String,
    note: Option<String>,
}
#[derive(Debug, Serialize, diesel::Queryable, diesel::Selectable)]
#[diesel(table_name = driver_probe)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Sparse {
    id: i64,
    version: i64,
    name: String,
}
#[derive(Serialize)]
enum Rows {
    Full(Vec<Full>),
    Sparse(Vec<Sparse>),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    full: bool,
    count: i64,
}
#[derive(Serialize)]
struct Response {
    rows: Rows,
    calls: u64,
    bytes: u64,
    nanos: u128,
    engine: String,
}
fn query(connection: &mut SqliteConnection, request: &Request) -> Rows {
    use crate::driver_probe::dsl::*;
    let query = driver_probe
        .filter(version.gt(0))
        .filter(name.like("row-%"))
        .order(id.asc())
        .limit(request.count);
    if request.full {
        Rows::Full(query.select(Full::as_select()).load(connection).unwrap())
    } else {
        Rows::Sparse(query.select(Sparse::as_select()).load(connection).unwrap())
    }
}
fn main() {
    let database = std::env::args()
        .nth(1)
        .expect("trusted fixture database required");
    assert!(std::path::Path::new(&database).is_file());
    let mut connection = SqliteConnection::establish(&database).unwrap();
    let engine: String = diesel::select(diesel::dsl::sql::<diesel::sql_types::Text>(
        "sqlite_version()",
    ))
    .get_result(&mut connection)
    .unwrap();
    let mut output = std::io::BufWriter::new(std::io::stdout().lock());
    for line in std::io::stdin().lock().lines() {
        let request: Request = serde_json::from_str(&line.unwrap()).unwrap();
        assert!(matches!(request.count, 1 | 100 | 10_000));
        // IPC, JSON and connection setup are outside the timed/allocation scope.
        let (rows, calls, bytes, nanos) =
            allocation_counter::measured(|| query(&mut connection, &request));
        serde_json::to_writer(
            &mut output,
            &Response {
                rows,
                calls,
                bytes,
                nanos,
                engine: engine.clone(),
            },
        )
        .unwrap();
        writeln!(output).unwrap();
        output.flush().unwrap();
    }
}
