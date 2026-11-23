//! Public runtime admission and SQLite codec probes, not a generated application.
//! Deliberately malformed physical rows are storage-negative controls only.
use futures_util::StreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{
    CompactRow, DataType, EntityDescriptor, Expr, OrderBy, PropertyDescriptor, SelectQuery, Value,
};
use teaql_data_service::{QueryExecutor, QueryRequest, SchemaProvider, StreamQueryExecutor};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_runtime::{
    CheckObjectStatus, CheckResults, CheckRule, Checker, EntityValues, InMemoryCheckerRegistry,
    InMemoryMetadataStore, JsonFieldNamingProfile, ObjectLocation, RuntimeError, UserContext,
    mark_entity_status,
};
use teaql_sql::{SqlDataServiceExecutor, SqlDialect};

const PRIVATE: &str = "private-boolean-carrier-must-not-appear";

fn descriptor() -> EntityDescriptor {
    EntityDescriptor::new("BooleanProbe")
        .table_name("boolean_probe")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("required_flag", DataType::Bool).not_null())
        .property(PropertyDescriptor::new("optional_flag", DataType::Bool))
        .property(PropertyDescriptor::new("ordinary_count", DataType::I64))
}

fn context() -> UserContext {
    UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(descriptor()))
}

fn values() -> EntityValues {
    let mut values = EntityValues::default();
    values.insert("id".to_owned(), Value::I64(1));
    values.insert("required_flag".to_owned(), Value::Bool(true));
    values
}

fn invalid(error: RuntimeError, field: &str) {
    let RuntimeError::Check(results) = error else {
        panic!("expected structured checker diagnostics")
    };
    let result = results
        .iter()
        .find(|r| r.rule == CheckRule::InvalidType && r.location.model_path().ends_with(field))
        .expect("field-specific invalid_type must be reported");
    assert_eq!(result.entity_type.as_deref(), Some("BooleanProbe"));
    assert_eq!(
        result.location.model_path(),
        format!("line_items[2].{field}")
    );
    let wire = result.to_wire(JsonFieldNamingProfile::CamelCase);
    let camel = field.replace("_flag", "Flag");
    assert_eq!(wire.instance_path, format!("/lineItems/2/{camel}"));
    assert!(wire.input_value.is_none(), "do not echo rejected values");
    let message = wire.message.expect("actionable type diagnostic");
    assert!(message.contains("Bool"), "{message}");
    assert!(!message.contains(PRIVATE), "do not echo private text");
}

#[test]
fn boolean_mutation_invalid_carriers_rejected_without_custom_checker() {
    for status in [CheckObjectStatus::Create, CheckObjectStatus::Update] {
        for field in ["required_flag", "optional_flag"] {
            for wrong in [
                Value::I64(0),
                Value::I64(1),
                Value::I64(2),
                Value::U64(1),
                Value::F64(1.0),
                Value::Text(PRIVATE.into()),
                Value::Json(serde_json::json!(true)),
                Value::List(vec![]),
            ] {
                let mut values = values();
                values.insert(field.to_owned(), wrong.clone());
                mark_entity_status(&mut values, status);
                let error = context()
                    .check_and_fix_values_at(
                        "BooleanProbe",
                        &mut values,
                        &ObjectLocation::root().member("line_items").element(2),
                    )
                    .expect_err("a modeled Boolean is not an integer or generic truthy value");
                invalid(error, field);
                assert_eq!(values.get(field), Some(&wrong));
            }
        }
    }
}

#[test]
fn boolean_mutation_typed_null_absence_and_required_null_controls() {
    for flag in [false, true] {
        for optional in [
            None,
            Some(Value::Bool(false)),
            Some(Value::Bool(true)),
            Some(Value::Null),
            Some(Value::TypedNull(DataType::Bool)),
        ] {
            let mut values = values();
            values.insert("required_flag".to_owned(), Value::Bool(flag));
            if let Some(value) = optional.clone() {
                values.insert("optional_flag".to_owned(), value);
            }
            context()
                .check_and_fix_values("BooleanProbe", &mut values)
                .unwrap();
            assert_eq!(values.get("required_flag"), Some(&Value::Bool(flag)));
            assert_eq!(values.get("optional_flag"), optional.as_ref());
        }
    }
    let mut null = values();
    null.insert("required_flag".to_owned(), Value::Null);
    let RuntimeError::Check(results) = context()
        .check_and_fix_values("BooleanProbe", &mut null)
        .unwrap_err()
    else {
        panic!("required null must be a checker failure")
    };
    assert!(
        results
            .iter()
            .any(|r| r.rule == CheckRule::Required && r.location.model_path() == "required_flag")
    );
}

struct FixFlag {
    value: Value,
    calls: Arc<AtomicUsize>,
}
impl Checker for FixFlag {
    fn entity(&self) -> &str {
        "BooleanProbe"
    }
    fn check_and_fix(
        &self,
        _: &UserContext,
        values: &mut EntityValues,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        values.insert("required_flag".to_owned(), self.value.clone());
    }
}

#[test]
fn boolean_fix_can_repair_before_intrinsic_validation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let context =
        context().with_checker_registry(InMemoryCheckerRegistry::new().with_checker(FixFlag {
            value: Value::Bool(false),
            calls: calls.clone(),
        }));
    let mut values = values();
    values.insert("required_flag".to_owned(), Value::Text(PRIVATE.into()));
    context
        .check_and_fix_values("BooleanProbe", &mut values)
        .unwrap();
    assert_eq!(values.get("required_flag"), Some(&Value::Bool(false)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn boolean_fix_invalid_final_value_rejected_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let context =
        context().with_checker_registry(InMemoryCheckerRegistry::new().with_checker(FixFlag {
            value: Value::Text(PRIVATE.into()),
            calls: calls.clone(),
        }));
    let error = context
        .check_and_fix_values_at(
            "BooleanProbe",
            &mut values(),
            &ObjectLocation::root().member("line_items").element(2),
        )
        .expect_err("Fix must not introduce an invalid final Boolean");
    invalid(error, "required_flag");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (name == self.0.name).then(|| self.0.clone())
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;
fn executor(required: rusqlite::types::Value, optional: rusqlite::types::Value) -> Executor {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    // This is an explicit physical storage oracle, including intentionally corrupt
    // legacy values. It does not stand in for an audited application mutation.
    connection
        .execute_batch(
            "CREATE TABLE boolean_probe (
        id INTEGER PRIMARY KEY, required_flag BOOLEAN NOT NULL,
        optional_flag BOOLEAN, ordinary_count INTEGER)",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO boolean_probe VALUES (1, ?1, ?2, 2)",
            rusqlite::params![required, optional],
        )
        .unwrap();
    SqlDataServiceExecutor::new(
        SqliteDialect,
        SqliteMutationExecutor::from_connection(connection),
        Schema(Arc::new(descriptor())),
    )
}

fn request(query: SelectQuery) -> QueryRequest {
    QueryRequest::from_query(
        query
            .limit(10)
            .comment("inspect modeled Boolean values")
            .purpose("verify strict Boolean admission and storage semantics"),
    )
    .unwrap()
}

fn query(executor: &Executor, query: SelectQuery) -> Result<Vec<CompactRow>, String> {
    futures_executor::block_on(executor.query(request(query)))
        .map(|r| r.rows)
        .map_err(|e| e.to_string())
}

fn safe_storage_error(error: &str, field: &str) {
    assert!(error.contains(field), "missing output column: {error}");
    assert!(
        error.contains("Bool") || error.contains("boolean"),
        "{error}"
    );
    assert!(!error.contains(PRIVATE), "private carrier leaked: {error}");
}

#[test]
fn boolean_sqlite_integer_domain_rejected() {
    use rusqlite::types::Value as Sql;
    for field in ["required_flag", "optional_flag"] {
        for number in [2, -1, i64::MAX, i64::MIN] {
            let executor = if field == "required_flag" {
                executor(Sql::Integer(number), Sql::Null)
            } else {
                executor(Sql::Integer(1), Sql::Integer(number))
            };
            let error = query(&executor, SelectQuery::new("BooleanProbe").project(field))
                .expect_err("only SQLite Boolean INTEGER 0/1 may hydrate as bool");
            safe_storage_error(&error, field);
        }
    }
}

#[test]
fn boolean_sqlite_other_storage_classes_rejected() {
    use rusqlite::types::Value as Sql;
    for wrong in [
        Sql::Real(0.5),
        Sql::Text(PRIVATE.into()),
        Sql::Blob(vec![0, 1, 2]),
    ] {
        for field in ["required_flag", "optional_flag"] {
            let executor = if field == "required_flag" {
                executor(wrong.clone(), Sql::Null)
            } else {
                executor(Sql::Integer(1), wrong.clone())
            };
            let error = query(&executor, SelectQuery::new("BooleanProbe").project(field))
                .expect_err("wrong SQLite storage classes must not escape a Boolean field");
            safe_storage_error(&error, field);
        }
    }
}

#[test]
fn boolean_sqlite_positive_controls_and_predicates() {
    use rusqlite::types::Value as Sql;
    let executor = executor(Sql::Integer(0), Sql::Null);
    executor
        .transport
        .connection()
        .lock()
        .unwrap()
        .execute("INSERT INTO boolean_probe VALUES (2, 1, 0, 3)", [])
        .unwrap();
    let rows = query(
        &executor,
        SelectQuery::new("BooleanProbe")
            .projects(["required_flag", "optional_flag", "ordinary_count"])
            .order_by(OrderBy::asc("id")),
    )
    .unwrap();
    assert_eq!(rows[0].get("required_flag"), Some(&Value::Bool(false)));
    assert_eq!(rows[0].get("optional_flag"), Some(&Value::Null));
    assert_eq!(rows[0].get("ordinary_count"), Some(&Value::I64(2)));
    assert_eq!(rows[1].get("required_flag"), Some(&Value::Bool(true)));
    assert_eq!(rows[1].get("optional_flag"), Some(&Value::Bool(false)));
    for filter in [
        Expr::eq("required_flag", false),
        Expr::eq("required_flag", true),
        Expr::is_null("optional_flag"),
    ] {
        assert_eq!(
            query(
                &executor,
                SelectQuery::new("BooleanProbe")
                    .project("id")
                    .filter(filter)
            )
            .unwrap()
            .len(),
            1
        );
    }
}

#[test]
fn boolean_sqlite_alias_diagnostic_uses_actual_output_column() {
    use rusqlite::types::Value as Sql;
    let executor = executor(Sql::Integer(2), Sql::Null);
    let error = query(
        &executor,
        SelectQuery::new("BooleanProbe").project_expr("flag_alias", Expr::column("required_flag")),
    )
    .expect_err("aliases must preserve source Boolean decoding");
    safe_storage_error(&error, "flag_alias");
}

#[test]
fn boolean_sqlite_count_alias_does_not_reclassify_numbers() {
    use rusqlite::types::Value as Sql;
    let executor = executor(Sql::Integer(1), Sql::Null);
    executor
        .transport
        .connection()
        .lock()
        .unwrap()
        .execute("INSERT INTO boolean_probe VALUES (2, 0, NULL, 3)", [])
        .unwrap();
    let count = query(
        &executor,
        SelectQuery::new("BooleanProbe").count("required_flag"),
    )
    .unwrap();
    assert_eq!(count[0].get("required_flag"), Some(&Value::I64(2)));
    let alias = query(
        &executor,
        SelectQuery::new("BooleanProbe")
            .project_expr("ordinary_count", Expr::column("required_flag")),
    )
    .unwrap();
    assert_eq!(alias[0].get("ordinary_count"), Some(&Value::Bool(true)));
    let rows = query(
        &executor,
        SelectQuery::new("BooleanProbe").project("ordinary_count"),
    )
    .unwrap();
    assert_eq!(rows[0].get("ordinary_count"), Some(&Value::I64(2)));
}

#[test]
fn boolean_sqlite_streaming_and_buffered_chunks_reject_invalid_integer() {
    use rusqlite::types::Value as Sql;
    let executor = executor(Sql::Integer(2), Sql::Null);
    let request = request(SelectQuery::new("BooleanProbe").project("required_flag"));
    let compiled = SqliteDialect
        .compile_select(&descriptor(), &request.query)
        .unwrap();
    let error = executor
        .transport
        .fetch_stream(&compiled, 1)
        .expect_err("buffered chunks must use the same strict decoder");
    safe_storage_error(&error.to_string(), "required_flag");
    futures_executor::block_on(async {
        let mut stream = executor.query_stream(request, 1);
        let error = stream
            .next()
            .await
            .expect("one terminal diagnostic")
            .expect_err("stream must not yield a fabricated true value");
        safe_storage_error(&error.to_string(), "required_flag");
        assert!(
            stream.next().await.is_none(),
            "error must terminate the stream"
        );
    });
}
