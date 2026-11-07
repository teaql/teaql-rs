//! Normal generated Q/E/audited-save paths. Direct SQL is limited to two
//! explicitly identified external-writer negative controls of database defense.
use checker_fix_advanced_core::{
    AuditedSave, E, OrderItem, OrderItemChecker, OrderItemCheckerLogic, PurchaseOrder, Q,
    checker_registry, service_runtime_from_pool,
};
use chrono::{TimeZone, Utc};
use serde_json::{Value as Json, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    AtomicCounterIdGenerator, CheckObjectStatus, CheckResults, FixedBusinessClock, ObjectLocation,
    RuntimeError, TypedEntityChecker, UserContext,
};

static SQL: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn observe(statement: &str) {
    SQL.lock().unwrap().push(statement.to_owned());
}
fn count() -> usize {
    SQL.lock().unwrap().len()
}
fn writes(start: usize, verb: &str, table: &str) -> usize {
    SQL.lock().unwrap()[start..]
        .iter()
        .filter(|s| {
            s.trim_start()
                .to_lowercase()
                .starts_with(&format!("{verb} "))
                && s.contains(table)
        })
        .count()
}
fn emit(round: usize, name: &str, mut facts: Json) {
    facts["round"] = json!(round);
    facts["case"] = json!(name);
    facts["passed"] = json!(true);
    println!("RUST_CHECKER_PIPELINE_CASE={facts}");
}
fn owner(context: &UserContext) -> PurchaseOrder {
    let mut value = Q::purchase_orders()
        .comment("Prepare a small Checker pipeline owner")
        .purpose("Verify generated pre-database validation and persistence")
        .new_entity(context).expect("reserve example identity");
    value.update_contact_phone("5550960");
    value.update_internal_note(Value::Null);
    value
}
async fn load_owner(context: &UserContext, id: u64) -> PurchaseOrder {
    Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .comment("Reload the complete pipeline owner")
        .purpose("Verify failed mutations preserve values and optimistic versions")
        .execute_for_list(context)
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone()
}
async fn load_child(context: &UserContext, id: u64) -> OrderItem {
    Q::order_items()
        .with_id_is(id)
        .limit(1)
        .comment("Reload the complete pipeline child")
        .purpose("Verify generated Checker/Fix and database fallback")
        .execute_for_list(context)
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone()
}
fn child(context: &UserContext, parent: Option<u64>) -> OrderItem {
    let mut value = Q::order_items()
        .comment("Prepare a small Checker pipeline child")
        .purpose("Verify required relation and post-Fix range constraints")
        .new_entity(context).expect("reserve example identity");
    if let Some(id) = parent {
        value.update_purchase_order_id(id);
    }
    value.update_product_name("Pipeline child");
    value.update_unit_price(Value::Decimal("10.00".parse().unwrap()));
    value
}
fn rejected<T>(result: Result<T, RuntimeError>, before: usize, rule: &str, pointer: &str) -> Json {
    let results = match result {
        Err(RuntimeError::Check(results)) => results,
        Err(error) => panic!("expected a structured Checker rejection, got {error:?}"),
        Ok(_) => panic!(
            "expected a structured Checker rejection, but Save accepted; provider statements={}",
            count() - before
        ),
    };
    assert_eq!(count() - before, 0, "invalid save touched SQLite");
    assert!(
        results
            .iter()
            .any(|r| r.rule.wire_id() == rule && r.location.instance_path() == pointer)
    );
    json!({"saveStatements":0,"rule":rule,"instancePath":pointer,
        "violations":results.iter().map(|v| v.to_wire(teaql_runtime::JsonFieldNamingProfile::CamelCase)).collect::<Vec<_>>()})
}
#[derive(Clone)]
struct DeliberatePriceFix {
    value: Value,
    product_name: Option<Value>,
    calls: Arc<AtomicUsize>,
}
impl OrderItemCheckerLogic for DeliberatePriceFix {
    fn check_and_fix_order_item(
        &self,
        _: &UserContext,
        entity: &mut OrderItem,
        _: CheckObjectStatus,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        // An explicitly installed customer hook, not a new default business rewrite.
        entity.update_unit_price(self.value.clone());
        if let Some(value) = &self.product_name {
            entity.update_product_name(value.clone());
            println!(
                "RUST_CHECKER_REQUIRED_FIX_INTERNAL={}",
                json!({"requested":value.to_json_value(),
                "typedProductName":entity.clone().into_values().get("product_name").map(Value::to_json_value)})
            );
        }
    }
}
async fn no_schema(round: usize, path: &str) {
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(path).unwrap()));
    pool.lock().unwrap().trace(Some(observe));
    let before = count();
    let mut context = service_runtime_from_pool(pool.clone())
        .await
        .unwrap()
        .with_active_root("Tenant", 1);
    context.set_internal_id_generator(AtomicCounterIdGenerator::new(1000));
    for entity in ["Tenant", "PurchaseOrder", "OrderItem"] {
        assert!(context.has_checker(entity));
    }
    let mut value = Q::purchase_orders()
        .comment("Construct an invalid owner before schema initialization")
        .purpose("Verify module-installed Checker without a database schema")
        .new_entity(&context).expect("reserve example identity");
    value.update_internal_note(Value::Null);
    let result = value
        .audit_as("Reject missing required input before any tables exist")
        .save(&context)
        .await;
    let mut facts = rejected(result, before, "required", "/contactPhone");
    let calibration = count();
    let known: i64 = pool
        .lock()
        .unwrap()
        .query_row("SELECT 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(known, 1);
    assert_eq!(
        count() - calibration,
        1,
        "the schema-free connection observer must remain live"
    );
    facts["observerCalibrationStatements"] = json!(1);
    pool.lock().unwrap().trace(None);
    let tables: i64 = pool
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tables, 0);
    facts["tables"] = json!(tables);
    facts["ensureSchemaCalled"] = json!(false);
    facts["checkerTypes"] = json!(["Tenant", "PurchaseOrder", "OrderItem"]);
    emit(round, "install-without-schema", facts);
}
fn external_writer(
    pool: &Arc<Mutex<rusqlite::Connection>>,
    id: u64,
    table: &str,
    column: &str,
) -> Json {
    let before = count();
    let sql = format!("UPDATE {table} SET {column}=NULL, version=version+1 WHERE id=?1");
    let sqlite_id = i64::try_from(id).expect("the test ID must fit SQLite INTEGER");
    let error = pool.lock().unwrap().execute(&sql, [sqlite_id]).unwrap_err();
    let (extended, message) = match error {
        rusqlite::Error::SqliteFailure(error, Some(message)) => {
            assert_eq!(error.code, rusqlite::ErrorCode::ConstraintViolation);
            (error.extended_code, message)
        }
        _ => panic!("the negative control did not reach a SQLite constraint"),
    };
    assert_eq!(extended, 1299);
    assert_eq!(
        message,
        format!("NOT NULL constraint failed: {table}.{column}")
    );
    assert_eq!(count() - before, 1);
    assert_eq!(writes(before, "update", table), 1);
    json!({"id":id,"checkerBypass":"external-sql-writer","statements":1,"updateAttempts":1,
        "constraint":"NOT NULL","column":format!("{table}.{column}"),"sqliteExtendedCode":extended})
}
pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("TEAQL_CHECKER_PIPELINE_DB")?;
    let round: usize = std::env::var("TEAQL_CHECKER_PIPELINE_ROUND")?.parse()?;
    let instant = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    no_schema(round, &format!("{path}.no-schema.sqlite")).await;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(&path)?));
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(instant));
    context.ensure_schema().await?;
    pool.lock().unwrap().trace(Some(observe));
    let value = owner(&context);
    let before_time = E::purchase_order(&value)
        .get_create_time()
        .try_eval()
        .ok()
        .flatten()
        .map(|v| v.as_millis());
    assert_ne!(before_time, Some(instant.timestamp_millis()));
    let before = count();
    let parent = value
        .audit_as("Create a valid generated owner after required time Fix")
        .save(&context)
        .await?;
    let calibration = count() - before;
    assert!(calibration > 0);
    assert_eq!(writes(before, "insert", "purchase_order_data"), 1);
    let stored = load_owner(&context, parent.id()).await;
    for value in [&parent, &stored] {
        assert_eq!(
            E::purchase_order(value)
                .get_create_time()
                .eval()
                .unwrap()
                .as_millis(),
            instant.timestamp_millis()
        );
        assert_eq!(
            E::purchase_order(value)
                .get_update_time()
                .eval()
                .unwrap()
                .as_millis(),
            instant.timestamp_millis()
        );
        assert_eq!(E::purchase_order(value).get_version().eval(), Some(1));
    }
    emit(
        round,
        "required-values-after-fix",
        json!({"id":parent.id(),"version":1,"beforeCreateTimeMillis":before_time,
        "fixedMillis":instant.timestamp_millis(),"providerInserts":1,"calibrationStatements":calibration}),
    );

    let mut value = load_owner(&context, parent.id()).await;
    value.update_contact_phone(Value::Null);
    let before = count();
    let result = value
        .audit_as("Reject explicit null in a required field")
        .save(&context)
        .await;
    emit(
        round,
        "required-null-update",
        rejected(result, before, "required", "/contactPhone"),
    );
    let values = Q::purchase_orders_minimal()
        .with_id_is(parent.id())
        .limit(1)
        .comment("Load an intentionally incomplete owner")
        .purpose("Verify mutation requires full loaded state")
        .execute_for_list(&context)
        .await?;
    let mut value = values.first().unwrap().clone();
    value.update_contact_phone("5550969");
    let before = count();
    let result = value
        .audit_as("Reject mutation of a partial owner")
        .save(&context)
        .await;
    emit(
        round,
        "partial-owner-update",
        rejected(result, before, "invalid_type", "/tenant"),
    );
    let value = child(&context, None);
    let before = count();
    let result = value
        .audit_as("Reject an omitted required parent relation")
        .save(&context)
        .await;
    emit(
        round,
        "required-relation-create",
        rejected(result, before, "required", "/purchaseOrder"),
    );

    let item = child(&context, Some(parent.id()))
        .audit_as("Create a valid child for controlled post-Fix and database tests")
        .save(&context)
        .await?;
    for (name, amount, rule) in [
        ("post-fix-min", "0.00", "min"),
        ("post-fix-max", "1000000.01", "max"),
    ] {
        let mut value = load_child(&context, item.id()).await;
        value.update_product_name("Rejected after customer Fix");
        assert_eq!(
            E::order_item(&value)
                .get_unit_price()
                .eval()
                .unwrap()
                .to_string(),
            "10.00"
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = checker_registry();
        registry.register(TypedEntityChecker::<OrderItem, _>::new(
            OrderItemChecker::new(DeliberatePriceFix {
                value: Value::Decimal(amount.parse()?),
                product_name: None,
                calls: calls.clone(),
            }),
        ));
        context.set_checker_registry(registry);
        let before = count();
        let result = value
            .audit_as("Reject an invalid result from an explicitly installed customer Fix")
            .save(&context)
            .await;
        context.set_checker_registry(checker_registry());
        let mut facts = rejected(result, before, rule, "/unitPrice");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        facts["fixCalls"] = json!(1);
        facts["beforeValue"] = json!("10.00");
        facts["fixedValue"] = json!(amount);
        let after = load_child(&context, item.id()).await;
        assert_eq!(E::order_item(&after).get_version().eval(), Some(1));
        assert_eq!(
            E::order_item(&after)
                .get_unit_price()
                .eval()
                .unwrap()
                .to_string(),
            "10.00"
        );
        assert_eq!(
            E::order_item(&after).get_product_name().eval().as_deref(),
            Some("Pipeline child")
        );
        facts["persistedVersion"] = json!(1);
        facts["persistedValue"] = json!("10.00");
        emit(round, name, facts);
    }
    let mut scalar = external_writer(&pool, parent.id(), "purchase_order_data", "contact_phone");
    let after = load_owner(&context, parent.id()).await;
    assert_eq!(
        E::purchase_order(&after)
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550960")
    );
    assert_eq!(E::purchase_order(&after).get_version().eval(), Some(1));
    scalar["persistedVersion"] = json!(1);
    emit(round, "database-required-scalar-defense", scalar);
    let mut relation = external_writer(&pool, item.id(), "order_item_data", "purchase_order");
    let mut after_item = load_child(&context, item.id()).await;
    assert_eq!(
        E::order_item(&after_item).get_purchase_order_id().eval(),
        Some(parent.id())
    );
    assert_eq!(E::order_item(&after_item).get_version().eval(), Some(1));
    relation["parentId"] = json!(parent.id());
    relation["persistedVersion"] = json!(1);
    emit(round, "database-required-relation-defense", relation);
    let mut after = after;
    after.update_contact_phone(format!("555096{round}"));
    after
        .audit_as("Retry a valid owner update after rejected Checker and external writes")
        .save(&context)
        .await?;
    after_item.update_unit_price(Value::Decimal("20.00".parse()?));
    after_item
        .audit_as("Retry a valid child update after rejected post-Fix values")
        .save(&context)
        .await?;
    let after = load_owner(&context, parent.id()).await;
    let after_item = load_child(&context, item.id()).await;
    assert_eq!(E::purchase_order(&after).get_version().eval(), Some(2));
    assert_eq!(E::order_item(&after_item).get_version().eval(), Some(2));
    assert_eq!(
        E::order_item(&after_item)
            .get_unit_price()
            .eval()
            .unwrap()
            .to_string(),
        "20.00"
    );
    emit(
        round,
        "valid-retry",
        json!({"parentId":parent.id(),"childId":item.id(),"parentVersion":2,"childVersion":2,
        "phone":E::purchase_order(&after).get_contact_phone().eval(),"price":"20.00"}),
    );
    // Check Required against the actual typed customer Fix, not merely the
    // incoming payload or a manually constructed diagnostic.
    let mut value = load_child(&context, item.id()).await;
    value.update_product_name("Pending required Fix");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = checker_registry();
    registry.register(TypedEntityChecker::<OrderItem, _>::new(
        OrderItemChecker::new(DeliberatePriceFix {
            value: Value::Decimal("20.00".parse()?),
            product_name: Some(Value::Null),
            calls: calls.clone(),
        }),
    ));
    context.set_checker_registry(registry);
    let before = count();
    let result = value
        .audit_as("Reject Required after an explicit typed customer Fix")
        .save(&context)
        .await;
    context.set_checker_registry(checker_registry());
    let mut facts = rejected(result, before, "required", "/productName");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let persisted = load_child(&context, item.id()).await;
    assert_eq!(
        E::order_item(&persisted)
            .get_product_name()
            .eval()
            .as_deref(),
        Some("Pipeline child")
    );
    assert_eq!(E::order_item(&persisted).get_version().eval(), Some(2));
    facts["round"] = json!(round);
    facts["case"] = json!("post-fix-required-rejection");
    facts["passed"] = json!(true);
    facts["fixCalls"] = json!(1);
    facts["fixedName"] = Json::Null;
    facts["persistedVersion"] = json!(2);
    println!("RUST_CHECKER_REQUIRED_FIX_CASE={facts}");

    let mut value = child(&context, Some(parent.id()));
    value.update_product_name(Value::Null);
    value.update_unit_price(Value::Decimal("0.00".parse()?));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = checker_registry();
    registry.register(TypedEntityChecker::<OrderItem, _>::new(
        OrderItemChecker::new(DeliberatePriceFix {
            value: Value::Decimal("12.34".parse()?),
            product_name: Some(Value::from("Fixed child")),
            calls: calls.clone(),
        }),
    ));
    context.set_checker_registry(registry);
    let before = count();
    let fixed = value
        .audit_as("Accept Required and range values repaired by explicit customer Fix")
        .save(&context)
        .await?;
    context.set_checker_registry(checker_registry());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(writes(before, "insert", "order_item_data"), 1);
    let fixed = load_child(&context, fixed.id()).await;
    assert_eq!(
        E::order_item(&fixed).get_product_name().eval().as_deref(),
        Some("Fixed child")
    );
    assert_eq!(
        E::order_item(&fixed)
            .get_unit_price()
            .eval()
            .unwrap()
            .to_string(),
        "12.34"
    );
    assert_eq!(E::order_item(&fixed).get_version().eval(), Some(1));
    println!(
        "RUST_CHECKER_REQUIRED_FIX_CASE={}",
        json!({"round":round,"case":"post-fix-required-range-repair","passed":true,
        "parentId":parent.id(),"childId":fixed.id(),"beforeName":null,"beforePrice":"0.00","storedName":"Fixed child","storedPrice":"12.34",
        "storedVersion":1,"fixCalls":1,"providerInserts":1})
    );
    println!(
        "RUST_CHECKER_REQUIRED_FIX_RESULT={}",
        json!({"language":"rust","round":round,"cases":2,"passed":true,
        "actualTypedCustomerFix":true,"allP0P1Complete":false})
    );
    pool.lock().unwrap().trace(None);
    println!(
        "RUST_CHECKER_PIPELINE_RESULT={}",
        json!({"language":"rust","round":round,"cases":10,"passed":true,
        "rangeAfterTypedCustomerFixAccepted":true,"externalWriterControls":2,"publicPublication":false})
    );
    Ok(())
}
