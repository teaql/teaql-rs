//! Modeled bounds must reject through normal generated Save, before SQLite.
use checker_fix_advanced_core::{AuditedSave, E, OrderItem, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{FixedBusinessClock, JsonFieldNamingProfile, RuntimeError, UserContext};

static SQL: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn observe(sql: &str) {
    SQL.lock().unwrap().push(sql.to_owned());
}
fn statements() -> usize {
    SQL.lock().unwrap().len()
}
fn item(context: &UserContext, parent: u64, name: &str, price: &str) -> OrderItem {
    let mut result = Q::order_items()
        .comment("Prepare a modeled range fixture")
        .purpose("Verify generated numeric and character bounds before SQL")
        .new_entity(context)
        .expect("reserve example identity");
    result.update_purchase_order_id(parent);
    result.update_product_name(name);
    result.update_unit_price(Value::Decimal(price.parse().unwrap()));
    result
}
async fn load(context: &UserContext, id: u64) -> Result<OrderItem, RuntimeError> {
    let rows = Q::order_items()
        .with_id_is(id)
        .limit(1)
        .comment("Load the complete bounded item")
        .purpose("Validate updates against original persisted values")
        .execute_for_list(context)
        .await?;
    Ok(rows.first().expect("bounded item exists").clone())
}
async fn rejection<T>(
    future: impl Future<Output = Result<T, RuntimeError>>,
    case: &str,
    rule: &str,
    pointer: &str,
) -> serde_json::Value {
    let before = statements();
    let (kind, checks) = match future.await {
        Err(RuntimeError::Check(results)) => ("check", results),
        Err(_) => ("provider_or_other_error", Vec::new()),
        Ok(_) => ("accepted", Vec::new()),
    };
    let sql = statements() - before;
    let target = checks.iter().any(|c| {
        c.entity_type.as_deref() == Some("OrderItem")
            && c.rule.wire_id() == rule
            && c.location.instance_path() == pointer
    });
    let views:Vec<_>=checks.iter().map(|c|serde_json::json!({"modelPath":c.location.model_path(),
        "camel":c.to_wire(JsonFieldNamingProfile::CamelCase),"snake":c.to_wire(JsonFieldNamingProfile::SnakeCase),
        "pascal":c.to_wire(JsonFieldNamingProfile::PascalCase)})).collect();
    serde_json::json!({"case":case,"expectedRule":rule,"expectedPointer":pointer,"errorKind":kind,
        "saveStatements":sql,"violations":views,"passed":kind=="check"&&target&&sql==0})
}
pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(Mutex::new(match std::env::var("TEAQL_FIX_E_DB") {
        Ok(path) => rusqlite::Connection::open(format!("{path}.checker-ranges.sqlite"))?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    }));
    let time = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(time));
    context.ensure_schema().await?;
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(|event: rusqlite::trace::TraceEvent<'_>| {
                if let rusqlite::trace::TraceEvent::Stmt(statement, sql) = event {
                    if sql.starts_with("--") {
                        observe(sql);
                    } else {
                        observe(&statement.expanded_sql().expect("expanded SQLite trace SQL"));
                    }
                }
            }),
        )
        .unwrap();
    let mut cases = Vec::new();
    let mut parent = Q::purchase_orders()
        .comment("Prepare the modeled bounds owner")
        .purpose("Test actual generated constraints without custom Checkers")
        .new_entity(&context)
        .expect("reserve example identity");
    parent.update_contact_phone("5550100");
    parent.update_internal_note(Value::Null);
    let parent = parent
        .audit_as("Create the range fixture owner")
        .save(&context)
        .await?;
    cases.push(serde_json::json!({"case":"valid-owner","id":parent.id(),"passed":E::purchase_order(&parent).get_version().eval()==Some(1)}));
    let good = item(&context, parent.id(), "A", "0.01")
        .audit_as("Create at inclusive minimum bounds")
        .save(&context)
        .await?;
    cases.push(serde_json::json!({"case":"inclusive-minimum","id":good.id(),"passed":E::order_item(&good).get_unit_price().eval().map(Value::Decimal)==Some(Value::Decimal("0.01".parse()?))&&E::order_item(&good).get_product_name().eval().as_deref()==Some("A")}));
    for (case, name, price, rule, pointer) in [
        (
            "create-below-minimum",
            "Valid item",
            "0.00",
            "min",
            "/unitPrice",
        ),
        (
            "create-above-maximum",
            "Valid item",
            "1000000.01",
            "max",
            "/unitPrice",
        ),
        (
            "create-short-text",
            "",
            "10.00",
            "min_string_length",
            "/productName",
        ),
        (
            "create-long-text",
            &"X".repeat(81),
            "10.00",
            "max_string_length",
            "/productName",
        ),
    ] {
        cases.push(
            rejection(
                item(&context, parent.id(), name, price)
                    .audit_as("Reject modeled bounds before the first SQL")
                    .save(&context),
                case,
                rule,
                pointer,
            )
            .await,
        );
    }
    for (case, price, rule) in [
        ("update-below-minimum", "0.00", "min"),
        ("update-above-maximum", "1000000.01", "max"),
    ] {
        let mut changed = load(&context, good.id()).await?;
        changed.update_unit_price(Value::Decimal(price.parse()?));
        cases.push(
            rejection(
                changed
                    .audit_as("Reject invalid numeric update before SQL")
                    .save(&context),
                case,
                rule,
                "/unitPrice",
            )
            .await,
        );
    }
    let mut changed = load(&context, good.id()).await?;
    changed.update_product_name("X".repeat(81));
    cases.push(
        rejection(
            changed
                .audit_as("Reject invalid character count update before SQL")
                .save(&context),
            "update-long-text",
            "max_string_length",
            "/productName",
        )
        .await,
    );
    let boundary = item(&context, parent.id(), &"🧪".repeat(80), "1000000.00")
        .audit_as("Accept inclusive maximum and Unicode character bounds")
        .save(&context)
        .await?;
    cases.push(serde_json::json!({"case":"inclusive-maximum-unicode","id":boundary.id(),
        "passed":E::order_item(&boundary).get_unit_price().eval().map(Value::Decimal)==Some(Value::Decimal("1000000.00".parse()?))
        &&E::order_item(&boundary).get_product_name().eval().unwrap().chars().count()==80}));
    let persisted = load(&context, good.id()).await?;
    cases.push(serde_json::json!({"case":"unchanged-readback","id":good.id(),"version":E::order_item(&persisted).get_version().eval(),
        "passed":E::order_item(&persisted).get_version().eval()==Some(1)&&E::order_item(&persisted).get_product_name().eval().as_deref()==Some("A")
        &&E::order_item(&persisted).get_unit_price().eval().map(Value::Decimal)==Some(Value::Decimal("0.01".parse()?))}));
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::empty(),
            None::<fn(rusqlite::trace::TraceEvent<'_>)>,
        )
        .unwrap();
    let passed = cases.iter().all(|r| r["passed"] == true);
    println!(
        "RUST_GENERATED_CHECKER_RANGES={}",
        serde_json::json!({"language":"rust","passed":passed,"cases":cases})
    );
    if !passed {
        return Err("generated modeled range observations failed".into());
    }
    Ok(())
}
