//! Real generated audited saves, without direct Checker calls or business SQL writes.
use checker_fix_advanced_core::{AuditedSave, E, PurchaseOrder, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    FixedBusinessClock, JsonFieldNamingProfile, LedgerEntity, RuntimeError, UserContext,
};

static SQL: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn observe(sql: &str) {
    SQL.lock().unwrap().push(sql.to_owned());
}
fn statements() -> usize {
    SQL.lock().unwrap().len()
}
fn parent(context: &UserContext, valid: bool) -> PurchaseOrder {
    let mut result = Q::purchase_orders()
        .comment("Prepare one Checker identity fixture")
        .purpose("Verify typed violations through ordinary generated saves")
        .new_entity(context).expect("reserve example identity");
    if valid {
        result.update_contact_phone("5550100");
    }
    result.update_internal_note(Value::Null);
    result
}
async fn rejection<T>(
    future: impl Future<Output = Result<T, RuntimeError>>,
    case: &str,
    entity: &str,
    rule: &str,
    pointer: &str,
) -> serde_json::Value {
    // Factories may allocate IDs. Count only the actual save boundary under test.
    let before = statements();
    let (kind, violations) = match future.await {
        Err(RuntimeError::Check(results)) => ("check", results),
        Err(_) => ("other_error", Vec::new()),
        Ok(_) => ("accepted", Vec::new()),
    };
    let sql = statements() - before;
    let typed = violations.iter().all(|v| {
        v.entity_type
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
    });
    let target = violations.iter().any(|v| {
        v.entity_type.as_deref() == Some(entity)
            && v.rule.wire_id() == rule
            && v.location.instance_path() == pointer
    });
    let views: Vec<_> = violations
        .iter()
        .map(|v| {
            serde_json::json!({
                "modelPath": v.location.model_path(), "nativePath":v.location.native_path(),
                "camel":v.to_wire(JsonFieldNamingProfile::CamelCase),
                "snake":v.to_wire(JsonFieldNamingProfile::SnakeCase),
                "pascal":v.to_wire(JsonFieldNamingProfile::PascalCase),
            })
        })
        .collect();
    serde_json::json!({"case":case,"expectedEntity":entity,"expectedRule":rule,"expectedPointer":pointer,
        "errorKind":kind,"saveStatements":sql,"violations":views,"passed":kind=="check"&&typed&&target&&sql==0})
}

pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(Mutex::new(match std::env::var("TEAQL_FIX_E_DB") {
        Ok(path) => rusqlite::Connection::open(format!("{path}.checker-types.sqlite"))?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    }));
    let first = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(first));
    context.ensure_schema().await?;
    pool.lock().unwrap().trace(Some(observe));
    let mut cases = Vec::new();
    let value = parent(&context, true);
    let before = statements();
    let good = value
        .audit_as("Create one valid typed Checker control")
        .save(&context)
        .await?;
    let inserts = SQL.lock().unwrap()[before..]
        .iter()
        .filter(|s| s.to_lowercase().contains("insert into") && s.contains("purchase_order_data"))
        .count();
    cases.push(serde_json::json!({"case":"valid-create","id":good.id(),"version":E::purchase_order(&good).get_version().eval(),
        "providerInserts":inserts,"passed":inserts==1&&E::purchase_order(&good).get_create_time().eval().unwrap().as_millis()==first.timestamp_millis()
        &&E::purchase_order(&good).get_update_time().eval().unwrap().as_millis()==first.timestamp_millis()}));
    cases.push(
        rejection(
            parent(&context, false)
                .audit_as("Reject a missing required scalar")
                .save(&context),
            "required-scalar-create",
            "PurchaseOrder",
            "required",
            "/contactPhone",
        )
        .await,
    );
    let mut missing_parent = Q::order_items()
        .comment("Prepare an item without its required parent")
        .purpose("Verify a relation violation keeps its entity identity")
        .new_entity(&context).expect("reserve example identity");
    missing_parent.update_product_name("Public item");
    missing_parent.update_unit_price(teaql_core::Value::Decimal("10.00".parse()?));
    cases.push(
        rejection(
            missing_parent
                .audit_as("Reject a missing required parent relation")
                .save(&context),
            "required-relation-create",
            "OrderItem",
            "required",
            "/purchaseOrder",
        )
        .await,
    );
    let partial = Q::purchase_orders_minimal()
        .with_id_is(good.id())
        .limit(1)
        .comment("Load a deliberately incomplete owner")
        .purpose("Verify partial mutations fail with typed diagnostics")
        .execute_for_list(&context)
        .await?;
    let mut partial = partial.first().ok_or("partial owner missing")?.clone();
    partial.update_contact_phone("5550199");
    cases.push(
        rejection(
            partial
                .audit_as("Reject a mutation of an incomplete owner")
                .save(&context),
            "partial-owner-update",
            "PurchaseOrder",
            "invalid_type",
            "/tenant",
        )
        .await,
    );
    let mut nested = parent(&context, true);
    let mut child = Q::order_items()
        .comment("Prepare a child with a missing required product name")
        .purpose("Verify child identity and indexed canonical paths survive graph validation")
        .new_entity(&context).expect("reserve example identity");
    child.update_purchase_order_id(nested.id());
    child.update_unit_price(teaql_core::Value::Decimal("10.00".parse()?));
    nested.include_pending_mutations_from(&child)?;
    cases.push(
        rejection(
            nested
                .audit_as("Reject the whole graph before SQL")
                .save(&context),
            "nested-required-child",
            "OrderItem",
            "required",
            "/orderItemList/0/productName",
        )
        .await,
    );
    context.set_locale_code("zh-CN")?;
    cases.push(
        rejection(
            parent(&context, false)
                .audit_as("Reject missing input with a localized message")
                .save(&context),
            "localized-required-scalar",
            "PurchaseOrder",
            "required",
            "/contactPhone",
        )
        .await,
    );
    let rows = Q::purchase_orders()
        .with_id_is(good.id())
        .limit(1)
        .comment("Reload the valid control after rejected saves")
        .purpose("Prove rejected mutations did not change business values or optimistic versions")
        .execute_for_list(&context)
        .await?;
    let persisted = rows.first().ok_or("valid control missing")?;
    cases.push(serde_json::json!({"case":"valid-readback","id":good.id(),"version":E::purchase_order(persisted).get_version().eval(),
        "passed":E::purchase_order(persisted).get_contact_phone().eval().as_deref()==Some("5550100")&&E::purchase_order(persisted).get_version().eval()==Some(1)}));
    pool.lock().unwrap().trace(None);
    let passed = cases.iter().all(|c| c["passed"] == true);
    println!(
        "RUST_GENERATED_CHECKER_TYPES={}",
        serde_json::json!({"language":"rust","passed":passed,"cases":cases})
    );
    if !passed {
        return Err("generated Checker identity observations failed".into());
    }
    Ok(())
}
