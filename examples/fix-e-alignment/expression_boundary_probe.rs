//! Normal generated Q/E and audited mutations against the retained three-object model.
use checker_fix_advanced_core::{AuditedSave, E, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use serde_json::{Value as Json, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::eval_location::NotLoadedDiagnostic;
use teaql_core::object_location::{JsonFieldNamingProfile, ObjectLocation};
use teaql_core::{Entity, Value};
use teaql_runtime::FixedBusinessClock;

static STATEMENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn observe(sql: &str) {
    STATEMENTS.lock().unwrap().push(sql.to_owned());
}
fn count() -> usize {
    STATEMENTS.lock().unwrap().len()
}
fn panic_text(action: impl FnOnce()) -> String {
    let value = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action))
        .expect_err("NotLoaded must fail rather than return a fallback or absence");
    value
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| value.downcast_ref::<&str>().map(|v| (*v).to_owned()))
        .expect("native panic text")
}
fn diagnostic(
    name: &str,
    value: &NotLoadedDiagnostic,
    missing: &str,
    index: usize,
    attempted: &str,
    native_chain: &str,
) -> Json {
    let wire = value.to_wire();
    assert_eq!(wire.instance_path, missing);
    assert_eq!(wire.attempted_instance_path, attempted);
    assert_eq!(wire.break_point_index, index);
    assert_eq!(
        wire.expression_with_broken_point.as_deref(),
        Some(native_chain)
    );
    assert_eq!(native_chain.matches("<broken>").count(), 1);
    let human = value.to_string();
    assert!(human.contains("TeaQLNotLoadedError: access_path="));
    assert!(human.contains(&format!("break_point={}", wire.break_point)));
    assert!(human.contains(native_chain));
    assert!(!human.contains("\"instancePath\""));
    let json = json!({"name":name,"wire":wire,"human":human,"passed":true});
    let text = json.to_string();
    assert!(!text.contains("PRIVATE-E-ARGUMENT"));
    json
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("TEAQL_EXPRESSION_DB")?;
    let round: usize = std::env::var("TEAQL_EXPRESSION_ROUND")?.parse()?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(path)?));
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
            .single()
            .unwrap(),
    ));
    context.ensure_schema().await?;
    pool.lock().unwrap().trace(Some(observe));
    let mut order = Q::purchase_orders()
        .comment("Prepare expression boundary owner")
        .purpose("Verify generated E without implicit loads")
        .new_entity(&context);
    order.update_contact_phone("5550390");
    order.update_internal_note(Value::Null);
    let saved = order
        .audit_as("Create expression boundary owner")
        .save(&context)
        .await?;
    let id = saved.id();
    let sparse = Q::purchase_orders_minimal()
        .with_id_is(id)
        .limit(1)
        .comment("Load identity only")
        .purpose("Observe a genuinely missing relation")
        .execute_for_list(&context)
        .await?;
    let sparse = sparse.first().unwrap();
    let late = Q::purchase_orders_minimal()
        .with_id_is(id)
        .select_tenant_with(
            Q::tenants_minimal()
                .limit(1)
                .comment("Load only target identity"),
        )
        .limit(1)
        .comment("Load relation but not its name")
        .purpose("Distinguish missing leaf from missing relation")
        .execute_for_list(&context)
        .await?;
    let late = late.first().unwrap();
    let empty = Q::purchase_orders()
        .with_id_is(id)
        .select_order_item_list_with(
            Q::order_items_minimal()
                .limit(10)
                .comment("Load initially empty child list"),
        )
        .limit(1)
        .comment("Load nullable field and empty relation")
        .purpose("Verify ordinary absence is not NotLoaded")
        .execute_for_list(&context)
        .await?;
    let empty = empty.first().unwrap();
    assert!(
        count() > 0,
        "physical observer must be calibrated by real provider work"
    );
    let before = count();
    let mut cases = Vec::new();
    let dirty_before = sparse.dirty_fields();
    let state_before = sparse.loaded_state_snapshot();
    let early = E::purchase_order(sparse)
        .get_tenant()
        .get_name()
        .try_eval()
        .unwrap_err();
    cases.push(diagnostic(
        "missing-relation",
        &early,
        "/tenant",
        0,
        "/tenant/name",
        "E::purchase_order(<root>).get_tenant()<broken>.get_name().try_eval()",
    ));
    assert_eq!(
        E::purchase_order(late)
            .get_tenant()
            .try_eval()?
            .map(|entity| entity.id()),
        Some(1)
    );
    let leaf = E::purchase_order(late)
        .get_tenant()
        .get_name()
        .try_eval()
        .unwrap_err();
    cases.push(diagnostic(
        "missing-late-leaf",
        &leaf,
        "/tenant/name",
        1,
        "/tenant/name",
        "E::purchase_order(<root>).get_tenant().get_name()<broken>.try_eval()",
    ));
    assert!(
        E::purchase_order(empty)
            .get_internal_note()
            .try_eval()?
            .is_none()
    );
    assert_eq!(
        E::purchase_order(empty)
            .get_order_item_list()
            .size()
            .try_eval()?,
        Some(0)
    );
    assert!(
        E::purchase_order(empty)
            .get_order_item_list()
            .first()
            .get_product_name()
            .try_eval()?
            .is_none()
    );
    assert!(
        E::purchase_order(empty)
            .get_order_item_list()
            .get(2)
            .get_product_name()
            .try_eval()?
            .is_none()
    );
    cases.push(json!({"name":"loaded-null-empty-and-out-of-range","null":true,"emptyCount":0,"firstAbsent":true,"indexAbsent":true,"passed":true}));
    let scalar = E::purchase_order(sparse).get_contact_phone();
    let original = scalar.clone().try_eval().unwrap_err().to_wire();
    let panic = panic_text(|| {
        let _ = scalar.clone().eval();
    });
    assert!(panic.contains("get_contact_phone()<broken>.eval()"));
    assert!(panic.contains("access_path=contact_phone break_point=contact_phone"));
    assert!(!panic.contains("\"instancePath\""));
    let eager = panic_text(|| {
        let _ = scalar.clone().or_if_null("PRIVATE-E-ARGUMENT".to_owned());
    });
    let lazy_calls = AtomicUsize::new(0);
    let lazy = panic_text(|| {
        let _ = scalar.clone().or_else_if_null(|| {
            lazy_calls.fetch_add(1, Ordering::SeqCst);
            "PRIVATE-E-ARGUMENT".to_owned()
        });
    });
    let defaulted = panic_text(|| {
        let _ = scalar.clone().or_default_if_null();
    });
    assert_eq!(lazy_calls.load(Ordering::SeqCst), 0);
    for (text, terminal) in [
        (&eager, "or_if_null(<fallback>)"),
        (&lazy, "or_else_if_null(<fallback>)"),
        (&defaulted, "or_default_if_null()"),
    ] {
        assert!(text.contains(&format!("get_contact_phone()<broken>.{terminal}")));
        assert!(!text.contains("PRIVATE-E-ARGUMENT"));
        assert_eq!(text.matches("<broken>").count(), 1);
    }
    assert_eq!(
        scalar.try_eval().unwrap_err().to_wire(),
        original,
        "evaluating cloned terminals cannot mutate their sibling path"
    );
    cases.push(json!({"name":"fallbacks-and-cloned-terminals","wire":original,"panic":panic,
        "eager":eager,"lazy":lazy,"defaulted":defaulted,"lazyCalls":0,"siblingUnchanged":true,"passed":true}));
    let fallback_calls = AtomicUsize::new(0);
    assert_eq!(
        E::purchase_order(empty)
            .get_internal_note()
            .or_else_if_null(|| {
                fallback_calls.fetch_add(1, Ordering::SeqCst);
                Some("loaded-null-default".to_owned())
            }),
        Some("loaded-null-default".to_owned())
    );
    assert_eq!(fallback_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        E::purchase_order(empty)
            .get_internal_note()
            .or_default_if_null(),
        None
    );
    assert!(
        E::purchase_order(empty)
            .get_internal_note()
            .try_eval()?
            .is_none(),
        "fallback cannot alter the loaded null"
    );
    cases.push(
        json!({"name":"loaded-null-fallback","calls":1,"originalNullUnchanged":true,"passed":true}),
    );
    assert_eq!(sparse.dirty_fields(), dirty_before);
    match (state_before, sparse.loaded_state_snapshot()) {
        (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, &after)),
        _ => panic!("generated projection must retain an indexed loaded snapshot"),
    }
    assert_eq!(
        count(),
        before,
        "E must not query, mutate or open a provider transaction"
    );
    cases.push(json!({"name":"pure-evaluation","providerStatements":0,"observerCalibrated":true,"loadedSnapshotUnchanged":true,"dirtyStateUnchanged":true,"passed":true}));
    // Add a real child only after preserving and exercising the loaded-empty snapshot.
    let mut item = Q::order_items()
        .comment("Prepare partial child projection")
        .purpose("Observe canonical list indices through generated E")
        .new_entity(&context);
    item.update_purchase_order_id(id);
    item.update_product_name("Private expression item");
    item.update_unit_price(Value::Decimal("1".parse()?));
    let child = item
        .audit_as("Create expression boundary child")
        .save(&context)
        .await?;
    let list = Q::purchase_orders_minimal()
        .with_id_is(id)
        .select_order_item_list_with(
            Q::order_items_minimal()
                .limit(10)
                .comment("Load only child identity"),
        )
        .limit(1)
        .comment("Load a real partial child")
        .purpose("Mark missing fields after the true list index")
        .execute_for_list(&context)
        .await?;
    let list = list.first().unwrap();
    let before = count();
    let child_error = E::purchase_order(list)
        .get_order_item_list()
        .first()
        .get_product_name()
        .try_eval()
        .unwrap_err();
    cases.push(diagnostic("missing-child-leaf", &child_error,"/orderItemList/0/productName",2,"/orderItemList/0/productName",
        "E::purchase_order(<root>).get_order_item_list().first().get_product_name()<broken>.try_eval()"));
    let profiles: Vec<_> = [
        (
            JsonFieldNamingProfile::CamelCase,
            "/orderItemList/0/productName",
        ),
        (
            JsonFieldNamingProfile::SnakeCase,
            "/order_item_list/0/product_name",
        ),
        (
            JsonFieldNamingProfile::PascalCase,
            "/OrderItemList/0/ProductName",
        ),
    ]
    .into_iter()
    .map(|(profile, pointer)| {
        let wire = child_error.to_wire_with(profile);
        assert_eq!(wire.instance_path, pointer);
        assert_eq!(wire.location, child_error.to_wire().location);
        assert_eq!(wire.break_point_index, 2);
        wire
    })
    .collect();
    cases.push(json!({"name":"generated-three-json-profiles","profiles":profiles,"passed":true}));
    assert_eq!(
        E::purchase_order(empty)
            .get_order_item_list()
            .size()
            .try_eval()?,
        Some(0),
        "new queries cannot mutate an older loaded-empty snapshot"
    );
    assert_eq!(
        E::purchase_order(list)
            .get_order_item_list()
            .size()
            .try_eval()?,
        Some(1)
    );
    assert_eq!(count(), before);
    cases.push(json!({"name":"independent-projection-snapshots","oldSize":0,"newSize":1,"providerStatements":0,"passed":true}));
    // Literal punctuation is a native structured-location subvector, not an invented KSML field.
    let punctuation = NotLoadedDiagnostic::new(
        ObjectLocation::root()
            .member("contact.phone/~alias")
            .element(2)
            .member("user_url"),
        "user_url",
    );
    let p = punctuation.to_wire();
    assert_eq!(p.instance_path, "/contact.phone~1~0alias/2/userUrl");
    assert_eq!(p.location.len(), 3);
    cases.push(json!({"name":"native-literal-location","wire":p,"scope":"native-location-only","passed":true}));
    for row in &cases {
        println!("RUST_E_BOUNDARY_CASE={row}");
    }
    println!(
        "RUST_E_BOUNDARY_RESULT={}",
        json!({"language":"rust","round":round,"passed":true,
        "normalGeneratedQAndE":true,"cases":cases,"orderID":id,"childID":child.id(),"version":E::purchase_order(&saved).get_version().try_eval()?,
        "fullDiagnosticContractAccepted":false,"allP0P1Complete":false})
    );
    Ok(())
}
