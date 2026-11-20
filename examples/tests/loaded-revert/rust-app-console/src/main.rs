use order_line_restoration_core::{
    service_runtime_from_pool, AuditedSave, ShippingLine, E, Q,
};
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use teaql_core::Entity;
use teaql_runtime::{RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext};

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;
static STATEMENTS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Default)]
struct AuditCount(Arc<AtomicUsize>);

impl SafeAuditEventSink for AuditCount {
    fn on_safe_event(&self, _: &UserContext, _: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn line_with_order(context: &UserContext, id: u64) -> AppResult<ShippingLine> {
    let line = Q::shipping_lines()
        .with_id_is(id)
        .select_shipping_order_with(Q::shipping_orders_minimal().select_order_number())
        .limit(1)
        .comment("Read the order line and parent order number")
        .purpose("Open the dispatch line editor with all editable values")
        .execute_for_one(context)
        .await?
        .ok_or("The order line was not found")?;
    Ok(line)
}

async fn main() -> AppResult<()> {
    let database = std::env::var("TEAQL_ORDER_EDIT_DATABASE")?;
    let round = std::env::var("TEAQL_SHOWCASE_ROUND")?;
    let connection = Arc::new(Mutex::new(rusqlite::Connection::open(database)?));
    let mut context = service_runtime_from_pool(connection.clone()).await?;
    context.ensure_schema().await?;

    let desk = Q::dispatch_desks_minimal()
        .with_id_is(1)
        .limit(1)
        .comment("Read the dispatch desk installed by the model")
        .purpose("Assign the shipping order to the dispatch team")
        .execute_for_one(&context)
        .await?
        .ok_or("Dispatch desk is missing")?;
    let order_number = format!("DISPATCH-104-{round}");
    let mut order = Q::shipping_orders()
        .comment("Prepare the shipping order")
        .purpose("Create an order whose line can be edited")
        .new_entity(&context)?;
    order
        .update_dispatch_desk_id(desk.id())
        .update_order_number(order_number.as_str());
    let order = order
        .audit_as("Open the dispatch order")
        .save(&context)
        .await?;
    let mut line = Q::shipping_lines()
        .comment("Prepare the order line")
        .purpose("Record the original carton quantity without a delivery note")
        .new_entity(&context)?;
    line.update_shipping_order_id(order.id())
        .update_item_code("TEA-CARTON")
        .update_quantity(2_i64)
        .update_delivery_note(None::<String>);
    let line = line
        .audit_as("Record the initial carton quantity")
        .save(&context)
        .await?;

    connection.lock().unwrap().trace_v2(
        TraceEventCodes::SQLITE_TRACE_STMT,
        Some(|event: TraceEvent<'_>| {
            if matches!(event, TraceEvent::Stmt(_, _)) {
                STATEMENTS.fetch_add(1, Ordering::SeqCst);
            }
        }),
    )?;
    let audit = AuditCount::default();
    context.set_custom_event_sink(audit.clone());
    let mut edited = line_with_order(&context, line.id()).await?;
    edited
        .update_quantity(9_i64)
        .update_delivery_note("Unsubmitted delivery instruction")
        .update_quantity(2_i64)
        .update_delivery_note(None::<String>);
    let before = STATEMENTS.load(Ordering::SeqCst);
    let restored = edited
        .audit_as("Close the editor after undoing both draft changes")
        .save(&context)
        .await?;
    assert_eq!(STATEMENTS.load(Ordering::SeqCst), before);
    assert_eq!(audit.0.load(Ordering::SeqCst), 0);
    let expression = E::shipping_line(&restored);
    assert_eq!(expression.clone().get_quantity().eval(), Some(2));
    assert_eq!(expression.clone().get_version().eval(), Some(1));
    assert!(restored.is_field_loaded("delivery_note"));
    assert_eq!(expression.clone().get_delivery_note().try_eval()?, None);
    assert_eq!(
        expression.get_shipping_order().eval().unwrap().order_number(),
        order_number
    );

    let mut sparse = Q::shipping_lines_minimal()
        .with_id_is(line.id())
        .select_item_code()
        .limit(1)
        .comment("Read a list-row projection without the delivery note")
        .purpose("Keep omitted fields unavailable after undo")
        .execute_for_one(&context)
        .await?
        .ok_or("The projected order line was not found")?;
    sparse.update_item_code("DRAFT").update_item_code("TEA-CARTON");
    let before = STATEMENTS.load(Ordering::SeqCst);
    let mut sparse = sparse.audit_as("Undo the edit from the list view").save(&context).await?;
    assert_eq!(STATEMENTS.load(Ordering::SeqCst), before);
    assert!(E::shipping_line(&sparse).get_delivery_note().try_eval().is_err());
    sparse.update_item_code("WRONG-CARTON");
    assert!(matches!(
        sparse.audit_as("Reject a real edit from an incomplete view").save(&context).await,
        Err(RuntimeError::Check(_))
    ));
    assert_eq!(STATEMENTS.load(Ordering::SeqCst), before);
    assert_eq!(audit.0.load(Ordering::SeqCst), 0);

    let mut complete = line_with_order(&context, line.id()).await?;
    complete
        .update_quantity(6_i64)
        .update_quantity(2_i64)
        .update_delivery_note("Leave at receiving");
    complete.audit_as("Save the confirmed receiving instruction").save(&context).await?;
    assert_eq!(audit.0.load(Ordering::SeqCst), 1);
    let updated = line_with_order(&context, line.id()).await?;
    let expression = E::shipping_line(&updated);
    assert_eq!(expression.clone().get_version().eval(), Some(2));
    assert_eq!(expression.clone().get_quantity().eval(), Some(2));
    assert_eq!(expression.clone().get_item_code().eval().as_deref(), Some("TEA-CARTON"));
    assert_eq!(expression.get_delivery_note().eval(), Some(Some("Leave at receiving".into())));

    let result = serde_json::json!({
        "round": round,
        "orderId": order.id(),
        "orderNumber": order_number,
        "lineId": line.id(),
        "restoredQuantity": 2,
        "persistedVersion": 2,
        "restoredNull": true,
        "omittedNoteStillNotLoaded": true,
        "providerStatementsOnUndo": 0,
        "auditEvents": 1
    });
    println!("ORDER_LINE_RESTORATION={result}");
    Ok(())
}
