use checker_fix_advanced_core::{AuditedSave, LedgerEntity, Q};
use teaql_core::Entity;
use teaql_runtime::UserContext;
#[tokio::main]
async fn main() {
    let context = UserContext::new();
    let mut owner = Q::purchase_orders()
        .comment("Create owner")
        .purpose("A child cannot authorize its owner")
        .new_entity(&context);
    let child = Q::order_items()
        .comment("Create child")
        .purpose("Prove root ownership")
        .new_entity(&context)
        .audit_as("A valid local child reason")
        .into_entity();
    owner.include_pending_mutations_from(&child).unwrap();
    let _ = owner.save(&context).await;
}
