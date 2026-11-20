use checker_fix_advanced_core::Q;
use teaql_runtime::UserContext;
#[tokio::main]
async fn main() {
    let context = UserContext::new();
    let _ = Q::purchase_orders()
        .limit(1)
        .comment("Cannot authorize this query")
        .execute_for_list(&context)
        .await;
}
