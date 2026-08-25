use runtime_example_conformance_service_core::teaql_core::Entity as _;
use runtime_example_conformance_service_core::{
    request_support::AuditedSave as _, service_runtime, ServiceRuntimeConfig, E, Q,
};
use std::path::PathBuf;

const PURPOSE: &str = "Run the retained Rust minimum conformance example";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".local/conformance.sqlite");
    if database.exists() {
        std::fs::remove_file(&database)?;
    }
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: database.to_string_lossy().into_owned(),
    })
    .await?;
    context.ensure_schema().await?;
    println!("PASS ensure_schema (explicit SQLite DDL from Runtime Module)");

    let platform = Q::platforms()
        .comment("Load the system-provided platform root")
        .purpose(PURPOSE)
        .execute_for_one(&context)
        .await?
        .ok_or("ensure_schema did not provide the platform root")?;

    let before_invalid = Q::work_items()
        .comment("Count work items before checker rejection")
        .purpose(PURPOSE)
        .execute_for_list(&context)
        .await?
        .len();
    let request = Q::work_items()
        .comment("Construct an invalid work item")
        .purpose(PURPOSE);
    let mut invalid = request.new_entity(&context);
    invalid.update_platform_id(platform.id());
    let invalid_error = invalid
        .audit_as("Reject a missing required title")
        .save(&context)
        .await
        .expect_err("Checker accepted a missing title");
    require(
        format!("{invalid_error:?}")
            .to_lowercase()
            .contains("title"),
        "Checker error must identify title",
    )?;
    let after_invalid = Q::work_items()
        .comment("Count work items after checker rejection")
        .purpose(PURPOSE)
        .execute_for_list(&context)
        .await?
        .len();
    require(
        before_invalid == after_invalid,
        "Checker failure reached persistence",
    )?;
    println!("PASS Checker (canonical title key, rejected before persistence)");

    let request = Q::work_items()
        .comment("Construct the conformance work item")
        .purpose(PURPOSE);
    let mut work_item = request.new_entity(&context);
    work_item
        .update_title("Initial title")
        .update_platform_id(platform.id());
    let created = work_item
        .audit_as("Create the conformance work item")
        .save(&context)
        .await?;
    require(
        created.id() > 0 && created.version() == 1,
        "Create did not return id/version",
    )?;
    println!(
        "PASS Create (id={}, version={})",
        created.id(),
        created.version()
    );

    let listed = Q::work_items()
        .with_title_is("Initial title")
        .order_by_id_asc()
        .comment("Load the conformance work item")
        .purpose(PURPOSE)
        .execute_for_list(&context)
        .await?;
    require(listed.len() == 1, "Q API must return one typed work item")?;
    println!("PASS Q API (typed SmartList<WorkItem>)");

    let full = listed.into_iter().next().expect("checked list cardinality");
    require(
        E::work_item(&full).get_title().eval().as_deref() == Some("Initial title"),
        "E loaded title mismatch",
    )?;
    require(
        E::work_item(&full)
            .get_description()
            .or_if_null(Some("N/A".to_string()))
            .as_deref()
            == Some("N/A"),
        "E loaded null fallback mismatch",
    )?;
    let minimal = Q::work_items_minimal()
        .select_title()
        .with_title_is("Initial title")
        .comment("Load a deliberately partial work item")
        .purpose(PURPOSE)
        .execute_for_one(&context)
        .await?
        .ok_or("minimal work item missing")?;
    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let not_loaded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        E::work_item(&minimal)
            .get_description()
            .or_if_null(Some("must-not-hide-not-loaded".to_string()))
    }));
    std::panic::set_hook(panic_hook);
    require(not_loaded.is_err(), "E fallback hid NotLoaded")?;
    println!("PASS E API (loaded, null fallback, and not-loaded are distinct)");

    let old_version = full.version();
    let mut updating = full;
    updating.update_title("Updated title");
    let mut updated = updating
        .audit_as("Update the conformance work item")
        .save(&context)
        .await?;
    require(
        updated.version() == old_version + 1,
        "Update must increment version",
    )?;
    println!(
        "PASS Update (version {} -> {})",
        old_version,
        updated.version()
    );

    updated.mark_as_delete();
    updated
        .audit_as("Delete the conformance work item")
        .save(&context)
        .await?;
    let remaining = Q::work_items()
        .with_title_is("Updated title")
        .comment("Verify ordinary queries exclude deleted rows")
        .purpose(PURPOSE)
        .execute_for_list(&context)
        .await?;
    require(
        remaining.is_empty(),
        "Deleted row remains visible to ordinary Q API",
    )?;
    println!("PASS Delete (default Q excludes deleted rows)");
    println!("PASS Rust minimum runtime conformance: 7/7");
    Ok(())
}

fn require(condition: bool, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    if condition {
        Ok(())
    } else {
        Err(message.to_string().into())
    }
}
