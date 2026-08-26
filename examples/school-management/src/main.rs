use school_management_service_core::{service_runtime, ServiceRuntimeConfig, Q};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::temp_dir().join(format!(
        "teaql-school-rust-{}.sqlite",
        std::process::id()
    ));
    if database.exists() {
        std::fs::remove_file(&database)?;
    }
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: database.to_string_lossy().into_owned(),
    })
    .await?;
    context.ensure_schema().await?;
    context.ensure_schema().await?;

    let platforms = Q::platforms()
        .comment("verify seeded Platform root")
        .purpose("local runtime verification")
        .execute_for_list(&context)
        .await?;
    let constants = Q::school_types()
        .order_by_id_asc()
        .comment("verify seeded SchoolType constants")
        .purpose("local runtime verification")
        .execute_for_list(&context)
        .await?;
    assert_eq!(platforms.len(), 1);
    assert_eq!(platforms[0].id(), 1);
    assert_eq!(constants.len(), 2);
    assert_eq!((constants[0].id(), constants[0].version()), (1001, 1));
    assert_eq!((constants[1].id(), constants[1].version()), (1002, 1));
    println!("PASS Rust School bootstrap with local runtime");
    std::fs::remove_file(database)?;
    Ok(())
}
