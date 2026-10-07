use school_management_service_core::{E, School};
use teaql_core::Entity;
use teaql_runtime::UserContext;

// Keep the wide typed object out of the enclosing async flow's stack frame.
pub fn verify(
    context: &UserContext,
    sparse_json: &teaql_core::serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut json = sparse_json.clone();
    json["name"] = "".into();
    let row = context.decode_json_entity::<School>(&json)?;
    assert_eq!(E::school(&row).get_name().eval().as_deref(), Some(""));
    assert!(row.is_field_loaded("name"));
    assert!(!row.is_field_loaded("address"));
    assert!(row.dirty_fields().is_none());
    println!("PASS generated Rust empty native value stays loaded without widening absent fields");
    Ok(())
}
