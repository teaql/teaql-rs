//! Build the selection with generated Q; runtime schema metadata adds no generated fields.
use school_management_service_core::{E, Q, School, ServiceRuntimeExecutor};
use std::sync::Arc;
use teaql_core::{DataType, Entity, dynamic_properties::DynamicPropertyDefinitions};
use teaql_runtime::{PurposedSelectQuery, UserContext};

pub async fn verify(
    context: &UserContext,
    first: &str,
    second: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let selection: teaql_core::request::QuerySelection = Q::schools_minimal()
        .with_name_in([first, second])
        .select_name()
        .order_by_id_asc()
        .limit(2)
        .comment("what: load generated School with readonly schema")
        .into();
    let definitions = DynamicPropertyDefinitions::new([
        ("_missing_count".into(), DataType::I64),
        ("_count".into(), DataType::I64),
    ])?;
    let query = selection
        .into_query()
        .with_dynamic_property_definitions(definitions.clone())
        .dynamic_property_raw("_count", "CASE WHEN id % 2 = 0 THEN NULL ELSE 0 END");
    let request = PurposedSelectQuery::new(
        query,
        "why: keep schema sharing distinct from value presence",
    );
    let rows = context
        .entity_data_service::<ServiceRuntimeExecutor>("School")?
        .fetch_enhanced_entities::<School>(&request)
        .await?
        .data;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        E::school(&rows[0]).get_name().eval().as_deref(),
        Some(first)
    );
    let state = rows[0].loaded_state_snapshot().unwrap();
    for row in &rows {
        assert!(Arc::ptr_eq(&state, &row.loaded_state_snapshot().unwrap()));
        assert!(Arc::ptr_eq(
            state.dynamic_property_definitions().unwrap(),
            &definitions
        ));
        assert_eq!(
            row.dynamic_property_type("_missing_count"),
            Some(DataType::I64)
        );
        assert!(row.dynamic_property("_missing_count").is_none());
        assert!(!row.has_dynamic_property("_missing_count"));
        assert!(!state.is_loaded("_missing_count"));
        assert!(row.dirty_fields().is_none());
        assert_eq!(row.dynamic_property_type("_count"), Some(DataType::I64));
        assert!(row.has_dynamic_property("_count"));
        assert_eq!(
            row.dynamic_property("_count"),
            if row.id() % 2 == 0 {
                None
            } else {
                Some(&teaql_core::Value::I64(0))
            }
        );
    }
    println!(
        "PASS generated Rust readonly property metadata shares schema without installing values or fixed slots"
    );
    Ok(())
}
