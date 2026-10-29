#[path = "support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use teaql_core::Value;
use teaql_data_service::{SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy};

#[test]
fn scalar_target_needs_only_its_text_and_final_result_storage() {
    let (_, calls, _, _) = measured(|| {
        let mut redactions = SqlIntentRedactions::default();
        redactions.capture_target_id(&Value::U64(42));
        redactions
    });
    assert_eq!(calls, 2);
}

#[test]
fn masked_scalar_binding_has_no_traversal_buffers() {
    let source = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![SqlParameterLogPolicy::Masked],
        ..Default::default()
    };
    let (_, calls, _, _) =
        measured(|| SqlIntentRedactions::from_bindings(&source, &[Value::U64(42)], ""));
    assert_eq!(calls, 2);
}
