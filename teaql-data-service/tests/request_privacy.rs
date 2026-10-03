use std::sync::Arc;
use teaql_core::{InsertCommand, Value};
use teaql_data_service::{
    MutationCommand, MutationRequest, SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy,
};

#[test]
fn mutation_privacy_is_request_owned_and_debug_does_not_expose_it() {
    let policy = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![SqlParameterLogPolicy::Masked],
        ..Default::default()
    };
    let mut source = Arc::new(SqlIntentRedactions::from_bindings(
        &policy,
        &[Value::from("PRIVATE-ORIGINAL")],
        "",
    ));
    let request = MutationRequest::new(
        MutationCommand::Insert(InsertCommand::new("CustomerOrder")),
        "save graph",
    )
    .unwrap()
    .with_diagnostic_redactions(Some(source.clone()));
    Arc::make_mut(&mut source).extend(&SqlIntentRedactions::from_bindings(
        &policy,
        &[Value::from("LATER-VALUE")],
        "",
    ));
    let mut values = Vec::new();
    request
        .diagnostic_redactions()
        .unwrap()
        .extend_secrets(false, &mut values);
    assert_eq!(values, ["PRIVATE-ORIGINAL"]);
    assert!(!format!("{request:?}").contains("PRIVATE-ORIGINAL"));
    let next = MutationRequest::new(request.command.clone(), "independent request").unwrap();
    assert!(next.diagnostic_redactions().is_none());
    let mut debug_values = Vec::new();
    request
        .diagnostic_redactions()
        .unwrap()
        .extend_secrets(true, &mut debug_values);
    assert!(debug_values.is_empty());
}
