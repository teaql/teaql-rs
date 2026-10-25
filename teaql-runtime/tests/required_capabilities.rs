use teaql_runtime::{RuntimeError, RuntimeModule, UserContext};

#[test]
fn base_has_no_requirements_but_passive_blob_manifest_fails_readiness() {
    UserContext::new().validate_capabilities().unwrap();
    let manifest = RuntimeModule::new()
        .require_capability("blob.v1")
        .require_capability("blob.v1");
    assert_eq!(manifest.required_capabilities().len(), 1);
    let context = manifest.into_context();
    assert!(matches!(
        context.validate_capabilities(),
        Err(RuntimeError::Capability {
            code: "BLOB_CAPABILITY_MISSING",
            ..
        })
    ));
}

#[test]
fn requirements_union_does_not_install_or_overwrite_shared_resources() {
    let mut context = UserContext::new();
    context.insert_resource(237_u64);
    RuntimeModule::new()
        .require_capability("secure_document.v1")
        .apply_to(&mut context);
    RuntimeModule::new()
        .require_capability("blob.v1")
        .apply_to(&mut context);
    assert_eq!(
        context.required_capabilities(),
        ["blob.v1".into(), "secure_document.v1".into()].into()
    );
    assert_eq!(context.get_resource::<u64>(), Some(&237));
    assert!(!context.has_round_trip_reference_provider());
}
