use std::sync::Arc;
use std::time::Duration;

use teaql_runtime::{
    ContextBoundReferenceRuntime, DeploymentProfile, ExternalEntityReference,
    ReferenceDocumentScope, ReferenceIdentity, ReferenceKey, ReferenceMode,
    RoundTripReferenceError, StaticReferenceKeyProvider, TrustedReferencePrincipal, UserContext,
};

fn profile() -> Result<DeploymentProfile, RoundTripReferenceError> {
    match std::env::var("TEAQL_REFERENCE_PROFILE").as_deref() {
        Ok("production") => Ok(DeploymentProfile::Production),
        Ok("development") => Ok(DeploymentProfile::Development),
        Ok("test") | Err(std::env::VarError::NotPresent) => Ok(DeploymentProfile::Test),
        _ => Err(RoundTripReferenceError::configuration()),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let keys = Arc::new(StaticReferenceKeyProvider::new(
        "2026-09",
        [ReferenceKey::new("2026-09", [0x27; 32])?],
    )?);
    let authorization = Arc::new(
        |principal: &TrustedReferencePrincipal,
         scope: &ReferenceDocumentScope,
         identity: &ReferenceIdentity| {
            if principal.subject == "alice"
                && scope.aggregate_type == "Order"
                && scope.aggregate_id == 1001
                && identity.entity_type == "OrderItem"
                && identity.id == 2001
            {
                Ok(())
            } else {
                Err(RoundTripReferenceError::authorization_required())
            }
        },
    );
    let runtime = Arc::new(ContextBoundReferenceRuntime::from_process_environment(
        profile()?,
        "order-service",
        "reference-example",
        keys,
        authorization,
    )?);
    let alice = TrustedReferencePrincipal::new("oidc", "alice", "Platform", 1)?;
    let bob = TrustedReferencePrincipal::new("oidc", "bob", "Platform", 1)?;
    let document = ReferenceDocumentScope::new("order-edit-1001", "edit-order", "Order", 1001, 7)?;
    let identity = ReferenceIdentity::new("OrderItem", 2001, 4)?;

    let context = UserContext::new()
        .with_round_trip_reference_runtime(runtime.clone())
        .with_trusted_reference_principal(alice);
    let reference = context.reference_for(identity.clone(), &document, Duration::from_secs(900))?;
    let wire = context.serialize_reference(&reference)?;
    let returned = context.deserialize_reference(&wire)?;
    let resolved = context.resolve_reference(&returned, "OrderItem", &document)?;
    assert_eq!(resolved.identity, identity);

    let unauthorized = UserContext::new()
        .with_round_trip_reference_runtime(runtime)
        .with_trusted_reference_principal(bob)
        .resolve_reference(&returned, "OrderItem", &document)
        .expect_err("current authorization must reject another actor");
    let expected_unauthorized_code = match context.reference_mode()? {
        ReferenceMode::Governed => "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH",
        ReferenceMode::Raw => "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED",
    };
    assert_eq!(unauthorized.code(), expected_unauthorized_code);

    match (&reference, context.reference_mode()?) {
        (ExternalEntityReference::Governed(token), ReferenceMode::Governed) => {
            assert!(token.starts_with("tqr1.2026-09."));
            assert!(!token.contains("2001"));
            println!("PASS Rust round-trip reference governed mode");
        }
        (ExternalEntityReference::Raw { id, version }, ReferenceMode::Raw) => {
            assert_eq!((*id, *version), (2001, 4));
            let notice = context
                .reference_startup_notice()?
                .expect("raw mode must expose downgrade metadata");
            assert_eq!(notice.level, "ERROR");
            assert_eq!(notice.response_headers[1], ("Cache-Control", "no-store"));
            println!("PASS Rust round-trip reference raw diagnostic mode");
        }
        _ => panic!("reference wire shape must match the assembly mode"),
    }
    Ok(())
}
