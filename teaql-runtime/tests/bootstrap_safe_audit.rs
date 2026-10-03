use teaql_core::{Record, Value};
use teaql_runtime::{BootstrapAuditIdentity, RawAuditEvent};

#[test]
fn safe_bootstrap_identity_retains_provenance_without_leaking_credentials_or_id_prose() {
    let mut values = Record::new();
    values.insert("id".into(), Value::U64(7123));
    values.insert(
        "password".into(),
        Value::Text("BOOTSTRAP-PASSWORD-CANARY".into()),
    );
    let mut raw = RawAuditEvent::created("Platform", values);
    raw.bootstrap_audit = Some(BootstrapAuditIdentity {
        actor: "bootstrap BOOTSTRAP-PASSWORD-CANARY".into(),
        category: "runtime-bootstrap".into(),
        reason: "seed 7123 BOOTSTRAP-PASSWORD-CANARY".into(),
        resulting_version: Some(1),
        occurred_at_millis: 42,
    });
    let safe = raw.build_safe_event(&[], None);
    let identity = safe
        .bootstrap_audit
        .expect("safe audit must retain bootstrap attribution");
    assert_eq!(identity.actor, "bootstrap [REDACTED]");
    assert_eq!(identity.category, "runtime-bootstrap");
    assert_eq!(identity.reason, "seed [REDACTED] [REDACTED]");
    assert_eq!(identity.resulting_version, Some(1));
    assert_eq!(identity.occurred_at_millis, 42);
    assert_eq!(
        raw.bootstrap_audit.as_ref().unwrap().reason,
        "seed 7123 BOOTSTRAP-PASSWORD-CANARY"
    );
    assert!(
        RawAuditEvent::created("Platform", Record::new())
            .build_safe_event(&[], None)
            .bootstrap_audit
            .is_none()
    );
}
