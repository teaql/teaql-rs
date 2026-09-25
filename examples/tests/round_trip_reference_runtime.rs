use teaql_examples::Order;
use teaql_runtime::{
    AeadRoundTripReferenceProvider, DerivedRoundTripReferenceKeyProvider, RAW_ID_ACKNOWLEDGEMENT,
    RawRoundTripReferenceProvider, RoundTripReferenceErrorCode, RoundTripReferenceKey,
    RoundTripReferenceMode, RoundTripReferenceService, StaticRoundTripReferenceMasterKeyRing,
    UserContext, round_trip_reference_mode,
};

fn key(id: &str, start: u8) -> RoundTripReferenceKey {
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = start.wrapping_add(index as u8);
    }
    RoundTripReferenceKey::new(id, bytes).expect("valid fixture key")
}

fn governed_service(
    current: RoundTripReferenceKey,
    previous: Vec<RoundTripReferenceKey>,
) -> RoundTripReferenceService {
    let ring = StaticRoundTripReferenceMasterKeyRing::new(current, previous);
    let bindings = |context: &UserContext| {
        context
            .get_named_resource::<String>("example.reference.binding")
            .map(|value| value.as_bytes().to_vec())
    };
    let keys = DerivedRoundTripReferenceKeyProvider::new(ring, bindings, "reference-example")
        .expect("build derived key provider");
    RoundTripReferenceService::new(AeadRoundTripReferenceProvider::new(keys))
}

fn context(service: RoundTripReferenceService, binding: &str) -> UserContext {
    let mut context = UserContext::new().with_round_trip_reference_service(service);
    context.insert_named_resource("example.reference.binding", binding.to_owned());
    context
}

fn order(id: u64, version: i64) -> Order {
    Order {
        root: Default::default(),
        id,
        version,
        name: "Round-trip reference fixture".to_owned(),
        lines: Default::default(),
    }
}

#[test]
fn framework_neutral_codec_round_trips_and_rejects_transfer_or_tampering() {
    let alice = context(
        governed_service(key("k2", 1), vec![key("k1", 33)]),
        "job-task:alice:42",
    );
    let wire = alice
        .reference_for(&order(8172, 3))
        .expect("issue reference");
    let second_wire = alice
        .reference_for(&order(8172, 3))
        .expect("issue reference with a fresh nonce");
    assert!(wire.0.starts_with("tqr1.k2."));
    assert!(!wire.0.contains("8172"));
    assert_ne!(wire, second_wire);

    let resolved = alice
        .resolve_reference_for::<Order>(&wire.0)
        .expect("resolve in same context");
    assert_eq!(
        (resolved.entity_type.as_str(), resolved.id, resolved.version),
        ("Order", 8172, 3)
    );

    let personal = context(
        governed_service(key("k2", 1), vec![key("k1", 33)]),
        "personal-task:alice:42",
    );
    let transfer = personal
        .resolve_reference_for::<Order>(&wire.0)
        .expect_err("different runtime binding must reject transfer");
    assert_eq!(transfer.code, RoundTripReferenceErrorCode::ContextMismatch);

    let mut tampered = wire.0.clone().into_bytes();
    let payload_start = tampered
        .iter()
        .rposition(|byte| *byte == b'.')
        .expect("reference payload separator")
        + 1;
    tampered[payload_start] = if tampered[payload_start] == b'A' {
        b'B'
    } else {
        b'A'
    };
    let tampered = String::from_utf8(tampered).expect("ASCII reference");
    assert!(alice.resolve_reference_for::<Order>(&tampered).is_err());
}

#[test]
fn missing_provider_binding_and_persisted_version_fail_closed() {
    let plain = UserContext::new();
    let missing_provider = plain
        .reference_for(&order(1, 1))
        .expect_err("missing provider must fail closed");
    assert_eq!(
        missing_provider.code,
        RoundTripReferenceErrorCode::ProviderNotConfigured
    );

    let ring = StaticRoundTripReferenceMasterKeyRing::new(key("k2", 1), vec![]);
    let keys = DerivedRoundTripReferenceKeyProvider::new(
        ring,
        |_context: &UserContext| None,
        "reference-example",
    )
    .expect("construct provider");
    let unbound = UserContext::new().with_round_trip_reference_service(
        RoundTripReferenceService::new(AeadRoundTripReferenceProvider::new(keys)),
    );
    let missing_binding = unbound
        .reference_for(&order(1, 1))
        .expect_err("missing binding must fail closed");
    assert_eq!(
        missing_binding.code,
        RoundTripReferenceErrorCode::ContextBindingRequired
    );

    let raw = UserContext::new().with_round_trip_reference_service(RoundTripReferenceService::new(
        RawRoundTripReferenceProvider,
    ));
    assert!(raw.reference_for(&order(1, 0)).is_err());
}

#[test]
fn previous_key_decodes_after_rotation_and_raw_mode_is_explicit() {
    let before = context(governed_service(key("k1", 33), vec![]), "task:7");
    let old = before.reference_for(&order(9, 1)).expect("old reference").0;

    let after = context(
        governed_service(key("k2", 1), vec![key("k1", 33)]),
        "task:7",
    );
    assert_eq!(
        after
            .resolve_reference_for::<Order>(&old)
            .expect("decode old key")
            .id,
        9
    );
    assert!(
        after
            .reference_for(&order(10, 2))
            .expect("new reference")
            .0
            .starts_with("tqr1.k2.")
    );

    assert_eq!(
        round_trip_reference_mode("development", Some("true")).expect("governed"),
        RoundTripReferenceMode::Governed
    );
    assert_eq!(
        round_trip_reference_mode("development", Some(RAW_ID_ACKNOWLEDGEMENT)).expect("raw"),
        RoundTripReferenceMode::RawDiagnostic
    );
    assert!(round_trip_reference_mode("production", Some(RAW_ID_ACKNOWLEDGEMENT)).is_err());

    let raw = context(
        RoundTripReferenceService::new(RawRoundTripReferenceProvider),
        "unused",
    );
    let readable = raw.reference_for(&order(8172, 3)).expect("raw reference").0;
    assert_eq!(readable, "raw1.Order.8172.3");
    assert_eq!(
        raw.resolve_reference_for::<Order>(&readable)
            .expect("raw resolve")
            .id,
        8172
    );
}
