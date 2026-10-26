use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use teaql_blob_reference::*;
use teaql_core::BlobFieldSpec;
fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).into()).collect()
}
fn field(formats: &[&str], profile: &str) -> BlobFieldSpec {
    BlobFieldSpec::new("Asset", "document", formats.iter().copied(), true, profile).unwrap()
}
fn policy(profile: BlobPolicyProfile, limits: PolicyLimits) -> BlobResult<BlobPolicies> {
    BlobPolicies::new(
        limits,
        BlobPolicyProfile::named("default"),
        BTreeMap::from([(profile.name.clone(), profile)]),
        BTreeMap::new(),
    )
}
#[test]
fn all_thirteen_shared_policy_and_retention_cases() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/policy-cases.json")).unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 13);
    for case in cases {
        let input = &case["input"];
        let expected = &case["expected"];
        match case["id"].as_str().unwrap() {
            "defaults" => {
                let snapshot = BlobPolicies::default()
                    .resolve(&field(&["pdf"], "default"))
                    .unwrap();
                assert_eq!(snapshot.max_size_bytes(), expected["maxSizeBytes"]);
                assert_eq!(
                    snapshot.upload_session_seconds(),
                    expected["uploadSessionTtlSeconds"]
                );
                assert_eq!(
                    snapshot.unbound_seconds(),
                    expected["unboundRetentionSeconds"]
                );
                assert_eq!(
                    snapshot.unreferenced_seconds(),
                    expected["unreferencedRetentionSeconds"]
                );
                assert_eq!(snapshot.read_url_seconds(), expected["readUrlTtlSeconds"]);
            }
            "intersections" | "empty-intersection" => {
                let formats = |key: &str| {
                    input[key]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|value| value.as_str().unwrap().to_owned())
                        .collect::<BTreeSet<_>>()
                };
                let model = BlobFieldSpec::new(
                    "Asset",
                    "document",
                    formats("modelExtensions"),
                    true,
                    "specific",
                )
                .unwrap();
                let mut limits = PolicyLimits::default();
                if input.get("deploymentExtensions").is_some() {
                    limits.allowed_extensions = formats("deploymentExtensions");
                }
                let mut profile = BlobPolicyProfile::named("specific");
                profile.overrides.allowed_extensions = Some(formats("profileExtensions"));
                let result = policy(profile, limits).and_then(|policies| policies.resolve(&model));
                if case.get("error").is_some() {
                    assert_eq!(result.unwrap_err().code.code(), case["error"]);
                } else {
                    let snapshot = result.unwrap();
                    assert_eq!(
                        serde_json::to_value(snapshot.field_spec().unwrap().allowed_extensions())
                            .unwrap(),
                        expected["allowedExtensions"]
                    );
                    assert_eq!(
                        serde_json::to_value(
                            snapshot.field_spec().unwrap().allowed_content_types()
                        )
                        .unwrap(),
                        expected["allowedContentTypes"]
                    );
                }
            }
            "hard-size-limit" => {
                let mut profile = BlobPolicyProfile::named("specific");
                profile.overrides.max_size_bytes = input["maxSizeBytes"].as_u64();
                let limits = PolicyLimits {
                    max_upload_size_bytes: input["maxUploadSizeBytes"].as_u64().unwrap(),
                    ..Default::default()
                };
                assert_eq!(
                    policy(profile, limits)
                        .err()
                        .expect("hard size limit must reject")
                        .code
                        .code(),
                    case["error"]
                );
            }
            "url-capped-by-document" => {
                let policies = BlobPolicies::default();
                let snapshot = policies.resolve(&field(&["pdf"], "default")).unwrap();
                assert_eq!(
                    policies
                        .read_ttl(
                            &snapshot,
                            input["requestedTtlSeconds"].as_u64(),
                            input["storeMaximumSeconds"].as_u64().unwrap(),
                            input["documentRemainingSeconds"].as_u64()
                        )
                        .unwrap(),
                    expected["readUrlTtlSeconds"]
                );
            }
            "upload-clock-independent" => {
                let snapshot = BlobPolicies::default()
                    .resolve(&field(&["pdf"], "default"))
                    .unwrap();
                assert_eq!(
                    input["nowSeconds"].as_u64().unwrap()
                        >= input["beginAtSeconds"].as_u64().unwrap()
                            + snapshot.upload_session_seconds(),
                    expected["uploadSessionExpired"]
                );
                assert_eq!(
                    BlobRetention::unbound_eligible_at(
                        input["completedAtSeconds"].as_i64().unwrap() * 1000,
                        snapshot.unbound_seconds()
                    )
                    .unwrap()
                        / 1000,
                    expected["unboundEligibleAtSeconds"]
                );
            }
            "unbound-deadline-rejects-bind-before-gc" => {
                let eligible = BlobRetention::unbound_eligible_at(
                    input["completedAtSeconds"].as_i64().unwrap() * 1000,
                    input["unboundRetentionSeconds"].as_u64().unwrap(),
                )
                .unwrap();
                assert_eq!(
                    BlobRetention::require_bindable(
                        BlobState::UploadedUnbound,
                        input["nowSeconds"].as_i64().unwrap() * 1000,
                        Some(eligible)
                    )
                    .unwrap_err()
                    .code
                    .code(),
                    case["error"]
                );
            }
            "active-reference-never-collected" => assert_eq!(
                BlobRetention::collectable(
                    BlobState::Available,
                    input["bindingCount"].as_i64().unwrap(),
                    input["nowSeconds"].as_i64().unwrap() * 1000,
                    Some(0)
                ),
                expected["collectable"]
            ),
            "maximum-retention-across-fields" => {
                let accepted = input["acceptedRetentionSeconds"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_u64().unwrap())
                    .fold(0, BlobRetention::accepted_unreferenced_seconds);
                assert_eq!(accepted, expected["effectiveUnreferencedRetentionSeconds"]);
                assert_eq!(
                    BlobRetention::unreferenced_eligible_at(
                        input["lastUnboundAtSeconds"].as_i64().unwrap() * 1000,
                        accepted,
                        Some(input["retentionFloorUntilSeconds"].as_i64().unwrap() * 1000)
                    )
                    .unwrap()
                        / 1000,
                    expected["unreferencedEligibleAtSeconds"]
                );
            }
            "profile-update-does-not-shorten" => assert_eq!(
                BlobRetention::accepted_unreferenced_seconds(
                    input["acceptedUnreferencedRetentionSeconds"]
                        .as_u64()
                        .unwrap(),
                    input["currentProfileRetentionSeconds"].as_u64().unwrap()
                ),
                expected["effectiveUnreferencedRetentionSeconds"]
            ),
            "read-expiry-does-not-delete" => {
                assert!(input["readUrlExpired"].as_bool().unwrap());
                assert!(!BlobRetention::collectable(
                    BlobState::Available,
                    input["bindingCount"].as_i64().unwrap(),
                    i64::MAX,
                    Some(0)
                ));
                assert_eq!(expected["requiresReadAuthorization"], true);
            }
            "deleting-rejects-binding" => assert_eq!(
                BlobRetention::require_bindable(BlobState::Deleting, 0, Some(0))
                    .unwrap_err()
                    .code
                    .code(),
                case["error"]
            ),
            "context-checked-read-is-explicit" => {
                let capabilities = input["storeCapabilities"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_owned())
                    .collect();
                assert_eq!(
                    BlobPolicies::require_context_checked_read(
                        input["requireContextCheckedRead"].as_bool().unwrap(),
                        &capabilities
                    )
                    .unwrap_err()
                    .code
                    .code(),
                    case["error"]
                );
            }
            unknown => panic!("uncovered shared case {unknown}"),
        }
    }
}

#[test]
fn actual_java_policy_wire_vector_round_trips_exactly() {
    let actual_java = include_str!("fixtures/policy-snapshot-v1.json").trim();
    let parsed = BlobPolicySnapshot::from_json(actual_java).unwrap();
    let resolved = BlobPolicies::default()
        .resolve(&field(&["pdf", "docx"], "default"))
        .unwrap();
    assert_eq!(parsed, resolved);
    assert_eq!(resolved.to_json().unwrap(), actual_java);
}

#[test]
fn closed_snapshot_decode_rejects_corrupt_fields_duplicates_and_deserializer_bypass() {
    let original: Value =
        serde_json::from_str(include_str!("fixtures/policy-snapshot-v1.json")).unwrap();
    for (key, value) in [
        ("schema", json!(2)),
        ("version", json!(0)),
        ("maxSize", json!(0)),
        ("uploadSession", json!(0)),
        ("minimum", json!(-1)),
        ("extensions", json!(["pdf", "pdf"])),
        ("extensions", json!(["exe"])),
        ("extensions", json!([])),
        ("cache", json!("public, max-age=1")),
        ("cache", json!("private, max-age=601")),
        ("unexpected", json!(true)),
    ] {
        let mut corrupt = original.clone();
        corrupt[key] = value;
        let encoded = corrupt.to_string();
        assert_eq!(
            BlobPolicySnapshot::from_json(&encoded).unwrap_err().code,
            BlobErrorCode::PolicyInvalid
        );
        assert!(
            serde_json::from_str::<BlobPolicySnapshot>(&encoded).is_err(),
            "direct Deserialize must validate {key}"
        );
    }
    let repeated = include_str!("fixtures/policy-snapshot-v1.json")
        .trim()
        .replacen("{", "{\"schema\":1,", 1);
    assert!(BlobPolicySnapshot::from_json(&repeated).is_err());
    assert!(BlobPolicySnapshot::from_json(&" ".repeat(16385)).is_err());
}

#[test]
fn startup_checks_fields_profiles_limits_inline_and_private_cache() {
    let defaults = BlobPolicyProfile::named("default");
    assert!(
        BlobPolicies::new(
            PolicyLimits::default(),
            defaults.clone(),
            BTreeMap::new(),
            BTreeMap::from([("Asset.document".into(), "missing".into())])
        )
        .is_err()
    );
    let policies = BlobPolicies::new(
        PolicyLimits::default(),
        defaults,
        BTreeMap::new(),
        BTreeMap::from([("Asset.unknown".into(), "default".into())]),
    )
    .unwrap();
    assert!(
        policies
            .validate_model([&field(&["pdf"], "default")])
            .is_err()
    );
    let mut specific = BlobPolicyProfile::named("specific");
    specific.overrides.disposition = Some(Disposition::Inline);
    let narrowed = policy(specific.clone(), PolicyLimits::default()).unwrap();
    assert!(narrowed.resolve(&field(&["pdf"], "specific")).is_err());
    assert!(narrowed.resolve(&field(&["jpeg"], "specific")).is_ok());
    specific.overrides.allowed_extensions = Some(set(&["jpg", "jpeg", "png"]));
    let policies = policy(specific, PolicyLimits::default()).unwrap();
    let snapshot = policies.resolve(&field(&["jpeg"], "specific")).unwrap();
    assert_eq!(
        policies.read_disposition(&snapshot, "image/jpeg"),
        Disposition::Inline
    );
    assert!(
        BlobPolicies::default()
            .read_ttl(&snapshot, None, 600, Some(0))
            .is_err()
    );
    let mut specific = BlobPolicyProfile::named("specific");
    specific.overrides.cache_policy = Some(CachePolicy::PrivateMaxAge(600));
    assert_eq!(
        policy(specific, PolicyLimits::default())
            .unwrap()
            .resolve(&field(&["pdf"], "specific"))
            .unwrap()
            .cache_policy(),
        CachePolicy::PrivateMaxAge(600)
    );
}
