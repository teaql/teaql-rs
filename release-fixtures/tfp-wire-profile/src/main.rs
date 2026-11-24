use std::collections::BTreeMap;

use serde_json::json;
use teaql_runtime::{CheckResult, JsonFieldNamingProfile, ObjectLocation};
use teaql_tfp_endpoint::{WireEntityMetadata, normalize_wire_object, retain_submitted_paths};

fn main() {
    let alias = "legacy/url~v1";
    let metadata = WireEntityMetadata::new(
        BTreeMap::from([
            ("user_url".into(), "userUrl".into()),
            ("school_type".into(), "schoolType".into()),
        ]),
        BTreeMap::from([(alias.into(), "user_url".into())]),
    )
    .expect("valid generated wire metadata");

    let submitted = json!({(alias): "https://teaql.io", "schoolType": 1001});
    let normalized =
        normalize_wire_object(submitted.as_object().expect("submitted object"), &metadata)
            .expect("known aliases normalize");
    assert_eq!(normalized.values["user_url"], json!("https://teaql.io"));
    assert_eq!(normalized.values["school_type"], json!(1001));
    assert_eq!(
        normalized.source_instance_paths["user_url"],
        "/legacy~1url~0v1"
    );
    assert_eq!(
        normalized.source_instance_paths["school_type"],
        "/schoolType"
    );

    let mut violations = vec![CheckResult::required(ObjectLocation::hash_root("user_url"))];
    retain_submitted_paths(&mut violations, &normalized);
    let wire = violations[0].to_wire(JsonFieldNamingProfile::SnakeCase);
    let wire_json = serde_json::to_value(wire).expect("wire violation serializes");
    assert_eq!(wire_json["ruleId"], json!("required"));
    assert_eq!(wire_json["location"][0]["name"], json!("user_url"));
    assert_eq!(wire_json["instancePath"], json!("/user_url"));
    assert_eq!(wire_json["sourceInstancePath"], json!("/legacy~1url~0v1"));

    let collision = json!({"userUrl": "a", (alias): "b"});
    assert_eq!(
        normalize_wire_object(collision.as_object().unwrap(), &metadata)
            .unwrap_err()
            .code(),
        "WIRE_FIELD_COLLISION"
    );
    let unknown = json!({"unknown": 1});
    assert_eq!(
        normalize_wire_object(unknown.as_object().unwrap(), &metadata)
            .unwrap_err()
            .code(),
        "WIRE_UNKNOWN_FIELD"
    );

    let ambiguous = WireEntityMetadata::new(
        BTreeMap::from([
            ("user_url".into(), "userUrl".into()),
            ("school_type".into(), "schoolType".into()),
        ]),
        BTreeMap::from([("schoolType".into(), "user_url".into())]),
    )
    .expect_err("one submitted name must not resolve to two canonical fields");
    assert!(ambiguous.contains("ambiguous"));

    println!(
        "TFP_WIRE_PROFILE_PUBLIC_REPLAY_PASS alias={alias} canonical=user_url collision=refused unknown=refused"
    );
}
