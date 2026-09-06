use std::collections::BTreeMap;
use teaql_core::{Expr, OrderBy, SelectQuery, dynamic_search::*};

fn models() -> SearchModels {
    BTreeMap::from([
        (
            "School".into(),
            SearchModel {
                fields: [
                    ("name", "string"),
                    ("id", "integer"),
                    ("active", "boolean"),
                    ("amount", "decimal"),
                    ("established_date", "date"),
                    ("create_time", "timestamp"),
                    ("capacity", "number"),
                ]
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
                relations: BTreeMap::from([("platform".into(), "Platform".into())]),
            },
        ),
        (
            "Platform".into(),
            SearchModel {
                fields: BTreeMap::from([("name".into(), "string".into())]),
                ..Default::default()
            },
        ),
    ])
}
#[test]
fn unknown_complete_clauses_warn_without_values() {
    let mut warnings = vec![];
    let result = normalize_dynamic_search(r#"{"filter":{"name":"School","old_name":"secret","platform.old_name":"secret","missing.name":"secret","platform.name":"Campus"},"orderBy":[{"field":"gone","direction":"asc"},{"field":"id","direction":"desc"}]}"#,
        "School", &models(), 100, Some(&mut |w| warnings.push(w.clone()))).unwrap();
    assert_eq!(
        result
            .filters
            .iter()
            .map(|f| f.field_path.as_str())
            .collect::<Vec<_>>(),
        ["name", "platform.name"]
    );
    assert_eq!(result.orders.len(), 1);
    assert_eq!(result.orders[0].field_path, "id");
    assert_eq!(warnings.len(), 4);
    let json = serde_json::to_string(&warnings).unwrap();
    assert!(!json.contains("secret"));
    assert!(json.contains("fieldPath"));
    assert!(
        warnings
            .iter()
            .all(|w| w.code == "DYNAMIC_SEARCH_UNKNOWN_FIELD")
    );
}
#[test]
fn invalid_input_remains_fatal() {
    for source in [
        "[]",
        "{} {}",
        r#"{"tenant":2}"#,
        r#"{"filter":{"id":true}}"#,
        r#"{"filter":{"id":1.2}}"#,
        r#"{"filter":{"capacity":1e999}}"#,
        r#"{"filter":{"established_date":"2026-02-30"}}"#,
        r#"{"filter":{"gone":{"$wat":1}}}"#,
        r#"{"filter":{"name":{"$in":1}}}"#,
        r#"{"filter":{"constructor":1}}"#,
        r#"{"filter":{"platform..name":1}}"#,
        r#"{"orderBy":[{"field":"id","direction":"bad"}]}"#,
    ] {
        let mut warnings = vec![];
        assert!(
            normalize_dynamic_search(
                source,
                "School",
                &models(),
                100,
                Some(&mut |w| warnings.push(w.clone()))
            )
            .is_err(),
            "{source}"
        );
        assert!(warnings.is_empty());
    }
}
#[test]
fn dates_booleans_and_exact_decimal_strings_are_retained() {
    let result = normalize_dynamic_search(r#"{"filter":{"id":1.0,"active":true,"amount":"12345678901234567890.123456789","established_date":"2024-02-29","create_time":1700000000000,"name":null}}"#,
        "School", &models(), 100, None).unwrap();
    assert_eq!(result.filters.len(), 6);
    assert_eq!(
        result
            .filters
            .iter()
            .find(|f| f.field_path == "amount")
            .unwrap()
            .value,
        "12345678901234567890.123456789"
    );
}
#[test]
fn limits_and_missing_relation_metadata_fail() {
    assert!(
        normalize_dynamic_search(
            r#"{"filter":{"name":"x","gone":1}}"#,
            "School",
            &models(),
            1,
            None
        )
        .is_err()
    );
    let source = serde_json::json!({"filter":{"id":{"$in":vec![1;1001]}}}).to_string();
    assert!(normalize_dynamic_search(&source, "School", &models(), 100, None).is_err());
    let mut broken = models();
    broken.remove("Platform");
    assert!(
        normalize_dynamic_search(
            r#"{"filter":{"platform.name":"x"}}"#,
            "School",
            &broken,
            100,
            None
        )
        .is_err()
    );
}
#[test]
fn composition_preserves_the_original_scope_and_limits() {
    let mut base = SelectQuery::new("School")
        .filter(Expr::eq("tenant_id", 7i64))
        .order_by(OrderBy::desc("id"))
        .limit(2)
        .comment("what: tenant search");
    base.hard_limit = 3;
    let before = base.clone();
    let mut warnings = vec![];
    let result = merge_dynamic_search(
        &base,
        r#"{"filter":{"name":"School","gone":1},"orderBy":[{"field":"name","direction":"asc"}]}"#,
        &models(),
        |f| Ok(Expr::eq(&f.field_path, f.value.as_str().unwrap())),
        |o| Ok(OrderBy::asc(&o.field_path)),
        Some(&mut |w| warnings.push(w.clone())),
    )
    .unwrap();
    assert_eq!(base, before);
    assert_ne!(result.query.filter, base.filter);
    assert_eq!(
        result.query.order_by,
        vec![OrderBy::desc("id"), OrderBy::asc("name")]
    );
    assert_eq!(result.query.slice, base.slice);
    assert_eq!(result.query.hard_limit, 3);
    assert_eq!(result.query.comment, base.comment);
    assert_eq!(warnings.len(), 1);
}
#[test]
fn late_failures_do_not_emit_warnings() {
    let mut warnings = vec![];
    assert!(
        normalize_dynamic_search(
            r#"{"filter":{"gone":1,"id":"bad"}}"#,
            "School",
            &models(),
            100,
            Some(&mut |w| warnings.push(w.clone()))
        )
        .is_err()
    );
    assert!(
        merge_dynamic_search(
            &SelectQuery::new("School"),
            r#"{"filter":{"gone":1,"name":"School"}}"#,
            &models(),
            |_| Err(DynamicSearchError("binding failure")),
            |o| Ok(OrderBy::asc(&o.field_path)),
            Some(&mut |w| warnings.push(w.clone()))
        )
        .is_err()
    );
    assert!(warnings.is_empty());
}
