//! Request construction must retain the same stable diagnostics as decoding.
use std::panic::{AssertUnwindSafe, catch_unwind};
use teaql_core::{QueryIntent, RequestKind, SelectQuery};
use teaql_runtime::PurposedSelectQuery;

fn panic_text(value: Box<dyn std::any::Any + Send>) -> String {
    value
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| value.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default()
}

#[test]
fn blank_purpose_constructor_matches_structured_request_diagnostics() {
    let mut failures = Vec::new();
    for (kind, purpose) in [
        ("empty", ""),
        ("ascii-whitespace", " \t\r\n"),
        ("NEL", "\u{85}"),
        ("NBSP", "\u{a0}"),
        ("em-space", "\u{2003}"),
    ] {
        let expected = QueryIntent::new("PRIVATE-REQUEST-CANARY", purpose).unwrap_err();
        assert_eq!(expected.code(), "QUERY_PURPOSE_REQUIRED");
        assert_eq!(expected.field, "purpose");
        assert_eq!(expected.request_kind, RequestKind::Query);
        let result = catch_unwind(AssertUnwindSafe(|| {
            PurposedSelectQuery::new(
                SelectQuery::new("Order").comment("PRIVATE-REQUEST-CANARY"),
                purpose,
            )
        }));
        let actual = result.err().map(panic_text).unwrap_or_default();
        let passed = actual == expected.to_string() && !actual.contains("PRIVATE-REQUEST-CANARY");
        println!(
            "RUST_PURPOSE_ADMISSION={}",
            serde_json::json!({"kind":kind,"code":expected.code(),"field":expected.field,"requestKind":"Query","actual":actual,"passed":passed})
        );
        if !passed {
            failures.push(kind);
        }
    }
    assert!(
        failures.is_empty(),
        "constructor lost the request diagnostic for {failures:?}"
    );
}

#[test]
fn valid_purpose_and_child_intent_are_not_trimmed_or_rewritten() {
    let comment = "  load selected orders  ";
    let purpose = "  render the operations queue  ";
    let child = SelectQuery::new("OrderItem")
        .comment("load bounded items")
        .purpose("render item rows")
        .limit(3);
    let query = PurposedSelectQuery::new(
        SelectQuery::new("Order")
            .comment(comment)
            .relation_query("items", child.clone())
            .limit(2),
        purpose,
    );
    let value = query.as_query();
    assert_eq!(value.comment.as_deref(), Some(comment));
    assert_eq!(value.purpose.as_deref(), Some(purpose));
    let scope = value
        .request_scope()
        .expect("valid intent owns a request scope");
    let path = scope.trace_chain();
    assert!(path.iter().any(|node| node.comment == comment));
    assert!(path.iter().any(|node| node.comment == purpose));
    assert_eq!(child.comment.as_deref(), Some("load bounded items"));
    assert_eq!(child.purpose.as_deref(), Some("render item rows"));
    println!(
        "RUST_PURPOSE_CONTROL={}",
        serde_json::json!({"commentPreserved":true,"purposePreserved":true,"childIntentPreserved":true,"requestScopePresent":true,"passed":true})
    );
}
