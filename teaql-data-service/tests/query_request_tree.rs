use teaql_core::{QueryIntent, QueryOperation, QueryOperationKind, SelectQuery, TraceKind};
use teaql_data_service::QueryRequest;

fn root(comment: &str) -> QueryRequest {
    QueryRequest::new(
        SelectQuery::new("School"),
        QueryIntent::new(comment, "render school list").unwrap(),
    )
}
fn comments(request: &QueryRequest) -> Vec<String> {
    request
        .business_trace_chain()
        .into_iter()
        .map(|node| node.comment)
        .collect()
}

#[test]
fn native_operation_and_explicit_child_keep_direct_parent_and_original_root() {
    let parent = root("load schools");
    let child = parent
        .derive(
            SelectQuery::new("SchoolType").comment("load visible types"),
            "school_type",
            Some(QueryOperation::new(
                QueryOperationKind::RelationSelect,
                "select_school_type_with",
            )),
        )
        .unwrap();
    assert_eq!(
        child.request_scope().root_request(),
        &parent.request_scope()
    );
    assert_eq!(
        child
            .request_scope()
            .parent_request()
            .unwrap()
            .parent_request()
            .unwrap(),
        &parent.request_scope()
    );
    assert_eq!(
        comments(&child),
        [
            "render school list",
            "load schools",
            "select_school_type_with(...)",
            "load visible types"
        ]
    );
    assert_eq!(child.comment(), "load schools");
}

#[test]
fn default_is_local_and_cannot_authorize_an_invalid_root() {
    let child = root("load schools")
        .derive(SelectQuery::new("SchoolType"), "school_type", None)
        .unwrap();
    assert_eq!(child.request_scope().comment(), "load SchoolType");
    assert!(QueryRequest::from_query(SelectQuery::new("School").purpose("render page")).is_err());
    for blank in ["", " \n ", "\u{85}"] {
        assert!(
            root("load schools")
                .derive(
                    SelectQuery::new("SchoolType").comment(blank),
                    "school_type",
                    None
                )
                .is_err()
        );
    }
}

#[test]
fn clones_and_query_replacements_preserve_small_shared_headers() {
    let parent = root("load schools");
    let child = parent
        .derive(
            SelectQuery::new("SchoolType").comment("load labels"),
            "school_type",
            None,
        )
        .unwrap();
    let clone = child.with_query(child.query.clone().limit(3)).unwrap();
    let captured = QueryRequest::from_query(clone.query.clone()).unwrap();
    assert_eq!(clone.request_scope(), child.request_scope());
    assert_eq!(
        captured.request_scope().root_request(),
        &parent.request_scope()
    );
    assert_eq!(comments(&captured), comments(&child));
    assert_eq!(
        clone
            .trace_chain
            .iter()
            .filter(|node| node.kind == TraceKind::Relation)
            .count(),
        1
    );
}

#[test]
fn unrelated_invocation_cannot_replace_owned_query_with_identical_intent() {
    let first = root("same comment");
    let second = root("same comment");
    assert!(first.with_query(second.query.clone()).is_err());
    let own_copy = first.with_query(first.query.clone()).unwrap();
    assert!(own_copy.with_query(first.query.clone()).is_ok());
    assert!(own_copy.with_query(second.query.clone()).is_err());
    // Rebinding a shared child is an explicit derivation, not replacement.
    assert!(first.derive(second.query.clone(), "schools", None).is_ok());
}

#[test]
fn reusable_templates_and_identical_comments_do_not_collapse_different_roots() {
    let first = root("load schools");
    let second = root("load schools");
    let template = SelectQuery::new("SchoolType").comment("load labels");
    let a = first.derive(template.clone(), "school_type", None).unwrap();
    let b = second
        .derive(template.clone(), "school_type", None)
        .unwrap();
    assert_ne!(
        a.request_scope().root_request(),
        b.request_scope().root_request()
    );
    assert!(template.request_scope().is_none());
    assert!(template.purpose.is_none());
}

#[test]
fn count_alias_uses_one_rust_literal_renderer_without_a_fake_relation() {
    let parent = root("load schools");
    let counted = parent.derive_operation(
        SelectQuery::new("School"),
        QueryOperation::new(QueryOperationKind::NamedCount, "count_as")
            .with_alias("total\"\\schools\n"),
    );
    assert_eq!(
        counted.request_scope().comment(),
        "count_as(\"total\\\"\\\\schools\\n\")"
    );
    assert_eq!(
        counted.request_scope().parent_request().unwrap(),
        &parent.request_scope()
    );
    assert!(counted.trace_chain.is_empty());
}

#[test]
fn comment_and_purpose_prefixes_are_ordinary_text() {
    let query = SelectQuery::new("School")
        .comment("what: load schools")
        .purpose("why: render schools");
    let request = QueryRequest::from_query(query).unwrap();
    assert_eq!(
        comments(&request),
        ["why: render schools", "what: load schools"]
    );
}

#[test]
fn control_characters_use_rust_not_json_unicode_literal_syntax() {
    let child = root("load schools").derive_operation(
        SelectQuery::new("School"),
        QueryOperation::new(QueryOperationKind::NamedCount, "count_as")
            .with_alias("total\u{b}schools"),
    );
    assert_eq!(
        child.request_scope().comment(),
        "count_as(\"total\\u{b}schools\")"
    );
}
