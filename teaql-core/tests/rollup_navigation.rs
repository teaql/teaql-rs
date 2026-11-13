use teaql_core::request::{clear_rollup_navigation, rollup_navigation_membership};
use teaql_core::{EntityDescriptor, Expr, SelectQuery};

fn parent() -> SelectQuery {
    SelectQuery::new("Kind").filter(Expr::eq("id", 1001_i64))
}
fn owned() -> Expr {
    rollup_navigation_membership("kind", "kind_id", EntityDescriptor::new("Kind"), parent())
}
fn clear(query: &mut SelectQuery) {
    clear_rollup_navigation(query, "kind", "id").unwrap();
}

#[test]
fn an_identical_independent_filter_is_not_navigation_owned() {
    let business = Expr::in_subquery("kind_id", EntityDescriptor::new("Kind"), parent(), "id");
    let mut query = SelectQuery::new("School")
        .filter(business.clone())
        .and_filter(owned());
    clear(&mut query);
    assert_eq!(query.filter, Some(business));
}

#[test]
fn cloning_preserves_the_origin_without_rebinding_the_original() {
    let original = SelectQuery::new("School").filter(owned());
    let mut copy = original.clone();
    clear(&mut copy);
    assert!(copy.filter.is_none());
    assert!(original.filter.is_some());
}

#[test]
fn boolean_alternatives_retain_unrelated_branches_for_validation() {
    let unknown = Expr::eq("unknown_logical_field", 1_i64);
    let mut query = SelectQuery::new("School").filter(Expr::Or(vec![owned(), unknown.clone()]));
    clear(&mut query);
    assert_eq!(
        query.filter,
        Some(Expr::Or(vec![Expr::is_not_null("id"), unknown]))
    );
}

#[test]
fn negated_navigation_stays_false_in_the_new_matching_population() {
    let mut query = SelectQuery::new("School").filter(Expr::Not(Box::new(owned())));
    clear(&mut query);
    assert_eq!(
        query.filter,
        Some(Expr::Not(Box::new(Expr::is_not_null("id"))))
    );
}

#[test]
fn repeated_rebinding_does_not_accumulate_navigation_predicates() {
    let business = Expr::gt("capacity", 0_i64);
    let mut query = SelectQuery::new("School").filter(business.clone());
    for _ in 0..100 {
        query = query.and_filter(owned());
        clear(&mut query);
    }
    assert_eq!(query.filter, Some(business));
}

#[test]
fn failed_rebinding_does_not_clear_the_original_filter() {
    let mut expr = owned();
    for _ in 0..65 {
        expr = Expr::Not(Box::new(expr));
    }
    let mut query = SelectQuery::new("School").filter(expr);
    let before = query.clone();
    assert!(clear_rollup_navigation(&mut query, "kind", "id").is_err());
    assert_eq!(query, before);
}

#[test]
fn clearing_a_copied_execution_scope_preserves_navigation_provenance() {
    let scope = teaql_core::QueryRequestScope::root(
        "Kind",
        teaql_core::QueryIntent::new("Select a navigation target", "Verify scoped copies").unwrap(),
    );
    let Expr::SubQuery { query, .. } = owned() else {
        unreachable!()
    };
    let original = query.with_request_scope(scope.clone());
    let mut copy = original.clone();
    copy.clear_request_scope();
    assert!(copy.request_scope().is_none());
    assert_eq!(original.request_scope(), Some(&scope));
    let mut source = SelectQuery::new("School").filter(Expr::in_subquery(
        "kind_id",
        EntityDescriptor::new("Kind"),
        copy,
        "id",
    ));
    clear(&mut source);
    assert!(source.filter.is_none());
}
