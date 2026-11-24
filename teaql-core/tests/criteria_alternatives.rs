use teaql_core::request::criteria_alternatives_excluding_version as alternatives;
use teaql_core::{DataType, EntityDescriptor, Expr, PropertyDescriptor, SelectQuery};

#[test]
fn empty_and_version_only_sources_are_no_ops() {
    assert_eq!(alternatives(None), None);
    assert_eq!(alternatives(Some(Expr::gt("version", 0))), None);
    assert_eq!(
        alternatives(Some(Expr::and([
            Expr::gt("version", 0),
            Expr::eq("version", 99)
        ]))),
        None
    );
    assert_eq!(alternatives(Some(Expr::and([]))), None);
}

#[test]
fn non_version_top_level_conditions_become_alternatives() {
    let first = Expr::eq("name", "Primary");
    let second = Expr::eq("code", "SECONDARY");
    assert_eq!(
        alternatives(Some(Expr::and([
            Expr::gt("version", 0),
            first.clone(),
            second.clone()
        ]))),
        Some(Expr::or([first, second]))
    );
}

#[test]
fn receiver_filters_and_version_guards_stay_conjunctive() {
    let receiver = SelectQuery::new("Kind")
        .filter(Expr::gt("version", 0))
        .and_filter(Expr::eq("id", 1001));
    let supplied = alternatives(Some(Expr::and([
        Expr::gt("version", 0),
        Expr::eq("name", "Primary"),
        Expr::eq("code", "SECONDARY"),
    ])))
    .unwrap();
    let combined = receiver.clone().and_filter(supplied.clone());
    assert_eq!(
        combined.filter,
        Some(Expr::and([
            Expr::gt("version", 0),
            Expr::eq("id", 1001),
            supplied
        ]))
    );
    assert_eq!(
        receiver.filter,
        Some(Expr::and([Expr::gt("version", 0), Expr::eq("id", 1001)]))
    );
}

#[test]
fn explicit_nested_boolean_groups_and_single_conditions_keep_their_meaning() {
    let nested = Expr::and([Expr::eq("capacity", 100), Expr::eq("owner", 42)]);
    let other = Expr::negate(Expr::or([Expr::eq("id", 1), Expr::eq("id", 2)]));
    assert_eq!(
        alternatives(Some(Expr::and([
            Expr::gt("version", 0),
            nested.clone(),
            other.clone()
        ]))),
        Some(Expr::or([nested.clone(), other]))
    );
    assert_eq!(
        alternatives(Some(nested.clone())),
        Some(Expr::or([Expr::eq("capacity", 100), Expr::eq("owner", 42)]))
    );
    let single = Expr::eq("capacity", 100);
    assert_eq!(alternatives(Some(single.clone())), Some(single));
}

#[test]
fn independent_statistical_subqueries_are_preserved_inside_an_alternative() {
    let descriptor =
        EntityDescriptor::new("Kind").property(PropertyDescriptor::new("id", DataType::U64).id());
    let child = SelectQuery::new("Kind")
        .group_by("id")
        .count("kinds")
        .having(Expr::gt("kinds", 1))
        .order_desc("id")
        .limit(1)
        .comment("Retain the independent statistical branch");
    let membership = Expr::in_subquery("kind", descriptor, child, "id");
    assert_eq!(
        alternatives(Some(Expr::and([
            Expr::gt("version", 0),
            membership.clone(),
            Expr::eq("id", 1)
        ]))),
        Some(Expr::or([membership, Expr::eq("id", 1)]))
    );
}
