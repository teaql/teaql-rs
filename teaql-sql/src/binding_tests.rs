use crate::{DatabaseKind, SqlCompileError, SqlDialect};
use teaql_core::{
    BatchInsertCommand, BatchUpdateCommand, DataType, DeleteCommand, EntityDescriptor, Expr,
    InsertCommand, PropertyDescriptor, RecoverCommand, SelectQuery, UpdateCommand, Value,
};
use teaql_data_service::SqlParameterLogPolicy::{
    self as Policy, Credential, Masked, Plain, Unknown,
};

#[derive(Clone, Copy)]
struct Dialect;
impl SqlDialect for Dialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::Sqlite
    }
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{name}\"")
    }
    fn placeholder(&self, _: usize) -> String {
        "?".into()
    }
}
fn entity() -> EntityDescriptor {
    EntityDescriptor::new("Customer")
        .property(PropertyDescriptor::new("id", DataType::U64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .property(
            PropertyDescriptor::new("display_name", DataType::Text).column_name("customer_name"),
        )
        .property(PropertyDescriptor::new("status", DataType::Text))
        .property(PropertyDescriptor::new("password", DataType::Text))
        .audit_mask_fields(vec!["display_name".into()])
}

#[test]
fn legacy_descriptor_without_field_policy_fails_closed() {
    let legacy = EntityDescriptor::new("Customer")
        .property(PropertyDescriptor::new("display_name", DataType::Text))
        .property(PropertyDescriptor::new("password", DataType::Text));
    let query = SelectQuery::new("Customer").filter(Expr::and([
        Expr::eq("display_name", "CUSTOMER-CANARY"),
        Expr::eq("password", "PASSWORD-CANARY"),
    ]));
    let compiled = Dialect.compile_select(&legacy, &query).unwrap();
    assert_eq!(
        compiled.log_context.parameter_policies,
        [Unknown, Credential]
    );
    assert_eq!(compiled.params[0], Value::from("CUSTOMER-CANARY"));
}

#[test]
fn explicitly_empty_field_policy_keeps_ordinary_parameters_replayable() {
    let declared = EntityDescriptor::new("Customer")
        .property(PropertyDescriptor::new("display_name", DataType::Text))
        .property(PropertyDescriptor::new("password", DataType::Text))
        .audit_mask_fields(vec![]);
    let query = SelectQuery::new("Customer").filter(Expr::and([
        Expr::eq("display_name", "CUSTOMER-CANARY"),
        Expr::eq("password", "PASSWORD-CANARY"),
    ]));
    let compiled = Dialect.compile_select(&declared, &query).unwrap();
    assert_eq!(compiled.log_context.parameter_policies, [Plain, Credential]);
}
fn policies(query: &SelectQuery) -> Vec<Policy> {
    let compiled = Dialect.compile_select(&entity(), query).unwrap();
    assert_eq!(
        compiled.params.len(),
        compiled.log_context.parameter_policies.len()
    );
    compiled.log_context.parameter_policies
}

#[test]
fn predicates_use_ksml_field_policy_not_physical_column_or_parameter_value() {
    let query = SelectQuery::new("Customer").filter(Expr::and([
        Expr::eq("display_name", "Riverside"),
        Expr::eq("status", "ACTIVE"),
        Expr::eq("password", "CREDENTIAL-CANARY"),
        Expr::eq("id", 1_u64),
    ]));
    assert_eq!(policies(&query), [Masked, Plain, Credential, Plain]);
    assert!(
        Dialect
            .compile_select(&entity(), &query)
            .unwrap()
            .log_context
            .generated_sql
    );
}

#[test]
fn scalar_function_and_between_propagate_policy_but_siblings_stay_independent() {
    let query = SelectQuery::new("Customer").filter(Expr::and([
        Expr::binary(
            Expr::soundex(Expr::column("display_name")),
            teaql_core::BinaryOp::Eq,
            Expr::soundex(Expr::value("Riverside")),
        ),
        Expr::between("id", 1_u64, 10_u64),
        Expr::in_list("display_name", [Value::from("Alpha"), Value::from("Beta")]),
        Expr::eq("status", "ACTIVE"),
    ]));
    assert_eq!(
        policies(&query),
        [Masked, Plain, Plain, Masked, Masked, Plain]
    );
}

#[test]
fn subquery_scope_uses_its_own_entity_descriptor() {
    let child = EntityDescriptor::new("Child")
        .property(PropertyDescriptor::new("id", DataType::U64).id())
        .property(PropertyDescriptor::new("display_name", DataType::Text));
    let query = SelectQuery::new("Customer").filter(Expr::and([
        Expr::in_subquery(
            "display_name",
            child,
            SelectQuery::new("Child").filter(Expr::eq("display_name", "public-child")),
            "display_name",
        ),
        Expr::eq("display_name", "private-parent"),
    ]));
    assert_eq!(policies(&query), [Unknown, Masked]);
}

#[test]
fn search_text_is_classified_for_each_target_property() {
    assert_eq!(
        policies(&SelectQuery::new("Customer").search_with_text("needle")),
        [Masked, Plain, Credential]
    );
    assert_eq!(
        policies(
            &SelectQuery::new("Customer").project_expr("literal", Expr::value("unclassified"))
        ),
        [Unknown]
    );
}

#[test]
fn raw_fragments_taint_the_whole_statement_including_nested_queries() {
    let root = entity();
    for query in [
        SelectQuery::new("Customer").raw_sql("SELECT 'RAW-CANARY'"),
        SelectQuery::new("Customer").project_raw("display_name", "'RAW-CANARY'"),
        SelectQuery::new("Customer").dynamic_property_raw("custom", "'RAW-CANARY'"),
        SelectQuery::new("Customer").raw_sql_search_criteria("status = 'RAW-CANARY'"),
        SelectQuery::new("Customer").filter(Expr::in_subquery(
            "id",
            root.clone(),
            SelectQuery::new("Customer").raw_sql_search_criteria("status = 'RAW-CANARY'"),
            "id",
        )),
    ] {
        assert!(
            !Dialect
                .compile_select(&root, &query)
                .unwrap()
                .log_context
                .generated_sql
        );
    }
}

#[test]
fn mutation_and_guards_preserve_per_field_policy() {
    let root = entity();
    let inserted = Dialect
        .compile_insert(
            &root,
            &InsertCommand::new("Customer")
                .value("id", 1_u64)
                .value("display_name", "Riverside")
                .value("status", "ACTIVE")
                .value("password", "secret"),
        )
        .unwrap();
    assert_eq!(
        inserted.log_context.parameter_policies,
        [Plain, Masked, Plain, Credential]
    );
    let updated = Dialect
        .compile_guarded_update(
            &root,
            &UpdateCommand::new("Customer", 1_u64)
                .expected_version(2)
                .value("display_name", "Riverside"),
            &Expr::eq("password", "guard-secret"),
        )
        .unwrap();
    assert_eq!(
        updated.log_context.parameter_policies,
        [Masked, Plain, Plain, Plain, Credential]
    );
    let deleted = Dialect
        .compile_guarded_delete(
            &root,
            &DeleteCommand::new("Customer", 1_u64).expected_version(2),
            &Expr::eq("display_name", "Riverside"),
        )
        .unwrap();
    assert_eq!(
        deleted.log_context.parameter_policies,
        [Plain, Plain, Plain, Masked]
    );
    let recovered = Dialect
        .compile_guarded_recover(
            &root,
            &RecoverCommand::new("Customer", 1_u64, -3),
            &Expr::eq("status", "ACTIVE"),
        )
        .unwrap();
    assert_eq!(
        recovered.log_context.parameter_policies,
        [Plain, Plain, Plain, Plain]
    );
}

#[test]
fn batch_compilation_keeps_field_policy_for_each_repeated_value() {
    let root = entity();
    let mut insert = BatchInsertCommand::new("Customer");
    for id in [1_u64, 2] {
        insert.batch_values.push(
            InsertCommand::new("Customer")
                .value("id", id)
                .value("display_name", "Riverside")
                .value("password", "secret")
                .values,
        );
    }
    assert_eq!(
        Dialect
            .compile_batch_insert(&root, &insert)
            .unwrap()
            .log_context
            .parameter_policies,
        [Plain, Masked, Credential, Plain, Masked, Credential]
    );
    let mut update = BatchUpdateCommand::new("Customer", vec!["display_name".into()]);
    update.batch_ids = vec![1_u64.into(), 2_u64.into()];
    update.batch_values = insert.batch_values;
    update.batch_expected_versions = vec![Some(1), Some(2)];
    let compiled = Dialect.compile_batch_update(&root, &update).unwrap();
    assert_eq!(
        compiled.log_context.parameter_policies,
        [
            Plain, Masked, Plain, Masked, Plain, Plain, Plain, Plain, Plain, Plain, Plain, Plain,
            Plain, Plain
        ]
    );
    assert_eq!(
        compiled.params.len(),
        compiled.log_context.parameter_policies.len()
    );
}

#[test]
fn error_restores_scope_and_concurrent_compilers_do_not_share_policies() {
    let mut bindings = crate::SqlBindings::new();
    assert!(matches!(
        Dialect.compile_expr(&entity(), &Expr::eq("absent", "private"), &mut bindings),
        Err(SqlCompileError::UnknownField(_))
    ));
    Dialect
        .compile_expr(&entity(), &Expr::eq("status", "ACTIVE"), &mut bindings)
        .unwrap();
    assert_eq!(
        bindings
            .log_context(DatabaseKind::Sqlite)
            .parameter_policies,
        [Plain]
    );
    std::thread::scope(|scope| {
        for index in 0..64 {
            scope.spawn(move || {
                let field = if index % 2 == 0 {
                    "display_name"
                } else {
                    "status"
                };
                assert_eq!(
                    policies(&SelectQuery::new("Customer").filter(Expr::eq(field, "shared-value"))),
                    [if index % 2 == 0 { Masked } else { Plain }]
                );
            });
        }
    });
}
