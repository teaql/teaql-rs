//! Generated count APIs discovered through the producer's reverse-field Assist.
use school_management_service_core::{service_runtime, ServiceRuntimeConfig, Q};
use teaql_core::{CompactRow, SmartList, TraceKind, Value};
use teaql_runtime::UserContext;

fn counts(rows: &SmartList<CompactRow>, alias: &str) -> Vec<(i64, i64)> {
    rows.data
        .iter()
        .map(|row| {
            (
                row.get("id")
                    .and_then(Value::try_i64)
                    .expect("actual target identity"),
                row.get(alias)
                    .and_then(Value::try_i64)
                    .expect("count alias must exist; never default to zero"),
            )
        })
        .collect()
}

fn statements(
    context: &UserContext,
    root: &str,
    logging: bool,
    routes: &[&[&str]],
    intent: Option<(&str, &str)>,
    expected_counts: usize,
) -> Vec<serde_json::Value> {
    let logs = context.sql_logs();
    assert_eq!(logs.len(), if logging { routes.len() } else { 0 });
    if logging {
        assert_eq!(
            logs.iter()
                .filter(|entry| entry.sql.to_ascii_uppercase().contains("COUNT("))
                .count(),
            expected_counts
        );
    }
    logs.iter().zip(routes).map(|(entry, route)| {
        let nodes = &entry.trace_path;
        assert!(entry.operation.is_select());
        assert_eq!(nodes.len(), route.len() + 4);
        assert_eq!((nodes[0].kind, nodes[0].entity_type.as_str(), nodes[0].comment.as_str()), (TraceKind::Operation, root, "query"));
        assert_eq!((nodes[1].kind, nodes[1].entity_type.as_str(), nodes[1].comment.as_str()), (TraceKind::Request, root, ""));
        let mut owner = root;
        for (node, name) in nodes[2..nodes.len()-2].iter().zip(*route) {
            assert_eq!((node.kind, node.entity_type.as_str()), (TraceKind::Relation, *name));
            assert_eq!(node.comment, format!("{owner}.{name}"));
            owner = match *name { "school_type" | "school_type_list" => "SchoolType", "school_list" => "School", "platform" => "Platform", _ => panic!("unexpected model edge") };
        }
        assert_eq!((nodes[nodes.len()-2].kind, nodes[nodes.len()-2].entity_type.as_str()), (TraceKind::Provider, "sqlite"));
        assert_eq!((nodes[nodes.len()-1].kind, nodes[nodes.len()-1].entity_type.as_str()), (TraceKind::Sql, "select"));
        assert!(nodes.iter().all(|node| node.entity_id.is_none()));
        if let Some((comment, purpose)) = intent {
            assert_eq!(entry.comment.as_deref(), Some(comment));
            assert_eq!(entry.purpose.as_deref(), Some(purpose));
        }
        serde_json::json!({"root":root,"route":route,"edges":nodes.iter().filter(|node|node.kind==TraceKind::Relation)
            .map(|node|node.comment.clone()).collect::<Vec<_>>(),"sql":entry.sql,
            "comment":entry.comment,"purpose":entry.purpose,
            "path":nodes.iter().map(|node|serde_json::json!({"kind":format!("{:?}",node.kind),
                "entity":node.entity_type,"id":node.entity_id,"detail":node.comment})).collect::<Vec<_>>()})
    }).collect()
}

#[tokio::test]
async fn root_count_uses_full_filtered_membership_not_the_visible_page() {
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    for logging in [true, false] {
        for filtered in [false, true] {
            for include_all in [false, true] {
                if logging {
                    context.enable_all_sql_log();
                } else {
                    context.disable_sql_log();
                }
                context.clear_sql_logs();
                let request = Q::school_types_minimal()
                    .select_code()
                    .order_by_id_asc()
                    .limit(1)
                    .facet_by_platform_as_with_options(
                        "platforms",
                        Q::platforms_minimal()
                            .select_name()
                            .limit(10)
                            .count_school_types_with(
                                "typeCount",
                                Q::school_types_minimal().limit(10),
                            ),
                        include_all,
                    );
                let request = if filtered {
                    request.with_code_is("PRIMARY")
                } else {
                    request
                };
                let rows = request
                    .comment("load bounded counted facets")
                    .purpose("verify full filtered membership")
                    .execute_for_list(&context)
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1);
                let values = counts(rows.facet("platforms").unwrap(), "typeCount");
                assert_eq!(values, [(1, if filtered { 1 } else { 2 })]);
                let sql = statements(
                    &context,
                    "SchoolType",
                    logging,
                    &[&[], &["platform"], &["platform", "school_type_list"]],
                    Some((
                        "load bounded counted facets",
                        "verify full filtered membership",
                    )),
                    1,
                );
                eprintln!(
                    "COUNTED_FACET {}",
                    serde_json::json!({"branch":"root","logging":logging,"filtered":filtered,
            "includeAll":include_all,"visible":rows.len(),"counts":values,"sql":sql})
                );
            }
        }
    }
}

#[tokio::test]
async fn nested_count_retains_original_root_and_empty_parent_metadata() {
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    for logging in [true, false] {
        for include_all in [false, true] {
            if logging {
                context.enable_all_sql_log();
            } else {
                context.disable_sql_log();
            }
            context.clear_sql_logs();
            let rows = Q::schools_minimal()
                .select_name()
                .order_by_id_asc()
                .limit(1)
                .facet_by_school_type_as_with_options(
                    "types",
                    Q::school_types_minimal()
                        .select_code()
                        .order_by_id_asc()
                        .limit(1)
                        .count_schools_with("schoolCount", Q::schools_minimal().limit(10))
                        .facet_by_platform_as_with_options(
                            "platforms",
                            Q::platforms_minimal()
                                .select_name()
                                .limit(10)
                                .count_school_types_with(
                                    "typeCount",
                                    Q::school_types_minimal().limit(10),
                                ),
                            true,
                        ),
                    include_all,
                )
                .comment("load nested counted facets")
                .purpose("verify inherited nested count ancestry")
                .execute_for_list(&context)
                .await
                .unwrap();
            assert!(rows.is_empty());
            let types = rows.facet("types").unwrap();
            let type_counts = counts(types, "schoolCount");
            assert_eq!(
                type_counts,
                if include_all { vec![(1001, 0)] } else { vec![] }
            );
            let platform_counts = counts(
                types
                    .facet("platforms")
                    .expect("requested metadata survives empty target"),
                "typeCount",
            );
            assert_eq!(platform_counts, [(1, if include_all { 2 } else { 0 })]);
            // An empty target has no IDs to aggregate. The runtime skips that
            // count statement, while nested requested metadata still executes.
            let routes: &[&[&str]] = if include_all {
                &[
                    &[],
                    &["school_type"],
                    &["school_type", "school_list"],
                    &["school_type", "platform"],
                    &["school_type", "platform", "school_type_list"],
                ]
            } else {
                &[
                    &[],
                    &["school_type"],
                    &["school_type", "platform"],
                    &["school_type", "platform", "school_type_list"],
                ]
            };
            let sql = statements(
                &context,
                "School",
                logging,
                routes,
                Some((
                    "load nested counted facets",
                    "verify inherited nested count ancestry",
                )),
                if include_all { 2 } else { 1 },
            );
            eprintln!(
                "COUNTED_FACET {}",
                serde_json::json!({"branch":"nested","logging":logging,"includeAll":include_all,
            "visible":rows.len(),"counts":type_counts,"nestedCounts":platform_counts,"sql":sql})
            );
        }
    }
}

#[tokio::test]
async fn loaded_relation_count_retains_ancestor_and_empty_collection() {
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    for logging in [true, false] {
        for empty in [false, true] {
            for include_all in [false, true] {
                if logging {
                    context.enable_all_sql_log();
                } else {
                    context.disable_sql_log();
                }
                context.clear_sql_logs();
                let children = Q::school_types_minimal()
                    .select_code()
                    .order_by_id_asc()
                    .limit(1)
                    .facet_by_platform_as_with_options(
                        "platforms",
                        Q::platforms_minimal()
                            .select_name()
                            .limit(10)
                            .count_school_types_with(
                                "typeCount",
                                Q::school_types_minimal().limit(10),
                            ),
                        include_all,
                    );
                let children = if empty {
                    children.with_code_is("NO_MATCH")
                } else {
                    children
                };
                let rows = Q::platforms_minimal()
                    .select_name()
                    .order_by_id_asc()
                    .limit(1)
                    .select_school_type_list_with(children)
                    .comment("load counted relation")
                    .purpose("verify loaded count ancestry")
                    .execute_for_list(&context)
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1);
                let handle = rows[0].school_type_list();
                let children = handle
                    .value()
                    .expect("selected typed collection is loaded, including empty");
                assert_eq!(children.len(), if empty { 0 } else { 1 });
                assert_eq!(
                    handle.state(),
                    if empty {
                        teaql_runtime::LoadedRelation::Empty
                    } else {
                        teaql_runtime::LoadedRelation::Loaded
                    }
                );
                let values = counts(
                    children
                        .facet("platforms")
                        .expect("selected empty relation retains Facet metadata"),
                    "typeCount",
                );
                assert_eq!(
                    values,
                    if empty && !include_all {
                        vec![]
                    } else {
                        vec![(1, if empty { 0 } else { 2 })]
                    }
                );
                // No candidate IDs means no aggregate SQL; do not invent a
                // physical trace node for a deliberately skipped statement.
                let routes: &[&[&str]] = if empty && !include_all {
                    &[
                        &[],
                        &["school_type_list"],
                        &["school_type_list", "platform"],
                    ]
                } else {
                    &[
                        &[],
                        &["school_type_list"],
                        &["school_type_list", "platform"],
                        &["school_type_list", "platform", "school_type_list"],
                    ]
                };
                let sql = statements(
                    &context,
                    "Platform",
                    logging,
                    routes,
                    Some(("load counted relation", "verify loaded count ancestry")),
                    if empty && !include_all { 0 } else { 1 },
                );
                eprintln!(
                    "COUNTED_FACET {}",
                    serde_json::json!({"branch":"loaded","logging":logging,"empty":empty,
            "includeAll":include_all,"visible":children.len(),"counts":values,"sql":sql})
                );
            }
        }
    }
}

#[tokio::test]
async fn future_count_binding_masks_first_statement_but_not_the_next_request() {
    const SECRET: &str = "count-private-future-school-name";
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    for logging in [true, false] {
        if logging {
            context.enable_all_sql_log();
        } else {
            context.disable_sql_log();
        }
        context.clear_sql_logs();
        let rows = Q::school_types_minimal()
            .select_code()
            .order_by_id_asc()
            .limit(1)
            .facet_by_platform_as_with_options(
                "platforms",
                Q::platforms_minimal()
                    .select_name()
                    .limit(10)
                    .count_school_types_with(
                        "typeCount",
                        Q::school_types_minimal()
                            .limit(10)
                            .with_school_list_matching(
                                Q::schools_minimal().with_name_is(SECRET).limit(10),
                            ),
                    ),
                true,
            )
            .comment(format!("load counted choices for {SECRET}"))
            .purpose(format!("verify count privacy for {SECRET}"))
            .execute_for_list(&context)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let values = counts(rows.facet("platforms").unwrap(), "typeCount");
        assert_eq!(values, [(1, 0)]);
        let sql = statements(
            &context,
            "SchoolType",
            logging,
            &[&[], &["platform"], &["platform", "school_type_list"]],
            None,
            1,
        );
        assert!(!serde_json::to_string(&sql).unwrap().contains(SECRET));
        for entry in &sql {
            assert!(entry["comment"]
                .as_str()
                .unwrap()
                .starts_with("load counted choices for "));
            assert!(entry["purpose"]
                .as_str()
                .unwrap()
                .starts_with("verify count privacy for "));
        }
        context.clear_sql_logs();
        let next = Q::platforms_minimal()
            .select_name()
            .limit(1)
            .comment(format!("independent {SECRET}"))
            .purpose("independent next request")
            .execute_for_list(&context)
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        let next_comment = format!("independent {SECRET}");
        let next_sql = statements(
            &context,
            "Platform",
            logging,
            &[&[]],
            Some((&next_comment, "independent next request")),
            0,
        );
        eprintln!(
            "COUNTED_FACET {}",
            serde_json::json!({"branch":"privacy","logging":logging,"visible":rows.len(),
                "counts":values,"sql":sql,"nextVisible":next.len(),"nextSql":next_sql})
        );
    }
}
