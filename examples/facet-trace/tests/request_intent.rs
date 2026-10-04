use school_management_service_core::{service_runtime, ServiceRuntimeConfig, Q};
use teaql_core::{Entity as _, TraceKind, Value};

#[tokio::test]
async fn generated_facet_inherits_explicit_root_comment_and_purpose() {
    eprintln!("FACET_PHASE runtime initialization");
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    eprintln!("FACET_PHASE bootstrap");
    context.ensure_schema().await.unwrap();
    eprintln!("FACET_PHASE root and derived query");
    for logging in [true, false] {
        for include_all in [false, true] {
            if logging {
                context.enable_all_sql_log();
            } else {
                context.disable_sql_log();
            }
            context.clear_sql_logs();
            let rows = Q::schools()
                .facet_by_school_type_as_with_options(
                    "typeChoices",
                    Q::school_types_minimal()
                        .select_code()
                        .order_by_id_asc()
                        .limit(10)
                        .facet_by_platform_as_with_options(
                            "originChoices",
                            Q::platforms_minimal().select_name().limit(10),
                            false,
                        ),
                    include_all,
                )
                .limit(10)
                .comment("load schools and type facets")
                .purpose("render the school search page")
                .execute_for_list(&context)
                .await
                .unwrap();
            let logs = context.sql_logs();
            for entry in &logs {
                eprintln!(
                    "FACET_SQL {:?} {:?} rows={:?} path={:?}",
                    entry.operation, entry.sql, entry.result_count, entry.trace_path
                );
            }
            assert!(
                rows.is_empty(),
                "bootstrap does not invent business records"
            );
            let types = rows
                .facet("typeChoices")
                .expect("includeAll type candidates");
            assert_eq!(types.len(), if include_all { 2 } else { 0 });
            if include_all {
                assert_eq!(types[0].get("code"), Some(&Value::from("PRIMARY")));
                assert_eq!(types[1].get("code"), Some(&Value::from("SECONDARY")));
            }
            let platforms = types
                .facet("originChoices")
                .expect("nested Facet must survive materialization");
            assert_eq!(platforms.len(), if include_all { 1 } else { 0 });
            if include_all {
                assert_eq!(
                    platforms[0].get("name"),
                    Some(&Value::from("Campus Learning Platform"))
                );
            }
            eprintln!(
                "FACET_CASE {}",
                serde_json::json!({"branch":"root", "logging":logging,
            "includeAll":include_all, "rows":rows.len(), "types":types.len(),
            "platforms":platforms.len(), "physical":logs.len()})
            );
            if !logging {
                assert!(logs.is_empty());
                continue;
            }
            assert_eq!(
                logs.len(),
                3,
                "root and both Facets reach the real provider"
            );
            let routes = [vec![], vec!["school_type"], vec!["school_type", "platform"]];
            for (entry, route) in logs.iter().zip(routes) {
                assert!(entry.operation.is_select());
                assert_eq!(
                    entry.comment.as_deref(),
                    Some("load schools and type facets")
                );
                assert_eq!(
                    entry.purpose.as_deref(),
                    Some("render the school search page")
                );
                assert_eq!(entry.trace_path[0].kind, TraceKind::Operation);
                assert_eq!(entry.trace_path[0].entity_type, "School");
                assert_eq!(entry.trace_path[1].kind, TraceKind::Request);
                assert_eq!(entry.trace_path[1].entity_type, "School");
                assert_eq!(entry.trace_path.len(), route.len() + 4);
                let tail = &entry.trace_path[entry.trace_path.len() - 2..];
                assert_eq!(
                    (tail[0].kind, tail[0].entity_type.as_str()),
                    (TraceKind::Provider, "sqlite")
                );
                assert_eq!(
                    (tail[1].kind, tail[1].entity_type.as_str()),
                    (TraceKind::Sql, "select")
                );
                assert_eq!(
                    entry
                        .trace_path
                        .iter()
                        .filter(|node| node.kind == TraceKind::Relation)
                        .map(|node| node.entity_type.as_str())
                        .collect::<Vec<_>>(),
                    route
                );
                assert_eq!(
                    entry
                        .trace_path
                        .iter()
                        .filter(|node| node.kind == TraceKind::Operation)
                        .count(),
                    1
                );
                assert_eq!(
                    entry
                        .trace_path
                        .iter()
                        .filter(|node| node.kind == TraceKind::Request)
                        .count(),
                    1
                );
                assert!(!entry.trace_path.iter().any(|node| matches!(
                    node.kind,
                    TraceKind::Comment | TraceKind::Purpose | TraceKind::AuditReason
                )));
            }
        }
    }
}

#[tokio::test]
async fn future_facet_binding_masks_the_first_root_statement() {
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    const SECRET: &str = "private-future-school-name";
    for logging in [true, false] {
        if logging {
            context.enable_all_sql_log();
        } else {
            context.disable_sql_log();
        }
        context.clear_sql_logs();
        let rows = Q::schools_minimal()
            .select_name()
            .facet_by_school_type_as_with_options(
                "types",
                Q::school_types_minimal()
                    .select_code()
                    .limit(10)
                    .with_school_list_matching(Q::schools_minimal().with_name_is(SECRET).limit(10))
                    .facet_by_platform_as_with_options(
                        "origins",
                        Q::platforms_minimal().select_name().limit(10),
                        true,
                    ),
                true,
            )
            .limit(10)
            .comment(format!("what: load choices for {SECRET}"))
            .purpose(format!("why: check privacy of {SECRET}"))
            .execute_for_list(&context)
            .await
            .unwrap();
        assert!(rows.is_empty());
        let types = rows.facet("types").unwrap();
        assert!(
            types.is_empty(),
            "the private child predicate still executes"
        );
        assert_eq!(
            types.facet("origins").unwrap().len(),
            1,
            "includeAll metadata survives an empty parent result"
        );
        let logs = context.sql_logs();
        eprintln!(
            "FACET_CASE {}",
            serde_json::json!({"branch":"root-privacy", "logging":logging,
            "rows":rows.len(), "types":types.len(), "platforms":types.facet("origins").unwrap().len(),
            "physical":logs.len()})
        );
        if !logging {
            assert!(logs.is_empty());
            continue;
        }
        assert_eq!(logs.len(), 3);
        for entry in logs {
            assert!(
                !format!("{entry:?}").contains(SECRET),
                "a future-only binding must be masked before the first root SQL: {entry:?}"
            );
            assert!(entry
                .comment
                .as_deref()
                .unwrap()
                .starts_with("what: load choices for "));
            assert!(entry
                .purpose
                .as_deref()
                .unwrap()
                .starts_with("why: check privacy of "));
            assert_eq!(entry.trace_path[0].entity_type, "School");
        }
        context.clear_sql_logs();
        Q::platforms_minimal()
            .select_name()
            .limit(1)
            .comment(format!("independent {SECRET}"))
            .purpose("not bound to a masked field")
            .execute_for_list(&context)
            .await
            .unwrap();
        assert_eq!(
            context.sql_logs()[0].comment.as_deref(),
            Some(format!("independent {SECRET}").as_str()),
            "request-local redaction must not become Context state"
        );
    }
}

#[tokio::test]
async fn loaded_typed_relation_retains_facets_even_when_empty() {
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    for logging in [true, false] {
        for (empty, include_all) in [(false, false), (false, true), (true, false), (true, true)] {
            if logging {
                context.enable_all_sql_log();
            } else {
                context.disable_sql_log();
            }
            context.clear_sql_logs();
            let child = Q::school_types_minimal()
                .select_code()
                .order_by_id_asc()
                .limit(1)
                .facet_by_platform_as_with_options(
                    "loadedOriginChoices",
                    Q::platforms_minimal().select_name().limit(10),
                    include_all,
                );
            let child = if empty {
                child.with_code_is("NO_MATCH")
            } else {
                child
            };
            let rows = Q::platforms_minimal()
                .select_name()
                .select_school_type_list_with(child)
                .limit(1)
                .comment("load typed collection facets")
                .purpose("render bounded parent details with choices")
                .execute_for_list(&context)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let handle = rows[0].school_type_list();
            let children = handle
                .value()
                .expect("explicitly selected relation is loaded");
            assert_eq!(children.len(), if empty { 0 } else { 1 });
            if !empty {
                assert_eq!(children[0].id(), 1001);
            }
            assert_eq!(
                handle.state(),
                if empty {
                    teaql_runtime::LoadedRelation::Empty
                } else {
                    teaql_runtime::LoadedRelation::Loaded
                }
            );
            let facets = children
                .facet("loadedOriginChoices")
                .expect("loaded relation SmartList must retain its Facet metadata");
            assert_eq!(facets.len(), if empty && !include_all { 0 } else { 1 });
            assert!(
                !format!("{:?}", rows[0].clone().into_values()).contains("loadedOriginChoices"),
                "query metadata must never become persistent entity fields"
            );
            assert!(rows[0].dirty_fields().unwrap_or_default().is_empty());
            let logs = context.sql_logs();
            eprintln!(
                "FACET_CASE {}",
                serde_json::json!({"branch":"loaded", "logging":logging,
                "empty":empty, "includeAll":include_all, "rows":rows.len(), "children":children.len(),
                "platforms":facets.len(), "physical":logs.len()})
            );
            for log in &logs {
                eprintln!(
                    "LOADED_FACET empty={empty} include_all={include_all} {:?}",
                    log.trace_path
                );
            }
            if !logging {
                assert!(logs.is_empty());
                continue;
            }
            assert_eq!(
                logs.len(),
                3,
                "root, loaded child and its Facet are physical statements"
            );
            assert_eq!(
                logs.iter()
                    .map(|log| log
                        .trace_path
                        .iter()
                        .filter(|node| node.kind == TraceKind::Relation)
                        .map(|node| node.entity_type.as_str())
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
                [
                    vec![],
                    vec!["school_type_list"],
                    vec!["school_type_list", "platform"]
                ]
            );
            if !empty || include_all {
                assert!(logs.len() >= 3);
            }
            if !empty || include_all {
                assert!(
                    logs.iter().any(|log| log
                        .trace_path
                        .iter()
                        .filter(|node| node.kind == TraceKind::Relation)
                        .map(|node| node.entity_type.as_str())
                        .collect::<Vec<_>>()
                        == ["school_type_list", "platform"]),
                    "the derived Facet must reach SQLite with its full relation ancestry"
                );
            }
            for log in logs {
                assert!(log.operation.is_select());
                assert_eq!(log.comment.as_deref(), Some("load typed collection facets"));
                assert_eq!(
                    log.purpose.as_deref(),
                    Some("render bounded parent details with choices")
                );
                assert_eq!(log.trace_path[0].kind, TraceKind::Operation);
                assert_eq!(log.trace_path[0].entity_type, "Platform");
                assert_eq!(log.trace_path[1].kind, TraceKind::Request);
                assert_eq!(log.trace_path[1].entity_type, "Platform");
                let route: Vec<_> = log
                    .trace_path
                    .iter()
                    .filter(|node| node.kind == TraceKind::Relation)
                    .map(|node| node.entity_type.as_str())
                    .collect();
                assert!(
                    route.is_empty()
                        || route == ["school_type_list"]
                        || route == ["school_type_list", "platform"],
                    "actual route {route:?}"
                );
                assert_eq!(log.trace_path.len(), route.len() + 4);
                let tail = &log.trace_path[log.trace_path.len() - 2..];
                assert_eq!(
                    (tail[0].kind, tail[0].entity_type.as_str()),
                    (TraceKind::Provider, "sqlite")
                );
                assert_eq!(
                    (tail[1].kind, tail[1].entity_type.as_str()),
                    (TraceKind::Sql, "select")
                );
            }
        }
    }
}

#[tokio::test]
async fn loaded_relation_future_binding_masks_root_sql() {
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    const SECRET: &str = "private-loaded-facet-school-name";
    for logging in [true, false] {
        if logging {
            context.enable_all_sql_log();
        } else {
            context.disable_sql_log();
        }
        context.clear_sql_logs();
        let rows = Q::platforms_minimal()
            .select_name()
            .select_school_type_list_with(
                Q::school_types_minimal()
                    .select_code()
                    .limit(1)
                    .facet_by_platform_as_with_options(
                        "loadedPrivateChoices",
                        Q::platforms_minimal()
                            .select_name()
                            .limit(10)
                            .with_school_list_matching(
                                Q::schools_minimal().with_name_is(SECRET).limit(10),
                            ),
                        true,
                    ),
            )
            .limit(1)
            .comment(format!("what: compare {SECRET}"))
            .purpose(format!("why: safely display {SECRET}"))
            .execute_for_list(&context)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let handle = rows[0].school_type_list();
        let children = handle.value().unwrap();
        assert_eq!(children.len(), 1);
        assert!(children
            .facet("loadedPrivateChoices")
            .expect("loaded Facet exists")
            .is_empty());
        let logs = context.sql_logs();
        eprintln!(
            "FACET_CASE {}",
            serde_json::json!({"branch":"loaded-privacy", "logging":logging,
            "rows":rows.len(), "children":children.len(),
            "platforms":children.facet("loadedPrivateChoices").unwrap().len(), "physical":logs.len()})
        );
        if !logging {
            assert!(logs.is_empty());
            continue;
        }
        assert!(logs.len() >= 3);
        for log in logs {
            assert!(
                !format!("{log:?}").contains(SECRET),
                "loaded Facet binding was lost before a physical statement: {log:?}"
            );
        }
        context.clear_sql_logs();
        Q::platforms_minimal()
            .select_name()
            .limit(1)
            .comment(format!("independent {SECRET}"))
            .purpose("independent loaded relation privacy control")
            .execute_for_list(&context)
            .await
            .unwrap();
        assert_eq!(
            context.sql_logs()[0].comment.as_deref(),
            Some(format!("independent {SECRET}").as_str())
        );
    }
}
