//! Generated field Assist -> real SQLite -> raw query intent and safe path assertions.
//! Expected nodes are compared only; no trace frames are supplied to execution.
use super::{AuditCapture, Observation, Outcome};
use teaql_data_service::DataServiceOperation;
use teaql_runtime::UserContext;
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind, TraceNode, Value};
use trace_chain_service_core::{AuditedSave as _, CustomerOrder, E, LedgerEntity as _, Q};

const SECRET: &str = "RUST-PRIVATE-AGGREGATE";
const COMMENT: &str = "inspect RUST-PRIVATE-AGGREGATE";
const PURPOSE: &str = "verify original aggregate ancestry";

fn count(row: &CustomerOrder, alias: &str) -> Option<i64> {
    // Native Entity projection includes dynamic aliases without changing the ledger.
    row.clone()
        .into_values()
        .get(alias)
        .and_then(Value::try_i64)
}

fn nodes(values: &[TraceNode]) -> Vec<serde_json::Value> {
    values
        .iter()
        .map(|n| {
            serde_json::json!({"kind":format!("{:?}",n.kind),
        "name":n.entity_type,"entityId":n.entity_id,"comment":n.comment})
        })
        .collect()
}

fn relation_nodes(root: &str, route: &[&str]) -> Vec<TraceNode> {
    let mut owner = root;
    route
        .iter()
        .map(|name| {
            let node =
                TraceNode::typed(TraceKind::Relation, *name, None, format!("{owner}.{name}"));
            owner = match *name {
                "customer_order" => "CustomerOrder",
                "order_item_list" => "OrderItem",
                _ => panic!("unexpected model edge"),
            };
            node
        })
        .collect()
}

fn statements(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
    root: &str,
    routes: &[&[&str]],
    logging: bool,
    comment: &str,
    purpose: &str,
    private: bool,
) -> serde_json::Value {
    let raw = observation.metadata();
    assert_eq!(
        raw.len(),
        routes.len(),
        "all physical query metadata, including logging-off"
    );
    assert!(observation.commands().is_empty(), "query must not write");
    assert!(
        capture.events().is_empty(),
        "query must not commit audit events"
    );
    if !logging {
        // Disabling diagnostics intentionally skips physical metadata allocation.
        // Do not force capture or invent paths merely to satisfy the test.
        assert!(context.sql_logs().is_empty());
        for entry in &raw {
            assert_eq!(entry.operation, DataServiceOperation::Query);
            assert!(entry.backend.is_empty());
            assert!(entry.parameterized_query.is_none());
            assert!(entry.trace_chain.is_empty());
            assert!(entry.comment.is_none());
        }
        return serde_json::json!({"raw":[],"safe":[],"diagnosticsDisabled":true,
            "unrecordedQueries":raw.len(),"mutationCommands":0,"committedAudits":0});
    }
    let mut facts = Vec::new();
    for (entry, route) in raw.iter().zip(routes) {
        assert_eq!(entry.operation, DataServiceOperation::Query);
        assert_eq!(entry.backend, "sqlite");
        assert_eq!(entry.comment.as_deref(), Some(comment));
        assert_eq!(entry.sql_log.execution_outcome.unwrap().as_str(), "success");
        let mut expected = vec![
            TraceNode::typed(TraceKind::Comment, root, None, comment),
            TraceNode::typed(TraceKind::Purpose, root, None, purpose),
        ];
        expected.extend(relation_nodes(root, route));
        assert_eq!(
            entry.trace_chain, expected,
            "exact original provider request path"
        );
        let sql = entry.parameterized_query.as_deref().expect("physical SQL");
        assert!(sql.to_ascii_uppercase().starts_with("SELECT"));
        facts.push(serde_json::json!({"path":nodes(&entry.trace_chain),"sql":sql,
            "comment":entry.comment,"purpose":purpose,"outcome":entry.sql_log.execution_outcome.unwrap().as_str()}));
    }
    let safe = context.sql_logs();
    assert_eq!(safe.len(), if logging { routes.len() } else { 0 });
    let mut projected = Vec::new();
    for (entry, route) in safe.iter().zip(routes) {
        assert!(entry.operation.is_select());
        assert_eq!(
            entry.comment.as_deref(),
            Some(if private {
                "inspect [REDACTED]"
            } else {
                comment
            })
        );
        assert_eq!(entry.purpose.as_deref(), Some(purpose));
        let mut expected = vec![
            TraceNode::typed(TraceKind::Operation, root, None, "query"),
            TraceNode::typed(TraceKind::Request, root, None, ""),
        ];
        expected.extend(relation_nodes(root, route));
        expected.extend([
            TraceNode::typed(TraceKind::Provider, "sqlite", None, ""),
            TraceNode::typed(TraceKind::Sql, "select", None, ""),
        ]);
        assert_eq!(
            entry.trace_path, expected,
            "canonical safe path is runtime-owned"
        );
        if private {
            assert!(!format!("{entry:?}").contains(SECRET));
        }
        projected.push(serde_json::json!({"path":nodes(&entry.trace_path),"sql":entry.sql,
            "comment":entry.comment,"purpose":entry.purpose,"outcome":entry.log_context.execution_outcome.unwrap().as_str()}));
    }
    serde_json::json!({"raw":facts,"safe":projected,"mutationCommands":observation.commands().len(),
        "committedAudits":capture.events().len()})
}

fn reset(context: &UserContext, capture: &AuditCapture, observation: &Observation) {
    context.clear_sql_logs();
    capture.clear();
    observation.clear();
}

async fn seed(context: &UserContext) -> Outcome<(u64, u64, Vec<u64>)> {
    let mut root = Q::customer_orders()
        .comment("prepare aggregate fixture")
        .purpose("isolate generated query acceptance")
        .new_entity(context);
    root.update_platform_id(1);
    root.update_order_number("AGGREGATE-ORDER");
    root.update_description("visible aggregate detail");
    let mut ids = Vec::new();
    for name in [SECRET, "public sibling"] {
        let mut child = Q::order_items()
            .comment("prepare aggregate child")
            .purpose("verify filtered counts and membership")
            .new_entity(context);
        child.update_customer_order_id(root.id());
        child.update_name(name);
        ids.push(child.id());
        root.include_pending_mutations_from(&child)?;
    }
    let mut payment = Q::payments()
        .comment("prepare aggregate ancestor")
        .purpose("verify nested query root")
        .new_entity(context);
    payment.update_customer_order_id(root.id());
    payment.update_reference_code("AGGREGATE-PAYMENT");
    let order_id = root.id();
    let payment_id = payment.id();
    root.include_pending_mutations_from(&payment)?;
    root.audit_as("seed one aggregate graph")
        .save(context)
        .await?;
    Ok((order_id, payment_id, ids))
}

pub async fn verify(
    context: &mut UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let (order_id, payment_id, item_ids) = seed(context).await?;
    for nested in [false, true] {
        for logging in [false, true] {
            if logging {
                context.enable_all_sql_log();
            } else {
                context.disable_sql_log();
            }
            reset(context, capture, observation);
            let counted = Q::customer_orders_minimal()
                .with_id_is(order_id)
                .limit(1)
                .count_order_items_with(
                    "selectedItemCount",
                    Q::order_items_minimal().with_name_is(SECRET).limit(10),
                );
            let order: CustomerOrder = if nested {
                let payment = Q::payments_minimal()
                    .with_id_is(payment_id)
                    .limit(1)
                    .select_customer_order_with(counted)
                    .comment(COMMENT)
                    .purpose(PURPOSE)
                    .execute_for_one(context)
                    .await?
                    .ok_or("payment")?;
                E::payment(&payment)
                    .get_customer_order()
                    .eval()
                    .ok_or("loaded order")?
                    .clone()
            } else {
                counted
                    .comment(COMMENT)
                    .purpose(PURPOSE)
                    .execute_for_one(context)
                    .await?
                    .ok_or("order")?
            };
            assert_eq!(
                count(&order, "selectedItemCount"),
                Some(1),
                "filtered count alias must exist"
            );
            let routes: Vec<&[&str]> = if nested {
                vec![
                    &[],
                    &["customer_order"],
                    &["customer_order", "order_item_list"],
                ]
            } else {
                vec![&[], &["order_item_list"]]
            };
            let mut report = statements(
                context,
                capture,
                observation,
                if nested { "Payment" } else { "CustomerOrder" },
                &routes,
                logging,
                COMMENT,
                PURPOSE,
                true,
            );
            if logging {
                assert_eq!(
                    observation
                        .metadata()
                        .iter()
                        .filter(|m| m
                            .parameterized_query
                            .as_deref()
                            .unwrap()
                            .to_ascii_uppercase()
                            .contains("COUNT("))
                        .count(),
                    1
                );
            }
            report["nested"] = nested.into();
            report["logging"] = logging.into();
            report["count"] = 1.into();
            report["rootID"] = order_id.into();
            println!("RUST_AGGREGATE_OBSERVED {report}");

            reset(context, capture, observation);
            let numeric = Q::customer_orders_minimal()
                .with_id_is(order_id)
                .group_by_id()
                .count_as("groupCount")
                .limit(1);
            let grouped = if nested {
                let payment = Q::payments_minimal()
                    .with_id_is(payment_id)
                    .limit(1)
                    .select_customer_order_with(numeric)
                    .comment(COMMENT)
                    .purpose("numeric grouping has no edge")
                    .execute_for_one(context)
                    .await?
                    .ok_or("payment")?;
                E::payment(&payment)
                    .get_customer_order()
                    .eval()
                    .ok_or("numeric order")?
                    .clone()
            } else {
                numeric
                    .comment(COMMENT)
                    .purpose("numeric grouping has no edge")
                    .execute_for_one(context)
                    .await?
                    .ok_or("numeric order")?
            };
            assert_eq!(
                count(&grouped, "groupCount"),
                Some(1),
                "numeric grouped count must exist"
            );
            let routes: Vec<&[&str]> = if nested {
                vec![&[], &["customer_order"]]
            } else {
                vec![&[]]
            };
            let mut report = statements(
                context,
                capture,
                observation,
                if nested { "Payment" } else { "CustomerOrder" },
                &routes,
                logging,
                COMMENT,
                "numeric grouping has no edge",
                false,
            );
            if logging {
                let sql = observation
                    .metadata()
                    .last()
                    .unwrap()
                    .parameterized_query
                    .clone()
                    .unwrap()
                    .to_ascii_uppercase();
                assert!(sql.contains("COUNT(") && sql.contains("GROUP BY"));
            }
            report["nested"] = nested.into();
            report["logging"] = logging.into();
            report["count"] = 1.into();
            println!("RUST_AGGREGATE_NUMERIC {report}");

            for filtered in [false, true] {
                reset(context, capture, observation);
                let detail = Q::customer_orders_minimal()
                    .select_description()
                    .with_id_is(if filtered { 0 } else { order_id })
                    .limit(1);
                let children = Q::order_items_minimal()
                    .select_name()
                    .select_customer_order_with(detail)
                    .order_by_id_asc()
                    .limit(10)
                    .top_n_probe_parent_threshold(0);
                let request = Q::customer_orders_minimal()
                    .with_id_is(order_id)
                    .limit(1)
                    .select_order_item_list_with(children)
                    .count_order_items_with(
                        "selectedItemCount",
                        Q::order_items_minimal().with_name_is(SECRET).limit(10),
                    );
                let order = if nested {
                    let payment = Q::payments_minimal()
                        .with_id_is(payment_id)
                        .limit(1)
                        .select_customer_order_with(request)
                        .comment(COMMENT)
                        .purpose(PURPOSE)
                        .execute_for_one(context)
                        .await?
                        .ok_or("payment")?;
                    E::payment(&payment)
                        .get_customer_order()
                        .eval()
                        .ok_or("order")?
                        .clone()
                } else {
                    request
                        .comment(COMMENT)
                        .purpose(PURPOSE)
                        .execute_for_one(context)
                        .await?
                        .ok_or("order")?
                };
                assert_eq!(
                    count(&order, "selectedItemCount"),
                    Some(1),
                    "count must not follow hydrated FK objects"
                );
                let handle = order.order_item_list();
                let members = handle.value().ok_or("loaded members")?;
                assert_eq!(members.len(), 2);
                let mut foreign_ids = Vec::new();
                for (member, expected_id) in members.iter().zip(&item_ids) {
                    assert_eq!(E::order_item(member).get_id().eval(), Some(*expected_id));
                    assert_eq!(
                        E::order_item(member).get_customer_order_id().eval(),
                        Some(order_id)
                    );
                    foreign_ids.push(order_id);
                    let parent = E::order_item(member)
                        .get_customer_order()
                        .eval()
                        .ok_or("known parent identity")?;
                    assert_eq!(E::customer_order(parent).get_id().eval(), Some(order_id));
                    assert_eq!(
                        parent.is_field_loaded("description"),
                        !filtered,
                        "filtered detail is NotLoaded, not Null"
                    );
                    if !filtered {
                        assert_eq!(
                            E::customer_order(parent)
                                .get_description()
                                .eval()
                                .as_deref(),
                            Some("visible aggregate detail")
                        );
                    }
                }
                let mut routes: Vec<&[&str]> = vec![
                    &[],
                    &["order_item_list"],
                    &["order_item_list"],
                    &["order_item_list", "customer_order"],
                ];
                if nested {
                    routes = vec![
                        &[],
                        &["customer_order"],
                        &["customer_order", "order_item_list"],
                        &["customer_order", "order_item_list"],
                        &["customer_order", "order_item_list", "customer_order"],
                    ];
                }
                let mut report = statements(
                    context,
                    capture,
                    observation,
                    if nested { "Payment" } else { "CustomerOrder" },
                    &routes,
                    logging,
                    COMMENT,
                    PURPOSE,
                    true,
                );
                report["nested"] = nested.into();
                report["logging"] = logging.into();
                report["filtered"] = filtered.into();
                report["rootID"] = order_id.into();
                report["foreignIDs"] = foreign_ids.into();
                report["members"] = 2.into();
                report["count"] = 1.into();
                report["detail"] = if filtered { "NotLoaded" } else { "Loaded" }.into();
                println!("RUST_AGGREGATE_MEMBERSHIP {report}");
                if filtered {
                    reset(context, capture, observation);
                    let full = Q::order_items_minimal()
                        .with_id_is(item_ids[0])
                        .limit(1)
                        .select_name()
                        .select_customer_order_with(
                            Q::customer_orders_minimal().select_description().limit(1),
                        )
                        .comment("independent full detail")
                        .purpose("verify edge-owned view")
                        .execute_for_one(context)
                        .await?
                        .ok_or("full item")?;
                    let parent = E::order_item(&full)
                        .get_customer_order()
                        .eval()
                        .ok_or("full parent")?;
                    assert_eq!(
                        E::customer_order(parent)
                            .get_description()
                            .eval()
                            .as_deref(),
                        Some("visible aggregate detail")
                    );
                    for member in members.iter() {
                        assert!(
                            !E::order_item(member)
                                .get_customer_order()
                                .eval()
                                .unwrap()
                                .is_field_loaded("description")
                        );
                    }
                    let mut report = statements(
                        context,
                        capture,
                        observation,
                        "OrderItem",
                        &[&[], &["customer_order"]],
                        logging,
                        "independent full detail",
                        "verify edge-owned view",
                        false,
                    );
                    report["nested"] = nested.into();
                    report["logging"] = logging.into();
                    report["originalDetailsStillNotLoaded"] = true.into();
                    report["fullDetailIndependent"] = true.into();
                    println!("RUST_AGGREGATE_FORWARD {report}");
                }
            }
        }
    }
    context.enable_all_sql_log();
    reset(context, capture, observation);
    println!(
        "TC-SQL-10 RUST GENERATED AGGREGATION PASSED raw/safe paths, numeric grouping, scoped counts and independent detail"
    );
    Ok(())
}
