//! Generated page/COUNT, relation privacy and per-row mutation ownership.
use super::{AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph};
use teaql_runtime::{RawAuditEventKind, UserContext};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q};

pub async fn paged_graph(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    const SECRET: &str = "PAGE-PRIVATE-CHILD-NAME";
    let mut ids = Vec::new();
    let mut run = String::new();
    for index in 0..3 {
        let mut root = Q::customer_orders()
            .comment("what: prepare page root")
            .purpose("why: isolate this replay")
            .new_entity(context);
        if run.is_empty() {
            run = format!("PAGE-RUN-{}", root.id());
        }
        root.update_platform_id(1);
        root.update_order_number(format!("PAGE-{index}"));
        root.update_description(run.clone());
        ids.push(root.id());
        let mut child = Q::order_items()
            .comment("what: prepare page child")
            .purpose("why: verify relation provenance")
            .new_entity(context);
        child.update_customer_order_id(root.id());
        child.update_name(SECRET);
        root.include_pending_mutations_from(&child)?;
        root.audit_as("create isolated page graph")
            .save(context)
            .await?;
    }
    observation.clear();
    context.clear_sql_logs();
    let mut page = Q::customer_orders()
        .with_description_is(run.clone())
        .select_order_item_list_with(
            Q::order_items()
                .with_name_is(SECRET)
                .order_by_id_asc()
                .limit(2),
        )
        .order_by_id_asc()
        .comment(format!("load {run} matching {SECRET}"))
        .purpose(format!("render {run} with {SECRET}"))
        .execute_for_page(context, 1, 2)
        .await?;
    assert_eq!(page.total_count, Some(3));
    assert_eq!(page.data.len(), 2);
    assert_eq!(
        page.data
            .iter()
            .map(|row| E::customer_order(row).get_id().eval().unwrap())
            .collect::<Vec<_>>(),
        ids[1..]
    );
    for row in &page.data {
        assert_eq!(
            E::customer_order(row).get_order_item_list().size().eval(),
            Some(1)
        );
        assert_eq!(
            E::customer_order(row)
                .get_order_item_list()
                .first()
                .get_name()
                .eval()
                .as_deref(),
            Some(SECRET)
        );
    }
    let logs = context.sql_logs();
    assert!(
        !format!("{logs:?}").contains(SECRET),
        "root COUNT, page and relation logs must all redact descendant secrets"
    );
    let counts: Vec<_> = logs
        .iter()
        .filter(|log| log.sql.to_uppercase().contains("COUNT("))
        .collect();
    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].result_count, Some(1));
    assert!(
        !counts[0].sql.contains("order_item_data"),
        "classification does not execute the removed relation"
    );
    for log in &logs {
        assert!(
            log.comment.as_deref().unwrap().contains(&run),
            "visible input is not blanket-masked"
        );
        assert!(log.purpose.as_deref().unwrap().contains(&run));
        assert!(
            log.trace_path
                .iter()
                .any(|node| node.kind == TraceKind::Request && node.entity_type == "CustomerOrder")
        );
        assert_eq!(log.trace_path.last().unwrap().entity_type, "select");
    }
    let count_metadata: Vec<_> = observation
        .metadata()
        .into_iter()
        .filter(|m| {
            m.parameterized_query
                .as_deref()
                .unwrap_or_default()
                .to_uppercase()
                .contains("COUNT(")
        })
        .collect();
    assert_eq!(count_metadata.len(), 1);
    assert!(
        count_metadata[0]
            .comment
            .as_deref()
            .unwrap()
            .contains(SECRET),
        "trusted execution intent stays original"
    );
    println!(
        "PAGE_OBSERVED {}",
        serde_json::json!({
            "ids": ids, "total": page.total_count, "offset": 1, "size": page.data.len(),
            "count_sql": counts[0].sql, "count_comment": counts[0].comment,
            "count_purpose": counts[0].purpose, "physical_queries": logs.len(),
        })
    );

    let mut second = page.data.pop().ok_or("second page row")?;
    let mut first = page.data.pop().ok_or("first page row")?;
    first.update_order_number("PAGE-SAVED-A");
    second.update_order_number("PAGE-PENDING-B");
    capture.clear();
    first
        .audit_as("save first page row only")
        .save(context)
        .await?;
    assert_audit_graph(
        &capture.events(),
        &[ExpectedItem {
            entity: "CustomerOrder",
            id: ids[1],
            kind: RawAuditEventKind::Updated,
            reasons: vec![("CustomerOrder", ids[1], "save first page row only")],
        }],
    );
    let untouched = Q::customer_orders()
        .with_id_is(ids[2])
        .limit(1)
        .comment("what: verify pending page sibling")
        .purpose("why: no shared mutation root")
        .execute_for_one(context)
        .await?
        .ok_or("pending sibling")?;
    assert_eq!(
        E::customer_order(&untouched)
            .get_order_number()
            .eval()
            .as_deref(),
        Some("PAGE-2")
    );
    assert_eq!(E::customer_order(&untouched).get_version().eval(), Some(1));
    capture.clear();
    let saved = second
        .audit_as("save second page row only")
        .save(context)
        .await?;
    assert_eq!(
        E::customer_order(&saved)
            .get_order_number()
            .eval()
            .as_deref(),
        Some("PAGE-PENDING-B")
    );
    assert_eq!(E::customer_order(&saved).get_version().eval(), Some(2));
    assert_audit_graph(
        &capture.events(),
        &[ExpectedItem {
            entity: "CustomerOrder",
            id: ids[2],
            kind: RawAuditEventKind::Updated,
            reasons: vec![("CustomerOrder", ids[2], "save second page row only")],
        }],
    );
    println!(
        "TC-REQ-10 GENERATED PAGE COUNT PASSED: total=3 offset=1 size=2; private descendant masked; independent row saves"
    );
    Ok(())
}
