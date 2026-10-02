//! Actual generated Q/E streams: deferred intent, overlapping consumption and Drop.
use super::{AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph};
use futures_util::{FutureExt, StreamExt};
use teaql_data_service::SqlExecutionOutcome;
use teaql_runtime::{RawAuditEventKind, UserContext};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{AuditedSave as _, E, Q};

fn terminal(
    context: &UserContext,
    observation: &Observation,
    comment: &str,
    purpose: &str,
    outcome: SqlExecutionOutcome,
    count: usize,
) {
    let metadata = observation.metadata();
    let matches: Vec<_> = metadata
        .iter()
        .filter(|m| m.comment.as_deref() == Some(comment))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "one generated stream terminal metadata: {comment}"
    );
    assert_eq!(matches[0].sql_log.execution_outcome, Some(outcome));
    assert_eq!(matches[0].result_count, Some(count));
    assert!(
        matches[0]
            .trace_chain
            .iter()
            .any(|node| node.kind == TraceKind::Purpose && node.comment == purpose)
    );
    let logs = context.sql_logs();
    let matches: Vec<_> = logs
        .iter()
        .filter(|m| m.comment.as_deref() == Some(comment))
        .collect();
    assert_eq!(matches.len(), 1, "one safe SQL terminal: {comment}");
    let log = matches[0];
    assert_eq!(log.purpose.as_deref(), Some(purpose));
    assert_eq!(log.result_count, Some(count));
    assert_eq!(log.log_context.execution_outcome, Some(outcome));
    let path: Vec<_> = log
        .trace_path
        .iter()
        .map(|node| (node.kind, node.entity_type.as_str()))
        .collect();
    assert_eq!(
        path,
        vec![
            (TraceKind::Operation, "CustomerOrder"),
            (TraceKind::Request, "CustomerOrder"),
            (TraceKind::Provider, "sqlite"),
            (TraceKind::Sql, "select"),
        ]
    );
    println!(
        "STREAM_OBSERVED {}",
        serde_json::json!({
            "comment": comment, "purpose": purpose, "outcome": format!("{outcome:?}"),
            "result_count": count, "trace_path": format!("{path:?}"),
            "sql": log.sql,
        })
    );
}

pub async fn scalar_streams(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let mut ids = Vec::new();
    for index in 0..3 {
        let mut row = Q::customer_orders()
            .comment("what: prepare stream fixture")
            .purpose("why: isolate bounded stream probes from prior rows")
            .new_entity(context);
        row.update_platform_id(1);
        row.update_order_number(format!("STREAM-{index}"));
        row.update_description("Stream fixture");
        let row = row.audit_as("create stream fixture").save(context).await?;
        ids.push(row.id());
    }
    ids.reverse();
    observation.clear();
    context.clear_sql_logs();
    let missing_comment = std::panic::AssertUnwindSafe(async {
        Q::customer_orders_minimal()
            .limit(3)
            .stream(1)
            .purpose("why: invalid request must fail before any SQL")
            .execute_for_stream(context)
            .await
    })
    .catch_unwind()
    .await;
    assert!(
        !matches!(missing_comment, Ok(Ok(_))),
        "stream creation requires its own comment"
    );
    let blank_comment = std::panic::AssertUnwindSafe(async {
        Q::customer_orders_minimal()
            .limit(3)
            .stream(1)
            .comment("  ")
            .purpose("why: reject whitespace intent")
            .execute_for_stream(context)
            .await
    })
    .catch_unwind()
    .await;
    assert!(
        !matches!(blank_comment, Ok(Ok(_))),
        "blank comment is rejected before SQL"
    );
    let mut unsupported = Q::customer_orders_minimal()
        .select_platform_with(Q::platforms_minimal())
        .limit(3)
        .stream(1)
        .comment("what: unsupported relation stream")
        .purpose("why: reject incomplete hydration rather than invent SQL")
        .execute_for_stream(context)
        .await?;
    let failure = unsupported
        .next()
        .await
        .ok_or("unsupported stream result")?;
    assert!(failure.is_err());
    assert!(
        failure
            .err()
            .unwrap()
            .to_string()
            .contains("streaming relation")
    );
    drop(unsupported);
    let untouched = Q::customer_orders_minimal()
        .select_description()
        .order_by_id_desc()
        .limit(3)
        .stream(1)
        .comment("what: never polled stream")
        .purpose("why: do not invent executed SQL")
        .execute_for_stream(context)
        .await?;
    drop(untouched);
    assert!(observation.metadata().is_empty());
    assert!(context.sql_logs().is_empty());

    let mut complete = Q::customer_orders()
        .order_by_id_desc()
        .limit(3)
        .stream(1)
        .comment("what: complete deferred stream")
        .purpose("why: preserve stream A intent")
        .execute_for_stream(context)
        .await?;
    let mut cancelled = Q::customer_orders_minimal()
        .select_description()
        .order_by_id_desc()
        .limit(3)
        .stream(1)
        .comment("what: cancel deferred stream")
        .purpose("why: preserve stream B intent")
        .execute_for_stream(context)
        .await?;
    assert!(observation.metadata().is_empty(), "creation is lazy");
    let b = cancelled.next().await.ok_or("stream B first row")??;
    assert_eq!(
        E::customer_order(&b).get_description().eval().as_deref(),
        Some("Stream fixture")
    );
    assert!(
        complete.next().now_or_never().is_none(),
        "second stream waits on SQLite connection lease"
    );
    assert!(
        observation.metadata().is_empty(),
        "neither request has terminated"
    );
    let independent = Q::customer_orders_minimal()
        .select_description()
        .with_id_is(ids[0])
        .limit(1)
        .comment("what: independent query while streams active")
        .purpose("why: must not replace either stream intent")
        .execute_for_one(context);
    futures_util::pin_mut!(independent);
    assert!(
        independent.as_mut().now_or_never().is_none(),
        "ordinary query also waits on lease"
    );
    drop(cancelled);
    terminal(
        context,
        observation,
        "what: cancel deferred stream",
        "why: preserve stream B intent",
        SqlExecutionOutcome::Cancelled,
        1,
    );
    let mut rows = Vec::new();
    while let Some(row) = complete.next().await {
        rows.push(row?);
    }
    drop(complete);
    independent.await?.ok_or("independent row")?;
    assert_eq!(
        rows.iter()
            .map(|row| E::customer_order(row).get_id().eval().unwrap())
            .collect::<Vec<_>>(),
        ids
    );
    terminal(
        context,
        observation,
        "what: complete deferred stream",
        "why: preserve stream A intent",
        SqlExecutionOutcome::Success,
        3,
    );
    assert_eq!(
        context.sql_logs().len(),
        3,
        "two streams plus independent query, no duplicates"
    );

    // Stream hydration must not accidentally join mutation ownership across rows.
    rows[0].update_description("Saved stream row A");
    rows[1].update_description("Pending stream row B");
    let second = rows.remove(1);
    let first = rows.remove(0);
    capture.clear();
    let saved_first = first
        .audit_as("save only stream row A")
        .save(context)
        .await?;
    assert_eq!(
        E::customer_order(&saved_first).get_version().eval(),
        Some(2)
    );
    assert_audit_graph(
        &capture.events(),
        &[ExpectedItem {
            entity: "CustomerOrder",
            id: ids[0],
            kind: RawAuditEventKind::Updated,
            reasons: vec![("CustomerOrder", ids[0], "save only stream row A")],
        }],
    );
    let unchanged = Q::customer_orders()
        .with_id_is(ids[1])
        .limit(1)
        .comment("what: check unsaved stream sibling")
        .purpose("why: prove per-row mutation isolation")
        .execute_for_one(context)
        .await?
        .ok_or("second row")?;
    assert_eq!(
        E::customer_order(&unchanged)
            .get_description()
            .eval()
            .as_deref(),
        Some("Stream fixture")
    );
    assert_eq!(E::customer_order(&unchanged).get_version().eval(), Some(1));
    capture.clear();
    let saved = second
        .audit_as("save only stream row B")
        .save(context)
        .await?;
    assert_audit_graph(
        &capture.events(),
        &[ExpectedItem {
            entity: "CustomerOrder",
            id: ids[1],
            kind: RawAuditEventKind::Updated,
            reasons: vec![("CustomerOrder", ids[1], "save only stream row B")],
        }],
    );
    assert_eq!(
        E::customer_order(&saved)
            .get_description()
            .eval()
            .as_deref(),
        Some("Pending stream row B")
    );
    assert_eq!(E::customer_order(&saved).get_version().eval(), Some(2));
    println!(
        "STREAM_ROWS {}",
        serde_json::json!({
            "saved_a": ids[0], "saved_b": ids[1], "unchanged": ids[2],
            "versions": [2, 2, 1], "audits_per_save": 1,
        })
    );
    println!(
        "TC-REQ-06 GENERATED SCALAR STREAM PASSED: independent row ledgers and terminal intent"
    );
    Ok(())
}
