//! Nonzero-offset page and cross-chunk scalar-stream deferred-save checks.
use checker_fix_advanced_core::{AuditedSave, E, PurchaseOrder, Q};
use futures_util::StreamExt;
use teaql_core::{Entity, Value};
use teaql_runtime::UserContext;

pub async fn verify(
    context: &UserContext,
    id_a: u64,
    id_b: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = Q::purchase_orders()
        .limit(20)
        .comment("Inspect the page ownership fixture")
        .purpose("Establish the bounded exact total")
        .execute_for_list(context)
        .await?;
    let mut sentinel = Q::purchase_orders()
        .comment("Prepare a page-offset sentinel")
        .purpose("Exercise a genuine nonzero page offset")
        .new_entity(context).expect("reserve example identity");
    sentinel.update_contact_phone("5550500");
    sentinel.update_internal_note(Value::Null);
    sentinel
        .audit_as("Save the synthetic page sentinel")
        .save(context)
        .await?;
    let expected = before.len() + 1;
    let page = Q::purchase_orders()
        .order_by_id_desc()
        .offset(1, 2)
        .count()
        .comment("Load a nonzero-offset page of independent roots")
        .purpose("Verify exact count and page ownership")
        .execute_for_list(context)
        .await?;
    assert_eq!(page.len(), 2);
    assert_eq!(page.total_count, Some(expected as u64));
    exercise(
        context,
        &page.data,
        (id_a, id_b),
        "a nonzero-offset page",
        "5550401",
        "5550402",
        2,
    )
    .await?;
    let mut stream = Q::purchase_orders()
        .order_by_id_asc()
        .limit(20)
        .stream(1)
        .comment("Stream independent roots across single-row chunks")
        .purpose("Verify cross-chunk deferred-save ownership")
        .execute_for_stream(context)
        .await?;
    let mut streamed = Vec::new();
    while let Some(row) = stream.next().await {
        streamed.push(row?);
    }
    drop(stream);
    assert_eq!(streamed.len(), expected);
    exercise(
        context,
        &streamed,
        (id_a, id_b),
        "a fully consumed scalar stream",
        "5550402",
        "5550403",
        3,
    )
    .await?;
    println!(
        "CORE_MUTATION_PAGING={}",
        serde_json::json!({"language": "rust", "passed": true,
        "pageOffset": 1, "pageSize": 2, "exactCount": expected, "pageRootIsolation": true,
        "deferredSaveAfterContextReuse": true, "streamProfile": "scalar-typed", "streamChunkSize": 1,
        "streamRootIsolation": true, "siblingPendingMutationRetained": true})
    );
    Ok(())
}

async fn exercise(
    context: &UserContext,
    rows: &[PurchaseOrder],
    ids: (u64, u64),
    label: &str,
    previous: &str,
    next: &str,
    version: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut a = rows.iter().find(|row| row.id() == ids.0).unwrap().clone();
    let mut b = rows.iter().find(|row| row.id() == ids.1).unwrap().clone();
    a.update_contact_phone(format!("55503{}", &next[next.len() - 2..]));
    b.update_contact_phone(next);
    Q::tenants_minimal()
        .limit(1)
        .comment("Run an unrelated query before deferred saves")
        .purpose("Verify deferred ownership survives Context reuse")
        .execute_for_list(context)
        .await?;
    a.audit_as(format!("Save only A from {label}"))
        .save(context)
        .await?;
    let persisted = Q::purchase_orders()
        .with_id_is(ids.1)
        .limit(1)
        .comment("Inspect B after deferred A save")
        .purpose("Verify unrelated root value and version remain untouched")
        .execute_for_list(context)
        .await?;
    assert_eq!(persisted.len(), 1);
    assert_eq!(
        E::purchase_order(persisted.first().unwrap())
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some(previous)
    );
    assert_eq!(
        E::purchase_order(persisted.first().unwrap())
            .get_version()
            .eval(),
        Some(version)
    );
    b.audit_as(format!("Save B later from {label}"))
        .save(context)
        .await?;
    let persisted = Q::purchase_orders()
        .with_id_is(ids.1)
        .limit(1)
        .comment("Inspect the later deferred B save")
        .purpose("Verify B's pending mutation survived A's commit")
        .execute_for_list(context)
        .await?;
    assert_eq!(persisted.len(), 1);
    assert_eq!(
        E::purchase_order(persisted.first().unwrap())
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some(next)
    );
    assert_eq!(
        E::purchase_order(persisted.first().unwrap())
            .get_version()
            .eval(),
        Some(version + 1)
    );
    Ok(())
}
