use teaql_core::{Record, TraceKind, TraceNode, Value};
use teaql_runtime::{RawAuditEvent, RawAuditEventKind};

fn root_only(event: &mut RawAuditEvent) {
    event.trace_chain = vec![TraceNode::typed(
        TraceKind::AuditReason,
        "CustomerOrder",
        Some(1),
        "submit order",
    )];
}

#[test]
fn safe_target_survives_root_only_lineage_and_non_id_changes_for_all_mutation_kinds() {
    let id = Value::U64(7123);
    let values = Record::from([
        ("id".into(), id.clone()),
        ("name".into(), "private name".into()),
    ]);
    let events = [
        RawAuditEvent::created("OrderItem", values.clone()),
        RawAuditEvent::updated_with_old_values(
            "OrderItem",
            values.clone(),
            None,
            values.clone(),
            vec!["name".into()],
        ),
        RawAuditEvent::deleted_with_old_values(
            "OrderItem",
            id.clone(),
            Some(1),
            Some(values.clone()),
        ),
        RawAuditEvent::recovered_with_old_values("OrderItem", id, -1, Some(values)),
    ];
    for mut raw in events {
        root_only(&mut raw);
        let safe = raw.build_safe_event(&["name".into()], Some(3));
        assert_eq!(safe.entity, "OrderItem");
        assert_eq!(safe.entity_id, Some(7123));
        assert_eq!(
            safe.trace_chain, raw.trace_chain,
            "target must not add a responsibility node"
        );
        if raw.kind != RawAuditEventKind::Created {
            assert!(
                !safe
                    .fields
                    .iter()
                    .any(|field| field.name == "id" && field.value.is_some()),
                "target identity is not an ID field update"
            );
        }
        assert!(!format!("{safe:?}").contains("private name"));
    }
}

#[test]
fn equal_ids_of_different_entity_types_remain_independent() {
    let mut order = RawAuditEvent::deleted("CustomerOrder", Value::U64(1), Some(1));
    let mut payment = RawAuditEvent::deleted("Payment", Value::U64(1), Some(1));
    root_only(&mut order);
    root_only(&mut payment);
    let order = order.build_safe_event(&[], None);
    let payment = payment.build_safe_event(&[], None);
    assert_eq!(order.entity_id, payment.entity_id);
    assert_ne!(
        (&order.entity, order.entity_id),
        (&payment.entity, payment.entity_id)
    );
    assert_eq!(order.trace_chain, payment.trace_chain);
}

#[test]
fn target_comes_from_authoritative_values_not_old_snapshot_or_lineage() {
    let mut raw = RawAuditEvent::updated_with_old_values(
        "Payment",
        Record::from([("id".into(), Value::I64(7123))]),
        Some(Record::from([("id".into(), Value::U64(999))])),
        Record::from([("id".into(), Value::U64(888))]),
        vec![],
    );
    root_only(&mut raw);
    let safe = raw.build_safe_event(&[], None);
    raw.values.insert("id".into(), Value::U64(456));
    assert_eq!(safe.entity_id, Some(7123), "owned target snapshot");
    assert!(safe.fields.is_empty());
    assert_eq!(safe.trace_chain[0].entity_id, Some(1));
}

#[test]
fn schema_missing_invalid_and_non_integer_ids_do_not_invent_a_target() {
    let raw = RawAuditEvent::schema_created("Payment", "payment_data", 3);
    assert_eq!(raw.build_safe_event(&[], None).entity_id, None);
    for id in [Value::U64(u64::MAX), Value::I64(i64::MAX)] {
        let raw = RawAuditEvent::created("Payment", Record::from([("id".into(), id.clone())]));
        assert_eq!(raw.build_safe_event(&[], None).entity_id, id.try_u64());
    }
    let mut missing = RawAuditEvent::updated("Payment", Record::new());
    root_only(&mut missing);
    assert_eq!(missing.build_safe_event(&[], None).entity_id, None);
    for id in [
        Value::U64(0),
        Value::I64(-1),
        Value::F64(7.5),
        "7123".into(),
        Value::Null,
        Value::Decimal("7.5".parse().unwrap()),
    ] {
        let raw = RawAuditEvent::created("Payment", Record::from([("id".into(), id)]));
        assert_eq!(raw.build_safe_event(&[], None).entity_id, None);
    }
}
