use std::collections::HashSet;

use teaql_core::business_id::{
    BUSINESS_ID_V1_DOMAIN_SIZE, BusinessIdAllocator, BusinessIdErrorCode, BusinessIdPlan,
    BusinessIdScope,
};
use teaql_runtime::{BusinessIdEncodingKey, BusinessIdPermutationV1, InMemoryBusinessIdAllocator};

const VECTORS: &str = include_str!("data/business-id-permutation-v1.csv");

#[test]
fn matches_every_cross_language_golden_vector() {
    let mut lines = VECTORS.lines();
    assert_eq!(
        lines.next(),
        Some(
            "case_id,key_hex,key_version,domain_root_key,aggregate_type,namespace,period_key,sequence,expected_code,expected_business_id"
        )
    );
    let mut rows = 0;
    for line in lines {
        let fields = line.split(',').collect::<Vec<_>>();
        assert_eq!(fields.len(), 10, "vector columns for {line}");
        let scope = BusinessIdScope {
            domain_root_key: fields[3].to_owned(),
            aggregate_type: fields[4].to_owned(),
            namespace: fields[5].to_owned(),
            period_key: fields[6].to_owned(),
        };
        let key =
            BusinessIdEncodingKey::new(fields[2].parse().expect("key version"), hex_key(fields[1]))
                .expect("valid key");
        let actual =
            BusinessIdPermutationV1::encode(fields[7].parse().expect("sequence"), &scope, &key)
                .expect("encode golden vector");
        assert_eq!(actual, fields[8], "{}", fields[0]);
        assert_eq!(
            format!("ORD-{}-{actual}", fields[6]),
            fields[9],
            "{}",
            fields[0]
        );
        rows += 1;
    }
    assert_eq!(rows, 10);
}

#[test]
fn deterministic_unique_and_canonical_for_retained_range() {
    let initial_scope = scope();
    let key = BusinessIdEncodingKey::new(
        1,
        hex_key("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
    )
    .expect("key");
    let mut values = HashSet::new();
    for sequence in 0..20_000 {
        let first =
            BusinessIdPermutationV1::encode(sequence, &initial_scope, &key).expect("encode");
        let after_restart = BusinessIdPermutationV1::encode(
            sequence,
            &scope(),
            &BusinessIdEncodingKey::new(
                1,
                hex_key("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
            )
            .expect("key"),
        )
        .expect("encode after restart");
        assert_eq!(first, after_restart);
        assert_eq!(first.len(), 6);
        assert!(
            first
                .bytes()
                .all(|value| value.is_ascii_digit() || value.is_ascii_uppercase())
        );
        assert!(values.insert(first), "duplicate at sequence {sequence}");
    }
}

#[test]
fn rejects_out_of_domain_sequence_key_version_and_blank_scope() {
    let key = BusinessIdEncodingKey::new(1, [0; 32]).expect("key");
    let error = BusinessIdPermutationV1::encode(BUSINESS_ID_V1_DOMAIN_SIZE, &scope(), &key)
        .expect_err("domain exhaustion");
    assert_eq!(error.code, BusinessIdErrorCode::Exhausted);
    assert!(BusinessIdEncodingKey::new(0, [0; 32]).is_err());

    let mut invalid_scope = scope();
    invalid_scope.namespace.clear();
    let error =
        BusinessIdPermutationV1::encode(0, &invalid_scope, &key).expect_err("blank scope field");
    assert_eq!(error.code, BusinessIdErrorCode::InvalidDefinition);
}

#[test]
fn rejects_invalid_allocation_range_before_consuming_a_sequence() {
    let plan = BusinessIdPlan {
        scope: scope(),
        prefix: "ORD".to_owned(),
        date_text: Some("20260925".to_owned()),
        preserve_digits: 6,
        separator: "-".to_owned(),
        initial_sequence: 2,
        max_sequence: 1,
    };
    let error = InMemoryBusinessIdAllocator::default()
        .allocate(&plan)
        .expect_err("invalid range");
    assert_eq!(error.code, BusinessIdErrorCode::InvalidDefinition);
}

fn scope() -> BusinessIdScope {
    BusinessIdScope {
        domain_root_key: "tenant-a".to_owned(),
        aggregate_type: "commerce_order".to_owned(),
        namespace: "order_number".to_owned(),
        period_key: "20260925".to_owned(),
    }
}

fn hex_key(value: &str) -> [u8; 32] {
    let mut key = [0; 32];
    for (index, target) in key.iter_mut().enumerate() {
        *target = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).expect("hex byte");
    }
    key
}
