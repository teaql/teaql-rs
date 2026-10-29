use super::*;

fn key(entity: &str, fields: &[&str]) -> UpdateBatchKey {
    let fields = fields
        .iter()
        .map(|field| field.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    UpdateBatchKey {
        entity: entity.into(),
        fields: fields.into_iter().collect(),
    }
}

#[test]
fn structured_keys_preserve_legacy_batch_order_and_grouping() {
    let names = [
        "#code",
        "a",
        "a!",
        "a.b",
        "a0",
        "id",
        "update_time",
        "version",
        "中文",
    ];
    let mut inputs = vec![key("Inventory", &[])];
    for entity in ["Inventory", "TransferOrder"] {
        for first in names {
            inputs.push(key(entity, &[first]));
            for second in names {
                inputs.push(key(entity, &[first, second]));
            }
        }
    }
    for a in &inputs {
        for b in &inputs {
            let legacy = (&a.entity, a.fields.join(",")).cmp(&(&b.entity, b.fields.join(",")));
            assert_eq!(a.cmp(b), legacy, "{a:?} / {b:?}");
            assert_eq!(a == b, a.cmp(b).is_eq());
        }
    }
    let mut old = BTreeMap::new();
    let mut new = BTreeMap::new();
    for (i, input) in inputs.into_iter().enumerate() {
        old.entry((input.entity.clone(), input.fields.join(",")))
            .or_insert_with(Vec::new)
            .push(i);
        new.entry(input).or_insert_with(Vec::new).push(i);
    }
    let consumed: Vec<_> = new
        .into_iter()
        .map(|(key, rows)| ((key.entity, key.fields.join(",")), rows))
        .collect();
    assert_eq!(consumed, old.into_iter().collect::<Vec<_>>());
}

#[test]
fn consuming_batch_preserves_owned_fields_and_unambiguous_names() {
    let single = key("Inventory", &["a,b"]);
    let multiple = key("Inventory", &["a", "b"]);
    assert_ne!(single.cmp(&multiple), std::cmp::Ordering::Equal);
    let ptr = single.fields[0].as_ptr();
    let batches = BTreeMap::from([(single, vec![1]), (multiple, vec![2])]);
    assert_eq!(batches.len(), 2);
    for (key, _) in batches {
        let fields = key.fields;
        if fields.len() == 1 {
            assert_eq!(fields[0], "a,b");
            assert_eq!(fields[0].as_ptr(), ptr);
        }
    }
}
