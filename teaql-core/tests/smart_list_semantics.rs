use std::collections::BTreeMap;
use teaql_core::{
    CompactRow, Decimal, Entity, EntityDescriptor, EntityError, IdentifiableEntity, MutationValues,
    SmartList, TeaqlEntity, Value, VersionedEntity,
};

fn vectors() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/smart-list-semantics.json")).unwrap()
}

#[test]
fn states_totals_and_derivations_follow_common_vectors() {
    let fixtures = vectors();
    for case in fixtures["states"].as_array().unwrap() {
        let rows = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect();
        let mut list: SmartList<i64> = if case["loaded"].as_bool().unwrap() {
            SmartList::new(rows)
        } else {
            SmartList::empty()
        };
        list.total_count = case["total"].as_u64();
        assert_eq!(list.is_loaded, case["loaded"].as_bool().unwrap());
        assert_eq!(list.total_count, case["total"].as_u64());
        assert_eq!(
            list.total_count_or_len(),
            list.total_count.unwrap_or(list.len() as u64)
        );
    }
    let source = SmartList::new(vec![1, 2, 3])
        .with_total_count(9)
        .with_summary("source", true)
        .with_aggregation("source", 5)
        .with_facet("empty", SmartList::new(vec![]));
    assert_eq!(source.clone().map(|v| v * 10).data, vec![10, 20, 30]);
    assert_eq!(source.clone().filter(|v| v % 2 == 1).data, vec![1, 3]);
    assert_eq!(
        source.clone().flat_map(|v| [v, v]).data,
        vec![1, 1, 2, 2, 3, 3]
    );
    for derived in [
        source.clone().filter(|_| true),
        source.clone().flat_map(|v| [v]),
    ] {
        assert!(derived.is_loaded);
        assert_eq!(derived.total_count, None);
        assert!(
            derived.aggregations.is_empty()
                && derived.summary.is_empty()
                && derived.facets.is_empty()
        );
    }
    assert!(!SmartList::<i32>::empty().map(|v| v).is_loaded);
    assert!(!SmartList::<i32>::empty().filter(|_| true).is_loaded);
    assert!(!SmartList::<i32>::empty().flat_map(|v| [v]).is_loaded);
    assert_eq!(source.total_count, Some(9));
}

#[test]
fn metadata_and_nested_facet_records_are_isolated_after_clone_and_map() {
    let decimal: Decimal = vectors()["metadata"]["decimal"]["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let facet_row = CompactRow::from(BTreeMap::from([("count".to_owned(), Value::I64(3))]));
    let source = SmartList::new(vec![1, 2])
        .with_total_count(9)
        .with_aggregation("decimal", decimal)
        .with_aggregation("nullable", Value::Null)
        .with_aggregation("nested", Value::List(vec![Value::I64(1)]))
        .with_summary(
            "nested",
            Value::Object(BTreeMap::from([(
                "values".to_owned(),
                Value::List(vec![Value::I64(3)]),
            )])),
        )
        .with_facet("status", SmartList::new(vec![facet_row]))
        .with_facet("empty", SmartList::new(vec![]));
    let mut mapped = source.clone().map(|v| v * 10);
    assert_eq!(source.total_count, mapped.total_count);
    assert_eq!(source.aggregations, mapped.aggregations);
    assert_eq!(source.summary, mapped.summary);
    assert_eq!(source.facets, mapped.facets);
    assert_eq!(mapped.aggregation("nullable"), Some(&Value::Null));
    assert_eq!(mapped.aggregation("missing"), None);
    let Value::List(nested) = mapped.aggregations.get_mut("nested").unwrap() else {
        panic!()
    };
    nested.push(Value::I64(99));
    let Value::Object(summary) = mapped.summary.get_mut("nested").unwrap() else {
        panic!()
    };
    summary.insert("changed".to_owned(), Value::Bool(true));
    mapped.facet_mut("status").unwrap().data[0].insert("count".to_owned(), Value::I64(99));
    mapped.remove_facet("empty");
    assert_eq!(
        source.aggregation("nested"),
        Some(&Value::List(vec![Value::I64(1)]))
    );
    assert_eq!(
        source.facet("status").unwrap().data[0].get("count"),
        Some(&Value::I64(3))
    );
    assert!(source.facet("empty").unwrap().is_loaded);
    assert_ne!(source.summary, mapped.summary);
    assert_eq!(
        mapped.aggregation("decimal"),
        Some(&Value::Decimal(decimal))
    );
}

#[test]
fn duplicate_merge_order_safe_access_group_and_facet_transfer_follow_vectors() {
    let fixtures = vectors();
    let case = &fixtures["merge"];
    let mut list = SmartList::new(case["target"].as_array().unwrap().clone()).with_total_count(9);
    list.merge_by(case["incoming"].as_array().unwrap().clone(), |v| {
        v["key"].as_str().unwrap().to_owned()
    });
    assert_eq!(list.data, *case["expected"].as_array().unwrap());
    assert_eq!(
        list.identity_map(|v| v["key"].as_str().unwrap().to_owned())["a"]["value"],
        4
    );
    assert_eq!(
        list.group_by(|v| v["key"].as_str().unwrap().to_owned())["a"],
        list.data[..2]
    );
    assert_eq!(list.get(100), None);
    assert_eq!(list.set(100, serde_json::Value::Null), None);
    assert_eq!(list.total_count, Some(9));
    let mut empty: SmartList<i64> = SmartList::new(vec![]);
    assert_eq!(empty.first(), None);
    assert_eq!(empty.last(), None);
    assert_eq!(empty.facet("absent"), None);
    empty.add_facet("present", SmartList::new(vec![]));
    assert!(empty.remove_facet("absent").is_none());
    assert!(empty.take_facets().contains_key("present"));
    assert!(empty.facets.is_empty());
}

#[derive(Clone, Debug, PartialEq)]
struct Item {
    id: Value,
    version: i64,
}
impl TeaqlEntity for Item {
    const ENTITY_NAME: &'static str = "Item";
    fn entity_descriptor() -> EntityDescriptor {
        EntityDescriptor::new("Item")
    }
}
impl Entity for Item {
    fn from_compact_row(row: CompactRow) -> Result<Self, EntityError> {
        let version = match row.get("version") {
            Some(Value::I64(value)) => *value,
            _ => 0,
        };
        Ok(Self {
            id: row.get("id").cloned().unwrap_or(Value::Null),
            version,
        })
    }
    fn into_values(self) -> MutationValues {
        BTreeMap::from([
            ("id".to_owned(), self.id),
            ("version".to_owned(), Value::I64(self.version)),
        ])
        .into()
    }
}
impl IdentifiableEntity for Item {
    fn id_value(&self) -> Value {
        self.id.clone()
    }
}
impl VersionedEntity for Item {
    fn version(&self) -> i64 {
        self.version
    }
}

#[test]
fn missing_ids_are_preserved_in_extraction_and_omitted_in_index() {
    let fixtures = vectors();
    let data: Vec<_> = fixtures["identity"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| Item {
            id: match &row["id"] {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::String(value) => Value::Text(value.clone()),
                value => Value::I64(value.as_i64().unwrap()),
            },
            version: row["version"].as_i64().unwrap(),
        })
        .collect();
    let list = SmartList::new(data);
    assert_eq!(list.ids().len(), 6);
    assert_eq!(list.ids()[1], Value::Null);
    assert_eq!(list.versions(), vec![1, 2, 3, 4, 5, 6]);
    let indexed = list.map_by_id();
    assert_eq!(indexed.len(), 4);
    assert!(!indexed.contains_key("null"));
    for (key, position) in [("i:0", 0), ("i:1", 3), ("t:1", 4), ("t:", 5)] {
        assert_eq!(indexed[key], list.data[position]);
    }
    assert!(SmartList::<Item>::empty().into_value().is_none());
    assert_eq!(
        SmartList::<Item>::new(vec![]).into_value(),
        Some(Value::List(vec![]))
    );
}
