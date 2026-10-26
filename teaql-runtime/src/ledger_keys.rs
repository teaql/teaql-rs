use std::collections::{BTreeMap, BTreeSet};

use teaql_core::{EntityDescriptor, MutationValues, RelationDescriptor, Value};

use crate::{EntityValues, RuntimeError};

fn writable_forward_relation<'a>(
    descriptor: &'a EntityDescriptor,
    relation: &'a RelationDescriptor,
) -> Result<&'a str, RuntimeError> {
    if relation.many
        || relation.local_key == "id"
        || relation.foreign_key != "id"
        || descriptor.property_by_name(&relation.local_key).is_none()
    {
        return Err(RuntimeError::Graph(format!(
            "ledger relation {}.{} has no writable local storage property",
            descriptor.name, relation.name
        )));
    }
    Ok(&relation.local_key)
}

/// Resolve canonical Ledger keys for the typed graph and provider command.
/// The input remains intact for policy and audit.
pub(crate) fn storage_values_from_ledger(
    descriptor: &EntityDescriptor,
    record: &MutationValues,
) -> Result<EntityValues, RuntimeError> {
    let mut storage_values = EntityValues::new();
    for (field, value) in record.iter() {
        let storage_field = if descriptor.property_by_name(field).is_some() {
            field.as_str()
        } else if let Some(relation) = descriptor.relation_by_name(field) {
            writable_forward_relation(descriptor, relation)?
        } else {
            return Err(RuntimeError::Graph(format!(
                "unknown ledger field {}.{}",
                descriptor.name, field
            )));
        };
        if storage_values
            .insert(storage_field.to_owned(), value.clone())
            .is_some()
        {
            return Err(RuntimeError::Graph(format!(
                "duplicate ledger mapping for {}.{}",
                descriptor.name, storage_field
            )));
        }
    }
    Ok(storage_values)
}

pub(crate) fn storage_fields_from_ledger(
    descriptor: &EntityDescriptor,
    fields: &BTreeSet<String>,
) -> Result<BTreeSet<String>, RuntimeError> {
    let mut storage = BTreeSet::new();
    for field in fields {
        let name = if descriptor.property_by_name(field).is_some() {
            field.as_str()
        } else if let Some(relation) = descriptor.relation_by_name(field) {
            writable_forward_relation(descriptor, relation)?
        } else if field.starts_with('#') && field.len() > 1 {
            // Dynamic-field dirty markers share this set with model fields.
            // Their mutations are saved through the separate dynamic store.
            field.as_str()
        } else {
            return Err(RuntimeError::Graph(format!(
                "unknown ledger field {}.{}",
                descriptor.name, field
            )));
        };
        if !storage.insert(name.to_owned()) {
            return Err(RuntimeError::Graph(format!(
                "duplicate ledger mapping for {}.{}",
                descriptor.name, name
            )));
        }
    }
    Ok(storage)
}

/// Resolve a provider/typed-graph property back to its KSML Ledger name.
pub(crate) fn canonical_field_from_storage(
    descriptor: &EntityDescriptor,
    field: &str,
) -> Result<String, RuntimeError> {
    if let Some(relation) = descriptor.relation_by_name(field) {
        writable_forward_relation(descriptor, relation)?;
        return Ok(field.to_owned());
    }
    if descriptor.property_by_name(field).is_none() {
        return Err(RuntimeError::Graph(format!(
            "unknown storage field {}.{}",
            descriptor.name, field
        )));
    }
    let mut relations = descriptor.relations.iter().filter(|relation| {
        !relation.many
            && relation.local_key != "id"
            && relation.foreign_key == "id"
            && relation.local_key == field
    });
    match (relations.next(), relations.next()) {
        (Some(relation), None) => Ok(relation.name.clone()),
        (None, None) => Ok(field.to_owned()),
        _ => Err(RuntimeError::Graph(format!(
            "ambiguous relation storage field {}.{}",
            descriptor.name, field
        ))),
    }
}

/// Child graph mutations use storage properties; the merged Ledger must not.
pub(crate) fn canonical_values_from_storage(
    descriptor: &EntityDescriptor,
    record: &BTreeMap<String, Value>,
) -> Result<MutationValues, RuntimeError> {
    let mut canonical = BTreeMap::new();
    for (field, value) in record {
        let name = canonical_field_from_storage(descriptor, field)?;
        if canonical.insert(name.clone(), value.clone()).is_some() {
            return Err(RuntimeError::Graph(format!(
                "duplicate canonical ledger field {}.{}",
                descriptor.name, name
            )));
        }
    }
    Ok(canonical.into())
}

/// Checker maps expose KSML fields; native typed carriers and provider graph
/// nodes use storage members. Rename only trusted forward aliases, retaining
/// framework hints and dynamic values. Validate the whole rename before moving
/// values so an equal-value alias collision cannot erase caller intent.
pub(crate) fn map_checker_values(
    descriptor: &EntityDescriptor,
    values: &mut EntityValues,
    canonical: bool,
) -> Result<(), RuntimeError> {
    if !descriptor
        .relations
        .iter()
        .any(|r| !r.many && r.local_key != "id" && r.name != r.local_key)
    {
        return Ok(());
    }
    fn name<'a>(
        descriptor: &'a EntityDescriptor,
        field: &'a str,
        canonical: bool,
    ) -> Result<&'a str, RuntimeError> {
        let mut result = None;
        for relation in descriptor
            .relations
            .iter()
            .filter(|r| !r.many && r.local_key != "id")
        {
            if field == relation.name || field == relation.local_key {
                writable_forward_relation(descriptor, relation)?;
                let target = if canonical {
                    relation.name.as_str()
                } else {
                    relation.local_key.as_str()
                };
                if result.is_some_and(|previous| previous != target) {
                    return Err(RuntimeError::Graph(format!(
                        "ambiguous checker field {}.{field}",
                        descriptor.name
                    )));
                }
                result = Some(target);
            }
        }
        Ok(result.unwrap_or(field))
    }
    fn plan(
        descriptor: &EntityDescriptor,
        values: &BTreeMap<String, Value>,
        canonical: bool,
    ) -> Result<Vec<(String, String)>, RuntimeError> {
        let mut targets = BTreeSet::new();
        let mut changes = Vec::new();
        for field in values.keys() {
            let target = name(descriptor, field, canonical)?;
            if !targets.insert(target.to_owned()) {
                return Err(RuntimeError::Graph(format!(
                    "duplicate checker mapping for {}.{target}",
                    descriptor.name
                )));
            }
            if field != target {
                changes.push((field.clone(), target.to_owned()));
            }
        }
        Ok(changes)
    }
    fn apply(values: &mut BTreeMap<String, Value>, changes: Vec<(String, String)>) {
        let moved: Vec<_> = changes
            .into_iter()
            .map(|(from, to)| (to, values.remove(&from).expect("validated checker key")))
            .collect();
        values.extend(moved);
    }
    let changes = plan(descriptor, values, canonical)?;
    let originals = match values.get("_original_values") {
        Some(Value::Object(record)) => Some(plan(descriptor, record, canonical)?),
        _ => None,
    };
    // Metadata lists describe the same modeled fields; validate before edits.
    for marker in ["_loaded_fields", "_dirty_fields"] {
        if let Some(Value::List(fields)) = values.get(marker) {
            for field in fields {
                if let Value::Text(field) = field {
                    name(descriptor, field, canonical)?;
                }
            }
        }
    }
    apply(values, changes);
    if let (Some(changes), Some(Value::Object(record))) =
        (originals, values.get_mut("_original_values"))
    {
        apply(record, changes);
    }
    for marker in ["_loaded_fields", "_dirty_fields"] {
        if let Some(Value::List(fields)) = values.get_mut(marker) {
            for field in fields {
                if let Value::Text(field) = field {
                    let target = name(descriptor, field, canonical)?;
                    if field.as_str() != target {
                        *field = target.to_owned();
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_values_from_storage, map_checker_values, storage_fields_from_ledger,
        storage_values_from_ledger,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use teaql_core::{
        DataType, EntityDescriptor, MutationValues, PropertyDescriptor, RelationDescriptor, Value,
    };

    fn school() -> EntityDescriptor {
        EntityDescriptor::new("School")
            .property(PropertyDescriptor::new("id", DataType::U64).id())
            .property(
                PropertyDescriptor::new("school_type_id", DataType::U64).column_name("school_type"),
            )
            .relation(
                RelationDescriptor::new("school_type", "SchoolType").local_key("school_type_id"),
            )
            .relation(RelationDescriptor::new("students", "Student").many())
    }

    #[test]
    fn maps_canonical_relation_to_storage_without_changing_ledger_input() {
        let input: MutationValues = BTreeMap::from([
            ("id".to_owned(), Value::U64(8)),
            ("school_type".to_owned(), Value::U64(1001)),
        ])
        .into();
        let output = storage_values_from_ledger(&school(), &input).unwrap();
        assert_eq!(output.get("school_type_id"), Some(&Value::U64(1001)));
        assert!(!output.contains_key("school_type"));
        assert_eq!(input.get("school_type"), Some(&Value::U64(1001)));
        assert_eq!(
            school()
                .property_by_name("school_type_id")
                .unwrap()
                .column_name,
            "school_type"
        );
    }

    #[test]
    fn maps_child_graph_storage_key_back_to_canonical_ledger_key() {
        let storage = BTreeMap::from([("school_type_id".to_owned(), Value::U64(1001))]);
        let ledger = canonical_values_from_storage(&school(), &storage).unwrap();
        assert_eq!(ledger.get("school_type"), Some(&Value::U64(1001)));
        assert!(!ledger.contains_key("school_type_id"));
    }

    #[test]
    fn rejects_duplicate_logical_and_storage_names_even_for_equal_values() {
        let values = BTreeMap::from([
            ("school_type".to_owned(), Value::U64(1001)),
            ("school_type_id".to_owned(), Value::U64(1001)),
        ]);
        let input: MutationValues = values.clone().into();
        assert!(
            storage_values_from_ledger(&school(), &input)
                .unwrap_err()
                .to_string()
                .contains("duplicate ledger mapping")
        );
        assert!(
            canonical_values_from_storage(&school(), &values)
                .unwrap_err()
                .to_string()
                .contains("duplicate canonical ledger field")
        );
    }

    #[test]
    fn rejects_unknown_and_reverse_relation_keys() {
        for name in ["unknown", "students"] {
            let input: MutationValues = BTreeMap::from([(name.to_owned(), Value::U64(1))]).into();
            assert!(storage_values_from_ledger(&school(), &input).is_err());
        }
    }

    #[test]
    fn preserves_dynamic_dirty_markers_separately_from_relation_mapping() {
        let fields = BTreeSet::from(["school_type".to_owned(), "#note".to_owned()]);
        let storage = storage_fields_from_ledger(&school(), &fields).unwrap();
        assert_eq!(
            storage,
            BTreeSet::from(["school_type_id".to_owned(), "#note".to_owned()])
        );
    }

    #[test]
    fn canonical_checker_renames_native_values_hints_and_originals_without_scalar_conversion() {
        let mut input: crate::EntityValues = BTreeMap::from([
            ("school_type_id".into(), Value::U64(1001)),
            ("amount".into(), Value::Decimal("1.2300".parse().unwrap())),
            (
                "_dirty_fields".into(),
                Value::List(vec![Value::Text("school_type_id".into())]),
            ),
            (
                "_loaded_fields".into(),
                Value::List(vec![Value::Text("school_type_id".into())]),
            ),
            (
                "_original_values".into(),
                Value::Object(BTreeMap::from([(
                    "school_type_id".into(),
                    Value::U64(1000),
                )])),
            ),
            ("__teaql_object_status".into(), Value::Text("update".into())),
        ])
        .into();
        let original = input.clone();
        map_checker_values(&school(), &mut input, true).unwrap();
        assert_eq!(input.get("school_type"), Some(&Value::U64(1001)));
        assert!(!input.contains_key("school_type_id"));
        assert_eq!(
            input["_dirty_fields"],
            Value::List(vec![Value::Text("school_type".into())])
        );
        let Value::Object(old) = &input["_original_values"] else {
            panic!("original map lost")
        };
        assert_eq!(old["school_type"], Value::U64(1000));
        map_checker_values(&school(), &mut input, false).unwrap();
        assert_eq!(input, original);
    }

    #[test]
    fn canonical_checker_equal_or_unequal_aliases_are_rejected_atomically() {
        for canonical in [false, true] {
            for value in [1001, 1002] {
                let mut input: crate::EntityValues = BTreeMap::from([
                    ("school_type".into(), Value::U64(1001)),
                    ("school_type_id".into(), Value::U64(value)),
                ])
                .into();
                let before = input.clone();
                assert!(map_checker_values(&school(), &mut input, canonical).is_err());
                assert_eq!(input, before);
            }
        }
    }

    #[test]
    fn canonical_checker_original_alias_collision_keeps_all_outer_values_intact() {
        let mut input: crate::EntityValues = BTreeMap::from([
            ("school_type_id".into(), Value::U64(1001)),
            (
                "_original_values".into(),
                Value::Object(BTreeMap::from([
                    ("school_type".into(), Value::U64(1000)),
                    ("school_type_id".into(), Value::U64(1000)),
                ])),
            ),
        ])
        .into();
        let before = input.clone();
        assert!(map_checker_values(&school(), &mut input, true).is_err());
        assert_eq!(input, before);
    }

    #[test]
    fn canonical_checker_callback_sees_and_fixes_logical_reference_and_metadata_reports_logical_path()
     {
        use crate::{
            CheckResults, CheckRule, Checker, EntityValues, InMemoryCheckerRegistry,
            InMemoryMetadataStore, ObjectLocation, UserContext,
        };
        struct Fix;
        impl Checker for Fix {
            fn entity(&self) -> &str {
                "School"
            }
            fn check_and_fix(
                &self,
                _: &UserContext,
                input: &mut EntityValues,
                _: &ObjectLocation,
                _: &mut CheckResults,
            ) {
                assert_eq!(input["school_type"], Value::U64(1001));
                assert!(!input.contains_key("school_type_id"));
                input.insert("school_type".into(), Value::U64(1002));
            }
        }
        let context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(school()))
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(Fix));
        let mut values: EntityValues = BTreeMap::from([
            ("id".into(), Value::U64(1)),
            ("school_type_id".into(), Value::U64(1001)),
        ])
        .into();
        context.check_and_fix_values("School", &mut values).unwrap();
        assert_eq!(values["school_type"], Value::U64(1002));
        assert!(!values.contains_key("school_type_id"));
        values.insert("school_type".into(), Value::U64(1001));
        context
            .check_and_fix_storage_values_at("School", &mut values, &ObjectLocation::root())
            .unwrap();
        assert_eq!(values["school_type_id"], Value::U64(1002));
        assert!(!values.contains_key("school_type"));
        let context =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(school()));
        values.insert(
            "school_type_id".into(),
            Value::Decimal("0.1".parse().unwrap()),
        );
        let error = context
            .check_and_fix_values("School", &mut values)
            .unwrap_err();
        let crate::RuntimeError::Check(results) = error else {
            panic!("expected canonical invalid_type")
        };
        assert!(results.iter().any(|r| r.rule == CheckRule::InvalidType
            && r.location == ObjectLocation::root().member("school_type")));
    }
}
