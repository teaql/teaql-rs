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

#[cfg(test)]
mod tests {
    use super::{canonical_values_from_storage, storage_values_from_ledger};
    use std::collections::BTreeMap;
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
}
