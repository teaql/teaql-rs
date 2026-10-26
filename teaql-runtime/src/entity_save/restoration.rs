//! Cancel only model-admitted native edits against a type-qualified persisted
//! snapshot. This belongs at save admission, never in the raw ledger setter.
use super::*;

pub(super) fn prepare(
    context: &UserContext,
    root: &crate::EntityRuntimeState,
    node: &mut GraphNode,
) -> Result<(), RuntimeError> {
    // Validate the complete input before discarding anything. Equal unknown,
    // readonly-result or native-member aliases must not evade admission.
    for (key, values) in root.current_change_set().changes() {
        storage_values_from_ledger(context.require_entity(&key.entity)?, values)?;
    }
    visit(context, root, node)
}

fn visit(
    context: &UserContext,
    root: &crate::EntityRuntimeState,
    node: &mut GraphNode,
) -> Result<(), RuntimeError> {
    if node.operation == GraphOperation::Upsert
        && let Some(id) = node.values.get("id")
    {
        let key = crate::EntityKey::new(node.entity.clone(), id.clone());
        if !root.is_new(&key)
            && !root.is_marked_as_delete(&key)
            && let Some(original) = root.original_values_for(&key)
            && original
                .get("version")
                .and_then(Value::try_i64)
                .is_some_and(|v| v > 0)
        {
            let descriptor = context.require_entity(&node.entity)?;
            let changes = root.current_change_set();
            if let Some(values) = changes.changes().get(&key) {
                let storage = storage_values_from_ledger(descriptor, values)?;
                for (field, value) in &storage {
                    // Identity/version edits and compatibility aliases retain
                    // their existing validation path, even if numerically equal.
                    if field == "id" || field == "version" || field.starts_with('_') {
                        continue;
                    }
                    let canonical = canonical_field_from_storage(descriptor, field)?;
                    if !values.contains_key(&canonical) {
                        continue;
                    }
                    if let Some(property) = descriptor.property_by_name(field)
                        && original.get(field).is_some_and(|before| {
                            same_property_value(property.data_type, before, value)
                        })
                    {
                        root.discard_restored_field(&key, &canonical);
                        if let Some(dirty) = &mut node.dirty_fields {
                            dirty.remove(field);
                            dirty.remove(&canonical);
                        }
                    }
                }
            }
            // An empty root alone is insufficient: recursion still visits dirty
            // children. Compare actual typed values too, so public-field edits
            // do not accidentally become no-ops when dirty tracking is absent.
            if root.changed_field_names(&key).is_empty()
                && node.dirty_fields.as_ref().is_none_or(BTreeSet::is_empty)
                && descriptor.properties.iter().all(|property| {
                    node.values.get(&property.name).is_none_or(|current| {
                        original.get(&property.name).is_some_and(|before| {
                            same_property_value(property.data_type, before, current)
                        })
                    })
                })
            {
                node.operation = GraphOperation::Reference;
            }
        }
    }
    for children in node.relations.values_mut() {
        for child in children {
            visit(context, root, child)?;
        }
    }
    Ok(())
}

pub(super) fn graph_is_unchanged(node: &GraphNode) -> bool {
    node.operation == GraphOperation::Reference
        && node.relations.values().flatten().all(graph_is_unchanged)
}

// Preserve representationally significant changes: decimal scale and signed
// floating zero matter for the exact-scalar contract. Unknown != loaded null.
fn same_property_value(data_type: teaql_core::DataType, before: &Value, current: &Value) -> bool {
    // SQLite's immutable loaded row retains integral milliseconds, while the
    // typed view contains Timestamp. Only timestamp metadata admits this
    // provider representation bridge; never coerce arbitrary text or floats.
    if data_type == teaql_core::DataType::Timestamp {
        match (before, current) {
            (Value::I64(a), Value::Timestamp(b)) | (Value::Timestamp(b), Value::I64(a)) => {
                return *a == b.0;
            }
            (Value::U64(a), Value::Timestamp(b)) | (Value::Timestamp(b), Value::U64(a)) => {
                return i64::try_from(*a).ok() == Some(b.0);
            }
            _ => {}
        }
    }
    same_value(before, current)
}

fn same_value(before: &Value, current: &Value) -> bool {
    match (before, current) {
        (Value::Null | Value::TypedNull(_), Value::Null | Value::TypedNull(_)) => true,
        (Value::I64(a), Value::U64(b)) | (Value::U64(b), Value::I64(a)) => {
            u64::try_from(*a).ok() == Some(*b)
        }
        (Value::F64(a), Value::F64(b)) => {
            a.is_finite() && b.is_finite() && a.to_bits() == b.to_bits()
        }
        (Value::Decimal(a), Value::Decimal(b)) => a.serialize() == b.serialize(),
        (Value::Json(a), Value::Json(b)) => a.to_string() == b.to_string(),
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, value)| b.get(key).is_some_and(|other| same_value(value, other)))
        }
        (Value::List(a), Value::List(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_value(a, b))
        }
        _ => before == current,
    }
}

#[cfg(test)]
mod tests;
