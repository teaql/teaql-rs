//! Model-owned save admission, independent of customizable Fix/Checker logic.
use super::*;
use crate::EntityValues;

pub(super) fn validate(
    context: &UserContext,
    node: &GraphNode,
    location: &ObjectLocation,
    root: Option<&crate::EntityRuntimeState>,
) -> Result<(), RuntimeError> {
    let mut checked = BTreeSet::new();
    visit(context, node, location, root, &mut checked)?;
    if let Some(root) = root {
        let changes = root.current_change_set();
        let keys: BTreeSet<_> = changes
            .changes()
            .keys()
            .cloned()
            .chain(
                changes
                    .dynamic_changes()
                    .into_iter()
                    .flat_map(|fields| fields.keys().cloned()),
            )
            .chain(root.deleted_keys())
            .chain(root.new_keys())
            .collect();
        let locations = ledger_object_locations(node);
        for key in keys {
            if checked.contains(&key) || (root.is_new(&key) && root.is_marked_as_delete(&key)) {
                continue;
            }
            let descriptor = context.require_entity(&key.entity)?;
            // Do not introduce snapshot/loading requirements for unrelated types.
            if !descriptor
                .properties
                .iter()
                .any(|p| p.context_root_entity.is_some())
            {
                continue;
            }
            let original = root.original_values_for(&key);
            let mut values: EntityValues = original
                .clone()
                .map(|v| BTreeMap::from(v).into())
                .unwrap_or_default();
            if let Some(pending) = changes.changes().get(&key) {
                values.extend(storage_values_from_ledger(descriptor, pending)?);
            }
            validate_values(
                context,
                descriptor,
                &values,
                original.as_ref(),
                root.is_new(&key),
                locations.get(&key).unwrap_or(location),
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor};

    fn descriptor() -> EntityDescriptor {
        EntityDescriptor::new("Document")
            .property(PropertyDescriptor::new("scope_id", DataType::U64).context_root("Account"))
            .relation(
                RelationDescriptor::new("scope", "Account")
                    .local_key("scope_id")
                    .foreign_key("id"),
            )
    }
    fn values(id: Value) -> EntityValues {
        BTreeMap::from([("scope_id".into(), id)]).into()
    }
    fn original(id: Value) -> teaql_core::EntitySnapshot {
        BTreeMap::from([("scope_id".into(), id)]).into()
    }
    #[test]
    fn context_roots_validate_both_original_and_requested_ownership() {
        let context = UserContext::new().with_active_root("Account", 1);
        let descriptor = descriptor();
        let at = ObjectLocation::root();
        for (current, original_id, allowed) in [
            (1, 1, true),
            (2, 1, false),
            (1, 2, false),
            (2, 2, false),
            (0, 1, false),
        ] {
            let result = validate_values(
                &context,
                &descriptor,
                &values(Value::U64(current)),
                Some(&original(Value::U64(original_id))),
                false,
                &at,
            );
            assert_eq!(
                result.is_ok(),
                allowed,
                "current={current}, original={original_id}"
            );
            if let Err(RuntimeError::Check(results)) = result {
                assert_eq!(results[0].entity_type.as_deref(), Some("Document"));
                assert_eq!(results[0].location.instance_path(), "/scope");
                assert_eq!(results[0].rule, crate::CheckRule::ContextRootMismatch);
            }
        }
    }
    #[test]
    fn missing_original_is_not_replaced_by_the_requested_owner() {
        assert!(
            validate_values(
                &UserContext::new().with_active_root("Account", 1),
                &descriptor(),
                &values(Value::U64(1)),
                None,
                false,
                &ObjectLocation::root()
            )
            .is_err()
        );
    }
    #[test]
    fn only_missing_or_zero_create_ownership_can_be_fixed() {
        let context = UserContext::new().with_active_root("Account", 1);
        for value in [
            None,
            Some(Value::Null),
            Some(Value::U64(0)),
            Some(Value::U64(1)),
        ] {
            let map = value.map(values).unwrap_or_default();
            validate_values(
                &context,
                &descriptor(),
                &map,
                None,
                true,
                &ObjectLocation::root(),
            )
            .unwrap();
        }
        for value in [
            Value::U64(2),
            Value::I64(-1),
            Value::from("1"),
            Value::Decimal("1".parse().unwrap()),
        ] {
            assert!(
                validate_values(
                    &context,
                    &descriptor(),
                    &values(value),
                    None,
                    true,
                    &ObjectLocation::root()
                )
                .is_err()
            );
        }
    }
    #[test]
    fn missing_and_incompatible_contexts_are_structured_failures() {
        for context in [
            UserContext::new(),
            UserContext::new().with_active_root("OtherType", 1),
        ] {
            let Err(RuntimeError::Check(results)) = validate_values(
                &context,
                &descriptor(),
                &values(Value::U64(1)),
                Some(&original(Value::U64(1))),
                false,
                &ObjectLocation::root().member("items").element(0),
            ) else {
                panic!("root accepted")
            };
            assert_eq!(results[0].rule, crate::CheckRule::ContextRootMissing);
            assert_eq!(results[0].location.instance_path(), "/items/0/scope");
        }
    }
    #[test]
    fn macro_carries_model_metadata_without_a_tenant_concept() {
        #[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
        struct MetadataProbe {
            #[teaql(id)]
            id: u64,
            #[teaql(context_root = "Account")]
            scope_id: u64,
        }
        let d = <MetadataProbe as teaql_core::TeaqlEntity>::entity_descriptor();
        assert_eq!(
            <MetadataProbe as teaql_core::TeaqlEntity>::CONTEXT_ROOT_METADATA_REVISION,
            1
        );
        assert_eq!(
            d.property_by_name("scope_id")
                .unwrap()
                .context_root_entity
                .as_deref(),
            Some("Account")
        );
        assert_eq!(d.property_by_name("id").unwrap().context_root_entity, None);
    }

    #[test]
    fn removed_nodes_do_not_bypass_original_ownership() {
        let context = UserContext::new()
            .with_metadata(crate::InMemoryMetadataStore::new().with_entity(descriptor()))
            .with_active_root("Account", 2);
        let mut node = GraphNode::new("Document");
        node.operation = GraphOperation::Remove;
        node.values = values(Value::U64(1));
        node.original_values = Some(original(Value::U64(1)));
        assert!(validate(&context, &node, &ObjectLocation::root(), None).is_err());
    }

    #[test]
    fn untouched_references_do_not_require_ownership_or_full_loading() {
        let context = UserContext::new();
        let mut node = GraphNode::new("Document");
        node.operation = GraphOperation::Reference;
        validate(&context, &node, &ObjectLocation::root(), None).unwrap();
    }

    #[test]
    fn dynamic_only_changes_require_the_same_original_context_root() {
        use teaql_core::dynamic_fields::{DynamicFieldDefinitions, DynamicFieldValues};
        let mut root = crate::EntityRuntimeState::default();
        root.set_original_compact_row(
            "Document",
            teaql_core::CompactRow::from_map(BTreeMap::from([
                ("id".into(), Value::U64(42)),
                ("version".into(), Value::I64(1)),
                ("scope_id".into(), Value::U64(1)),
            ])),
        );
        let definitions = DynamicFieldDefinitions::new(
            "Document",
            "root-test",
            [("note".into(), DataType::Text)],
        )
        .unwrap();
        let fields =
            DynamicFieldValues::from_batch(definitions, vec![std::collections::HashMap::new()])
                .unwrap()
                .remove(0);
        root.install_loaded_dynamic_fields(fields);
        root.update_dynamic_field(
            crate::EntityKey::new_static("Document", 42u64),
            "note",
            Value::from("changed"),
        )
        .unwrap();
        assert!(root.current_change_set().changes().is_empty());
        assert!(root.has_pending_dynamic_mutations());
        let mut node = GraphNode::new("Document");
        node.operation = GraphOperation::Reference;
        for id in [1, 2] {
            let context = UserContext::new()
                .with_active_root("Account", id)
                .with_metadata(crate::InMemoryMetadataStore::new().with_entity(descriptor()));
            assert_eq!(
                validate(&context, &node, &ObjectLocation::root(), Some(&root)).is_ok(),
                id == 1
            );
        }
    }

    #[test]
    fn pending_deletions_outside_the_loaded_graph_are_checked() {
        let context = UserContext::new()
            .with_metadata(crate::InMemoryMetadataStore::new().with_entity(descriptor()));
        let mut node = GraphNode::new("UnmodifiedRoot");
        node.operation = GraphOperation::Reference;
        let root = crate::EntityRuntimeState::default();
        root.mark_as_delete(crate::EntityKey::new_static("Document", 42u64));
        let Err(RuntimeError::Check(results)) =
            validate(&context, &node, &ObjectLocation::root(), Some(&root))
        else {
            panic!("deleted pending node skipped")
        };
        assert_eq!(results[0].entity_type.as_deref(), Some("Document"));
        assert_eq!(results[0].rule, crate::CheckRule::ContextRootMissing);
    }

    #[test]
    fn cancelled_new_children_require_no_context_because_they_are_not_persisted() {
        let context = UserContext::new()
            .with_metadata(crate::InMemoryMetadataStore::new().with_entity(descriptor()));
        let root = crate::EntityRuntimeState::default();
        let key = crate::EntityKey::new_static("Document", 42u64);
        root.mark_as_new(key.clone());
        root.mark_as_delete(key);
        let mut node = GraphNode::new("UnchangedOwner");
        node.operation = GraphOperation::Reference;
        validate(&context, &node, &ObjectLocation::root(), Some(&root)).unwrap();
    }

    #[test]
    fn cancelled_new_children_in_the_materialized_graph_are_omitted_too() {
        let context = UserContext::new()
            .with_metadata(crate::InMemoryMetadataStore::new().with_entity(descriptor()));
        let root = crate::EntityRuntimeState::default();
        let key = crate::EntityKey::new_static("Document", 42u64);
        root.mark_as_new(key.clone());
        root.mark_as_delete(key);
        let mut parent = GraphNode::new("UnchangedOwner");
        parent.operation = GraphOperation::Reference;
        let mut child = GraphNode::new("Document");
        child.operation = GraphOperation::Remove;
        child.values.insert("id".into(), Value::U64(42));
        parent.relations.insert("items".into(), vec![child]);
        validate(&context, &parent, &ObjectLocation::root(), Some(&root)).unwrap();
    }

    #[test]
    fn the_entire_graph_is_admitted_before_any_custom_fix() {
        struct NeverFix;
        impl crate::Checker for NeverFix {
            fn entity(&self) -> &str {
                "Parent"
            }
            fn check_and_fix(
                &self,
                _: &UserContext,
                _: &mut EntityValues,
                _: &ObjectLocation,
                _: &mut crate::CheckResults,
            ) {
                panic!("parent Fix ran before child ownership admission");
            }
        }
        let context = UserContext::new()
            .with_active_root("Account", 2)
            .with_metadata(
                crate::InMemoryMetadataStore::new()
                    .with_entity(EntityDescriptor::new("Parent"))
                    .with_entity(descriptor()),
            )
            .with_checker_registry(crate::InMemoryCheckerRegistry::new().with_checker(NeverFix));
        let mut node = GraphNode::new("Parent");
        let mut child = GraphNode::new("Document");
        child.values = values(Value::U64(1));
        child.original_values = Some(original(Value::U64(1)));
        node.relations.insert("items".into(), vec![child]);
        let Err(RuntimeError::Check(results)) =
            preflight_graph(&context, &mut node, &ObjectLocation::root(), None)
        else {
            panic!("child ownership accepted")
        };
        assert_eq!(results[0].location.instance_path(), "/items/0/scope");
    }
}

fn visit(
    context: &UserContext,
    node: &GraphNode,
    location: &ObjectLocation,
    root: Option<&crate::EntityRuntimeState>,
    checked: &mut BTreeSet<crate::EntityKey>,
) -> Result<(), RuntimeError> {
    if node.operation != GraphOperation::Reference {
        let descriptor = context.require_entity(&node.entity)?;
        if descriptor
            .properties
            .iter()
            .any(|property| property.context_root_entity.is_some())
        {
            let key = node
                .values
                .get("id")
                .map(|id| crate::EntityKey::new(node.entity.clone(), id.clone()));
            let original = key
                .as_ref()
                .and_then(|key| root.and_then(|root| root.original_values_for(key)))
                .or_else(|| node.original_values.clone());
            if !key.as_ref().is_some_and(|key| {
                root.is_some_and(|root| root.is_new(key) && root.is_marked_as_delete(key))
            }) {
                validate_values(
                    context,
                    descriptor,
                    &node.values,
                    original.as_ref(),
                    node.operation == GraphOperation::Create,
                    location,
                )?;
            }
            if let Some(key) = key {
                checked.insert(key);
            }
        }
    }
    for (name, children) in &node.relations {
        for (index, child) in children.iter().enumerate() {
            visit(
                context,
                child,
                &location.clone().member(name).element(index),
                root,
                checked,
            )?;
        }
    }
    Ok(())
}

fn validate_values(
    context: &UserContext,
    descriptor: &teaql_core::EntityDescriptor,
    values: &EntityValues,
    original: Option<&teaql_core::EntitySnapshot>,
    create: bool,
    location: &ObjectLocation,
) -> Result<(), RuntimeError> {
    let mut results = Vec::new();
    for property in &descriptor.properties {
        let Some(target) = &property.context_root_entity else {
            continue;
        };
        let field = canonical_field_from_storage(descriptor, &property.name)?;
        let at = location.clone().member(field);
        let root = match context.require_active_root(target) {
            Ok(root) => root,
            Err(error) => {
                results.push(
                    crate::CheckResult::new(crate::CheckRule::ContextRootMissing, at)
                        .with_entity_type(&descriptor.name)
                        .with_message(error.to_string()),
                );
                continue;
            }
        };
        let current = values.get(&property.name);
        if !create && original.and_then(|row| row.get(&property.name)).is_none() {
            results.push(
                crate::CheckResult::new(crate::CheckRule::InvalidType, at)
                    .with_entity_type(&descriptor.name)
                    .with_message(
                        "Load the modeled context-root field before mutating this object",
                    ),
            );
            continue;
        }
        // Only a missing/zero CREATE can be filled by the generated contextual Fix.
        let empty_create = create
            && (matches!(current, None | Some(Value::Null))
                || current.and_then(root_id) == Some(0));
        let current_matches = current.and_then(root_id) == Some(root.id);
        let original_matches = create
            || original
                .and_then(|row| row.get(&property.name))
                .and_then(root_id)
                == Some(root.id);
        if (!empty_create && !current_matches) || !original_matches {
            results.push(
                crate::CheckResult::new(crate::CheckRule::ContextRootMismatch, at)
                    .with_entity_type(&descriptor.name),
            );
        }
    }
    if results.is_empty() {
        Ok(())
    } else {
        context.translate_check_results(&mut results);
        Err(RuntimeError::Check(results))
    }
}

fn root_id(value: &Value) -> Option<u64> {
    match value {
        Value::U64(id) => Some(*id),
        Value::I64(id) => u64::try_from(*id).ok(),
        _ => None,
    }
}
