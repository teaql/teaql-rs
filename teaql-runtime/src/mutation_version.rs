use crate::{CheckResult, CheckRule, ObjectLocation, RuntimeError};
use teaql_core::mutation_version::{self, MutationVersionError};

pub(crate) fn error(entity: &str, error: MutationVersionError) -> RuntimeError {
    let rule = match error {
        MutationVersionError::InvalidDelete(_) => CheckRule::InvalidDeletionState,
        MutationVersionError::Exhausted { .. } => CheckRule::VersionExhausted,
        _ => CheckRule::InvalidVersionState,
    };
    RuntimeError::Check(vec![
        CheckResult::new(rule, ObjectLocation::root().member("version"))
            .with_entity_type(entity)
            .with_input_value(error.version())
            .with_message(error.to_string()),
    ])
}

pub(crate) fn updated(entity: &str, version: Option<i64>) -> Result<Option<i64>, RuntimeError> {
    version
        .map(mutation_version::updated)
        .transpose()
        .map_err(|cause| error(entity, cause))
}

pub(crate) fn deleted(entity: &str, version: Option<i64>) -> Result<Option<i64>, RuntimeError> {
    version
        .map(mutation_version::deleted)
        .transpose()
        .map_err(|cause| error(entity, cause))
}

pub(crate) fn recovered(entity: &str, version: i64) -> Result<i64, RuntimeError> {
    mutation_version::recovered(version).map_err(|cause| error(entity, cause))
}

/// Check known mutation inputs before an upsert planner performs existence reads.
/// An unclassified Upsert with version zero may still be a new, preassigned-ID
/// object; it needs normal planning unless an original snapshot/dirty set proves
/// it is an update. Explicit Create and Reference retain their distinct roles.
pub(crate) fn graph_input(
    context: &crate::UserContext,
    root: &crate::GraphNode,
) -> Result<(), RuntimeError> {
    let mut pending = vec![(root, false)];
    while let Some((node, parent_is_create)) = pending.pop() {
        if node.operation == crate::GraphOperation::Reference {
            continue;
        }
        let descriptor = context.require_entity(&node.entity)?;
        let version = descriptor
            .version_property()
            .and_then(|field| node.values.get(&field.name))
            .and_then(teaql_core::Value::try_i64);
        if node.operation == crate::GraphOperation::Remove {
            deleted(&node.entity, version)?;
            continue;
        }
        if node.operation == crate::GraphOperation::Recover {
            let version = version.ok_or_else(|| {
                RuntimeError::Graph("recovery requires a loaded original tombstone version".into())
            })?;
            recovered(&node.entity, version)?;
            for children in node.relations.values().rev() {
                pending.extend(children.iter().rev().map(|child| (child, false)));
            }
            continue;
        }
        let is_create = node.operation == crate::GraphOperation::Create
            || (parent_is_create && node.operation == crate::GraphOperation::Upsert);
        let unchanged = node
            .dirty_fields
            .as_ref()
            .is_some_and(|fields| fields.is_empty());
        if !is_create
            && !unchanged
            && let Some(version) = version
            && (version != 0 || node.original_values.is_some() || node.dirty_fields.is_some())
        {
            updated(&node.entity, Some(version))?;
        }
        for children in node.relations.values().rev() {
            pending.extend(children.iter().rev().map(|child| (child, is_create)));
        }
    }
    Ok(())
}

#[cfg(test)]
mod version_lifecycle_tests {
    use super::*;
    use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, Value};
    fn context() -> crate::UserContext {
        crate::UserContext::default().with_metadata(
            crate::InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("Probe")
                    .property(PropertyDescriptor::new("id", DataType::U64).id())
                    .property(PropertyDescriptor::new("version", DataType::I64).version()),
            ),
        )
    }
    #[test]
    fn version_lifecycle_preflight_keeps_unclassified_zero_version_new_object() {
        let node = crate::GraphNode::new("Probe")
            .value("id", 1_u64)
            .value("version", 0_i64);
        assert!(graph_input(&context(), &node).is_ok());
        let mut known_update = node;
        known_update.dirty_fields = Some(std::collections::BTreeSet::from(["name".into()]));
        assert!(graph_input(&context(), &known_update).is_err());
    }
    #[test]
    fn version_lifecycle_preflight_checks_nested_update_before_planner_reads() {
        let mut node = crate::GraphNode::new("Probe")
            .value("id", 1_u64)
            .value("version", 1_i64);
        node.relations.insert(
            "children".into(),
            vec![
                crate::GraphNode::new("Probe")
                    .value("id", 2_u64)
                    .value("version", i64::MAX),
            ],
        );
        assert!(graph_input(&context(), &node).is_err());
    }
    #[test]
    fn version_lifecycle_preflight_keeps_unchanged_exhausted_association() {
        let mut node = crate::GraphNode::new("Probe")
            .value("id", 1_u64)
            .value("version", i64::MAX);
        node.dirty_fields = Some(std::collections::BTreeSet::new());
        assert!(graph_input(&context(), &node).is_ok());
        node.dirty_fields = Some(std::collections::BTreeSet::from(["name".into()]));
        assert!(graph_input(&context(), &node).is_err());
    }
    #[test]
    fn version_lifecycle_preflight_preserves_explicit_create_and_reference_roles() {
        let mut node = crate::GraphNode::new("Probe")
            .value("id", 1_u64)
            .value("version", 0_i64);
        node.operation = crate::GraphOperation::Create;
        node.relations.insert(
            "children".into(),
            vec![
                crate::GraphNode::new("Probe")
                    .value("id", 2_u64)
                    .value("version", 0_i64),
            ],
        );
        assert!(graph_input(&context(), &node).is_ok());
        node.operation = crate::GraphOperation::Reference;
        node.values.insert("version".into(), Value::I64(i64::MIN));
        assert!(graph_input(&context(), &node).is_ok());
    }
}
