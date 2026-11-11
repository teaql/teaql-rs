//! Roll back only staged automatic Context intent. Caller/business edits, other
//! entity types with the same numeric ID, and dynamic mutations are untouched.
use super::*;

pub(super) struct ContextUndo {
    root: crate::EntityRuntimeState,
    fields: Vec<(crate::EntityKey, String, Option<Value>)>,
    armed: bool,
}

impl ContextUndo {
    pub(super) fn capture(
        context: &UserContext,
        root: &crate::EntityRuntimeState,
        node: &GraphNode,
    ) -> Self {
        fn collect(node: &GraphNode, keys: &mut BTreeSet<crate::EntityKey>) {
            if let Some(id) = node.values.get("id") {
                keys.insert(crate::EntityKey::new(node.entity.clone(), id.clone()));
            }
            for children in node.relations.values() {
                for child in children {
                    collect(child, keys);
                }
            }
        }
        let changes = root.current_change_set();
        let mut keys: BTreeSet<_> = changes.changes().keys().cloned().collect();
        if let Some(dynamic) = changes.dynamic_changes() {
            keys.extend(dynamic.keys().cloned());
        }
        collect(node, &mut keys);
        let mut fields = vec![];
        for key in keys {
            if let Some(checker) = context
                .checker_registry
                .as_ref()
                .and_then(|registry| registry.checker(&key.entity))
            {
                let names: BTreeSet<_> = checker
                    .managed_context_fields()
                    .iter()
                    .chain(checker.custom_context_fields())
                    .copied()
                    .collect();
                for name in names {
                    fields.push((key.clone(), name.to_owned(), root.get(&key, name)));
                }
            }
        }
        Self {
            root: root.clone(),
            fields,
            armed: true,
        }
    }
    pub(super) fn accept(&mut self) {
        self.armed = false;
    }
}
impl Drop for ContextUndo {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for (key, field, before) in &self.fields {
            if let Some(value) = before {
                self.root.set(key.clone(), field.clone(), value.clone());
            } else {
                self.root.discard_restored_field(key, field);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CheckResults, Checker, InMemoryCheckerRegistry};
    struct Automatic;
    impl Checker for Automatic {
        fn entity(&self) -> &str {
            "Entry"
        }
        fn managed_context_fields(&self) -> &'static [&'static str] {
            &["touched_at"]
        }
        fn custom_context_fields(&self) -> &'static [&'static str] {
            &["last_ip"]
        }
        fn check_and_fix(
            &self,
            _: &UserContext,
            _: &mut crate::EntityValues,
            _: &ObjectLocation,
            _: &mut CheckResults,
        ) {
        }
    }
    fn fixture() -> (
        UserContext,
        crate::EntityRuntimeState,
        GraphNode,
        crate::EntityKey,
    ) {
        let mut context = UserContext::new();
        context.set_checker_registry(InMemoryCheckerRegistry::new().with_checker(Automatic));
        let root = crate::EntityRuntimeState::default();
        let key = crate::EntityKey::new("Entry", 7_u64);
        root.set(key.clone(), "name", "caller edit");
        root.set(key.clone(), "last_ip", "caller-provided value");
        let mut node = GraphNode::new("Entry");
        node.values.insert("id".into(), Value::U64(7));
        (context, root, node, key)
    }
    #[test]
    fn rejection_restores_only_declared_context_fields_and_preserves_other_types() {
        let (context, root, node, key) = fixture();
        let other = crate::EntityKey::new("Unrelated", 7_u64);
        let undo = ContextUndo::capture(&context, &root, &node);
        root.set(key.clone(), "touched_at", 900_i64);
        root.set(key.clone(), "last_ip", "automatic value");
        root.set(key.clone(), "name", "business repair");
        root.set(other.clone(), "touched_at", 123_i64);
        drop(undo);
        assert_eq!(root.get(&key, "touched_at"), None);
        assert_eq!(
            root.get(&key, "last_ip"),
            Some(Value::from("caller-provided value"))
        );
        assert_eq!(root.get(&key, "name"), Some(Value::from("business repair")));
        assert_eq!(root.get(&other, "touched_at"), Some(Value::I64(123)));
    }
    #[test]
    fn accepted_staging_does_not_restore_caller_intent_after_commit() {
        let (context, root, node, key) = fixture();
        let mut undo = ContextUndo::capture(&context, &root, &node);
        root.set(key.clone(), "touched_at", 900_i64);
        undo.accept();
        drop(undo);
        assert_eq!(root.get(&key, "touched_at"), Some(Value::I64(900)));
    }
}
