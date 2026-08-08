use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use teaql_core::{Entity, Record, Value};

use crate::{DataServiceError, GraphNode, GraphOperation, RuntimeError, UserContext};

// ---------------------------------------------------------------------------
// DynGraphSaver — type-erased graph save capability
// ---------------------------------------------------------------------------

/// Object-safe trait for saving a [`GraphNode`] tree to the database.
///
/// A concrete implementation is registered in [`UserContext`] during setup so
/// that [`Audited::save`] can persist entities without exposing the underlying
/// executor type to business code.
pub(crate) trait DynGraphSaver: Send + Sync {
    fn save_graph_dyn<'a>(
        &'a self,
        ctx: &'a UserContext,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;

    fn save_ledger_dyn<'a>(
        &'a self,
        ctx: &'a UserContext,
        node: GraphNode,
        root: crate::EntityRoot,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;
}

/// Marker struct that implements [`DynGraphSaver`] for a specific executor type `E`.
///
/// `E` is the full executor type (e.g. `SqlDataServiceExecutor<SqliteDialect, …>`).
/// The struct itself is zero-sized; the actual executor is retrieved from
/// [`UserContext`] at call time.
pub(crate) struct GraphSaverFor<E> {
    _marker: PhantomData<fn() -> E>,
}

impl<E> GraphSaverFor<E> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<E> DynGraphSaver for GraphSaverFor<E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + Send
        + Sync
        + 'static,
{
    fn save_graph_dyn<'a>(
        &'a self,
        ctx: &'a UserContext,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity = node.entity.clone();
            let eds = ctx
                .entity_data_service::<E>(entity)
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            eds.save_graph_internal(node).await.map_err(|e| match e {
                DataServiceError::Runtime(r) => r,
                other => RuntimeError::Graph(other.to_string()),
            })
        })
    }

    fn save_ledger_dyn<'a>(
        &'a self,
        ctx: &'a UserContext,
        mut node: GraphNode,
        root: crate::EntityRoot,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity = node.entity.clone();
            let eds = ctx
                .entity_data_service::<E>(&entity)
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let generated_ids =
                eds.execute_ledger_plan_internal(root)
                    .await
                    .map_err(|e| match e {
                        DataServiceError::Runtime(r) => r,
                        other => RuntimeError::Graph(other.to_string()),
                    })?;

            let descriptor = ctx.require_entity(&entity).unwrap();
            if let Some(id_prop) = descriptor.id_property() {
                let current_id = node
                    .values
                    .get(&id_prop.name)
                    .cloned()
                    .unwrap_or(Value::I64(0));
                let root_key = crate::EntityKey::new(entity.clone(), current_id);
                if let Some(new_id) = generated_ids.get(&root_key) {
                    node.values.insert(id_prop.name.clone(), new_id.clone());
                }
            }
            Ok(node)
        })
    }
}

// ---------------------------------------------------------------------------
// Standalone graph-node extraction (no executor needed)
// ---------------------------------------------------------------------------

/// Convert a typed entity into a [`GraphNode`] tree.
///
/// This only requires metadata (entity descriptors) from the [`UserContext`],
/// **not** the database executor.  It is the standalone equivalent of
/// [`EntityDataService::graph_node_from_entity`].
pub fn graph_node_from_entity<T: Entity>(
    ctx: &UserContext,
    entity: T,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = T::entity_descriptor();
    let dirty_fields = entity.dirty_fields();
    let original_values = entity.original_values();
    let is_deleted = entity.is_marked_as_delete();
    let comment = entity.get_comment();
    let mut node = graph_node_from_record(ctx, &descriptor.name, entity.into_record())?;
    node.dirty_fields = dirty_fields;
    node.original_values = original_values;
    if is_deleted {
        node.operation = GraphOperation::Remove;
        node.relations.clear();
    }
    if let Some(c) = comment {
        node.set_comment(c);
    }
    Ok(node)
}

/// Recursively convert a [`Record`] into a [`GraphNode`] tree.
///
/// Relations are resolved via the entity descriptors stored in `ctx`.
fn graph_node_from_record(
    ctx: &UserContext,
    entity: &str,
    record: Record,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = ctx.require_entity(entity)?;
    let mut node = GraphNode::new(entity);

    for (field, value) in record {
        if field == "_comment" {
            if let Value::Text(comment) = value {
                node.set_comment(comment);
            }
            continue;
        }
        if field == "_dirty_fields" {
            if let Value::List(fields) = value {
                let mut dirty = BTreeSet::new();
                for f in fields {
                    if let Value::Text(t) = f {
                        dirty.insert(t);
                    }
                }
                node.dirty_fields = Some(dirty);
            }
            continue;
        }
        if field == "_original_values" {
            if let Value::Object(orig) = value {
                node.original_values = Some(orig);
            }
            continue;
        }
        let Some(relation) = descriptor.relation_by_name(&field) else {
            node.values.insert(field, value);
            continue;
        };

        match value {
            Value::Null => {
                node.relations.entry(field).or_default();
            }
            Value::Object(record) => {
                let child = graph_node_from_record(ctx, &relation.target_entity, record)?;
                node.relations.entry(field).or_default().push(child);
            }
            Value::List(values) => {
                let children = node.relations.entry(field.clone()).or_default();
                for value in values {
                    let Value::Object(record) = value else {
                        return Err(RuntimeError::Graph(format!(
                            "relation {}.{} expects object children, got {:?}",
                            entity, field, value
                        )));
                    };
                    children.push(graph_node_from_record(
                        ctx,
                        &relation.target_entity,
                        record,
                    )?);
                }
            }
            other => {
                return Err(RuntimeError::Graph(format!(
                    "relation {}.{} expects object/list/null, got {:?}",
                    entity, field, other
                )));
            }
        }
    }

    Ok(node)
}

// ---------------------------------------------------------------------------
// AuditedSaveExt — the `.save(&ctx)` method on `Audited<T>`
// ---------------------------------------------------------------------------

/// Extension trait that provides the `.save(&ctx)` method on [`Audited<T>`](teaql_core::Audited).
///
/// # Example
/// ```ignore
/// use teaql_runtime::AuditedSaveExt;
///
/// school.audit_as("创建学校").save(&ctx).await?;
/// ```
pub trait AuditedSaveExt {
    fn save<'a>(
        self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;
}

impl<T> AuditedSaveExt for teaql_core::Audited<T>
where
    T: Entity + Send + 'static,
{
    fn save<'a>(
        self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity_name = T::entity_descriptor().name;
            let entity = self.into_entity(); // applies comment onto the entity
            let node = graph_node_from_entity(ctx, entity)?;
            let saver = ctx
                .require_resource::<Arc<dyn DynGraphSaver>>()
                .map_err(|e| {
                    RuntimeError::Graph(format!(
                        "no DynGraphSaver registered — did you call register_executor()? ({})",
                        e
                    ))
                })?;
            saver.save_graph_dyn(ctx, node).await
        })
    }
}

/// Persist an audited generated entity, including pending ledger changes that
/// may span multiple related entities sharing the same [`EntityRoot`](crate::EntityRoot).
///
/// Generated service crates use this as the implementation behind
/// `entity.audit_as("why").save(&ctx)`. The audited wrapper is required by the
/// function signature; no unaudited entity write entry point is exposed.
#[doc(hidden)]
pub async fn save_audited_ledger_entity<T>(
    audited: teaql_core::Audited<T>,
    ctx: &UserContext,
) -> Result<GraphNode, RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
{
    let entity_name = T::entity_descriptor().name;
    let entity = audited.into_entity();
    let root = entity.entity_root();
    let node = graph_node_from_entity(ctx, entity)?;
    let saver = ctx
        .require_resource::<Arc<dyn DynGraphSaver>>()
        .map_err(|e| {
            RuntimeError::Graph(format!(
                "no DynGraphSaver registered — did you call register_executor()? ({e})"
            ))
        })?;

    if let Some(root) = root {
        let has_ledger_changes = !root.current_change_set().changes().is_empty()
            || !root.deleted_keys().is_empty()
            || !root.new_keys().is_empty();
        if has_ledger_changes {
            return saver.save_ledger_dyn(ctx, node, root).await;
        }
    }

    saver.save_graph_dyn(ctx, node).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryMetadataStore, UserContext};
    use teaql_core::{
        DataType, EntityDescriptor, PropertyDescriptor, Record, RelationDescriptor, Value,
    };

    #[test]
    fn test_graph_node_from_record() {
        let mut registry = InMemoryMetadataStore::new();

        let mut user = EntityDescriptor::new("User");
        user.properties
            .push(PropertyDescriptor::new("id", DataType::U64));
        user.relations
            .push(RelationDescriptor::new("profile", "Profile"));
        registry = registry.with_entity(user);

        let mut profile = EntityDescriptor::new("Profile");
        profile
            .properties
            .push(PropertyDescriptor::new("id", DataType::U64));
        registry = registry.with_entity(profile);

        let ctx = UserContext::new().with_metadata(registry);

        let mut profile_record = Record::new();
        profile_record.insert("id".into(), Value::U64(2));

        let mut record = Record::new();
        record.insert("id".into(), Value::U64(1));
        record.insert("_comment".into(), Value::Text("hello".into()));
        record.insert("profile".into(), Value::Object(profile_record));

        let node = graph_node_from_record(&ctx, "User", record).unwrap();

        assert_eq!(node.entity, "User");
        assert_eq!(node.values.get("id"), Some(&Value::U64(1)));
        assert_eq!(node.comment, Some("hello".into()));

        let rels = node.relations.get("profile").unwrap();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].entity, "Profile");
        assert_eq!(rels[0].values.get("id"), Some(&Value::U64(2)));
    }

    #[test]
    fn test_graph_node_from_record_edge_cases() {
        let mut registry = InMemoryMetadataStore::new();

        let mut user = EntityDescriptor::new("User");
        user.relations
            .push(RelationDescriptor::new("profile", "Profile"));
        user.relations
            .push(RelationDescriptor::new("posts", "Post"));
        user.relations
            .push(RelationDescriptor::new("invalid_rel", "Invalid"));
        registry = registry.with_entity(user);

        let profile = EntityDescriptor::new("Profile");
        registry = registry.with_entity(profile);

        let post = EntityDescriptor::new("Post");
        registry = registry.with_entity(post);

        let invalid = EntityDescriptor::new("Invalid");
        registry = registry.with_entity(invalid);

        let ctx = UserContext::new().with_metadata(registry);

        // Test _dirty_fields and _original_values
        let mut record = Record::new();
        record.insert(
            "_dirty_fields".into(),
            Value::List(vec![Value::Text("name".into())]),
        );

        let mut orig = Record::new();
        orig.insert("name".into(), Value::Text("old".into()));
        record.insert("_original_values".into(), Value::Object(orig.clone()));

        let node = graph_node_from_record(&ctx, "User", record).unwrap();
        assert_eq!(
            node.dirty_fields.unwrap().iter().next().unwrap().as_str(),
            "name"
        );
        assert_eq!(
            node.original_values.unwrap().get("name"),
            Some(&Value::Text("old".into()))
        );

        // Test relation with null
        let mut record_null = Record::new();
        record_null.insert("profile".into(), Value::Null);
        let node_null = graph_node_from_record(&ctx, "User", record_null).unwrap();
        assert!(node_null.relations.get("profile").unwrap().is_empty());

        // Test relation with list
        let mut record_list = Record::new();
        let post_record = Record::new();
        record_list.insert(
            "posts".into(),
            Value::List(vec![Value::Object(post_record)]),
        );
        let node_list = graph_node_from_record(&ctx, "User", record_list).unwrap();
        assert_eq!(node_list.relations.get("posts").unwrap().len(), 1);

        // Test relation list with invalid item
        let mut record_list_invalid = Record::new();
        record_list_invalid.insert("posts".into(), Value::List(vec![Value::U64(1)]));
        let err = graph_node_from_record(&ctx, "User", record_list_invalid).unwrap_err();
        assert!(err.to_string().contains("expects object children"));

        // Test relation with invalid type (not null/object/list)
        let mut record_invalid_type = Record::new();
        record_invalid_type.insert("profile".into(), Value::U64(1));
        let err = graph_node_from_record(&ctx, "User", record_invalid_type).unwrap_err();
        assert!(err.to_string().contains("expects object/list/null"));
    }

    #[derive(Debug, Clone)]
    struct DummyEntity {
        id: u64,
        dirty_fields: BTreeSet<String>,
        original_values: Option<Record>,
        is_deleted: bool,
        comment: Option<String>,
    }

    impl teaql_core::TeaqlEntity for DummyEntity {
        const ENTITY_NAME: &'static str = "Dummy";
        fn entity_descriptor() -> EntityDescriptor {
            let mut desc = EntityDescriptor::new("Dummy");
            desc.properties
                .push(PropertyDescriptor::new("id", DataType::U64));
            desc
        }
    }

    impl Entity for DummyEntity {
        fn from_record(record: Record) -> Result<Self, teaql_core::EntityError> {
            Ok(Self {
                id: record
                    .get("id")
                    .and_then(|v| match v {
                        Value::U64(n) => Some(*n),
                        _ => None,
                    })
                    .unwrap_or(0),
                dirty_fields: BTreeSet::new(),
                original_values: None,
                is_deleted: false,
                comment: None,
            })
        }
        fn into_record(self) -> Record {
            Record::from([("id".to_string(), Value::U64(self.id))])
        }
        fn dirty_fields(&self) -> Option<BTreeSet<String>> {
            if self.dirty_fields.is_empty() {
                None
            } else {
                Some(self.dirty_fields.clone())
            }
        }
        fn original_values(&self) -> Option<Record> {
            self.original_values.clone()
        }
        fn is_marked_as_delete(&self) -> bool {
            self.is_deleted
        }
        fn get_comment(&self) -> Option<String> {
            self.comment.clone()
        }
        fn set_comment(&mut self, comment: String) {
            self.comment = Some(comment);
        }
    }

    #[test]
    fn test_graph_node_from_entity() {
        let mut registry = InMemoryMetadataStore::new();
        registry =
            registry.with_entity(<DummyEntity as teaql_core::TeaqlEntity>::entity_descriptor());
        let ctx = UserContext::new().with_metadata(registry);

        let entity = DummyEntity {
            id: 42,
            dirty_fields: vec!["foo".to_string()].into_iter().collect(),
            original_values: Some(Record::from([(
                "foo".to_string(),
                Value::Text("bar".into()),
            )])),
            is_deleted: true,
            comment: Some("delete me".to_string()),
        };

        let node = graph_node_from_entity(&ctx, entity).unwrap();
        assert_eq!(node.entity, "Dummy");
        assert_eq!(node.values.get("id"), Some(&Value::U64(42)));
        assert_eq!(node.operation, GraphOperation::Remove);
        assert_eq!(node.comment, Some("delete me".into()));
        assert_eq!(
            node.dirty_fields.unwrap().iter().next().unwrap().as_str(),
            "foo"
        );
        assert_eq!(
            node.original_values.unwrap().get("foo"),
            Some(&Value::Text("bar".into()))
        );
    }

    struct MockGraphSaver {
        saved_nodes: std::sync::Mutex<Vec<GraphNode>>,
        saved_ledgers: std::sync::Mutex<Vec<(GraphNode, crate::EntityRoot)>>,
    }

    impl DynGraphSaver for MockGraphSaver {
        fn save_graph_dyn<'a>(
            &'a self,
            _ctx: &'a UserContext,
            node: GraphNode,
        ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
            self.saved_nodes.lock().unwrap().push(node.clone());
            Box::pin(std::future::ready(Ok(node)))
        }

        fn save_ledger_dyn<'a>(
            &'a self,
            _ctx: &'a UserContext,
            node: GraphNode,
            root: crate::EntityRoot,
        ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
            self.saved_ledgers
                .lock()
                .unwrap()
                .push((node.clone(), root));
            Box::pin(std::future::ready(Ok(node)))
        }
    }

    #[tokio::test]
    async fn test_audited_save_ext() {
        use teaql_core::Audited;
        let mut registry = InMemoryMetadataStore::new();
        registry =
            registry.with_entity(<DummyEntity as teaql_core::TeaqlEntity>::entity_descriptor());
        let saver: Arc<dyn DynGraphSaver> = Arc::new(MockGraphSaver {
            saved_nodes: std::sync::Mutex::new(Vec::new()),
            saved_ledgers: std::sync::Mutex::new(Vec::new()),
        });
        let mut ctx = UserContext::new().with_metadata(registry);
        ctx.insert_resource(saver.clone());

        let entity = DummyEntity {
            id: 10,
            dirty_fields: BTreeSet::new(),
            original_values: None,
            is_deleted: false,
            comment: None,
        };
        let audited = Audited::new(entity, "creation");
        let node = audited.save(&ctx).await.unwrap();
        assert_eq!(node.entity, "Dummy");
        assert_eq!(node.comment, Some("creation".into()));
    }

    impl crate::LedgerEntity for DummyEntity {
        fn entity_root(&self) -> Option<crate::EntityRoot> {
            let root = crate::EntityRoot::default();
            // To simulate ledger changes
            if self.id == 999 {
                root.set(crate::EntityKey::new("Dummy", self.id), "foo", "bar");
            }
            Some(root)
        }
    }

    #[tokio::test]
    async fn test_save_audited_ledger_entity() {
        use teaql_core::Audited;
        let mut registry = InMemoryMetadataStore::new();
        registry =
            registry.with_entity(<DummyEntity as teaql_core::TeaqlEntity>::entity_descriptor());
        let saver: Arc<dyn DynGraphSaver> = Arc::new(MockGraphSaver {
            saved_nodes: std::sync::Mutex::new(Vec::new()),
            saved_ledgers: std::sync::Mutex::new(Vec::new()),
        });
        let mut ctx = UserContext::new().with_metadata(registry);
        ctx.insert_resource(saver.clone());

        // Test with ledger changes (id = 999 triggers it based on our mock)
        let entity1 = DummyEntity {
            id: 999,
            dirty_fields: BTreeSet::new(),
            original_values: None,
            is_deleted: false,
            comment: None,
        };
        let audited1 = Audited::new(entity1, "ledger update");
        let node1 = save_audited_ledger_entity(audited1, &ctx).await.unwrap();
        assert_eq!(node1.entity, "Dummy");
        assert_eq!(node1.comment, Some("ledger update".into()));

        // Test without ledger changes
        let entity2 = DummyEntity {
            id: 111,
            dirty_fields: BTreeSet::new(),
            original_values: None,
            is_deleted: false,
            comment: None,
        };
        let audited2 = Audited::new(entity2, "graph update");
        let node2 = save_audited_ledger_entity(audited2, &ctx).await.unwrap();
        assert_eq!(node2.entity, "Dummy");
        assert_eq!(node2.comment, Some("graph update".into()));
    }

    #[tokio::test]
    async fn test_graph_saver_for() {
        use crate::tests::StubExecutor;
        let saver = GraphSaverFor::<StubExecutor>::new();

        let mut registry = InMemoryMetadataStore::new();
        registry =
            registry.with_entity(<DummyEntity as teaql_core::TeaqlEntity>::entity_descriptor());
        let ctx = UserContext::new().with_metadata(registry);

        // We can't fully run save_graph_dyn without setting up the executor in ctx,
        // but we can at least test that new() works and it is a DynGraphSaver.
        let dyn_saver: Arc<dyn DynGraphSaver> = Arc::new(saver);

        // Since UserContext doesn't have the Executor setup correctly in this context (requires DataService),
        // we'll just check if the failure path (executor missing or similar) returns expected RuntimeError.
        let node = GraphNode::new("Dummy");
        let err = dyn_saver.save_graph_dyn(&ctx, node).await.unwrap_err();
        assert!(matches!(err, RuntimeError::Graph(_)));
    }
}
