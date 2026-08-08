use std::collections::BTreeMap;
use std::sync::Arc;

use teaql_core::{
    DeleteCommand, EntityDescriptor, EntityDescriptorStore, InsertCommand, RecoverCommand,
    SelectQuery, TeaqlEntity, UpdateCommand,
};

use crate::{
    Checker, GraphNode, InMemoryCheckerRegistry, InMemoryRawAuditEventSink, Language,
    RawAuditEventSink, RuntimeError, UserContext,
};

pub trait MetadataStore: Send + Sync {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor>;
    fn all_entities(&self) -> Vec<&EntityDescriptor>;
    fn record_metadata_log(&self, _metadata: &teaql_data_service::ExecutionMetadata) {}
}

pub trait EntityRegistry: Send + Sync {
    fn contains(&self, entity: &str) -> bool;
}

pub trait RequestPolicy: Send + Sync {
    fn enforce_select(
        &self,
        _ctx: &UserContext,
        _query: &mut SelectQuery,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn enforce_insert(
        &self,
        _ctx: &UserContext,
        _command: &mut InsertCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn enforce_update(
        &self,
        _ctx: &UserContext,
        _command: &mut UpdateCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn enforce_delete(
        &self,
        _ctx: &UserContext,
        _command: &mut DeleteCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn enforce_recover(
        &self,
        _ctx: &UserContext,
        _command: &mut RecoverCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

pub trait EntityDataServiceBehavior: Send + Sync {
    fn before_select(
        &self,
        _ctx: &UserContext,
        _query: &mut SelectQuery,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_insert(
        &self,
        _ctx: &UserContext,
        _command: &mut InsertCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_update(
        &self,
        _ctx: &UserContext,
        _command: &mut UpdateCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_delete(
        &self,
        _ctx: &UserContext,
        _command: &mut DeleteCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_recover(
        &self,
        _ctx: &UserContext,
        _command: &mut RecoverCommand,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
        Vec::new()
    }
}

pub trait EntityDataServiceBehaviorRegistry: Send + Sync {
    fn behavior(&self, entity: &str) -> Option<Arc<dyn EntityDataServiceBehavior>>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryMetadataStore {
    entities: BTreeMap<String, EntityDescriptor>,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, entity: EntityDescriptor) {
        self.entities.insert(entity.name.clone(), entity);
    }

    pub fn with_entity(mut self, entity: EntityDescriptor) -> Self {
        self.register(entity);
        self
    }
}

impl MetadataStore for InMemoryMetadataStore {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.entities.get(name)
    }

    fn all_entities(&self) -> Vec<&EntityDescriptor> {
        self.entities.values().collect()
    }
}

impl teaql_data_service::SchemaProvider for InMemoryMetadataStore {
    fn get_entity(&self, name: &str) -> Option<std::sync::Arc<teaql_core::EntityDescriptor>> {
        self.entities
            .get(name)
            .map(|e| std::sync::Arc::new(e.clone()))
    }
}

impl EntityDescriptorStore for InMemoryMetadataStore {
    fn register_descriptor(&mut self, descriptor: EntityDescriptor) {
        self.register(descriptor);
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryEntityRegistry {
    entities: BTreeMap<String, String>,
}

impl InMemoryEntityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, entity: impl Into<String>) {
        let entity = entity.into();
        self.entities.insert(entity.clone(), entity);
    }

    pub fn with_entity(mut self, entity: impl Into<String>) -> Self {
        self.register(entity);
        self
    }
}

impl EntityRegistry for InMemoryEntityRegistry {
    fn contains(&self, entity: &str) -> bool {
        self.entities.contains_key(entity)
    }
}

#[derive(Default, Clone)]
pub struct InMemoryEntityDataServiceBehaviorRegistry {
    behaviors: BTreeMap<String, Arc<dyn EntityDataServiceBehavior>>,
}

impl InMemoryEntityDataServiceBehaviorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        entity: impl Into<String>,
        behavior: impl EntityDataServiceBehavior + 'static,
    ) {
        self.behaviors.insert(entity.into(), Arc::new(behavior));
    }

    pub fn with_behavior(
        mut self,
        entity: impl Into<String>,
        behavior: impl EntityDataServiceBehavior + 'static,
    ) -> Self {
        self.register(entity, behavior);
        self
    }
}

impl EntityDataServiceBehaviorRegistry for InMemoryEntityDataServiceBehaviorRegistry {
    fn behavior(&self, entity: &str) -> Option<Arc<dyn EntityDataServiceBehavior>> {
        self.behaviors.get(entity).cloned()
    }
}

#[derive(Default, Clone)]
pub struct RuntimeModule {
    pub metadata: InMemoryMetadataStore,
    entity_registry: InMemoryEntityRegistry,
    behaviors: InMemoryEntityDataServiceBehaviorRegistry,
    checkers: InMemoryCheckerRegistry,
    event_sinks: InMemoryRawAuditEventSink,
    language: Option<Language>,
    initial_graphs: Vec<GraphNode>,
}

impl RuntimeModule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entity<T: TeaqlEntity>(mut self) -> Self {
        let descriptor = T::entity_descriptor();
        self.entity_registry.register(descriptor.name.clone());
        self.metadata.register(descriptor);
        self
    }

    pub fn entity_with_behavior<T, B>(mut self, behavior: B) -> Self
    where
        T: TeaqlEntity,
        B: EntityDataServiceBehavior + 'static,
    {
        let descriptor = T::entity_descriptor();
        let entity_name = descriptor.name.clone();
        self.entity_registry.register(entity_name.clone());
        self.metadata.register(descriptor);
        self.behaviors.register(entity_name, behavior);
        self
    }

    pub fn descriptor(mut self, descriptor: EntityDescriptor) -> Self {
        self.entity_registry.register(descriptor.name.clone());
        self.metadata.register(descriptor);
        self
    }

    pub fn behavior(
        mut self,
        entity: impl Into<String>,
        behavior: impl EntityDataServiceBehavior + 'static,
    ) -> Self {
        self.behaviors.register(entity, behavior);
        self
    }

    pub fn checker(mut self, checker: impl Checker + 'static) -> Self {
        self.checkers.register(checker);
        self
    }

    pub fn event_sink(mut self, sink: impl RawAuditEventSink + 'static) -> Self {
        self.event_sinks.register(sink);
        self
    }

    pub fn language(mut self, language: Language) -> Self {
        self.language = Some(language);
        self
    }

    pub fn initial_graph(mut self, graph: GraphNode) -> Self {
        self.initial_graphs.push(graph);
        self
    }

    pub fn initial_graphs(mut self, graphs: impl IntoIterator<Item = GraphNode>) -> Self {
        self.initial_graphs.extend(graphs);
        self
    }

    pub fn apply_to(self, ctx: &mut UserContext) {
        ctx.set_metadata(self.metadata);
        ctx.set_entity_registry(self.entity_registry);
        ctx.set_entity_data_service_behavior_registry(self.behaviors);
        ctx.set_checker_registry(self.checkers);
        ctx.set_event_sink(self.event_sinks);
        ctx.set_initial_graphs(self.initial_graphs);
        if let Some(language) = self.language {
            ctx.set_language(language);
        }
    }

    pub fn into_context(self) -> UserContext {
        let mut ctx = UserContext::new();
        self.apply_to(&mut ctx);
        ctx
    }
}

#[macro_export]
macro_rules! module {
    ($($entity:ty $(=> $behavior:expr)?),+ $(,)?) => {{
        let module = $crate::RuntimeModule::new();
        $crate::module!(@build module; $($entity $(=> $behavior)?),+)
    }};

    (@build $module:expr; $entity:ty => $behavior:expr, $($rest:tt)*) => {{
        let module = $module.entity_with_behavior::<$entity, _>($behavior);
        $crate::module!(@build module; $($rest)*)
    }};

    (@build $module:expr; $entity:ty, $($rest:tt)*) => {{
        let module = $module.entity::<$entity>();
        $crate::module!(@build module; $($rest)*)
    }};

    (@build $module:expr; $entity:ty => $behavior:expr) => {
        $module.entity_with_behavior::<$entity, _>($behavior)
    };

    (@build $module:expr; $entity:ty) => {
        $module.entity::<$entity>()
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GraphNode, GraphOperation, Language};
    use std::collections::BTreeMap;
    use teaql_core::{
        DeleteCommand, EntityDescriptor, InsertCommand, RecoverCommand, SelectQuery, UpdateCommand,
    };
    use teaql_data_service::SchemaProvider;

    struct DummyPolicy;
    impl RequestPolicy for DummyPolicy {}

    struct DummyBehavior;
    impl EntityDataServiceBehavior for DummyBehavior {}

    struct DummyEntity;
    impl teaql_core::TeaqlEntity for DummyEntity {
        const ENTITY_NAME: &'static str = "DummyEntity";
        fn entity_descriptor() -> EntityDescriptor {
            EntityDescriptor {
                name: Self::ENTITY_NAME.to_string(),
                table_name: "dummy_entity".to_string(),
                data_service: None,
                properties: vec![],
                relations: vec![],
                audit_mask_fields: vec![],
                audit_value_max_len: None,
            }
        }
        fn register_into(_store: &mut impl teaql_core::EntityDescriptorStore) {}
    }

    #[test]
    fn test_request_policy_defaults() {
        let policy = DummyPolicy;
        let ctx = UserContext::new();

        let mut sq = SelectQuery::new("Test");
        assert!(policy.enforce_select(&ctx, &mut sq).is_ok());

        let mut ic = InsertCommand::new("Test");
        assert!(policy.enforce_insert(&ctx, &mut ic).is_ok());

        let mut uc = UpdateCommand::new("Test", 1);
        assert!(policy.enforce_update(&ctx, &mut uc).is_ok());

        let mut dc = DeleteCommand::new("Test", 1);
        assert!(policy.enforce_delete(&ctx, &mut dc).is_ok());

        let mut rc = RecoverCommand::new("Test", 1, 1);
        assert!(policy.enforce_recover(&ctx, &mut rc).is_ok());
    }

    #[test]
    fn test_behavior_defaults() {
        let behavior = DummyBehavior;
        let ctx = UserContext::new();

        let mut sq = SelectQuery::new("Test");
        assert!(behavior.before_select(&ctx, &mut sq).is_ok());

        let mut ic = InsertCommand::new("Test");
        assert!(behavior.before_insert(&ctx, &mut ic).is_ok());

        let mut uc = UpdateCommand::new("Test", 1);
        assert!(behavior.before_update(&ctx, &mut uc).is_ok());

        let mut dc = DeleteCommand::new("Test", 1);
        assert!(behavior.before_delete(&ctx, &mut dc).is_ok());

        let mut rc = RecoverCommand::new("Test", 1, 1);
        assert!(behavior.before_recover(&ctx, &mut rc).is_ok());

        assert_eq!(behavior.relation_loads(&ctx).len(), 0);
    }

    #[test]
    fn test_metadata_registry_register_and_get() {
        let mut registry = InMemoryMetadataStore::new();
        let desc = EntityDescriptor {
            name: "TestEntity".to_owned(),
            table_name: "test_entity".to_owned(),
            data_service: None,
            properties: vec![],
            relations: vec![],
            audit_mask_fields: vec![],
            audit_value_max_len: None,
        };

        registry.register_descriptor(desc.clone());

        // Assert we can get it via get_entity
        let fetched = registry.get_entity("TestEntity").unwrap();
        assert_eq!(fetched.name, "TestEntity");

        // Assert it exists in all_entities
        let all = registry.all_entities();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "TestEntity");

        // Assert MetadataStore trait method works
        let trait_fetched = registry.entity("TestEntity").unwrap();
        assert_eq!(trait_fetched.name, "TestEntity");

        // record_metadata_log default
        registry.record_metadata_log(&teaql_data_service::ExecutionMetadata {
            backend: "".into(),
            operation: teaql_data_service::DataServiceOperation::Batch,
            started_at: std::time::SystemTime::now(),
            ended_at: std::time::SystemTime::now(),
            affected_rows: None,
            result_count: None,
            trace_chain: vec![],
            comment: None,
            backend_request_id: None,
            debug_query: None,
        });

        // with_entity
        let registry2 = InMemoryMetadataStore::new().with_entity(desc);
        assert_eq!(registry2.all_entities().len(), 1);
    }

    #[test]
    fn test_entity_registry_contains() {
        let mut registry = InMemoryEntityRegistry::new();
        assert!(!registry.contains("UnknownEntity"));

        registry.register("KnownEntity");
        assert!(registry.contains("KnownEntity"));

        let registry2 = InMemoryEntityRegistry::new().with_entity("KnownEntity2");
        assert!(registry2.contains("KnownEntity2"));
    }

    #[test]
    fn test_entity_behavior_registry() {
        let mut registry = InMemoryEntityDataServiceBehaviorRegistry::new();
        assert!(registry.behavior("Test").is_none());

        registry.register("Test", DummyBehavior);
        assert!(registry.behavior("Test").is_some());

        let registry2 =
            InMemoryEntityDataServiceBehaviorRegistry::new().with_behavior("Test2", DummyBehavior);
        assert!(registry2.behavior("Test2").is_some());
    }

    #[test]
    fn test_runtime_module() {
        let desc = EntityDescriptor {
            name: "TestEntity".to_owned(),
            table_name: "test_entity".to_owned(),
            data_service: None,
            properties: vec![],
            relations: vec![],
            audit_mask_fields: vec![],
            audit_value_max_len: None,
        };

        let mut node = GraphNode::new("TestEntity");
        node.operation = GraphOperation::Upsert;

        let module = RuntimeModule::new()
            .entity::<DummyEntity>()
            .entity_with_behavior::<DummyEntity, _>(DummyBehavior)
            .descriptor(desc)
            .behavior("TestEntity", DummyBehavior)
            .language(Language::English)
            .initial_graph(node.clone())
            .initial_graphs(vec![node]);

        let mut ctx = UserContext::new();
        module.apply_to(&mut ctx);

        // Also test into_context
        let _ctx2 = RuntimeModule::new()
            .language(Language::English)
            .into_context();
    }
}
