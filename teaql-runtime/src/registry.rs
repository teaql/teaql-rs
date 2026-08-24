use std::collections::BTreeMap;
use std::sync::Arc;

use teaql_core::{
    CompactRow, DeleteCommand, Entity, EntityDescriptor, EntityDescriptorStore, EntityError,
    IdentifiableEntity, InsertCommand, RecoverCommand, SelectQuery, TeaqlEntity, UpdateCommand,
};

use crate::{
    Checker, EntityGraphBuilder, EntityRoot, GraphNode, InMemoryCheckerRegistry,
    InMemoryRawAuditEventSink, Language, RawAuditEventSink, RuntimeError, UserContext,
};

type CompactEntityGraphDecoder =
    fn(CompactRow, &EntityRoot, &mut EntityGraphBuilder) -> Result<(), EntityError>;
type CompactEntityGraphBatchDecoder =
    fn(Vec<CompactRow>, &EntityRoot, &mut EntityGraphBuilder) -> Result<(), EntityError>;
type CompactEntityGraphListDecoder = fn(
    Vec<CompactRow>,
    &EntityRoot,
    &mut EntityGraphBuilder,
    &str,
    u64,
    &str,
) -> Result<(), EntityError>;

#[derive(Default, Clone)]
pub struct InMemoryEntityGraphDecoderRegistry {
    compact_decoders: BTreeMap<String, CompactEntityGraphDecoder>,
    compact_batch_decoders: BTreeMap<String, CompactEntityGraphBatchDecoder>,
    compact_list_decoders: BTreeMap<String, CompactEntityGraphListDecoder>,
    compact_option_decoders: BTreeMap<String, CompactEntityGraphListDecoder>,
}

impl InMemoryEntityGraphDecoderRegistry {
    pub fn contains(&self, entity: &str) -> bool {
        self.compact_decoders.contains_key(entity)
    }

    pub fn register<T>(&mut self)
    where
        T: Entity + IdentifiableEntity + Send + Sync + 'static,
    {
        fn decode_compact<T>(
            row: CompactRow,
            root: &EntityRoot,
            graph: &mut EntityGraphBuilder,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRoot::fresh_with_weak_graph(root);
            let entity = T::from_compact_row_with_context(row, &graph_root as &dyn std::any::Any)?;
            let id = entity.id_value().try_u64().ok_or_else(|| {
                EntityError::new(T::ENTITY_NAME, "identity graph requires a u64 entity id")
            })?;
            graph.install(id, entity);
            Ok(())
        }

        fn decode_compact_list<T>(
            rows: Vec<CompactRow>,
            root: &EntityRoot,
            graph: &mut EntityGraphBuilder,
            owner_entity: &str,
            owner_id: u64,
            relation: &str,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRoot::fresh_with_weak_graph(root);
            let entities = rows
                .into_iter()
                .map(|row| T::from_compact_row_with_context(row, &graph_root as &dyn std::any::Any))
                .collect::<Result<Vec<T>, EntityError>>()?;
            graph.install_relation_list(
                owner_entity,
                owner_id,
                relation,
                teaql_core::SmartList::new(entities),
            );
            Ok(())
        }

        fn decode_compact_batch<T>(
            rows: Vec<CompactRow>,
            root: &EntityRoot,
            graph: &mut EntityGraphBuilder,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRoot::fresh_with_weak_graph(root);
            for row in rows {
                let entity =
                    T::from_compact_row_with_context(row, &graph_root as &dyn std::any::Any)?;
                let id = entity.id_value().try_u64().ok_or_else(|| {
                    EntityError::new(T::ENTITY_NAME, "identity graph requires a u64 entity id")
                })?;
                graph.install(id, entity);
            }
            Ok(())
        }

        fn decode_compact_option<T>(
            rows: Vec<CompactRow>,
            root: &EntityRoot,
            graph: &mut EntityGraphBuilder,
            owner_entity: &str,
            owner_id: u64,
            relation: &str,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRoot::fresh_with_weak_graph(root);
            let value = rows
                .into_iter()
                .next()
                .map(|row| T::from_compact_row_with_context(row, &graph_root as &dyn std::any::Any))
                .transpose()?;
            graph.install_relation_option(owner_entity, owner_id, relation, value);
            Ok(())
        }

        self.compact_decoders
            .insert(T::ENTITY_NAME.to_owned(), decode_compact::<T>);
        self.compact_batch_decoders
            .insert(T::ENTITY_NAME.to_owned(), decode_compact_batch::<T>);
        self.compact_list_decoders
            .insert(T::ENTITY_NAME.to_owned(), decode_compact_list::<T>);
        self.compact_option_decoders
            .insert(T::ENTITY_NAME.to_owned(), decode_compact_option::<T>);
    }

    pub fn decode_compact(
        &self,
        entity: &str,
        row: CompactRow,
        root: &EntityRoot,
        graph: &mut EntityGraphBuilder,
    ) -> Result<(), EntityError> {
        self.compact_decoders.get(entity).ok_or_else(|| {
            EntityError::new(
                entity,
                "entity has no compact identity graph decoder in RuntimeModule",
            )
        })?(row, root, graph)
    }

    pub fn decode_compact_list(
        &self,
        entity: &str,
        rows: Vec<CompactRow>,
        root: &EntityRoot,
        graph: &mut EntityGraphBuilder,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> Result<(), EntityError> {
        self.compact_list_decoders.get(entity).ok_or_else(|| {
            EntityError::new(
                entity,
                "entity has no compact identity graph list decoder in RuntimeModule",
            )
        })?(rows, root, graph, owner_entity, owner_id, relation)
    }

    pub fn decode_compact_batch(
        &self,
        entity: &str,
        rows: Vec<CompactRow>,
        root: &EntityRoot,
        graph: &mut EntityGraphBuilder,
    ) -> Result<(), EntityError> {
        self.compact_batch_decoders.get(entity).ok_or_else(|| {
            EntityError::new(
                entity,
                "entity has no compact identity graph batch decoder in RuntimeModule",
            )
        })?(rows, root, graph)
    }

    pub fn decode_compact_option(
        &self,
        entity: &str,
        rows: Vec<CompactRow>,
        root: &EntityRoot,
        graph: &mut EntityGraphBuilder,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> Result<(), EntityError> {
        self.compact_option_decoders.get(entity).ok_or_else(|| {
            EntityError::new(
                entity,
                "entity has no compact identity graph option decoder in RuntimeModule",
            )
        })?(rows, root, graph, owner_entity, owner_id, relation)
    }
}

pub trait MetadataStore: Send + Sync {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor>;
    fn all_entities(&self) -> Vec<&EntityDescriptor>;
    fn record_metadata_log(&self, _metadata: &teaql_data_service::ExecutionMetadata) {}
    fn capture_query_debug(&self) -> bool {
        true
    }
    fn capture_execution_metadata(&self) -> bool {
        true
    }
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
    root_graphs: Vec<GraphNode>,
    graph_decoders: InMemoryEntityGraphDecoderRegistry,
}

impl RuntimeModule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entity<T>(mut self) -> Self
    where
        T: Entity + IdentifiableEntity + Send + Sync + 'static,
    {
        let descriptor = T::entity_descriptor();
        self.entity_registry.register(descriptor.name.clone());
        self.metadata.register(descriptor);
        self.graph_decoders.register::<T>();
        self
    }

    pub fn entity_with_behavior<T, B>(mut self, behavior: B) -> Self
    where
        T: Entity + IdentifiableEntity + Send + Sync + 'static,
        B: EntityDataServiceBehavior + 'static,
    {
        let descriptor = T::entity_descriptor();
        let entity_name = descriptor.name.clone();
        self.entity_registry.register(entity_name.clone());
        self.metadata.register(descriptor);
        self.behaviors.register(entity_name, behavior);
        self.graph_decoders.register::<T>();
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

    /// Register create-if-absent root data. Unlike constant initial graphs,
    /// existing root rows are never reconciled from module defaults.
    pub fn root_graph(mut self, graph: GraphNode) -> Self {
        self.root_graphs.push(graph);
        self
    }

    pub fn root_graphs(mut self, graphs: impl IntoIterator<Item = GraphNode>) -> Self {
        self.root_graphs.extend(graphs);
        self
    }

    pub fn apply_to(self, context: &mut UserContext) {
        context.set_metadata(self.metadata);
        context.set_entity_registry(self.entity_registry);
        context.set_entity_data_service_behavior_registry(self.behaviors);
        context.set_checker_registry(self.checkers);
        context.set_event_sink(self.event_sinks);
        context.set_initial_graphs(self.initial_graphs);
        context.set_root_graphs(self.root_graphs);
        context.set_entity_graph_decoder_registry(self.graph_decoders);
        if let Some(language) = self.language {
            context.set_language(language);
        }
    }

    pub fn into_context(self) -> UserContext {
        let mut context = UserContext::new();
        self.apply_to(&mut context);
        context
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
