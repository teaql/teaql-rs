use std::collections::BTreeMap;
use std::sync::Arc;

use teaql_core::{
    CompactRow, DeleteCommand, Entity, EntityDescriptor, EntityDescriptorStore, EntityError,
    IdentifiableEntity, InsertCommand, RecoverCommand, SelectQuery, SmartList, UpdateCommand,
};

use crate::{
    Checker, EntityGraphBuilder, EntityRuntimeState, GraphNode, InMemoryCheckerRegistry,
    InMemoryRawAuditEventSink, Language, RawAuditEventSink, RuntimeError, UserContext,
};

pub(crate) fn decode_compact_rows_with_read_metadata<T: Entity>(
    mut rows: Vec<CompactRow>,
    root: &EntityRuntimeState,
) -> Result<Vec<T>, EntityError> {
    CompactRow::share_layouts(&mut rows);
    let mut entities = Vec::with_capacity(rows.len());
    let mut states = ReadHydrationShapes::default();
    for row in rows {
        entities.push(decode_compact_row_with_read_metadata::<T>(
            row,
            root,
            &mut states,
        )?);
    }
    Ok(entities)
}

#[derive(Default)]
pub(crate) struct ReadHydrationShapes {
    // Keep source pointers alive; an allocator-reused address must never merge
    // different projection/selection geometry. The cache contains no row values.
    #[allow(clippy::type_complexity)]
    states: std::collections::HashMap<
        (usize, usize),
        (
            Arc<teaql_core::LoadedSnapshot>,
            Arc<std::collections::HashSet<String>>,
            Arc<teaql_core::LoadedSnapshot>,
        ),
    >,
}

fn decode_compact_row_with_read_metadata<T: Entity>(
    mut row: CompactRow,
    root: &EntityRuntimeState,
    shapes: &mut ReadHydrationShapes,
) -> Result<T, EntityError> {
    let fields = row.take_loaded_dynamic_fields();
    if fields.is_some() && !T::supports_dynamic_field_load() {
        return Err(EntityError::new(
            T::ENTITY_NAME,
            "dynamic fields require a runtime-owned indexed entity carrier",
        ));
    }
    let mut entity = T::from_compact_row_with_context(row, root as &dyn std::any::Any)?;
    if let Some(fields) = fields {
        let base = entity.loaded_state_snapshot().ok_or_else(|| {
            EntityError::new(
                T::ENTITY_NAME,
                "indexed load state missing during dynamic hydration",
            )
        })?;
        let key = (
            Arc::as_ptr(&base) as usize,
            Arc::as_ptr(fields.selected_codes()) as usize,
        );
        let state = if let Some((_, _, state)) = shapes.states.get(&key) {
            Arc::clone(state)
        } else {
            let state = teaql_core::LoadedSnapshot::with_dynamic_fields(&base, &fields)
                .map_err(|message| EntityError::new(T::ENTITY_NAME, message))?;
            shapes
                .states
                .insert(key, (base, fields.selected_codes().clone(), state.clone()));
            state
        };
        entity.install_loaded_dynamic_fields(fields, state)?;
    }
    Ok(entity)
}

type CompactEntityGraphDecoder =
    fn(CompactRow, &EntityRuntimeState, &mut EntityGraphBuilder) -> Result<(), EntityError>;
type CompactEntityGraphBatchDecoder =
    fn(Vec<CompactRow>, &EntityRuntimeState, &mut EntityGraphBuilder) -> Result<(), EntityError>;
type CompactEntityGraphListDecoder = fn(
    SmartList<CompactRow>,
    &EntityRuntimeState,
    &mut EntityGraphBuilder,
    &str,
    u64,
    &str,
) -> Result<(), EntityError>;
type CompactEntityGraphOptionDecoder = fn(
    Vec<CompactRow>,
    &EntityRuntimeState,
    &mut EntityGraphBuilder,
    &str,
    u64,
    &str,
) -> Result<(), EntityError>;

type JsonReadCapability = fn(&EntityDescriptor) -> Result<bool, EntityError>;

#[derive(Default, Clone)]
pub struct InMemoryEntityGraphDecoderRegistry {
    json_capabilities: BTreeMap<String, JsonReadCapability>,
    compact_decoders: BTreeMap<String, CompactEntityGraphDecoder>,
    compact_batch_decoders: BTreeMap<String, CompactEntityGraphBatchDecoder>,
    compact_list_decoders: BTreeMap<String, CompactEntityGraphListDecoder>,
    compact_option_decoders: BTreeMap<String, CompactEntityGraphOptionDecoder>,
}

impl InMemoryEntityGraphDecoderRegistry {
    pub fn contains(&self, entity: &str) -> bool {
        self.compact_decoders.contains_key(entity)
    }

    pub fn register<T>(&mut self)
    where
        T: Entity + IdentifiableEntity + Send + Sync + 'static,
    {
        fn json_capability<T: Entity>(installed: &EntityDescriptor) -> Result<bool, EntityError> {
            let expected = T::entity_descriptor();
            if installed.properties.len() != expected.properties.len()
                || expected
                    .properties
                    .iter()
                    .any(|p| !installed.properties.contains(p))
                || installed.relations.len() != expected.relations.len()
                || expected
                    .relations
                    .iter()
                    .any(|r| !installed.relations.contains(r))
                || T::field_layout()?.is_none()
            {
                return Err(EntityError::new(
                    T::ENTITY_NAME,
                    "JSON_ENTITY_INPUT: installed graph metadata does not match its typed decoder",
                ));
            }
            Ok(T::supports_dynamic_property_load())
        }
        self.json_capabilities
            .insert(T::ENTITY_NAME.to_owned(), json_capability::<T>);
        fn decode_compact<T>(
            row: CompactRow,
            root: &EntityRuntimeState,
            graph: &mut EntityGraphBuilder,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRuntimeState::fresh_with_weak_graph(root);
            let entity = decode_compact_row_with_read_metadata::<T>(
                row,
                &graph_root,
                &mut ReadHydrationShapes::default(),
            )?;
            let id = entity.id_value().try_u64().ok_or_else(|| {
                EntityError::new(T::ENTITY_NAME, "identity graph requires a u64 entity id")
            })?;
            graph.install(id, entity);
            Ok(())
        }

        fn decode_compact_list<T>(
            rows: SmartList<CompactRow>,
            root: &EntityRuntimeState,
            graph: &mut EntityGraphBuilder,
            owner_entity: &str,
            owner_id: u64,
            relation: &str,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRuntimeState::fresh_with_weak_graph(root);
            // `collect::<Result<Vec<_>, _>>()` cannot retain the exact size hint
            // through the fallible adapter. For large generated entities that
            // grows 4 -> 8 -> 16 even when the relation cardinality is already
            // known. Reserve the exact row count and decode directly into the
            // final SmartList allocation.
            let entities = decode_compact_rows_with_read_metadata::<T>(rows.data, &graph_root)?;
            graph.install_relation_list(
                owner_entity,
                owner_id,
                relation,
                SmartList {
                    data: entities,
                    total_count: rows.total_count,
                    aggregations: rows.aggregations,
                    summary: rows.summary,
                    facets: rows.facets,
                    is_loaded: rows.is_loaded,
                },
            );
            Ok(())
        }

        fn decode_compact_batch<T>(
            mut rows: Vec<CompactRow>,
            root: &EntityRuntimeState,
            graph: &mut EntityGraphBuilder,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRuntimeState::fresh_with_weak_graph(root);
            CompactRow::share_layouts(&mut rows);
            let mut shapes = ReadHydrationShapes::default();
            for row in rows {
                let entity =
                    decode_compact_row_with_read_metadata::<T>(row, &graph_root, &mut shapes)?;
                let id = entity.id_value().try_u64().ok_or_else(|| {
                    EntityError::new(T::ENTITY_NAME, "identity graph requires a u64 entity id")
                })?;
                graph.install(id, entity);
            }
            Ok(())
        }

        fn decode_compact_option<T>(
            rows: Vec<CompactRow>,
            root: &EntityRuntimeState,
            graph: &mut EntityGraphBuilder,
            owner_entity: &str,
            owner_id: u64,
            relation: &str,
        ) -> Result<(), EntityError>
        where
            T: Entity + IdentifiableEntity + Send + Sync + 'static,
        {
            let graph_root = EntityRuntimeState::fresh_with_weak_graph(root);
            let value = rows
                .into_iter()
                .next()
                .map(|row| {
                    decode_compact_row_with_read_metadata::<T>(
                        row,
                        &graph_root,
                        &mut ReadHydrationShapes::default(),
                    )
                })
                .transpose()?;
            graph.install_typed_relation_option(owner_entity, owner_id, relation, value);
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

    pub(crate) fn json_capability(
        &self,
        descriptor: &EntityDescriptor,
    ) -> Result<bool, EntityError> {
        self.json_capabilities
            .get(&descriptor.name)
            .ok_or_else(|| {
                EntityError::new(
                    &descriptor.name,
                    "JSON_ENTITY_INPUT: graph type has no installed typed decoder",
                )
            })?(descriptor)
    }

    pub fn decode_compact(
        &self,
        entity: &str,
        row: CompactRow,
        root: &EntityRuntimeState,
        graph: &mut EntityGraphBuilder,
    ) -> Result<(), EntityError> {
        self.compact_decoders.get(entity).ok_or_else(|| {
            EntityError::new(
                entity,
                "entity has no compact identity graph decoder in RuntimeModule",
            )
        })?(row, root, graph)
    }

    #[allow(clippy::too_many_arguments)] // Stable generated decoder boundary.
    pub fn decode_compact_list(
        &self,
        entity: &str,
        rows: Vec<CompactRow>,
        root: &EntityRuntimeState,
        graph: &mut EntityGraphBuilder,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> Result<(), EntityError> {
        self.decode_compact_smart_list(
            entity,
            SmartList::new(rows),
            root,
            graph,
            owner_entity,
            owner_id,
            relation,
        )
    }

    #[allow(clippy::too_many_arguments)] // Metadata-preserving counterpart of the Vec boundary.
    pub fn decode_compact_smart_list(
        &self,
        entity: &str,
        rows: SmartList<CompactRow>,
        root: &EntityRuntimeState,
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
        root: &EntityRuntimeState,
        graph: &mut EntityGraphBuilder,
    ) -> Result<(), EntityError> {
        self.compact_batch_decoders.get(entity).ok_or_else(|| {
            EntityError::new(
                entity,
                "entity has no compact identity graph batch decoder in RuntimeModule",
            )
        })?(rows, root, graph)
    }

    #[allow(clippy::too_many_arguments)] // Stable generated decoder boundary.
    pub fn decode_compact_option(
        &self,
        entity: &str,
        rows: Vec<CompactRow>,
        root: &EntityRuntimeState,
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
    #[doc(hidden)]
    fn query_diagnostic_observer(&self) -> Option<teaql_data_service::ExecutionObserver<'_>> {
        self.capture_execution_metadata().then(|| {
            std::sync::Arc::new(move |metadata| self.record_metadata_log(&metadata))
                as teaql_data_service::ExecutionObserver<'_>
        })
    }
    #[doc(hidden)]
    fn mutation_diagnostic_observer(&self) -> Option<teaql_data_service::ExecutionObserver<'_>> {
        Some(std::sync::Arc::new(move |metadata| {
            self.record_metadata_log(&metadata)
        }))
    }
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
    generated_schema_bootstraps: Vec<crate::GeneratedSchemaBootstrap>,
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

    pub fn generated_schema_bootstrap(
        mut self,
        bootstrap: crate::GeneratedSchemaBootstrap,
    ) -> Self {
        self.generated_schema_bootstraps.push(bootstrap);
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
        context.set_generated_schema_bootstraps(self.generated_schema_bootstraps);
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
