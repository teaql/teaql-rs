use std::collections::BTreeMap;
use std::sync::Arc;

use teaql_core::{
    DeleteCommand, EntityDescriptor, EntityDescriptorStore, InsertCommand, RecoverCommand,
    SelectQuery, TeaqlEntity, UpdateCommand,
};

use crate::{
    Checker, EntityEventSink, InMemoryCheckerRegistry, InMemoryEntityEventSink, RuntimeError,
    UserContext,
};

pub trait MetadataStore: Send + Sync {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor>;
    fn all_entities(&self) -> Vec<&EntityDescriptor>;
}

pub trait RepositoryRegistry: Send + Sync {
    fn contains(&self, entity: &str) -> bool;
}

pub trait RepositoryBehavior: Send + Sync {
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

pub trait RepositoryBehaviorRegistry: Send + Sync {
    fn behavior(&self, entity: &str) -> Option<Arc<dyn RepositoryBehavior>>;
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

impl EntityDescriptorStore for InMemoryMetadataStore {
    fn register_descriptor(&mut self, descriptor: EntityDescriptor) {
        self.register(descriptor);
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryRepositoryRegistry {
    entities: BTreeMap<String, String>,
}

impl InMemoryRepositoryRegistry {
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

impl RepositoryRegistry for InMemoryRepositoryRegistry {
    fn contains(&self, entity: &str) -> bool {
        self.entities.contains_key(entity)
    }
}

#[derive(Default, Clone)]
pub struct InMemoryRepositoryBehaviorRegistry {
    behaviors: BTreeMap<String, Arc<dyn RepositoryBehavior>>,
}

impl InMemoryRepositoryBehaviorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        entity: impl Into<String>,
        behavior: impl RepositoryBehavior + 'static,
    ) {
        self.behaviors.insert(entity.into(), Arc::new(behavior));
    }

    pub fn with_behavior(
        mut self,
        entity: impl Into<String>,
        behavior: impl RepositoryBehavior + 'static,
    ) -> Self {
        self.register(entity, behavior);
        self
    }
}

impl RepositoryBehaviorRegistry for InMemoryRepositoryBehaviorRegistry {
    fn behavior(&self, entity: &str) -> Option<Arc<dyn RepositoryBehavior>> {
        self.behaviors.get(entity).cloned()
    }
}

#[derive(Default, Clone)]
pub struct RuntimeModule {
    metadata: InMemoryMetadataStore,
    repositories: InMemoryRepositoryRegistry,
    behaviors: InMemoryRepositoryBehaviorRegistry,
    checkers: InMemoryCheckerRegistry,
    event_sinks: InMemoryEntityEventSink,
}

impl RuntimeModule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entity<T: TeaqlEntity>(mut self) -> Self {
        let descriptor = T::entity_descriptor();
        self.repositories.register(descriptor.name.clone());
        self.metadata.register(descriptor);
        self
    }

    pub fn entity_with_behavior<T, B>(mut self, behavior: B) -> Self
    where
        T: TeaqlEntity,
        B: RepositoryBehavior + 'static,
    {
        let descriptor = T::entity_descriptor();
        let entity_name = descriptor.name.clone();
        self.repositories.register(entity_name.clone());
        self.metadata.register(descriptor);
        self.behaviors.register(entity_name, behavior);
        self
    }

    pub fn descriptor(mut self, descriptor: EntityDescriptor) -> Self {
        self.repositories.register(descriptor.name.clone());
        self.metadata.register(descriptor);
        self
    }

    pub fn behavior(
        mut self,
        entity: impl Into<String>,
        behavior: impl RepositoryBehavior + 'static,
    ) -> Self {
        self.behaviors.register(entity, behavior);
        self
    }

    pub fn checker(mut self, checker: impl Checker + 'static) -> Self {
        self.checkers.register(checker);
        self
    }

    pub fn event_sink(mut self, sink: impl EntityEventSink + 'static) -> Self {
        self.event_sinks.register(sink);
        self
    }

    pub fn apply_to(self, ctx: &mut UserContext) {
        ctx.set_metadata(self.metadata);
        ctx.set_repository_registry(self.repositories);
        ctx.set_repository_behavior_registry(self.behaviors);
        ctx.set_checker_registry(self.checkers);
        ctx.set_event_sink(self.event_sinks);
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
