use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};

use teaql_core::{EntityDescriptor, Value};

use crate::{
    ContextError, InternalIdGenerator, MetadataStore, RepositoryBehavior, RepositoryBehaviorRegistry,
    RepositoryRegistry, RuntimeError, local_id_generator,
};

#[derive(Default)]
pub struct UserContext {
    pub(crate) metadata: Option<Box<dyn MetadataStore>>,
    pub(crate) repository_registry: Option<Box<dyn RepositoryRegistry>>,
    pub(crate) repository_behavior_registry: Option<Box<dyn RepositoryBehaviorRegistry>>,
    pub(crate) internal_id_generator: Option<Box<dyn InternalIdGenerator>>,
    typed_resources: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    named_resources: BTreeMap<String, Box<dyn Any + Send + Sync>>,
    locals: BTreeMap<String, Value>,
}

impl UserContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_module(mut self, module: crate::RuntimeModule) -> Self {
        module.apply_to(&mut self);
        self
    }

    pub fn with_metadata(mut self, metadata: impl MetadataStore + 'static) -> Self {
        self.metadata = Some(Box::new(metadata));
        self
    }

    pub fn set_metadata(&mut self, metadata: impl MetadataStore + 'static) {
        self.metadata = Some(Box::new(metadata));
    }

    pub fn with_repository_registry(
        mut self,
        registry: impl RepositoryRegistry + 'static,
    ) -> Self {
        self.repository_registry = Some(Box::new(registry));
        self
    }

    pub fn set_repository_registry(&mut self, registry: impl RepositoryRegistry + 'static) {
        self.repository_registry = Some(Box::new(registry));
    }

    pub fn with_repository_behavior_registry(
        mut self,
        registry: impl RepositoryBehaviorRegistry + 'static,
    ) -> Self {
        self.repository_behavior_registry = Some(Box::new(registry));
        self
    }

    pub fn set_repository_behavior_registry(
        &mut self,
        registry: impl RepositoryBehaviorRegistry + 'static,
    ) {
        self.repository_behavior_registry = Some(Box::new(registry));
    }

    pub fn with_internal_id_generator(
        mut self,
        generator: impl InternalIdGenerator + 'static,
    ) -> Self {
        self.internal_id_generator = Some(Box::new(generator));
        self
    }

    pub fn set_internal_id_generator(
        &mut self,
        generator: impl InternalIdGenerator + 'static,
    ) {
        self.internal_id_generator = Some(Box::new(generator));
    }

    pub fn generate_id(&self, entity: &str) -> Result<Option<u64>, RuntimeError> {
        self.internal_id_generator
            .as_ref()
            .map(|generator| generator.generate_id(entity))
            .transpose()
    }

    pub fn next_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        match self.generate_id(entity)? {
            Some(id) => Ok(id),
            None => local_id_generator().generate_id(entity),
        }
    }

    pub fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.metadata.as_ref().and_then(|metadata| metadata.entity(name))
    }

    pub fn require_entity(&self, name: &str) -> Result<&EntityDescriptor, RuntimeError> {
        self.entity(name)
            .ok_or_else(|| RuntimeError::MissingEntity(name.to_owned()))
    }

    pub fn insert_resource<T>(&mut self, resource: T)
    where
        T: Send + Sync + 'static,
    {
        self.typed_resources
            .insert(TypeId::of::<T>(), Box::new(resource));
    }

    pub fn get_resource<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.typed_resources
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn require_resource<T>(&self) -> Result<&T, ContextError>
    where
        T: Send + Sync + 'static,
    {
        self.get_resource::<T>()
            .ok_or(ContextError::MissingTypedResource(std::any::type_name::<T>()))
    }

    pub fn insert_named_resource<T>(&mut self, name: impl Into<String>, resource: T)
    where
        T: Send + Sync + 'static,
    {
        self.named_resources.insert(name.into(), Box::new(resource));
    }

    pub fn get_named_resource<T>(&self, name: &str) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.named_resources
            .get(name)
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn require_named_resource<T>(&self, name: &str) -> Result<&T, ContextError>
    where
        T: Send + Sync + 'static,
    {
        self.get_named_resource::<T>(name)
            .ok_or_else(|| ContextError::MissingResource(name.to_owned()))
    }

    pub fn put_local(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.locals.insert(key.into(), value.into());
    }

    pub fn local(&self, key: &str) -> Option<&Value> {
        self.locals.get(key)
    }

    pub fn remove_local(&mut self, key: &str) -> Option<Value> {
        self.locals.remove(key)
    }

    pub fn has_repository(&self, entity: &str) -> bool {
        let in_registry = self
            .repository_registry
            .as_ref()
            .map(|registry| registry.contains(entity))
            .unwrap_or(false);
        in_registry || self.entity(entity).is_some()
    }

    pub fn repository_behavior(&self, entity: &str) -> Option<std::sync::Arc<dyn RepositoryBehavior>> {
        self.repository_behavior_registry
            .as_ref()
            .and_then(|registry| registry.behavior(entity))
    }
}
