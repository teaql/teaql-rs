use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};

use teaql_core::{EntityDescriptor, Record, Value};

use crate::{
    CheckResults, CheckerRegistry, ContextError, EntityEvent, EntityEventSink, InternalIdGenerator,
    Language, MetadataStore, ObjectLocation, RepositoryBehavior, RepositoryBehaviorRegistry,
    RepositoryRegistry, RuntimeError, local_id_generator, translate_check_result,
};

#[derive(Default)]
pub struct UserContext {
    pub(crate) metadata: Option<Box<dyn MetadataStore>>,
    pub(crate) repository_registry: Option<Box<dyn RepositoryRegistry>>,
    pub(crate) repository_behavior_registry: Option<Box<dyn RepositoryBehaviorRegistry>>,
    pub(crate) checker_registry: Option<Box<dyn CheckerRegistry>>,
    pub(crate) event_sink: Option<Box<dyn EntityEventSink>>,
    pub(crate) internal_id_generator: Option<Box<dyn InternalIdGenerator>>,
    language: Language,
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

    pub fn with_repository_registry(mut self, registry: impl RepositoryRegistry + 'static) -> Self {
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

    pub fn with_checker_registry(mut self, registry: impl CheckerRegistry + 'static) -> Self {
        self.checker_registry = Some(Box::new(registry));
        self
    }

    pub fn set_checker_registry(&mut self, registry: impl CheckerRegistry + 'static) {
        self.checker_registry = Some(Box::new(registry));
    }

    pub fn with_event_sink(mut self, sink: impl EntityEventSink + 'static) -> Self {
        self.event_sink = Some(Box::new(sink));
        self
    }

    pub fn set_event_sink(&mut self, sink: impl EntityEventSink + 'static) {
        self.event_sink = Some(Box::new(sink));
    }

    pub fn with_internal_id_generator(
        mut self,
        generator: impl InternalIdGenerator + 'static,
    ) -> Self {
        self.internal_id_generator = Some(Box::new(generator));
        self
    }

    pub fn set_internal_id_generator(&mut self, generator: impl InternalIdGenerator + 'static) {
        self.internal_id_generator = Some(Box::new(generator));
    }

    pub fn with_language(mut self, language: Language) -> Self {
        self.language = language;
        self
    }

    pub fn set_language(&mut self, language: Language) {
        self.language = language;
    }

    pub fn language(&self) -> Language {
        self.language
    }

    pub fn set_language_code(&mut self, code: &str) -> Result<(), RuntimeError> {
        let Some(language) = Language::from_code(code) else {
            return Err(RuntimeError::Language(format!(
                "unsupported language code: {code}"
            )));
        };
        self.language = language;
        Ok(())
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
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.entity(name))
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
            .ok_or(ContextError::MissingTypedResource(
                std::any::type_name::<T>(),
            ))
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

    pub fn repository_behavior(
        &self,
        entity: &str,
    ) -> Option<std::sync::Arc<dyn RepositoryBehavior>> {
        self.repository_behavior_registry
            .as_ref()
            .and_then(|registry| registry.behavior(entity))
    }

    pub fn has_checker(&self, entity: &str) -> bool {
        self.checker_registry
            .as_ref()
            .and_then(|registry| registry.checker(entity))
            .is_some()
    }

    pub fn check_and_fix_record(
        &self,
        entity: &str,
        record: &mut Record,
    ) -> Result<(), RuntimeError> {
        self.check_and_fix_record_at(entity, record, &ObjectLocation::root())
    }

    pub fn check_and_fix_record_at(
        &self,
        entity: &str,
        record: &mut Record,
        location: &ObjectLocation,
    ) -> Result<(), RuntimeError> {
        let Some(checker) = self
            .checker_registry
            .as_ref()
            .and_then(|registry| registry.checker(entity))
        else {
            return Ok(());
        };
        let mut results = CheckResults::new();
        checker.check_and_fix(self, record, location, &mut results);
        if results.is_empty() {
            Ok(())
        } else {
            self.translate_check_results(&mut results);
            Err(RuntimeError::Check(results))
        }
    }

    pub fn translate_check_results(&self, results: &mut CheckResults) {
        for result in results {
            result.message = Some(translate_check_result(self.language, result));
        }
    }

    pub fn send_event(&self, event: EntityEvent) -> Result<(), RuntimeError> {
        let Some(sink) = self.event_sink.as_ref() else {
            return Ok(());
        };
        sink.on_event(self, &event)
    }
}
