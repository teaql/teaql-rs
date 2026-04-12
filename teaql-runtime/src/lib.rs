use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::slice;
use std::sync::Arc;

use teaql_core::{
    DeleteCommand, EntityDescriptor, EntityDescriptorStore, Expr, InsertCommand, Record,
    RecoverCommand, SelectQuery, TeaqlEntity, UpdateCommand, Value,
};
use teaql_sql::{CompiledQuery, SqlCompileError, SqlDialect};

#[derive(Debug)]
pub enum RuntimeError {
    MissingEntity(String),
    SqlCompile(SqlCompileError),
    Behavior(String),
    MissingRelation {
        entity: String,
        relation: String,
    },
    OptimisticLockConflict {
        entity: String,
        id: String,
    },
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEntity(entity) => write!(f, "missing entity descriptor: {entity}"),
            Self::SqlCompile(err) => err.fmt(f),
            Self::Behavior(message) => write!(f, "repository behavior error: {message}"),
            Self::MissingRelation { entity, relation } => {
                write!(f, "missing relation {relation} on entity {entity}")
            }
            Self::OptimisticLockConflict { entity, id } => {
                write!(f, "optimistic lock conflict on {entity}({id})")
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<SqlCompileError> for RuntimeError {
    fn from(value: SqlCompileError) -> Self {
        Self::SqlCompile(value)
    }
}

pub trait MetadataStore: Send + Sync {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor>;
}

pub trait RepositoryRegistry: Send + Sync {
    fn contains(&self, entity: &str) -> bool;
}

pub trait RepositoryBehavior: Send + Sync {
    fn before_select(&self, _ctx: &UserContext, _query: &mut SelectQuery) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_insert(&self, _ctx: &UserContext, _command: &mut InsertCommand) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_update(&self, _ctx: &UserContext, _command: &mut UpdateCommand) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_delete(&self, _ctx: &UserContext, _command: &mut DeleteCommand) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn before_recover(&self, _ctx: &UserContext, _command: &mut RecoverCommand) -> Result<(), RuntimeError> {
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

    pub fn apply_to(self, ctx: &mut UserContext) {
        ctx.set_metadata(self.metadata);
        ctx.set_repository_registry(self.repositories);
        ctx.set_repository_behavior_registry(self.behaviors);
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

#[derive(Debug)]
pub enum ContextError {
    MissingResource(String),
    MissingTypedResource(&'static str),
    MissingRepository(String),
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingResource(name) => write!(f, "missing named resource: {name}"),
            Self::MissingTypedResource(name) => write!(f, "missing typed resource: {name}"),
            Self::MissingRepository(name) => write!(f, "missing repository for entity: {name}"),
        }
    }
}

impl std::error::Error for ContextError {}

#[derive(Default)]
pub struct UserContext {
    metadata: Option<Box<dyn MetadataStore>>,
    repository_registry: Option<Box<dyn RepositoryRegistry>>,
    repository_behavior_registry: Option<Box<dyn RepositoryBehaviorRegistry>>,
    typed_resources: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    named_resources: BTreeMap<String, Box<dyn Any + Send + Sync>>,
    locals: BTreeMap<String, Value>,
}

impl UserContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_module(mut self, module: RuntimeModule) -> Self {
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

    pub fn repository_behavior(&self, entity: &str) -> Option<Arc<dyn RepositoryBehavior>> {
        self.repository_behavior_registry
            .as_ref()
            .and_then(|registry| registry.behavior(entity))
    }
}

pub trait QueryExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error>;
    fn execute(&self, query: &CompiledQuery) -> Result<u64, Self::Error>;
}

pub struct Repository<'a, D, M, E> {
    dialect: &'a D,
    metadata: &'a M,
    executor: &'a E,
}

pub struct ContextRepository<'a, D, E> {
    metadata: UserContextMetadata<'a>,
    dialect: &'a D,
    executor: &'a E,
}

pub struct ResolvedRepository<'a, D, E> {
    entity: String,
    repository: ContextRepository<'a, D, E>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationLoadPlan {
    pub parent_entity: String,
    pub relation_name: String,
    pub path: String,
    pub target_entity: String,
    pub local_key: String,
    pub foreign_key: String,
    pub many: bool,
    pub children: Vec<RelationLoadPlan>,
}

impl<'a, D, M, E> Repository<'a, D, M, E>
where
    D: SqlDialect,
    M: MetadataStore,
    E: QueryExecutor,
{
    pub fn new(dialect: &'a D, metadata: &'a M, executor: &'a E) -> Self {
        Self {
            dialect,
            metadata,
            executor,
        }
    }

    pub fn compile(&self, query: &SelectQuery) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&query.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(query.entity.clone()))?;
        Ok(self.dialect.compile_select(entity, query)?)
    }

    pub fn compile_insert(&self, command: &InsertCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_insert(entity, command)?)
    }

    pub fn compile_update(&self, command: &UpdateCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_update(entity, command)?)
    }

    pub fn compile_delete(&self, command: &DeleteCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_delete(entity, command)?)
    }

    pub fn compile_recover(&self, command: &RecoverCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_recover(entity, command)?)
    }

    pub fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let compiled = self.compile(query).map_err(RepositoryError::Runtime)?;
        self.executor
            .fetch_all(&compiled)
            .map_err(RepositoryError::Executor)
    }

    pub fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_insert(command)
            .map_err(RepositoryError::Runtime)?;
        self.executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_update(command)
            .map_err(RepositoryError::Runtime)?;
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;

        if command.expected_version.is_some() && affected == 0 {
            return Err(RepositoryError::Runtime(RuntimeError::OptimisticLockConflict {
                entity: command.entity.clone(),
                id: format!("{:?}", command.id),
            }));
        }

        Ok(affected)
    }

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_delete(command)
            .map_err(RepositoryError::Runtime)?;
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;

        if command.expected_version.is_some() && affected == 0 {
            return Err(RepositoryError::Runtime(RuntimeError::OptimisticLockConflict {
                entity: command.entity.clone(),
                id: format!("{:?}", command.id),
            }));
        }

        Ok(affected)
    }

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_recover(command)
            .map_err(RepositoryError::Runtime)?;
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;

        if affected == 0 {
            return Err(RepositoryError::Runtime(RuntimeError::OptimisticLockConflict {
                entity: command.entity.clone(),
                id: format!("{:?}", command.id),
            }));
        }

        Ok(affected)
    }

    pub fn insert_many(
        &self,
        commands: &[InsertCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.insert(command)?;
        }
        Ok(total)
    }

    pub fn update_many(
        &self,
        commands: &[UpdateCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.update(command)?;
        }
        Ok(total)
    }

    pub fn delete_many(
        &self,
        commands: &[DeleteCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.delete(command)?;
        }
        Ok(total)
    }

    pub fn recover_many(
        &self,
        commands: &[RecoverCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.recover(command)?;
        }
        Ok(total)
    }
}

struct UserContextMetadata<'a> {
    context: &'a UserContext,
}

impl MetadataStore for UserContextMetadata<'_> {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.context.entity(name)
    }
}

impl UserContext {
    pub fn repository<D, E>(&self) -> Result<ContextRepository<'_, D, E>, ContextError>
    where
        D: SqlDialect + Send + Sync + 'static,
        E: QueryExecutor + Send + Sync + 'static,
    {
        if self.metadata.is_none() {
            return Err(ContextError::MissingResource("metadata".to_owned()));
        }

        let dialect = self.require_resource::<D>()?;
        let executor = self.require_resource::<E>()?;
        Ok(ContextRepository {
            metadata: UserContextMetadata { context: self },
            dialect,
            executor,
        })
    }

    pub fn resolve_repository<D, E>(
        &self,
        entity: impl Into<String>,
    ) -> Result<ResolvedRepository<'_, D, E>, ContextError>
    where
        D: SqlDialect + Send + Sync + 'static,
        E: QueryExecutor + Send + Sync + 'static,
    {
        let entity = entity.into();
        if !self.has_repository(&entity) {
            return Err(ContextError::MissingRepository(entity));
        }
        Ok(ResolvedRepository {
            entity,
            repository: self.repository::<D, E>()?,
        })
    }
}

impl<'a, D, E> ContextRepository<'a, D, E>
where
    D: SqlDialect,
    E: QueryExecutor,
{
    fn repository(&self) -> Repository<'_, D, UserContextMetadata<'_>, E> {
        Repository::new(self.dialect, &self.metadata, self.executor)
    }

    pub fn compile(&self, query: &SelectQuery) -> Result<CompiledQuery, RuntimeError> {
        self.repository().compile(query)
    }

    pub fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        self.repository().fetch_all(query)
    }

    pub fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        self.repository().insert(command)
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        self.repository().update(command)
    }

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        self.repository().delete(command)
    }

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        self.repository().recover(command)
    }
}

impl<'a, D, E> ResolvedRepository<'a, D, E>
where
    D: SqlDialect,
    E: QueryExecutor,
{
    fn query_behavior(&self, entity: &str) -> Option<Arc<dyn RepositoryBehavior>> {
        self.repository.metadata.context.repository_behavior(entity)
    }

    fn behavior(&self) -> Option<Arc<dyn RepositoryBehavior>> {
        self.repository.metadata.context.repository_behavior(&self.entity)
    }

    pub fn entity(&self) -> &str {
        &self.entity
    }

    pub fn select(&self) -> SelectQuery {
        SelectQuery::new(self.entity.clone())
    }

    pub fn insert_command(&self) -> InsertCommand {
        InsertCommand::new(self.entity.clone())
    }

    pub fn update_command(&self, id: impl Into<Value>) -> UpdateCommand {
        UpdateCommand::new(self.entity.clone(), id)
    }

    pub fn delete_command(&self, id: impl Into<Value>) -> DeleteCommand {
        DeleteCommand::new(self.entity.clone(), id)
    }

    pub fn recover_command(&self, id: impl Into<Value>, expected_version: i64) -> RecoverCommand {
        RecoverCommand::new(self.entity.clone(), id, expected_version)
    }

    pub fn compile(&self, query: &SelectQuery) -> Result<CompiledQuery, RuntimeError> {
        let mut query = query.clone();
        if let Some(behavior) = self.query_behavior(&query.entity) {
            behavior.before_select(self.repository.metadata.context, &mut query)?;
        }
        self.repository.compile(&query)
    }

    pub fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let mut query = query.clone();
        if let Some(behavior) = self.query_behavior(&query.entity) {
            behavior
                .before_select(self.repository.metadata.context, &mut query)
                .map_err(RepositoryError::Runtime)?;
        }
        self.repository.fetch_all(&query)
    }

    pub fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_insert(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.repository.insert(&command)
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_update(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.repository.update(&command)
    }

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_delete(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.repository.delete(&command)
    }

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_recover(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.repository.recover(&command)
    }

    pub fn relation_loads(&self) -> Vec<String> {
        self.behavior()
            .map(|behavior| behavior.relation_loads(self.repository.metadata.context))
            .unwrap_or_default()
    }

    pub fn relation_plans(&self) -> Result<Vec<RelationLoadPlan>, RuntimeError> {
        self.build_relation_plans(&self.entity, &self.relation_loads())
    }

    pub fn relation_query(
        &self,
        relation_name: &str,
        parent_rows: &[Record],
    ) -> Result<SelectQuery, RuntimeError> {
        let plan = self
            .relation_plans()?
            .into_iter()
            .find(|plan| plan.relation_name == relation_name)
            .ok_or_else(|| RuntimeError::MissingRelation {
                entity: self.entity.clone(),
                relation: relation_name.to_owned(),
            })?;
        Ok(self.query_for_plan(&plan, parent_rows))
    }

    pub fn enhance_relations(
        &self,
        parent_rows: &mut [Record],
    ) -> Result<(), RepositoryError<E::Error>> {
        let plans = self.relation_plans().map_err(RepositoryError::Runtime)?;
        for plan in plans {
            self.enhance_plan(parent_rows, &plan)?;
        }
        Ok(())
    }

    fn build_relation_plans(
        &self,
        entity: &str,
        loads: &[String],
    ) -> Result<Vec<RelationLoadPlan>, RuntimeError> {
        let descriptor = self.repository.metadata.context.require_entity(entity)?;
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for load in loads {
            if let Some((head, tail)) = load.split_once('.') {
                grouped
                    .entry(head.to_owned())
                    .or_default()
                    .push(tail.to_owned());
            } else {
                grouped.entry(load.clone()).or_default();
            }
        }

        grouped
            .into_iter()
            .map(|(name, child_loads)| {
                let relation = descriptor.relation_by_name(&name).ok_or_else(|| {
                    RuntimeError::MissingRelation {
                        entity: entity.to_owned(),
                        relation: name.clone(),
                    }
                })?;
                let child_repo = self.scoped_repository(relation.target_entity.clone());
                let children = child_repo.build_relation_plans(&relation.target_entity, &child_loads)?;
                Ok(RelationLoadPlan {
                    parent_entity: entity.to_owned(),
                    relation_name: relation.name.clone(),
                    path: relation.name.clone(),
                    target_entity: relation.target_entity.clone(),
                    local_key: relation.local_key.clone(),
                    foreign_key: relation.foreign_key.clone(),
                    many: relation.many,
                    children,
                })
            })
            .collect()
    }

    fn scoped_repository(&self, entity: String) -> ResolvedRepository<'a, D, E> {
        ResolvedRepository {
            entity,
            repository: ContextRepository {
                metadata: UserContextMetadata {
                    context: self.repository.metadata.context,
                },
                dialect: self.repository.dialect,
                executor: self.repository.executor,
            },
        }
    }

    fn enhance_plan(
        &self,
        parent_rows: &mut [Record],
        plan: &RelationLoadPlan,
    ) -> Result<(), RepositoryError<E::Error>> {
        let child_repo = self.scoped_repository(plan.target_entity.clone());
        let query = self.query_for_plan(plan, parent_rows);
        let child_rows = child_repo.fetch_all(&query)?;
        self.attach_relation_rows(parent_rows, plan, child_rows);

        if !plan.children.is_empty() {
            for parent in parent_rows.iter_mut() {
                match parent.get_mut(&plan.relation_name) {
                    Some(Value::Object(child)) => {
                        child_repo.enhance_child_record(child, &plan.children)?;
                    }
                    Some(Value::List(values)) => {
                        for value in values.iter_mut() {
                            if let Value::Object(child) = value {
                                child_repo.enhance_child_record(child, &plan.children)?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn enhance_child_record(
        &self,
        child: &mut Record,
        plans: &[RelationLoadPlan],
    ) -> Result<(), RepositoryError<E::Error>> {
        for plan in plans {
            self.enhance_plan(slice::from_mut(child), plan)?;
        }
        Ok(())
    }

    fn query_for_plan(
        &self,
        plan: &RelationLoadPlan,
        parent_rows: &[Record],
    ) -> SelectQuery {
        let ids = parent_rows
            .iter()
            .filter_map(|row| row.get(&plan.local_key).cloned())
            .collect::<Vec<_>>();

        let mut query = SelectQuery::new(plan.target_entity.clone());
        if !ids.is_empty() {
            query = query.filter(Expr::in_list(plan.foreign_key.clone(), ids));
        }
        query
    }

    fn attach_relation_rows(
        &self,
        parent_rows: &mut [Record],
        plan: &RelationLoadPlan,
        child_rows: Vec<Record>,
    ) {
        let mut buckets: BTreeMap<String, Vec<Record>> = BTreeMap::new();
        for child in child_rows {
            if let Some(key) = child.get(&plan.foreign_key) {
                buckets
                    .entry(relation_bucket_key(key))
                    .or_default()
                    .push(child);
            }
        }

        for parent in parent_rows.iter_mut() {
            let Some(local_value) = parent.get(&plan.local_key) else {
                continue;
            };
            let bucket_key = relation_bucket_key(local_value);
            let related = buckets.get(&bucket_key).cloned().unwrap_or_default();
            if plan.many {
                parent.insert(
                    plan.relation_name.clone(),
                    Value::List(related.into_iter().map(Value::object).collect()),
                );
            } else {
                let value = related
                    .into_iter()
                    .next()
                    .map(Value::object)
                    .unwrap_or(Value::Null);
                parent.insert(plan.relation_name.clone(), value);
            }
        }
    }
}

fn relation_bucket_key(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(v) => format!("b:{v}"),
        Value::I64(v) => format!("i:{v}"),
        Value::F64(v) => format!("f:{v}"),
        Value::Text(v) => format!("t:{v}"),
        Value::Object(_) => "o".to_owned(),
        Value::List(_) => "l".to_owned(),
    }
}

#[derive(Debug)]
pub enum RepositoryError<ExecError> {
    Runtime(RuntimeError),
    Executor(ExecError),
}

impl<ExecError> std::fmt::Display for RepositoryError<ExecError>
where
    ExecError: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(err) => err.fmt(f),
            Self::Executor(err) => err.fmt(f),
        }
    }
}

impl<ExecError> std::error::Error for RepositoryError<ExecError>
where
    ExecError: std::error::Error + 'static,
{
}

#[cfg(feature = "sqlx")]
pub mod sqlx_support {
    use std::collections::BTreeMap;

    use sqlx::postgres::{PgArguments, PgPool, PgRow};
    use sqlx::sqlite::{SqliteArguments, SqlitePool, SqliteRow};
    use sqlx::{Arguments, Column, Error as SqlxError, Row, TypeInfo, ValueRef, query_with};
    use teaql_core::{Record, Value};
    use teaql_sql::CompiledQuery;

    #[derive(Debug)]
    pub enum MutationExecutorError {
        Sqlx(SqlxError),
        UnsupportedValue(&'static str),
        UnsupportedColumnType(String),
        Bind(String),
    }

    impl std::fmt::Display for MutationExecutorError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Sqlx(err) => err.fmt(f),
                Self::UnsupportedValue(kind) => {
                    write!(f, "unsupported sqlx bind value for mutation executor: {kind}")
                }
                Self::UnsupportedColumnType(kind) => {
                    write!(f, "unsupported sqlx column type for record decoding: {kind}")
                }
                Self::Bind(message) => write!(f, "sqlx bind error: {message}"),
            }
        }
    }

    impl std::error::Error for MutationExecutorError {}

    impl From<SqlxError> for MutationExecutorError {
        fn from(value: SqlxError) -> Self {
            Self::Sqlx(value)
        }
    }

    #[derive(Clone)]
    pub struct PgMutationExecutor {
        pool: PgPool,
    }

    impl PgMutationExecutor {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }

        pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
            let mut args = PgArguments::default();
            for value in &query.params {
                bind_pg(&mut args, value)?;
            }
            let result = query_with(&query.sql, args).execute(&self.pool).await?;
            Ok(result.rows_affected())
        }

        pub async fn fetch_all(
            &self,
            query: &CompiledQuery,
        ) -> Result<Vec<Record>, MutationExecutorError> {
            let mut args = PgArguments::default();
            for value in &query.params {
                bind_pg(&mut args, value)?;
            }
            let rows = query_with(&query.sql, args).fetch_all(&self.pool).await?;
            rows.iter().map(decode_pg_row).collect()
        }
    }

    #[derive(Clone)]
    pub struct SqliteMutationExecutor {
        pool: SqlitePool,
    }

    impl SqliteMutationExecutor {
        pub fn new(pool: SqlitePool) -> Self {
            Self { pool }
        }

        pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
            let mut args = SqliteArguments::default();
            for value in &query.params {
                bind_sqlite(&mut args, value)?;
            }
            let result = query_with(&query.sql, args).execute(&self.pool).await?;
            Ok(result.rows_affected())
        }

        pub async fn fetch_all(
            &self,
            query: &CompiledQuery,
        ) -> Result<Vec<Record>, MutationExecutorError> {
            let mut args = SqliteArguments::default();
            for value in &query.params {
                bind_sqlite(&mut args, value)?;
            }
            let rows = query_with(&query.sql, args).fetch_all(&self.pool).await?;
            rows.iter().map(decode_sqlite_row).collect()
        }
    }

    fn bind_pg(args: &mut PgArguments, value: &Value) -> Result<(), MutationExecutorError> {
        match value {
            Value::Null => {
                let v: Option<String> = None;
                args.add(v)
                    .map_err(|err| MutationExecutorError::Bind(err.to_string()))?;
            }
            Value::Bool(v) => args
                .add(*v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::I64(v) => args
                .add(*v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::F64(v) => args
                .add(*v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::Text(v) => args
                .add(v.clone())
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object")),
            Value::List(_) => return Err(MutationExecutorError::UnsupportedValue("list")),
        }
        Ok(())
    }

    fn bind_sqlite(
        args: &mut SqliteArguments<'_>,
        value: &Value,
    ) -> Result<(), MutationExecutorError> {
        match value {
            Value::Null => {
                let v: Option<String> = None;
                args.add(v)
                    .map_err(|err| MutationExecutorError::Bind(err.to_string()))?;
            }
            Value::Bool(v) => args
                .add(*v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::I64(v) => args
                .add(*v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::F64(v) => args
                .add(*v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::Text(v) => args
                .add(v.clone())
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
            Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object")),
            Value::List(_) => return Err(MutationExecutorError::UnsupportedValue("list")),
        }
        Ok(())
    }

    fn decode_pg_row(row: &PgRow) -> Result<Record, MutationExecutorError> {
        let mut record = BTreeMap::new();
        for (index, column) in row.columns().iter().enumerate() {
            let name = column.name().to_owned();
            let raw = row.try_get_raw(index).map_err(MutationExecutorError::Sqlx)?;
            if raw.is_null() {
                record.insert(name, Value::Null);
                continue;
            }
            let type_name = column.type_info().name().to_ascii_uppercase();
            let value = match type_name.as_str() {
                "BOOL" | "BOOLEAN" => Value::Bool(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
                "INT2" => Value::I64(row.try_get::<i16, _>(index).map_err(MutationExecutorError::Sqlx)? as i64),
                "INT4" => Value::I64(row.try_get::<i32, _>(index).map_err(MutationExecutorError::Sqlx)? as i64),
                "INT8" => Value::I64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
                "FLOAT4" => Value::F64(row.try_get::<f32, _>(index).map_err(MutationExecutorError::Sqlx)? as f64),
                "FLOAT8" | "NUMERIC" => Value::F64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
                "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "UUID" => {
                    Value::Text(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
                }
                other => return Err(MutationExecutorError::UnsupportedColumnType(other.to_owned())),
            };
            record.insert(name, value);
        }
        Ok(record)
    }

    fn decode_sqlite_row(row: &SqliteRow) -> Result<Record, MutationExecutorError> {
        let mut record = BTreeMap::new();
        for (index, column) in row.columns().iter().enumerate() {
            let name = column.name().to_owned();
            let raw = row.try_get_raw(index).map_err(MutationExecutorError::Sqlx)?;
            if raw.is_null() {
                record.insert(name, Value::Null);
                continue;
            }
            let type_name = column.type_info().name().to_ascii_uppercase();
            let value = match type_name.as_str() {
                "BOOLEAN" => Value::Bool(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
                "INTEGER" | "INT8" | "INT4" | "INT2" => {
                    Value::I64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
                }
                "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" => {
                    Value::F64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
                }
                "TEXT" | "VARCHAR" => {
                    Value::Text(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
                }
                other => return Err(MutationExecutorError::UnsupportedColumnType(other.to_owned())),
            };
            record.insert(name, value);
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry,
        MetadataStore, QueryExecutor, Repository, RepositoryBehavior, RepositoryError, RuntimeError,
        RuntimeModule, UserContext,
    };
    use teaql_core::{
        DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record, UpdateCommand,
        Value,
    };
    use teaql_dialect_pg::PostgresDialect;
    use teaql_macros::TeaqlEntity as DeriveTeaqlEntity;
    use teaql_sql::CompiledQuery;

    fn entity() -> EntityDescriptor {
        EntityDescriptor::new("Order")
            .table_name("orders")
            .property(PropertyDescriptor::new("id", DataType::I64).column_name("id").id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("version")
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
            .relation(
                teaql_core::RelationDescriptor::new("lines", "OrderLine")
                    .local_key("id")
                    .foreign_key("order_id")
                    .many(),
            )
    }

    fn line_entity() -> EntityDescriptor {
        EntityDescriptor::new("OrderLine")
            .table_name("orderline")
            .property(PropertyDescriptor::new("id", DataType::I64).column_name("id").id().not_null())
            .property(
                PropertyDescriptor::new("order_id", DataType::I64)
                    .column_name("order_id")
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
            .property(
                PropertyDescriptor::new("product_id", DataType::I64)
                    .column_name("product_id")
                    .not_null(),
            )
            .relation(
                teaql_core::RelationDescriptor::new("product", "Product")
                    .local_key("product_id")
                    .foreign_key("id"),
            )
    }

    fn product_entity() -> EntityDescriptor {
        EntityDescriptor::new("Product")
            .table_name("product")
            .property(PropertyDescriptor::new("id", DataType::I64).column_name("id").id().not_null())
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
    }

    #[derive(Debug, Default)]
    struct StubExecutor {
        affected: u64,
        rows: Vec<Record>,
    }

    struct OrderBehavior;

    #[allow(dead_code)]
    #[derive(DeriveTeaqlEntity)]
    #[teaql(entity = "CatalogProduct", table = "catalog_product")]
    struct CatalogProductRow {
        #[teaql(id)]
        id: i64,
        name: String,
    }

    #[derive(Debug)]
    struct StubError;

    impl std::fmt::Display for StubError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "stub error")
        }
    }

    impl std::error::Error for StubError {}

    impl QueryExecutor for StubExecutor {
        type Error = StubError;

        fn fetch_all(&self, _query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
            Ok(self.rows.clone())
        }

        fn execute(&self, _query: &CompiledQuery) -> Result<u64, Self::Error> {
            Ok(self.affected)
        }
    }

    impl RepositoryBehavior for OrderBehavior {
        fn before_select(
            &self,
            _ctx: &UserContext,
            query: &mut teaql_core::SelectQuery,
        ) -> Result<(), RuntimeError> {
            query.filter = Some(Expr::eq("version", 1_i64));
            Ok(())
        }

        fn before_insert(
            &self,
            _ctx: &UserContext,
            command: &mut InsertCommand,
        ) -> Result<(), RuntimeError> {
            command
                .values
                .entry("version".to_owned())
                .or_insert(Value::I64(1));
            Ok(())
        }

        fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
            vec!["lines".to_owned()]
        }
    }

    #[test]
    fn metadata_store_registers_entities() {
        let store = InMemoryMetadataStore::new().with_entity(entity());
        assert!(store.entity("Order").is_some());
    }

    #[test]
    fn runtime_module_registers_descriptor_into_context() {
        let ctx = UserContext::new().with_module(RuntimeModule::new().descriptor(entity()));
        assert!(ctx.entity("Order").is_some());
        assert!(ctx.has_repository("Order"));
    }

    #[test]
    fn runtime_module_registers_derived_entity_and_behavior() {
        let ctx = UserContext::new().with_module(
            RuntimeModule::new().entity_with_behavior::<CatalogProductRow, _>(OrderBehavior),
        );
        assert!(ctx.entity("CatalogProduct").is_some());
        assert!(ctx.has_repository("CatalogProduct"));
        assert!(ctx.repository_behavior("CatalogProduct").is_some());
    }

    #[test]
    fn module_macro_registers_multiple_entities() {
        let ctx = UserContext::new().with_module(crate::module!(CatalogProductRow));
        assert!(ctx.entity("CatalogProduct").is_some());
        assert!(ctx.has_repository("CatalogProduct"));
    }

    #[test]
    fn module_macro_registers_entity_behavior_pairs() {
        let ctx = UserContext::new().with_module(crate::module!(CatalogProductRow => OrderBehavior));
        assert!(ctx.entity("CatalogProduct").is_some());
        assert!(ctx.repository_behavior("CatalogProduct").is_some());
    }

    #[test]
    fn repository_returns_optimistic_lock_conflict() {
        let store = InMemoryMetadataStore::new().with_entity(entity());
        let executor = StubExecutor {
            affected: 0,
            rows: Vec::new(),
        };
        let repo = Repository::new(&PostgresDialect, &store, &executor);

        let err = repo
            .update(
                &UpdateCommand::new("Order", 1_i64)
                    .expected_version(3)
                    .value("name", "next"),
            )
            .unwrap_err();

        match err {
            RepositoryError::Runtime(RuntimeError::OptimisticLockConflict { .. }) => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn user_context_indexes_resources_and_locals() {
        let mut ctx = UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(entity()));
        ctx.insert_resource::<u64>(42);
        ctx.insert_named_resource("tenant", String::from("acme"));
        ctx.put_local("trace_id", "req-1");

        assert!(ctx.entity("Order").is_some());
        assert_eq!(ctx.get_resource::<u64>(), Some(&42));
        assert_eq!(ctx.get_named_resource::<String>("tenant"), Some(&String::from("acme")));
        assert_eq!(ctx.local("trace_id"), Some(&Value::Text("req-1".to_owned())));
    }

    #[test]
    fn user_context_builds_context_repository() {
        let mut ctx = UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(entity()));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx.repository::<PostgresDialect, StubExecutor>().unwrap();
        let affected = repo
            .update(
                &UpdateCommand::new("Order", 1_i64)
                    .expected_version(3)
                    .value("name", "next"),
            )
            .unwrap();

        assert_eq!(affected, 1);
    }

    #[test]
    fn user_context_resolves_repository_by_entity_type() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        assert_eq!(repo.entity(), "Order");
        assert_eq!(repo.select().entity, "Order");

        let affected = repo
            .insert(&repo.insert_command().value("id", 1_i64).value("version", 1_i64).value("name", "n"))
            .unwrap();
        assert_eq!(affected, 1);
    }

    #[test]
    fn resolved_repository_applies_behavior_hooks() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();

        let compiled = repo.compile(&repo.select()).unwrap();
        assert!(compiled.sql.contains("WHERE (\"version\" = $1)"));

        let insert = repo.insert_command().value("id", 1_i64).value("name", "n");
        let affected = repo.insert(&insert).unwrap();
        assert_eq!(affected, 1);
        assert_eq!(repo.relation_loads(), vec!["lines".to_owned()]);
    }

    #[test]
    fn resolved_repository_builds_relation_plans() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let plans = repo.relation_plans().unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].relation_name, "lines");
        assert_eq!(plans[0].target_entity, "OrderLine");
        assert_eq!(plans[0].local_key, "id");
        assert_eq!(plans[0].foreign_key, "order_id");
        assert!(plans[0].many);
    }

    #[test]
    fn resolved_repository_builds_relation_query_from_parent_rows() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let parent_rows = vec![
            Record::from([(String::from("id"), Value::I64(11))]),
            Record::from([(String::from("id"), Value::I64(12))]),
        ];

        let query = repo.relation_query("lines", &parent_rows).unwrap();
        let compiled = repo.compile(&query).unwrap();
        assert!(compiled.sql.contains("FROM \"orderline\""));
        assert!(compiled.sql.contains("\"order_id\" IN ($1, $2)"));
        assert_eq!(compiled.params, vec![Value::I64(11), Value::I64(12)]);
    }

    #[test]
    fn resolved_repository_enhances_parent_rows_with_relations() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![
                Record::from([
                    (String::from("id"), Value::I64(101)),
                    (String::from("order_id"), Value::I64(11)),
                    (String::from("name"), Value::Text(String::from("l1"))),
                ]),
                Record::from([
                    (String::from("id"), Value::I64(102)),
                    (String::from("order_id"), Value::I64(11)),
                    (String::from("name"), Value::Text(String::from("l2"))),
                ]),
                Record::from([
                    (String::from("id"), Value::I64(201)),
                    (String::from("order_id"), Value::I64(12)),
                    (String::from("name"), Value::Text(String::from("l3"))),
                ]),
            ],
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let mut parents = vec![
            Record::from([(String::from("id"), Value::I64(11))]),
            Record::from([(String::from("id"), Value::I64(12))]),
        ];

        repo.enhance_relations(&mut parents).unwrap();

        match parents[0].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 2),
            other => panic!("unexpected lines payload: {other:?}"),
        }
        match parents[1].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 1),
            other => panic!("unexpected lines payload: {other:?}"),
        }
    }
}

#[cfg(all(test, feature = "sqlx"))]
mod sqlx_integration_tests {
    use super::sqlx_support::{MutationExecutorError, SqliteMutationExecutor};
    use super::{
        InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry, QueryExecutor,
        RepositoryBehavior, UserContext,
    };
    use teaql_core::{
        DataType, DeleteCommand, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, RecoverCommand,
        Record, SelectQuery, UpdateCommand, Value,
    };
    use teaql_dialect_sqlite::SqliteDialect;
    use teaql_sql::SqlDialect;
    use tokio::runtime::Handle;
    use tokio::task::block_in_place;

    fn entity() -> EntityDescriptor {
        EntityDescriptor::new("Order")
            .table_name("orders")
            .property(PropertyDescriptor::new("id", DataType::I64).column_name("id").id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("version")
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name").not_null())
            .relation(
                teaql_core::RelationDescriptor::new("lines", "OrderLine")
                    .local_key("id")
                    .foreign_key("order_id")
                    .many(),
            )
    }

    fn line_entity() -> EntityDescriptor {
        EntityDescriptor::new("OrderLine")
            .table_name("orderline")
            .property(PropertyDescriptor::new("id", DataType::I64).column_name("id").id().not_null())
            .property(
                PropertyDescriptor::new("order_id", DataType::I64)
                    .column_name("order_id")
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name").not_null())
            .property(
                PropertyDescriptor::new("product_id", DataType::I64)
                    .column_name("product_id")
                    .not_null(),
            )
            .relation(
                teaql_core::RelationDescriptor::new("product", "Product")
                    .local_key("product_id")
                    .foreign_key("id"),
            )
    }

    fn product_entity() -> EntityDescriptor {
        EntityDescriptor::new("Product")
            .table_name("product")
            .property(PropertyDescriptor::new("id", DataType::I64).column_name("id").id().not_null())
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name").not_null())
    }

    struct OrderBehavior;
    struct NestedOrderBehavior;

    impl RepositoryBehavior for OrderBehavior {
        fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
            vec!["lines".to_owned()]
        }
    }

    impl RepositoryBehavior for NestedOrderBehavior {
        fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
            vec!["lines.product".to_owned()]
        }
    }

    #[derive(Clone)]
    struct SqliteSyncExecutor {
        inner: SqliteMutationExecutor,
    }

    impl SqliteSyncExecutor {
        fn new(inner: SqliteMutationExecutor) -> Self {
            Self { inner }
        }
    }

    impl QueryExecutor for SqliteSyncExecutor {
        type Error = MutationExecutorError;

        fn fetch_all(&self, query: &teaql_sql::CompiledQuery) -> Result<Vec<Record>, Self::Error> {
            let handle = Handle::current();
            block_in_place(|| handle.block_on(self.inner.fetch_all(query)))
        }

        fn execute(&self, query: &teaql_sql::CompiledQuery) -> Result<u64, Self::Error> {
            let handle = Handle::current();
            block_in_place(|| handle.block_on(self.inner.execute(query)))
        }
    }

    #[tokio::test]
    async fn sqlite_executor_runs_crud_flow() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::query(
            "CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                version INTEGER NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteMutationExecutor::new(pool.clone());
        let dialect = SqliteDialect;
        let entity = entity();

        let insert = dialect
            .compile_insert(
                &entity,
                &InsertCommand::new("Order")
                    .value("id", 1_i64)
                    .value("version", 1_i64)
                    .value("name", "first"),
            )
            .unwrap();
        assert_eq!(executor.execute(&insert).await.unwrap(), 1);

        let select = dialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order")
                    .project("id")
                    .project("version")
                    .project("name")
                    .filter(Expr::eq("id", 1_i64)),
            )
            .unwrap();
        let rows = executor.fetch_all(&select).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("version"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("name"), Some(&Value::Text("first".to_owned())));

        let update = dialect
            .compile_update(
                &entity,
                &UpdateCommand::new("Order", 1_i64)
                    .expected_version(1)
                    .value("name", "second"),
            )
            .unwrap();
        assert_eq!(executor.execute(&update).await.unwrap(), 1);

        let after_update = executor.fetch_all(&select).await.unwrap();
        assert_eq!(after_update[0].get("version"), Some(&Value::I64(2)));
        assert_eq!(
            after_update[0].get("name"),
            Some(&Value::Text("second".to_owned()))
        );

        let delete = dialect
            .compile_delete(&entity, &DeleteCommand::new("Order", 1_i64).expected_version(2))
            .unwrap();
        assert_eq!(executor.execute(&delete).await.unwrap(), 1);

        let after_delete = executor.fetch_all(&select).await.unwrap();
        assert_eq!(after_delete[0].get("version"), Some(&Value::I64(-3)));

        let recover = dialect
            .compile_recover(&entity, &RecoverCommand::new("Order", 1_i64, -3))
            .unwrap();
        assert_eq!(executor.execute(&recover).await.unwrap(), 1);

        let after_recover = executor.fetch_all(&select).await.unwrap();
        assert_eq!(after_recover[0].get("version"), Some(&Value::I64(4)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_executor_enhances_relations() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::query(
            "CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                version INTEGER NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "CREATE TABLE orderline (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query("INSERT INTO orders (id, version, name) VALUES (1, 1, 'o1'), (2, 1, 'o2')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO orderline (id, order_id, name) VALUES
                (101, 1, 'l1'),
                (102, 1, 'l2'),
                (201, 2, 'l3')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteSyncExecutor::new(SqliteMutationExecutor::new(pool.clone()));
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let mut parents = repo
            .fetch_all(
                &repo.select()
                    .project("id")
                    .project("version")
                    .project("name")
                    .order_by(teaql_core::OrderBy::asc("id")),
            )
            .unwrap();

        repo.enhance_relations(&mut parents).unwrap();

        assert_eq!(parents.len(), 2);
        match parents[0].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 2),
            other => panic!("unexpected first lines payload: {other:?}"),
        }
        match parents[1].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 1),
            other => panic!("unexpected second lines payload: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_executor_enhances_nested_relations() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::query(
            "CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                version INTEGER NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "CREATE TABLE orderline (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "CREATE TABLE product (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query("INSERT INTO orders (id, version, name) VALUES (1, 1, 'o1')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO orderline (id, order_id, product_id, name) VALUES
                (101, 1, 1001, 'l1'),
                (102, 1, 1002, 'l2')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO product (id, name) VALUES
                (1001, 'p1'),
                (1002, 'p2')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteSyncExecutor::new(SqliteMutationExecutor::new(pool.clone()));
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new()
                    .with_behavior("Order", NestedOrderBehavior),
            );
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let mut parents = repo
            .fetch_all(
                &repo.select()
                    .project("id")
                    .project("version")
                    .project("name"),
            )
            .unwrap();

        repo.enhance_relations(&mut parents).unwrap();

        match parents[0].get("lines") {
            Some(Value::List(lines)) => {
                assert_eq!(lines.len(), 2);
                for line in lines {
                    match line {
                        Value::Object(line_record) => match line_record.get("product") {
                            Some(Value::Object(product)) => {
                                assert!(product.get("name").is_some());
                            }
                            other => panic!("unexpected product payload: {other:?}"),
                        },
                        other => panic!("unexpected line payload: {other:?}"),
                    }
                }
            }
            other => panic!("unexpected nested lines payload: {other:?}"),
        }
    }
}
