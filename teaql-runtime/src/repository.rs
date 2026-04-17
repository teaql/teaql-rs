use std::collections::BTreeMap;
use std::slice;
use std::sync::Arc;

use teaql_core::{
    DeleteCommand, Entity, EntityDescriptor, Expr, InsertCommand, Record, RecoverCommand,
    SelectQuery, SmartList, UpdateCommand, Value,
};
use teaql_sql::{CompiledQuery, SqlDialect};

use crate::{
    ContextError, MetadataStore, RepositoryBehavior, RepositoryError, RuntimeError, UserContext,
};

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
    pub(crate) dialect: &'a D,
    pub(crate) executor: &'a E,
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

pub(crate) struct UserContextMetadata<'a> {
    pub(crate) context: &'a UserContext,
}

impl MetadataStore for UserContextMetadata<'_> {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.context.entity(name)
    }

    fn all_entities(&self) -> Vec<&EntityDescriptor> {
        self.context
            .metadata
            .as_ref()
            .map(|metadata| metadata.all_entities())
            .unwrap_or_default()
    }
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

    pub fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        self.fetch_all(query).map(SmartList::from)
    }

    pub fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_all(query)?
            .into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_entities(query)
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
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
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
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
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
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
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

    pub fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        self.repository().fetch_smart_list(query)
    }

    pub fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.repository().fetch_entities(query)
    }

    pub fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.repository().fetch_enhanced_entities(query)
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
        self.repository
            .metadata
            .context
            .repository_behavior(&self.entity)
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

    pub fn prepare_insert_command(
        &self,
        command: &InsertCommand,
    ) -> Result<InsertCommand, RuntimeError> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior.before_insert(self.repository.metadata.context, &mut command)?;
        }

        let entity = self
            .repository
            .metadata
            .context
            .require_entity(&command.entity)?;
        if let Some(id_property) = entity.id_property() {
            if !command.values.contains_key(&id_property.name) {
                let id = self.repository.metadata.context.next_id(&command.entity)?;
                command
                    .values
                    .insert(id_property.name.clone(), Value::U64(id));
            }
        }

        Ok(command)
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

    pub fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        let mut query = query.clone();
        if let Some(behavior) = self.query_behavior(&query.entity) {
            behavior
                .before_select(self.repository.metadata.context, &mut query)
                .map_err(RepositoryError::Runtime)?;
        }
        self.repository.fetch_smart_list(&query)
    }

    pub fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let mut query = query.clone();
        if let Some(behavior) = self.query_behavior(&query.entity) {
            behavior
                .before_select(self.repository.metadata.context, &mut query)
                .map_err(RepositoryError::Runtime)?;
        }
        self.repository.fetch_entities(&query)
    }

    pub fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let mut query = query.clone();
        if let Some(behavior) = self.query_behavior(&query.entity) {
            behavior
                .before_select(self.repository.metadata.context, &mut query)
                .map_err(RepositoryError::Runtime)?;
        }

        let mut rows = self.repository.fetch_all(&query)?;
        self.enhance_relations(&mut rows)?;
        rows.into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let command = self
            .prepare_insert_command(command)
            .map_err(RepositoryError::Runtime)?;
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
                let children =
                    child_repo.build_relation_plans(&relation.target_entity, &child_loads)?;
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

    fn query_for_plan(&self, plan: &RelationLoadPlan, parent_rows: &[Record]) -> SelectQuery {
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
        Value::U64(v) => format!("u:{v}"),
        Value::F64(v) => format!("f:{v}"),
        Value::Text(v) => format!("t:{v}"),
        Value::Json(v) => format!("j:{v}"),
        Value::Date(v) => format!("d:{v}"),
        Value::Timestamp(v) => format!("ts:{}", v.to_rfc3339()),
        Value::Object(_) => "o".to_owned(),
        Value::List(_) => "l".to_owned(),
    }
}
