use std::collections::BTreeMap;
use std::slice;
use std::sync::Arc;

use teaql_core::{
    DeleteCommand, Entity, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record,
    RecoverCommand, SelectQuery, SmartList, UpdateCommand, Value,
};
use teaql_sql::{CompiledQuery, SqlDialect};

use crate::{
    CheckObjectStatus, ContextError, EntityEvent, GraphMutationKind, GraphMutationPlan, GraphNode,
    GraphOperation, MetadataStore, RepositoryBehavior, RepositoryError, RuntimeError, UserContext,
    clear_record_status, mark_record_status,
};

pub trait QueryExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error>;
    fn execute(&self, query: &CompiledQuery) -> Result<u64, Self::Error>;

    fn begin_transaction(&self) -> Result<GraphTransactionBoundary, Self::Error> {
        Ok(GraphTransactionBoundary::Unsupported)
    }

    fn commit_transaction(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn rollback_transaction(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphTransactionBoundary {
    Started,
    AlreadyActive,
    Unsupported,
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
            let needs_id = !command.values.contains_key(&id_property.name)
                || matches!(command.values.get(&id_property.name), Some(Value::Null));
            if needs_id {
                let id = self.repository.metadata.context.next_id(&command.entity)?;
                command
                    .values
                    .insert(id_property.name.clone(), Value::U64(id));
            }
        }
        mark_record_status(&mut command.values, CheckObjectStatus::Create);
        let check_result = self
            .repository
            .metadata
            .context
            .check_and_fix_record(&command.entity, &mut command.values);
        clear_record_status(&mut command.values);
        check_result?;

        Ok(command)
    }

    pub fn update_command(&self, id: impl Into<Value>) -> UpdateCommand {
        UpdateCommand::new(self.entity.clone(), id)
    }

    pub fn prepare_update_command(
        &self,
        command: &UpdateCommand,
    ) -> Result<UpdateCommand, RuntimeError> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior.before_update(self.repository.metadata.context, &mut command)?;
        }
        mark_record_status(&mut command.values, CheckObjectStatus::Update);
        let check_result = self
            .repository
            .metadata
            .context
            .check_and_fix_record(&command.entity, &mut command.values);
        clear_record_status(&mut command.values);
        check_result?;
        Ok(command)
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
        self.execute_prepared_insert(command)
    }

    pub fn save_graph(&self, node: GraphNode) -> Result<GraphNode, RepositoryError<E::Error>> {
        if node.entity != self.entity {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "resolved repository {} cannot save graph root {}",
                self.entity, node.entity
            ))));
        }
        let boundary = self
            .repository
            .executor
            .begin_transaction()
            .map_err(RepositoryError::Executor)?;
        if matches!(boundary, GraphTransactionBoundary::Unsupported) {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(
                "save_graph requires a transactional executor".to_owned(),
            )));
        }
        let _plan = self.plan_graph(&node)?;
        let result = self.upsert_graph_node(node);
        match result {
            Ok(saved) => {
                if matches!(boundary, GraphTransactionBoundary::Started) {
                    self.repository
                        .executor
                        .commit_transaction()
                        .map_err(RepositoryError::Executor)?;
                }
                Ok(saved)
            }
            Err(err) => {
                if !matches!(boundary, GraphTransactionBoundary::Unsupported) {
                    self.repository
                        .executor
                        .rollback_transaction()
                        .map_err(RepositoryError::Executor)?;
                }
                Err(err)
            }
        }
    }

    pub fn save_entity_graph<T>(&self, entity: T) -> Result<GraphNode, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let node = self
            .graph_node_from_entity(entity)
            .map_err(RepositoryError::Runtime)?;
        self.save_graph(node)
    }

    pub fn plan_graph(
        &self,
        node: &GraphNode,
    ) -> Result<GraphMutationPlan, RepositoryError<E::Error>> {
        if node.entity != self.entity {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "resolved repository {} cannot plan graph root {}",
                self.entity, node.entity
            ))));
        }
        let mut plan = GraphMutationPlan::default();
        self.collect_graph_plan(node, &mut plan)?;
        Ok(plan)
    }

    pub fn graph_node_from_entity<T>(&self, entity: T) -> Result<GraphNode, RuntimeError>
    where
        T: Entity,
    {
        let descriptor = T::entity_descriptor();
        if descriptor.name != self.entity {
            return Err(RuntimeError::Graph(format!(
                "resolved repository {} cannot extract graph root {}",
                self.entity, descriptor.name
            )));
        }
        self.graph_node_from_record(&descriptor.name, entity.into_record())
    }

    fn collect_graph_plan(
        &self,
        node: &GraphNode,
        plan: &mut GraphMutationPlan,
    ) -> Result<(), RepositoryError<E::Error>> {
        match node.operation {
            GraphOperation::Reference => {
                plan.push(node.entity.clone(), GraphMutationKind::Reference);
                return Ok(());
            }
            GraphOperation::Remove => {
                plan.push(node.entity.clone(), GraphMutationKind::Delete);
                return Ok(());
            }
            GraphOperation::Upsert => {}
        }

        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(&node.entity)
            .map_err(RepositoryError::Runtime)?;
        let id_property = descriptor.id_property();
        let id = id_property.and_then(|property| {
            node.values
                .get(&property.name)
                .filter(|value| !matches!(value, Value::Null))
                .cloned()
        });
        let is_update = match (id_property, id.as_ref()) {
            (Some(id_property), Some(id)) => self
                .fetch_graph_current_row(&node.entity, &id_property.name, id)?
                .is_some(),
            _ => false,
        };
        plan.push(
            node.entity.clone(),
            if is_update {
                GraphMutationKind::Update
            } else {
                GraphMutationKind::Create
            },
        );

        for (name, children) in &node.relations {
            let relation = descriptor.relation_by_name(name).ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::MissingRelation {
                    entity: node.entity.clone(),
                    relation: name.clone(),
                })
            })?;
            let child_repo = self.scoped_repository(relation.target_entity.clone());
            for child in children {
                ensure_relation_target(&node.entity, name, &relation.target_entity, child)?;
                child_repo.collect_graph_plan(child, plan)?;
            }
        }
        Ok(())
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let command = self
            .prepare_update_command(command)
            .map_err(RepositoryError::Runtime)?;
        self.execute_prepared_update(command)
    }

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_delete(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        let affected = self.repository.delete(&command)?;
        self.emit_event(EntityEvent::deleted(
            command.entity,
            command.id,
            command.expected_version,
        ))
        .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_recover(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        let affected = self.repository.recover(&command)?;
        self.emit_event(EntityEvent::recovered(
            command.entity,
            command.id,
            command.expected_version,
        ))
        .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    fn emit_event(&self, event: EntityEvent) -> Result<(), RuntimeError> {
        self.repository.metadata.context.send_event(event)
    }

    fn execute_prepared_insert(
        &self,
        command: InsertCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository.insert(&command)?;
        self.emit_event(EntityEvent::created(command.entity, command.values))
            .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    fn execute_prepared_update(
        &self,
        command: UpdateCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository.update(&command)?;
        let updated_fields = command.values.keys().cloned().collect();
        let mut values = command.values.clone();
        values.insert("id".to_owned(), command.id.clone());
        if let Some(version) = command.expected_version {
            values.insert("version".to_owned(), Value::I64(version + 1));
        }
        self.emit_event(EntityEvent {
            kind: crate::EntityEventKind::Updated,
            entity: command.entity,
            values,
            updated_fields,
        })
        .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    fn insert_graph_node(
        &self,
        mut node: GraphNode,
    ) -> Result<GraphNode, RepositoryError<E::Error>> {
        match node.operation {
            GraphOperation::Upsert => {}
            GraphOperation::Reference => return self.validate_reference_node(node),
            GraphOperation::Remove => {
                return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "create graph cannot remove node {}",
                    node.entity
                ))));
            }
        }

        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(&node.entity)
            .map_err(RepositoryError::Runtime)?;

        let mut one_relations = Vec::new();
        let mut many_relations = Vec::new();
        for (name, children) in std::mem::take(&mut node.relations) {
            let relation = descriptor.relation_by_name(&name).ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::MissingRelation {
                    entity: node.entity.clone(),
                    relation: name.clone(),
                })
            })?;
            if relation.many {
                many_relations.push((name, relation.clone(), children));
            } else {
                one_relations.push((name, relation.clone(), children));
            }
        }

        for (name, relation, children) in one_relations {
            if children.len() > 1 {
                return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "relation {}.{} expects one child, got {}",
                    node.entity,
                    name,
                    children.len()
                ))));
            }
            let mut saved_children = Vec::new();
            for child in children {
                ensure_relation_target(&node.entity, &name, &relation.target_entity, &child)?;
                let child_repo = self.scoped_repository(child.entity.clone());
                let saved_child = child_repo.insert_graph_node(child)?;
                if relation.attach {
                    let foreign_value = saved_child
                        .values
                        .get(&relation.foreign_key)
                        .cloned()
                        .ok_or_else(|| {
                            RepositoryError::Runtime(RuntimeError::Graph(format!(
                                "saved child {} missing foreign key {} for relation {}.{}",
                                relation.target_entity, relation.foreign_key, node.entity, name
                            )))
                        })?;
                    node.values
                        .insert(relation.local_key.clone(), foreign_value);
                }
                saved_children.push(saved_child);
            }
            node.relations.insert(name, saved_children);
        }

        let command = self
            .prepare_insert_command(&InsertCommand {
                entity: node.entity.clone(),
                values: node.values.clone(),
            })
            .map_err(RepositoryError::Runtime)?;
        self.execute_prepared_insert(command.clone())?;
        node.values = command.values;

        for (name, relation, children) in many_relations {
            let local_value = node
                .values
                .get(&relation.local_key)
                .cloned()
                .ok_or_else(|| {
                    RepositoryError::Runtime(RuntimeError::Graph(format!(
                        "parent {} missing local key {} for relation {}",
                        node.entity, relation.local_key, name
                    )))
                })?;
            let mut saved_children = Vec::new();
            for mut child in children {
                ensure_relation_target(&node.entity, &name, &relation.target_entity, &child)?;
                if relation.attach {
                    child
                        .values
                        .insert(relation.foreign_key.clone(), local_value.clone());
                }
                let child_repo = self.scoped_repository(child.entity.clone());
                saved_children.push(child_repo.insert_graph_node(child)?);
            }
            node.relations.insert(name, saved_children);
        }

        Ok(node)
    }

    fn upsert_graph_node(
        &self,
        mut node: GraphNode,
    ) -> Result<GraphNode, RepositoryError<E::Error>> {
        match node.operation {
            GraphOperation::Upsert => {}
            GraphOperation::Reference => return self.validate_reference_node(node),
            GraphOperation::Remove => {
                self.validate_remove_node(&node)?;
                self.delete_graph_node(&node)?;
                return Ok(node);
            }
        }

        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(&node.entity)
            .map_err(RepositoryError::Runtime)?;
        let Some(id_property) = descriptor.id_property() else {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "entity {} has no id property for graph upsert",
                node.entity
            ))));
        };
        let Some(id) = node
            .values
            .get(&id_property.name)
            .filter(|value| !matches!(value, Value::Null))
            .cloned()
        else {
            return self.insert_graph_node(node);
        };

        if self
            .fetch_graph_current_row(&node.entity, &id_property.name, &id)?
            .is_none()
        {
            return self.insert_graph_node(node);
        }

        let mut one_relations = Vec::new();
        let mut many_relations = Vec::new();
        for (name, children) in std::mem::take(&mut node.relations) {
            let relation = descriptor.relation_by_name(&name).ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::MissingRelation {
                    entity: node.entity.clone(),
                    relation: name.clone(),
                })
            })?;
            if relation.many {
                many_relations.push((name, relation.clone(), children));
            } else {
                one_relations.push((name, relation.clone(), children));
            }
        }

        for (name, relation, children) in one_relations {
            if children.len() > 1 {
                return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "relation {}.{} expects one child, got {}",
                    node.entity,
                    name,
                    children.len()
                ))));
            }
            let mut saved_children = Vec::new();
            for child in children {
                ensure_relation_target(&node.entity, &name, &relation.target_entity, &child)?;
                let child_repo = self.scoped_repository(child.entity.clone());
                let saved_child = child_repo.upsert_graph_node(child)?;
                if relation.attach {
                    let foreign_value = saved_child
                        .values
                        .get(&relation.foreign_key)
                        .cloned()
                        .ok_or_else(|| {
                            RepositoryError::Runtime(RuntimeError::Graph(format!(
                                "saved child {} missing foreign key {} for relation {}.{}",
                                relation.target_entity, relation.foreign_key, node.entity, name
                            )))
                        })?;
                    node.values
                        .insert(relation.local_key.clone(), foreign_value);
                }
                saved_children.push(saved_child);
            }
            node.relations.insert(name, saved_children);
        }

        let update = self.graph_update_command(&node, descriptor, id_property, &id);
        if !update.values.is_empty() || update.expected_version.is_some() {
            let prepared_update = self
                .prepare_update_command(&update)
                .map_err(RepositoryError::Runtime)?;
            self.execute_prepared_update(prepared_update.clone())?;
            for (field, value) in &prepared_update.values {
                node.values.insert(field.clone(), value.clone());
            }
            if let Some(version_property) = descriptor.version_property() {
                if let Some(expected_version) = prepared_update.expected_version {
                    node.values.insert(
                        version_property.name.clone(),
                        Value::I64(expected_version + 1),
                    );
                }
            }
        }

        for (name, relation, children) in many_relations {
            let local_value = node
                .values
                .get(&relation.local_key)
                .cloned()
                .ok_or_else(|| {
                    RepositoryError::Runtime(RuntimeError::Graph(format!(
                        "parent {} missing local key {} for relation {}",
                        node.entity, relation.local_key, name
                    )))
                })?;
            let child_repo = self.scoped_repository(relation.target_entity.clone());
            let existing_children = child_repo.fetch_graph_children(
                &relation.target_entity,
                &relation.foreign_key,
                &local_value,
            )?;
            let child_descriptor = self
                .repository
                .metadata
                .context
                .require_entity(&relation.target_entity)
                .map_err(RepositoryError::Runtime)?;
            let child_id_property = child_descriptor.id_property().ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "entity {} has no id property for graph diff",
                    relation.target_entity
                )))
            })?;
            let mut existing_by_id = BTreeMap::new();
            for child in existing_children {
                if let Some(id) = child.get(&child_id_property.name) {
                    existing_by_id.insert(graph_identity_key(id), child);
                }
            }

            let mut seen = std::collections::BTreeSet::new();
            let mut saved_children = Vec::new();
            for mut child in children {
                ensure_relation_target(&node.entity, &name, &relation.target_entity, &child)?;
                if relation.attach && child.operation != GraphOperation::Reference {
                    child
                        .values
                        .insert(relation.foreign_key.clone(), local_value.clone());
                }
                if let Some(child_id) = child
                    .values
                    .get(&child_id_property.name)
                    .filter(|value| !matches!(value, Value::Null))
                {
                    let key = graph_identity_key(child_id);
                    if !seen.insert(key.clone()) {
                        return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                            "duplicate child id {key} in relation {}.{}",
                            node.entity, name
                        ))));
                    }
                }
                saved_children.push(child_repo.upsert_graph_node(child)?);
            }

            if relation.delete_missing {
                for (id_key, existing) in existing_by_id {
                    if seen.contains(&id_key) {
                        continue;
                    }
                    let Some(existing_id) = existing.get(&child_id_property.name).cloned() else {
                        continue;
                    };
                    let mut delete =
                        DeleteCommand::new(relation.target_entity.clone(), existing_id);
                    if let Some(version) = graph_record_version(&existing, child_descriptor) {
                        delete = delete.expected_version(version);
                    }
                    child_repo.delete(&delete)?;
                }
            }

            node.relations.insert(name, saved_children);
        }

        Ok(node)
    }

    fn validate_reference_node(
        &self,
        node: GraphNode,
    ) -> Result<GraphNode, RepositoryError<E::Error>> {
        if !node.relations.is_empty() {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "reference node {} cannot contain child relations",
                node.entity
            ))));
        }
        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(&node.entity)
            .map_err(RepositoryError::Runtime)?;
        let id_property = descriptor.id_property().ok_or_else(|| {
            RepositoryError::Runtime(RuntimeError::Graph(format!(
                "entity {} has no id property for graph reference",
                node.entity
            )))
        })?;
        let id = node
            .values
            .get(&id_property.name)
            .filter(|value| !matches!(value, Value::Null))
            .cloned()
            .ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "reference node {} missing id property {}",
                    node.entity, id_property.name
                )))
            })?;

        for field in node.values.keys() {
            if field == &id_property.name {
                continue;
            }
            if descriptor
                .version_property()
                .map(|property| field == &property.name)
                .unwrap_or(false)
            {
                continue;
            }
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "reference node {} cannot carry mutable field {}",
                node.entity, field
            ))));
        }

        let current = self
            .fetch_graph_current_row(&node.entity, &id_property.name, &id)?
            .ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "reference node {}({}) does not exist",
                    node.entity,
                    graph_identity_key(&id)
                )))
            })?;

        if let Some(version_property) = descriptor.version_property() {
            if let Some(Value::I64(existing_version)) = current.get(&version_property.name) {
                if *existing_version < 0 {
                    return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                        "reference node {}({}) is deleted",
                        node.entity,
                        graph_identity_key(&id)
                    ))));
                }
                if let Some(Value::I64(expected_version)) = node.values.get(&version_property.name)
                {
                    if expected_version != existing_version {
                        return Err(RepositoryError::Runtime(
                            RuntimeError::OptimisticLockConflict {
                                entity: node.entity,
                                id: graph_identity_key(&id),
                            },
                        ));
                    }
                }
            }
        }

        Ok(GraphNode {
            entity: node.entity,
            values: current,
            relations: BTreeMap::new(),
            operation: GraphOperation::Reference,
        })
    }

    fn validate_remove_node(&self, node: &GraphNode) -> Result<(), RepositoryError<E::Error>> {
        if !node.relations.is_empty() {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "remove node {} cannot contain child relations",
                node.entity
            ))));
        }
        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(&node.entity)
            .map_err(RepositoryError::Runtime)?;
        let id_property = descriptor.id_property().ok_or_else(|| {
            RepositoryError::Runtime(RuntimeError::Graph(format!(
                "entity {} has no id property for graph remove",
                node.entity
            )))
        })?;
        let id = node
            .values
            .get(&id_property.name)
            .filter(|value| !matches!(value, Value::Null))
            .cloned()
            .ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "remove node {} missing id property {}",
                    node.entity, id_property.name
                )))
            })?;
        let current = self
            .fetch_graph_current_row(&node.entity, &id_property.name, &id)?
            .ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "remove node {}({}) does not exist",
                    node.entity,
                    graph_identity_key(&id)
                )))
            })?;
        if let Some(version_property) = descriptor.version_property() {
            if let Some(Value::I64(existing_version)) = current.get(&version_property.name) {
                if *existing_version < 0 {
                    return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                        "remove node {}({}) is already deleted",
                        node.entity,
                        graph_identity_key(&id)
                    ))));
                }
            }
        }
        Ok(())
    }

    fn graph_node_from_record(
        &self,
        entity: &str,
        record: Record,
    ) -> Result<GraphNode, RuntimeError> {
        let descriptor = self.repository.metadata.context.require_entity(entity)?;
        let mut node = GraphNode::new(entity);

        for (field, value) in record {
            let Some(relation) = descriptor.relation_by_name(&field) else {
                node.values.insert(field, value);
                continue;
            };

            match value {
                Value::Null => {
                    node.relations.entry(field).or_default();
                }
                Value::Object(record) => {
                    let child = self.graph_node_from_record(&relation.target_entity, record)?;
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
                        children
                            .push(self.graph_node_from_record(&relation.target_entity, record)?);
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

    fn graph_update_command(
        &self,
        node: &GraphNode,
        descriptor: &EntityDescriptor,
        id_property: &PropertyDescriptor,
        id: &Value,
    ) -> UpdateCommand {
        let mut command = UpdateCommand::new(node.entity.clone(), id.clone());
        if let Some(version_property) = descriptor.version_property() {
            if let Some(Value::I64(version)) = node.values.get(&version_property.name) {
                command = command.expected_version(*version);
            }
        }
        for property in descriptor.properties.iter().filter(|property| {
            !property.is_id && !property.is_version && node.values.contains_key(&property.name)
        }) {
            if property.name == id_property.name {
                continue;
            }
            if let Some(value) = node.values.get(&property.name) {
                command.values.insert(property.name.clone(), value.clone());
            }
        }
        command
    }

    fn delete_graph_node(&self, node: &GraphNode) -> Result<u64, RepositoryError<E::Error>> {
        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(&node.entity)
            .map_err(RepositoryError::Runtime)?;
        let id_property = descriptor.id_property().ok_or_else(|| {
            RepositoryError::Runtime(RuntimeError::Graph(format!(
                "entity {} has no id property for graph remove",
                node.entity
            )))
        })?;
        let id = node.values.get(&id_property.name).cloned().ok_or_else(|| {
            RepositoryError::Runtime(RuntimeError::Graph(format!(
                "remove node {} missing id property {}",
                node.entity, id_property.name
            )))
        })?;
        let mut delete = DeleteCommand::new(node.entity.clone(), id);
        if let Some(version_property) = descriptor.version_property() {
            if let Some(Value::I64(version)) = node.values.get(&version_property.name) {
                delete = delete.expected_version(*version);
            }
        }
        self.delete(&delete)
    }

    fn fetch_graph_current_row(
        &self,
        entity: &str,
        id_property: &str,
        id: &Value,
    ) -> Result<Option<Record>, RepositoryError<E::Error>> {
        let mut rows = self
            .scoped_repository(entity.to_owned())
            .fetch_all(&SelectQuery::new(entity).filter(Expr::eq(id_property, id.clone())))?;
        Ok(rows.pop())
    }

    fn fetch_graph_children(
        &self,
        entity: &str,
        foreign_key: &str,
        parent_value: &Value,
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        self.scoped_repository(entity.to_owned()).fetch_all(
            &SelectQuery::new(entity).filter(Expr::eq(foreign_key, parent_value.clone())),
        )
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
        Value::Decimal(v) => format!("d:{v}"),
        Value::Text(v) => format!("t:{v}"),
        Value::Json(v) => format!("j:{v}"),
        Value::Date(v) => format!("d:{v}"),
        Value::Timestamp(v) => format!("ts:{}", v.to_rfc3339()),
        Value::Object(_) => "o".to_owned(),
        Value::List(_) => "l".to_owned(),
    }
}

fn graph_record_version(record: &Record, descriptor: &EntityDescriptor) -> Option<i64> {
    descriptor
        .version_property()
        .and_then(|property| match record.get(&property.name) {
            Some(Value::I64(version)) => Some(*version),
            _ => None,
        })
}

fn graph_identity_key(value: &Value) -> String {
    match value {
        Value::I64(value) if *value >= 0 => format!("u:{}", *value as u64),
        Value::U64(value) => format!("u:{value}"),
        _ => relation_bucket_key(value),
    }
}

fn ensure_relation_target<ExecError>(
    parent_entity: &str,
    relation_name: &str,
    expected_entity: &str,
    child: &GraphNode,
) -> Result<(), RepositoryError<ExecError>> {
    if child.entity == expected_entity {
        return Ok(());
    }
    Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
        "relation {parent_entity}.{relation_name} expects {expected_entity}, got {}",
        child.entity
    ))))
}
