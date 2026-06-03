use std::collections::BTreeMap;
use std::sync::Arc;

use teaql_core::{
    DeleteCommand, Entity, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record,
    SelectQuery, UpdateCommand, Value,
};

use crate::{
    GraphMutationKind, GraphMutationPlan, GraphNode, GraphOperation, RepositoryError,
    RuntimeError, ScopedCommentNode, TraceScopeToken, sorted_update_fields,
};
use crate::entity_status::EntityStatus;

use super::{ResolvedRepository, helpers::*};

impl<'a, E> ResolvedRepository<'a, E>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync + 'static,
{
    pub async fn save_graph(&self, node: GraphNode) -> Result<GraphNode, RepositoryError<E::Error>> {
        if node.entity != self.entity {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "resolved repository {} cannot save graph root {}",
                self.entity, node.entity
            ))));
        }
        self.upsert_graph_node_scoped(node, None).await
    }

    pub async fn save_entity_graph_from(&self, graph: teaql_core::EntityGraph) -> Result<GraphNode, RepositoryError<E::Error>> {
        fn convert(node: teaql_core::EntityGraphNode) -> GraphNode {
            let mut relations = BTreeMap::new();
            for (rel_name, child) in node.children {
                relations.entry(rel_name).or_insert_with(Vec::new).push(convert(child));
            }
            GraphNode {
                entity: node.entity_type,
                values: node.record,
                relations,
                operation: match node.operation {
                    teaql_core::EntityGraphOperation::Save => crate::GraphOperation::Upsert,
                    teaql_core::EntityGraphOperation::Delete => crate::GraphOperation::Remove,
                },
                comment: node.comment,
                dirty_fields: None, original_values: None,
            }
        }
        self.save_graph(convert(graph.root)).await
    }

    pub async fn save_entity_graph<T>(&self, entity: T) -> Result<GraphNode, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let node = self
            .graph_node_from_entity(entity)
            .map_err(RepositoryError::Runtime)?;
        self.save_graph(node).await
    }

    pub async fn save_entity<T>(&self, entity: T, status: EntityStatus) -> Result<GraphNode, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        if !status.need_persist() {
            return Ok(GraphNode::new(&self.entity));
        }
        if status.is_deleted() {
            let mut node = self.graph_node_from_entity(entity)
                .map_err(RepositoryError::Runtime)?;
            node.operation = GraphOperation::Remove;
            node.relations.clear();
            self.save_graph(node).await
        } else {
            self.save_entity_graph(entity).await
        }
    }
    pub async fn save_entity_with_comment<T>(&self, entity: T, status: EntityStatus, comment: impl Into<String>) -> Result<GraphNode, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        if status.is_deleted() {
            let mut node = self.graph_node_from_entity(entity)
                .map_err(RepositoryError::Runtime)?;
            node.operation = GraphOperation::Remove;
            node.relations.clear();
            node.set_comment(comment);
            self.save_graph(node).await
        } else {
            self.save_entity_graph_with_comment(entity, comment).await
        }
    }
    pub async fn save_entity_graph_with_comment<T>(
        &self,
        entity: T,
        comment: impl Into<String>,
    ) -> Result<GraphNode, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let mut node = self
            .graph_node_from_entity(entity)
            .map_err(RepositoryError::Runtime)?;
        node.set_comment(comment);
        self.save_graph(node).await
    }

    /// Create a new entity graph with an annotation comment on the root node.
    /// This assumes all new nodes do not exist in the database, skipping existence checks
    /// and throwing an exception on primary key conflict.
    pub async fn create_entity_graph_with_comment<T>(
        &self,
        entity: T,
        comment: impl Into<String>,
    ) -> Result<GraphNode, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let mut node = self
            .graph_node_from_entity(entity)
            .map_err(RepositoryError::Runtime)?;
        node.operation = GraphOperation::Create;
        node.set_comment(comment);
        self.save_graph(node).await
    }

    pub async fn plan_graph(
        &self,
        node: GraphNode,
    ) -> Result<GraphMutationPlan, RepositoryError<E::Error>> {
        if node.entity != self.entity {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                "resolved repository {} cannot plan graph root {}",
                self.entity, node.entity
            ))));
        }
        let mut node = node;
        let mut plan = GraphMutationPlan::default();
        self.collect_graph_plan(&mut node, &mut plan, None, None, false).await?;
        plan.planned_root = Some(node);
        plan.rebuild_batches();
        Ok(plan)
    }

    pub async fn execute_graph_plan(
        &self,
        plan: GraphMutationPlan,
    ) -> Result<GraphNode, RepositoryError<E::Error>> {
        let Some(root) = plan.planned_root else {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(
                "graph mutation plan has no planned root".to_owned(),
            )));
        };

        for batch in plan.batches {
            if batch.items.is_empty() {
                continue;
            }
            match batch.kind {
                GraphMutationKind::Create => {
                    let mut cmd = teaql_core::BatchInsertCommand::new(&batch.entity);
                    for item in batch.items {
                        cmd.batch_values.push(item.values);
                        if let Some(token) = item.scope_token {
                            cmd.trace_chains.push(token.recover_trace_chain());
                        } else {
                            cmd.trace_chains.push(Vec::new());
                        }
                    }
                    self.execute_prepared_batch_insert(cmd).await?;
                }
                GraphMutationKind::Update => {
                    let mut cmd = teaql_core::BatchUpdateCommand::new(&batch.entity, batch.update_fields);
                    for item in batch.items {
                        let id = item.values.get("id").cloned().ok_or_else(|| {
                            RepositoryError::Runtime(RuntimeError::Graph(format!(
                                "update item in batch missing id for {}", batch.entity
                            )))
                        })?;
                        let version = item.values.get("version").and_then(|v| {
                            if let teaql_core::Value::I64(n) = v { Some(*n) } else { None }
                        });
                        cmd.batch_values.push(item.values);
                        cmd.batch_ids.push(id);
                        cmd.batch_expected_versions.push(version);
                        cmd.batch_old_values.push(item.old_values);
                        if let Some(token) = item.scope_token {
                            cmd.trace_chains.push(token.recover_trace_chain());
                        } else {
                            cmd.trace_chains.push(Vec::new());
                        }
                    }
                    self.execute_prepared_batch_update(cmd).await?;
                }
                GraphMutationKind::Delete => {
                    // For now, loop individually since we lack BatchDeleteCommand
                    for item in batch.items {
                        let id = item.values.get("id").cloned().ok_or_else(|| {
                            RepositoryError::Runtime(RuntimeError::Graph(format!(
                                "delete item in batch missing id for {}", batch.entity
                            )))
                        })?;
                        let mut cmd = teaql_core::DeleteCommand::new(&batch.entity, id);
                        if let Some(teaql_core::Value::I64(version)) = item.values.get("version") {
                            cmd = cmd.expected_version(*version);
                        }
                        let trace_chain = if let Some(token) = item.scope_token { token.recover_trace_chain() } else { Vec::new() };
                        self.delete_scoped(&cmd, trace_chain).await?;
                    }
                }
                GraphMutationKind::Reference => {
                    // References are skipped in execution, they only validate during traversal
                }
            }
        }

        Ok(root)
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
        // Extract dirty field names BEFORE into_record() consumes the entity.
        // This is the Rust equivalent of Java's entity.getUpdatedProperties().
        let dirty_fields = entity.dirty_fields();
        let original_values = entity.original_values();
        let is_deleted = entity.is_marked_as_delete();
        let comment = entity.comment();
        let mut node = self.graph_node_from_record(&descriptor.name, entity.into_record())?;
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

    fn collect_graph_plan<'b, 's: 'b>(
        &'b self,
        node: &'b mut GraphNode,
        plan: &'b mut GraphMutationPlan,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
        parent_token: Option<Arc<TraceScopeToken>>,
        parent_is_create: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), RepositoryError<E::Error>>> + Send + '_>> {
        Box::pin(async move {
        match node.operation {
            GraphOperation::Reference => {
                plan.push(
                    node.entity.clone(),
                    GraphMutationKind::Reference,
                    node.values.clone(),
                    Vec::new(),
                    parent_token,
                    node.original_values.clone(),
                );
                return Ok(());
            }
            GraphOperation::Remove => {
                plan.push(
                    node.entity.clone(),
                    GraphMutationKind::Delete,
                    node.values.clone(),
                    Vec::new(),
                    parent_token,
                    node.original_values.clone(),
                );
                return Ok(());
            }
            GraphOperation::Upsert | GraphOperation::Create => {}
        }

        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(&node.entity)
            .map_err(RepositoryError::Runtime)?;

        // Create scope node on the current stack frame if this node has a comment
        let current_scope = node.comment.as_ref().map(|c| ScopedCommentNode {
            parent: parent_scope,
            track: teaql_core::TraceNode {
                entity_type: node.entity.clone(),
                entity_id: node.id().and_then(|v| match v {
                    Value::U64(n) => Some(*n),
                    Value::I64(n) => Some(*n as u64),
                    _ => None,
                }),
                comment: c.clone(),
            },
        });
        let active_scope = current_scope.as_ref().or(parent_scope);

        let id_property = descriptor.id_property().cloned();
        let id = id_property.as_ref().and_then(|property| {
            node.values
                .get(&property.name)
                .filter(|value| !is_unassigned_id_value(value))
                .cloned()
        });

        let is_create_op = node.operation == GraphOperation::Create || (parent_is_create && node.operation == GraphOperation::Upsert);

        let is_update = if is_create_op {
            false
        } else {
            match (id_property.as_ref(), id.as_ref()) {
                (Some(id_property), Some(id)) => self
                    .fetch_graph_current_row(&node.entity, &id_property.name, id, active_scope.map(|s| s.to_trace_chain()).unwrap_or_default()).await?
                    .is_some(),
                _ => false,
            }
        };
        if !is_update {
            if let Some(id_property) = id_property.as_ref() {
                let needs_id = !node.values.contains_key(&id_property.name)
                    || node
                        .values
                        .get(&id_property.name)
                        .is_some_and(is_unassigned_id_value);
                if needs_id {
                    let id = self
                        .repository
                        .metadata
                        .context
                        .next_id(&node.entity)
                        .map_err(RepositoryError::Runtime)?;
                    node.values.insert(id_property.name.clone(), Value::U64(id));
                }
            }
            ensure_initial_version(&mut node.values, descriptor);
        }
        let update_fields = if is_update {
            let mut excluded = Vec::new();
            if let Some(id_property) = id_property.as_ref() {
                excluded.push(id_property.name.clone());
            }
            if let Some(version_property) = descriptor.version_property() {
                excluded.push(version_property.name.clone());
            }
            sorted_update_fields(&node.values, excluded)
        } else {
            Vec::new()
        };

        // Build the TraceScopeToken for this node (only if it has a comment).
        // This is an Arc-linked persistent list: zero-copy, O(1) creation.
        let current_token = if let Some(c) = &node.comment {
            Some(Arc::new(TraceScopeToken {
                parent: parent_token.clone(),
                track: teaql_core::TraceNode {
                    entity_type: node.entity.clone(),
                    entity_id: node.id().and_then(|v| match v {
                        Value::U64(n) => Some(*n),
                        Value::I64(n) => Some(*n as u64),
                        _ => None,
                    }),
                    comment: c.clone(),
                },
                node_index: plan.next_item_index,
            }))
        } else {
            parent_token.clone()
        };

        plan.push(
            node.entity.clone(),
            if is_update {
                GraphMutationKind::Update
            } else {
                GraphMutationKind::Create
            },
            node.values.clone(),
            update_fields,
            current_token.clone(),
            node.original_values.clone(),
        );

        for (name, children) in &mut node.relations {
            let relation = descriptor.relation_by_name(name).ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::MissingRelation {
                    entity: node.entity.clone(),
                    relation: name.clone(),
                })
            })?;
            let child_repo = self.scoped_repository(relation.target_entity.clone());
            for child in children {
                ensure_relation_target(&node.entity, name, &relation.target_entity, child)?;
                child_repo.collect_graph_plan(child, plan, active_scope, current_token.clone(), is_create_op).await?;
            }
        }
        Ok(())
        })
    }

    fn insert_graph_node_scoped<'b, 's: 'b>(
        &'b self,
        mut node: GraphNode,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GraphNode, RepositoryError<E::Error>>> + Send + '_>> {
        Box::pin(async move {
        match node.operation {
            GraphOperation::Upsert | GraphOperation::Create => {}
            GraphOperation::Reference => return self.validate_reference_node(node, parent_scope.map(|s| s.to_trace_chain()).unwrap_or_default()).await,
            GraphOperation::Remove => {
                return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "create graph cannot remove node {}",
                    node.entity
                ))));
            }
        }

        // Create scope node on the current stack frame if this node has a comment
        let current_scope = node.comment.as_ref().map(|c| ScopedCommentNode {
            parent: parent_scope,
            track: teaql_core::TraceNode {
                entity_type: node.entity.clone(),
                entity_id: node
                    .id()
                    .and_then(|v| match v {
                        Value::U64(n) => Some(*n),
                        Value::I64(n) => Some(*n as u64),
                        _ => None,
                    }),
                comment: c.clone(),
            },
        });
        let active_scope = current_scope.as_ref().or(parent_scope);

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
                let saved_child = child_repo.insert_graph_node_scoped(child, active_scope).await?;
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
                trace_chain: Vec::new(),
            })
            .map_err(RepositoryError::Runtime)?;
        let lineage = active_scope.map(|s| s.to_trace_chain()).unwrap_or_default();
        self.execute_prepared_insert_with_comment(command.clone(), lineage).await?;
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
                saved_children.push(child_repo.insert_graph_node_scoped(child, active_scope).await?);
            }
            node.relations.insert(name, saved_children);
        }

        Ok(node)
        })
    }

    fn upsert_graph_node_scoped<'b, 's: 'b>(
        &'b self,
        mut node: GraphNode,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GraphNode, RepositoryError<E::Error>>> + Send + '_>> {
        Box::pin(async move {
        // Create scope node on the current stack frame if this node has a comment
        let current_scope = node.comment.as_ref().map(|c| ScopedCommentNode {
            parent: parent_scope,
            track: teaql_core::TraceNode {
                entity_type: node.entity.clone(),
                entity_id: node
                    .id()
                    .and_then(|v| match v {
                        Value::U64(n) => Some(*n),
                        Value::I64(n) => Some(*n as u64),
                        _ => None,
                    }),
                comment: c.clone(),
            },
        });
        let active_scope = current_scope.as_ref().or(parent_scope);

        match node.operation {
            GraphOperation::Upsert | GraphOperation::Create => {}
            GraphOperation::Reference => return self.validate_reference_node(node, active_scope.map(|s| s.to_trace_chain()).unwrap_or_default()).await,
            GraphOperation::Remove => {
                self.validate_remove_node(&node, active_scope.map(|s| s.to_trace_chain()).unwrap_or_default()).await?;
                self.delete_graph_node(&node, parent_scope).await?;
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
            .filter(|value| !is_unassigned_id_value(value))
            .cloned()
        else {
            // Strip comment to prevent duplicate scope — already captured in active_scope
            node.comment = None;
            return self.insert_graph_node_scoped(node, active_scope).await;
        };

        if node.operation == GraphOperation::Create || self
            .fetch_graph_current_row(&node.entity, &id_property.name, &id, active_scope.map(|s| s.to_trace_chain()).unwrap_or_default()).await?
            .is_none()
        {
            node.comment = None;
            return self.insert_graph_node_scoped(node, active_scope).await;
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
                let saved_child = child_repo.upsert_graph_node_scoped(child, active_scope).await?;
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

        let update = self.graph_update_command(&mut node, descriptor, id_property, &id)?;
        if !update.values.is_empty() || update.expected_version.is_some() {
            let prepared_update = self
                .prepare_update_command(&update)
                .map_err(RepositoryError::Runtime)?;
            let lineage = active_scope.map(|s| s.to_trace_chain()).unwrap_or_default();
            self.execute_prepared_update_with_comment(prepared_update.clone(), lineage).await?;
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
            let child_descriptor = self
                .repository
                .metadata
                .context
                .require_entity(&relation.target_entity)
                .map_err(RepositoryError::Runtime)?;
            let child_id_property = child_descriptor.id_property().ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "entity {} has no id property",
                    relation.target_entity
                )))
            })?;

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
                    .filter(|value| !is_unassigned_id_value(value))
                {
                    let key = graph_identity_key(child_id);
                    if !seen.insert(key.clone()) {
                        return Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
                            "duplicate child id {key} in relation {}.{}",
                            node.entity, name
                        ))));
                    }
                }
                saved_children.push(child_repo.upsert_graph_node_scoped(child, active_scope).await?);
            }



            node.relations.insert(name, saved_children);
        }

        Ok(node)
        })
    }

    async fn validate_reference_node(
        &self,
        node: GraphNode,
        trace_chain: Vec<teaql_core::TraceNode>,
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
            .filter(|value| !is_unassigned_id_value(value))
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
            .fetch_graph_current_row(&node.entity, &id_property.name, &id, trace_chain).await?
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
            comment: None,
            dirty_fields: None, original_values: None,
        })
    }

    async fn validate_remove_node(&self, node: &GraphNode, trace_chain: Vec<teaql_core::TraceNode>) -> Result<(), RepositoryError<E::Error>> {
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
            .filter(|value| !is_unassigned_id_value(value))
            .cloned()
            .ok_or_else(|| {
                RepositoryError::Runtime(RuntimeError::Graph(format!(
                    "remove node {} missing id property {}",
                    node.entity, id_property.name
                )))
            })?;
        let current = self
            .fetch_graph_current_row(&node.entity, &id_property.name, &id, trace_chain).await?
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
            if field == "_comment" {
                if let Value::Text(comment) = value {
                    node.set_comment(comment);
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
        node: &mut GraphNode,
        descriptor: &EntityDescriptor,
        id_property: &PropertyDescriptor,
        id: &Value,
    ) -> Result<UpdateCommand, RepositoryError<E::Error>> {
        crate::mark_record_status(&mut node.values, crate::CheckObjectStatus::Update);
        let check_result = self
            .repository
            .metadata
            .context
            .check_and_fix_record(&node.entity, &mut node.values);
        crate::clear_record_status(&mut node.values);
        check_result.map_err(RepositoryError::Runtime)?;

        let mut command = UpdateCommand::new(node.entity.clone(), id.clone());
        command.old_values = node.original_values.clone();
        if let Some(version_property) = descriptor.version_property() {
            if let Some(Value::I64(version)) = node.values.get(&version_property.name) {
                command = command.expected_version(*version);
            }
        }
        // Filter properties by dirty_fields when available (Java-style minimal UPDATE).
        // When dirty_fields is Some, only modified fields are included in the SET clause.
        // When dirty_fields is None (no tracking), fall back to all fields in node.values.
        for property in descriptor.properties.iter().filter(|property| {
            !property.is_id
                && !property.is_version
                && property.name != id_property.name
                && match &node.dirty_fields {
                    Some(dirty) => dirty.contains(&property.name),
                    None => node.values.contains_key(&property.name),
                }
        }) {
            if let Some(value) = node.values.get(&property.name) {
                command.values.insert(property.name.clone(), value.clone());
            }
        }
        Ok(command)
    }

    fn delete_graph_node<'b, 's: 'b>(
        &'b self,
        node: &'b GraphNode,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, RepositoryError<E::Error>>> + Send + '_>> {
        Box::pin(async move {
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
            .filter(|value| !is_unassigned_id_value(value))
            .cloned()
            .ok_or_else(|| {
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

        // Create scope node for deletion if parent/node comment is present
        let current_scope = node.comment.as_ref().map(|c| ScopedCommentNode {
            parent: parent_scope,
            track: teaql_core::TraceNode {
                entity_type: node.entity.clone(),
                entity_id: node
                    .id()
                    .and_then(|v| match v {
                        Value::U64(n) => Some(*n),
                        Value::I64(n) => Some(*n as u64),
                        _ => None,
                    }),
                comment: c.clone(),
            },
        });
        let active_scope = current_scope.as_ref().or(parent_scope);
        let lineage = active_scope.map(|s| s.to_trace_chain()).unwrap_or_default();

        self.delete_scoped(&delete, lineage).await
        })
    }

    pub async fn fetch_graph_current_row(
        &self,
        entity: &str,
        id_property: &str,
        id: &Value,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<Option<Record>, RepositoryError<E::Error>> {
        let mut query = SelectQuery::new(entity).filter(Expr::eq(id_property, id.clone()));
        query.trace_chain = trace_chain;
        let mut rows = self
            .scoped_repository(entity.to_owned())
            .fetch_all(&query).await?;
        Ok(rows.pop())
    }

    async fn fetch_graph_children(
        &self,
        entity: &str,
        foreign_key: &str,
        parent_value: &Value,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let mut query = SelectQuery::new(entity).filter(Expr::eq(foreign_key, parent_value.clone()));
        query.trace_chain = trace_chain;
        self.scoped_repository(entity.to_owned()).fetch_all(&query).await
    }
}
