use std::collections::BTreeMap;

use teaql_core::{
    DeleteCommand, Entity, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record,
    SelectQuery, UpdateCommand, Value,
};
use teaql_sql::SqlDialect;

use crate::{
    CommentTrack, GraphMutationKind, GraphMutationPlan, GraphNode, GraphOperation, RepositoryError,
    RuntimeError, ScopedCommentNode, sorted_update_fields,
};
use crate::entity_status::EntityStatus;

use super::{GraphTransactionBoundary, QueryExecutor, ResolvedRepository, helpers::*};

impl<'a, D, E> ResolvedRepository<'a, D, E>
where
    D: SqlDialect,
    E: QueryExecutor,
{
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
        let result = self.upsert_graph_node_scoped(node, None);
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

    pub fn save_entity<T>(&self, entity: T, status: EntityStatus) -> Result<GraphNode, RepositoryError<E::Error>>
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
            self.save_graph(node)
        } else {
            self.save_entity_graph(entity)
        }
    }
    pub fn save_entity_with_comment<T>(&self, entity: T, status: EntityStatus, comment: impl Into<String>) -> Result<GraphNode, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        if status.is_deleted() {
            let mut node = self.graph_node_from_entity(entity)
                .map_err(RepositoryError::Runtime)?;
            node.operation = GraphOperation::Remove;
            node.relations.clear();
            node.set_comment(comment);
            self.save_graph(node)
        } else {
            self.save_entity_graph_with_comment(entity, comment)
        }
    }
    pub fn save_entity_graph_with_comment<T>(
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
        self.save_graph(node)
    }

    /// Create a new entity graph with an annotation comment on the root node.
    /// This assumes all new nodes do not exist in the database, skipping existence checks
    /// and throwing an exception on primary key conflict.
    pub fn create_entity_graph_with_comment<T>(
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
        self.save_graph(node)
    }

    pub fn plan_graph(
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
        self.collect_graph_plan(&mut node, &mut plan, None, false)?;
        plan.planned_root = Some(node);
        plan.rebuild_batches();
        Ok(plan)
    }

    pub fn execute_graph_plan(
        &self,
        plan: GraphMutationPlan,
    ) -> Result<GraphNode, RepositoryError<E::Error>> {
        let Some(root) = plan.planned_root else {
            return Err(RepositoryError::Runtime(RuntimeError::Graph(
                "graph mutation plan has no planned root".to_owned(),
            )));
        };

        self.upsert_graph_node_scoped(root, None)
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

    fn collect_graph_plan<'s>(
        &self,
        node: &mut GraphNode,
        plan: &mut GraphMutationPlan,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
        parent_is_create: bool,
    ) -> Result<(), RepositoryError<E::Error>> {
        match node.operation {
            GraphOperation::Reference => {
                plan.push(
                    node.entity.clone(),
                    GraphMutationKind::Reference,
                    node.values.clone(),
                    Vec::new(),
                );
                return Ok(());
            }
            GraphOperation::Remove => {
                plan.push(
                    node.entity.clone(),
                    GraphMutationKind::Delete,
                    node.values.clone(),
                    Vec::new(),
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
            track: CommentTrack {
                entity_type: node.entity.clone(),
                entity_id: node
                    .id()
                    .map(|v| match v {
                        Value::U64(n) => n.to_string(),
                        Value::I64(n) => n.to_string(),
                        Value::Text(s) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .unwrap_or_else(|| "(pending)".into()),
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
                    .fetch_graph_current_row(&node.entity, &id_property.name, id, active_scope.map(|s| s.to_lineage_string()))?
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
        plan.push(
            node.entity.clone(),
            if is_update {
                GraphMutationKind::Update
            } else {
                GraphMutationKind::Create
            },
            node.values.clone(),
            update_fields,
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
                child_repo.collect_graph_plan(child, plan, active_scope, is_create_op)?;
            }
        }
        Ok(())
    }

    fn insert_graph_node_scoped<'s>(
        &self,
        mut node: GraphNode,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
    ) -> Result<GraphNode, RepositoryError<E::Error>> {
        match node.operation {
            GraphOperation::Upsert | GraphOperation::Create => {}
            GraphOperation::Reference => return self.validate_reference_node(node, parent_scope.map(|s| s.to_lineage_string())),
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
            track: CommentTrack {
                entity_type: node.entity.clone(),
                entity_id: node
                    .id()
                    .map(|v| match v {
                        Value::U64(n) => n.to_string(),
                        Value::I64(n) => n.to_string(),
                        Value::Text(s) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .unwrap_or_else(|| "(pending)".into()),
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
                let saved_child = child_repo.insert_graph_node_scoped(child, active_scope)?;
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
        let lineage = active_scope.map(|s| s.to_lineage_string());
        self.execute_prepared_insert_with_comment(command.clone(), lineage)?;
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
                saved_children.push(child_repo.insert_graph_node_scoped(child, active_scope)?);
            }
            node.relations.insert(name, saved_children);
        }

        Ok(node)
    }

    fn upsert_graph_node_scoped<'s>(
        &self,
        mut node: GraphNode,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
    ) -> Result<GraphNode, RepositoryError<E::Error>> {
        // Create scope node on the current stack frame if this node has a comment
        let current_scope = node.comment.as_ref().map(|c| ScopedCommentNode {
            parent: parent_scope,
            track: CommentTrack {
                entity_type: node.entity.clone(),
                entity_id: node
                    .id()
                    .map(|v| match v {
                        Value::U64(n) => n.to_string(),
                        Value::I64(n) => n.to_string(),
                        Value::Text(s) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .unwrap_or_else(|| "(pending)".into()),
                comment: c.clone(),
            },
        });
        let active_scope = current_scope.as_ref().or(parent_scope);

        match node.operation {
            GraphOperation::Upsert | GraphOperation::Create => {}
            GraphOperation::Reference => return self.validate_reference_node(node, active_scope.map(|s| s.to_lineage_string())),
            GraphOperation::Remove => {
                self.validate_remove_node(&node, active_scope.map(|s| s.to_lineage_string()))?;
                self.delete_graph_node(&node, parent_scope)?;
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
            return self.insert_graph_node_scoped(node, active_scope);
        };

        if node.operation == GraphOperation::Create || self
            .fetch_graph_current_row(&node.entity, &id_property.name, &id, active_scope.map(|s| s.to_lineage_string()))?
            .is_none()
        {
            node.comment = None;
            return self.insert_graph_node_scoped(node, active_scope);
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
                let saved_child = child_repo.upsert_graph_node_scoped(child, active_scope)?;
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
            let lineage = active_scope.map(|s| s.to_lineage_string());
            self.execute_prepared_update_with_comment(prepared_update.clone(), lineage)?;
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
                active_scope.map(|s| s.to_lineage_string()),
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
                saved_children.push(child_repo.upsert_graph_node_scoped(child, active_scope)?);
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
                    let lineage = active_scope.map(|s| s.to_lineage_string());
                    child_repo.delete_scoped(&delete, lineage)?;
                }
            }

            node.relations.insert(name, saved_children);
        }

        Ok(node)
    }

    fn validate_reference_node(
        &self,
        node: GraphNode,
        lineage: Option<String>,
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
            .fetch_graph_current_row(&node.entity, &id_property.name, &id, lineage)?
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
        })
    }

    fn validate_remove_node(&self, node: &GraphNode, lineage: Option<String>) -> Result<(), RepositoryError<E::Error>> {
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
            .fetch_graph_current_row(&node.entity, &id_property.name, &id, lineage)?
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

    fn delete_graph_node<'s>(
        &self,
        node: &GraphNode,
        parent_scope: Option<&'s ScopedCommentNode<'s>>,
    ) -> Result<u64, RepositoryError<E::Error>> {
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
            track: CommentTrack {
                entity_type: node.entity.clone(),
                entity_id: node
                    .id()
                    .map(|v| match v {
                        Value::U64(n) => n.to_string(),
                        Value::I64(n) => n.to_string(),
                        Value::Text(s) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .unwrap_or_else(|| "(pending)".into()),
                comment: c.clone(),
            },
        });
        let active_scope = current_scope.as_ref().or(parent_scope);
        let lineage = active_scope.map(|s| s.to_lineage_string());

        self.delete_scoped(&delete, lineage)
    }

    fn fetch_graph_current_row(
        &self,
        entity: &str,
        id_property: &str,
        id: &Value,
        lineage: Option<String>,
    ) -> Result<Option<Record>, RepositoryError<E::Error>> {
        let mut query = SelectQuery::new(entity).filter(Expr::eq(id_property, id.clone()));
        if let Some(lineage) = lineage {
            query = query.comment(lineage);
        }
        let mut rows = self
            .scoped_repository(entity.to_owned())
            .fetch_all(&query)?;
        Ok(rows.pop())
    }

    fn fetch_graph_children(
        &self,
        entity: &str,
        foreign_key: &str,
        parent_value: &Value,
        lineage: Option<String>,
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let mut query = SelectQuery::new(entity).filter(Expr::eq(foreign_key, parent_value.clone()));
        if let Some(lineage) = lineage {
            query = query.comment(lineage);
        }
        self.scoped_repository(entity.to_owned()).fetch_all(&query)
    }
}
