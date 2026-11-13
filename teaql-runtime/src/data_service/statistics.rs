//! Scope and authorize every statistical population before the SQL executor
//! sees a combined query. A parent layer is not an authorization-free join.
use teaql_core::{
    EntityDescriptor, Expr, QueryIntent, QueryOperation, QueryOperationKind, QueryRequestScope,
    SelectQuery,
};

use super::EntityDataService;
use crate::RuntimeError;

impl<E> EntityDataService<'_, E>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
{
    pub(super) fn prepare_statistical_layers(
        &self,
        mut root: SelectQuery,
    ) -> Result<SelectQuery, RuntimeError> {
        let mut count = 1;
        self.prepare_statistical_children(&mut root, &mut count)?;
        Ok(root)
    }

    fn prepare_statistical_children(
        &self,
        parent: &mut SelectQuery,
        count: &mut usize,
    ) -> Result<(), RuntimeError> {
        let context = self.data_service.metadata.context;
        let descriptor = context.require_entity(&parent.entity)?;
        let parent_scope = parent.request_scope().cloned().ok_or_else(|| {
            RuntimeError::Graph("statistics requires a prepared request scope".into())
        })?;
        if let Some(filter) = &mut parent.filter {
            self.prepare_statistical_memberships(filter, descriptor, &parent_scope, count, 0)?;
        }
        for group in &mut parent.object_group_bys {
            *count += 1;
            if *count > 64 {
                return Err(RuntimeError::Graph(
                    "statistical plan exceeds 64 layers".into(),
                ));
            }
            let relation = descriptor
                .relation_by_name(&group.property_name)
                .ok_or_else(|| RuntimeError::MissingRelation {
                    entity: descriptor.name.clone(),
                    relation: group.property_name.clone(),
                })?;
            let target = context.require_entity(&relation.target_entity)?;
            if relation.many
                || group.storage_field != relation.local_key
                || group.query.entity != target.name
            {
                return Err(RuntimeError::Graph(
                    "statistics requires a matching to-one relation".into(),
                ));
            }
            if descriptor.data_service != target.data_service {
                return Err(RuntimeError::Graph(
                    "statistical SQL populations must use the same data service".into(),
                ));
            }
            let operation = parent_scope.derive_operation(
                &target.name,
                QueryOperation::framework(QueryOperationKind::Aggregate)
                    .with_relation(&group.property_name),
            );
            let scope = operation.derive_child(&target.name, group.query.comment.as_deref())?;
            let mut service = self
                .scoped_data_service_internal(target.name.clone())
                .with_request_scope(scope.clone());
            service.request_intent = Some(QueryIntent::new(scope.comment(), scope.purpose())?);
            let mut query = group.query.clone();
            query.clear_request_scope();
            group.query = service.prepare_select_query_owned(query)?;
            if group.query.entity != target.name {
                return Err(RuntimeError::Graph(
                    "policy changed the statistics relation target".into(),
                ));
            }
            // Hooks may introduce additional paths; those get the same bounds
            // and policy treatment, rather than being compiled unchecked.
            service.prepare_statistical_children(&mut group.query, count)?;
        }
        Ok(())
    }

    fn prepare_statistical_memberships(
        &self,
        expression: &mut Expr,
        owner: &EntityDescriptor,
        scope: &QueryRequestScope,
        count: &mut usize,
        depth: usize,
    ) -> Result<(), RuntimeError> {
        if depth > 64 {
            return Err(RuntimeError::Graph(
                "statistical membership expression exceeds 64 levels".into(),
            ));
        }
        match expression {
            Expr::SubQuery {
                left,
                entity,
                query,
                ..
            } => {
                self.prepare_statistical_memberships(left, owner, scope, count, depth + 1)?;
                *count += 1;
                if *count > 64 {
                    return Err(RuntimeError::Graph(
                        "statistical plan exceeds 64 layers".into(),
                    ));
                }
                let target = self
                    .data_service
                    .metadata
                    .context
                    .require_entity(&query.entity)?;
                if entity.name != target.name || owner.data_service != target.data_service {
                    return Err(RuntimeError::Graph(
                        "statistical membership requires a matching entity and data service".into(),
                    ));
                }
                let operation = scope.derive_operation(
                    &target.name,
                    QueryOperation::framework(QueryOperationKind::Aggregate),
                );
                let child_scope = operation.derive_child(&target.name, query.comment.as_deref())?;
                let mut service = self
                    .scoped_data_service_internal(target.name.clone())
                    .with_request_scope(child_scope.clone());
                service.request_intent = Some(QueryIntent::new(
                    child_scope.comment(),
                    child_scope.purpose(),
                )?);
                let mut child = query.as_ref().clone();
                child.clear_request_scope();
                let mut prepared = service.prepare_select_query_owned(child)?;
                if prepared.entity != target.name {
                    return Err(RuntimeError::Graph(
                        "policy changed the statistical membership target".into(),
                    ));
                }
                service.prepare_statistical_children(&mut prepared, count)?;
                *entity = target.clone();
                **query = prepared;
            }
            Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
                for argument in args {
                    self.prepare_statistical_memberships(argument, owner, scope, count, depth + 1)?;
                }
            }
            Expr::Binary { left, right, .. } => {
                self.prepare_statistical_memberships(left, owner, scope, count, depth + 1)?;
                self.prepare_statistical_memberships(right, owner, scope, count, depth + 1)?;
            }
            Expr::Between { expr, lower, upper } => {
                for argument in [expr, lower, upper] {
                    self.prepare_statistical_memberships(argument, owner, scope, count, depth + 1)?;
                }
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::Not(expr) => {
                self.prepare_statistical_memberships(expr, owner, scope, count, depth + 1)?;
            }
            Expr::Column(_) | Expr::Value(_) | Expr::LikePattern { .. } => {}
        }
        Ok(())
    }
}
