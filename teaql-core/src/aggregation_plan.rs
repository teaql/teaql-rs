//! Logical, metadata-bound statistics plan. A grouped parent has dimension
//! identity, not entity identity; its rows must never be used as an ID subquery.
//!
//! Plans retain the population request for every layer and the original owner
//! grain of every measure. SQL providers can therefore aggregate source values
//! at the final dimensions without summing averages or multiplying parent values
//! by the number of source children. No Context, database or shared builder state
//! is required to construct a plan.

use std::collections::BTreeSet;
use std::fmt;

use crate::{
    Aggregate, AggregateFunction, DataType, EntityDescriptor, Expr, ExprFunction, SelectQuery,
};

mod measure_expression;

const MAX_LAYERS: usize = 64;

/// The requests embedded in one statistical SQL statement, including independent
/// predicate subqueries. Entity relation enhancements execute separately.
pub fn statistical_query_graph(
    query: &SelectQuery,
) -> Result<Vec<&SelectQuery>, StatisticalPlanError> {
    fn append<'a>(
        queries: &mut Vec<&'a SelectQuery>,
        query: &'a SelectQuery,
    ) -> Result<(), StatisticalPlanError> {
        if queries.len() >= MAX_LAYERS {
            return Err(StatisticalPlanError(
                "statistical request graph exceeds 64 queries".into(),
            ));
        }
        queries.push(query);
        Ok(())
    }
    fn expression<'a>(
        expr: &'a Expr,
        queries: &mut Vec<&'a SelectQuery>,
        depth: usize,
    ) -> Result<(), StatisticalPlanError> {
        if depth > 64 {
            return Err(StatisticalPlanError(
                "statistical request expression exceeds 64 levels".into(),
            ));
        }
        match expr {
            Expr::SubQuery { left, query, .. } => {
                expression(left, queries, depth + 1)?;
                append(queries, query)?;
            }
            Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
                for arg in args {
                    expression(arg, queries, depth + 1)?;
                }
            }
            Expr::Binary { left, right, .. } => {
                expression(left, queries, depth + 1)?;
                expression(right, queries, depth + 1)?;
            }
            Expr::Between { expr, lower, upper } => {
                for arg in [expr, lower, upper] {
                    expression(arg, queries, depth + 1)?;
                }
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::Not(expr) => {
                expression(expr, queries, depth + 1)?
            }
            Expr::Column(_) | Expr::Value(_) | Expr::LikePattern { .. } => {}
        }
        Ok(())
    }
    let mut queries = vec![query];
    let mut index = 0;
    while index < queries.len() {
        let query = queries[index];
        index += 1;
        for group in &query.object_group_bys {
            append(&mut queries, &group.query)?;
        }
        for expr in query.filter.iter().chain(query.having.iter()) {
            expression(expr, &mut queries, 0)?;
        }
        for projection in &query.expr_projection {
            expression(&projection.expr, &mut queries, 0)?;
        }
        for order in &query.order_by {
            if let Some(expr) = &order.expr {
                expression(expr, &mut queries, 0)?;
            }
        }
    }
    Ok(queries)
}

pub fn has_nested_statistics(query: &SelectQuery) -> Result<bool, StatisticalPlanError> {
    fn contains_query(expr: &Expr, depth: usize) -> Result<bool, StatisticalPlanError> {
        if depth > 64 {
            return Err(StatisticalPlanError(
                "statistical request expression exceeds 64 levels".into(),
            ));
        }
        let mut found = false;
        match expr {
            Expr::SubQuery { .. } => return Ok(true),
            Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
                for arg in args {
                    found |= contains_query(arg, depth + 1)?;
                }
            }
            Expr::Binary { left, right, .. } => {
                found |= contains_query(left, depth + 1)?;
                found |= contains_query(right, depth + 1)?;
            }
            Expr::Between { expr, lower, upper } => {
                for arg in [expr, lower, upper] {
                    found |= contains_query(arg, depth + 1)?;
                }
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::Not(expr) => {
                found = contains_query(expr, depth + 1)?
            }
            Expr::Column(_) | Expr::Value(_) | Expr::LikePattern { .. } => {}
        }
        Ok(found)
    }
    // Ordinary scalar predicates need neither a heap-allocated request graph
    // nor copied compilation metadata on the hot query path.
    if query.object_group_bys.is_empty() {
        let mut found = false;
        for expr in query
            .filter
            .iter()
            .chain(query.having.iter())
            .chain(
                query
                    .expr_projection
                    .iter()
                    .map(|projection| &projection.expr),
            )
            .chain(
                query
                    .order_by
                    .iter()
                    .filter_map(|order| order.expr.as_ref()),
            )
        {
            found |= contains_query(expr, 0)?;
        }
        if !found {
            return Ok(false);
        }
    }
    Ok(statistical_query_graph(query)?
        .into_iter()
        .skip(1)
        .any(|child| {
            !child.group_by.is_empty()
                || !child.aggregates.is_empty()
                || child.has_aggregate_projection()
                || child.has_layered_statistics()
        }))
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatisticalPlan {
    /// Source first; each incoming edge refers to an earlier layer.
    pub layers: Vec<StatisticalLayer>,
    pub dimensions: Vec<DimensionBinding>,
    pub measures: Vec<MeasureBinding>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatisticalLayer {
    pub descriptor: EntityDescriptor,
    /// Unmodified request: filters, intent, HAVING, ordering and slicing belong
    /// to this layer, not to whichever parent happens to be visited last.
    pub request: SelectQuery,
    pub incoming: Option<RollupEdge>,
    pub output_path: Vec<String>,
    /// The entity grain to deduplicate before computing this layer's measures.
    pub identity_field: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupEdge {
    pub source_layer: usize,
    /// Logical keys, never physical SQL column identifiers.
    pub source_field: String,
    pub target_field: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DimensionBinding {
    pub owner_layer: usize,
    pub field: String,
    pub output_path: Vec<String>,
    /// Validated scalar transform over `field`, evaluated before owner deduplication.
    pub expression: Option<Expr>,
    pub result_type: DataType,
}

/// Bind one scalar grouping expression without interpreting strings as code.
/// Aggregate functions, subqueries and literals are
/// not grouping transforms. Keep this validation shared by ordinary/layered SQL.
pub fn grouping_expression_source(
    descriptor: &EntityDescriptor,
    expression: &Expr,
) -> Result<(String, DataType), StatisticalPlanError> {
    fn bind(
        descriptor: &EntityDescriptor,
        expression: &Expr,
        depth: usize,
    ) -> Result<(String, DataType), StatisticalPlanError> {
        if depth > 64 {
            return Err(StatisticalPlanError(
                "statistical grouping expression exceeds 64 levels".into(),
            ));
        }
        match expression {
            Expr::Function {
                function: ExprFunction::Identity,
                args,
            } if args.len() == 1 => bind(descriptor, &args[0], depth + 1),
            Expr::Column(field) => descriptor
                .property_by_name(field)
                .map(|property| (field.clone(), property.data_type))
                .ok_or_else(|| {
                    StatisticalPlanError(format!(
                        "unknown logical grouping field {}.{field}",
                        descriptor.name
                    ))
                }),
            Expr::Function {
                function: ExprFunction::Soundex,
                args,
            } if args.len() == 1 => {
                let (field, input) = bind(descriptor, &args[0], depth + 1)?;
                if !matches!(input, DataType::Text | DataType::LargeText) {
                    return Err(StatisticalPlanError(
                        "SOUNDEX grouping requires a text field".into(),
                    ));
                }
                Ok((field, DataType::Text))
            }
            Expr::Function {
                function: ExprFunction::Gbk,
                args,
            } if args.len() == 1 => {
                let (field, input) = bind(descriptor, &args[0], depth + 1)?;
                if !matches!(input, DataType::Text | DataType::LargeText) {
                    return Err(StatisticalPlanError(
                        "GBK grouping requires a text field".into(),
                    ));
                }
                Ok((field, DataType::Binary))
            }
            _ => Err(StatisticalPlanError(
                "unsupported statistical grouping expression; a scalar field transform is required"
                    .into(),
            )),
        }
    }
    bind(descriptor, expression, 0)
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeasureBinding {
    pub owner_layer: usize,
    pub aggregate: Aggregate,
    pub output_path: Vec<String>,
    /// Scalar input evaluated at the owner's row grain, never at child grain.
    /// None retains the ordinary field/all-rows aggregate representation.
    pub operand: Option<Expr>,
    /// Logical source field, absent for a scalar parameter or COUNT(*).
    pub input_field: Option<String>,
    pub input_type: DataType,
}

impl MeasureBinding {
    pub fn all_rows(&self) -> bool {
        self.operand.is_none() && self.input_field.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticalPlanError(pub String);

impl fmt::Display for StatisticalPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for StatisticalPlanError {}

impl StatisticalPlan {
    pub fn build<'a>(
        source: &SelectQuery,
        lookup: impl Fn(&str) -> Option<&'a EntityDescriptor>,
    ) -> Result<Self, StatisticalPlanError> {
        let mut plan = Self {
            layers: Vec::new(),
            dimensions: Vec::new(),
            measures: Vec::new(),
        };
        plan.add_layer(source, None, Vec::new(), &lookup)?;
        let mut paths = BTreeSet::new();
        for path in plan
            .dimensions
            .iter()
            .map(|binding| &binding.output_path)
            .chain(plan.measures.iter().map(|binding| &binding.output_path))
        {
            if paths.iter().any(|existing: &Vec<String>| {
                existing.starts_with(path)
                    || path.starts_with(existing)
                    || existing.join(".") == path.join(".")
            }) {
                return Err(StatisticalPlanError(format!(
                    "duplicate statistical result path: {path:?}"
                )));
            }
            paths.insert(path.clone());
        }
        Ok(plan)
    }

    fn add_layer<'a>(
        &mut self,
        query: &SelectQuery,
        incoming: Option<RollupEdge>,
        path: Vec<String>,
        lookup: &impl Fn(&str) -> Option<&'a EntityDescriptor>,
    ) -> Result<(), StatisticalPlanError> {
        if self.layers.len() >= MAX_LAYERS {
            return Err(StatisticalPlanError(
                "statistical plan exceeds 64 layers".to_owned(),
            ));
        }
        let descriptor = lookup(&query.entity).ok_or_else(|| {
            StatisticalPlanError(format!("missing statistics metadata for {}", query.entity))
        })?;
        if self
            .layers
            .first()
            .is_some_and(|source| source.descriptor.data_service != descriptor.data_service)
        {
            return Err(StatisticalPlanError(
                "statistical SQL populations must use the same data service".into(),
            ));
        }
        let identity = descriptor.id_property().ok_or_else(|| {
            StatisticalPlanError(format!(
                "statistics layer {} has no identity grain",
                query.entity
            ))
        })?;
        let owner = self.layers.len();
        self.layers.push(StatisticalLayer {
            descriptor: descriptor.clone(),
            request: query.clone(),
            incoming,
            output_path: path.clone(),
            identity_field: identity.name.clone(),
        });
        let field_exists = |field: &str| -> Result<(), StatisticalPlanError> {
            if descriptor.property_by_name(field).is_none() {
                return Err(StatisticalPlanError(format!(
                    "unknown logical statistics field {}.{field}",
                    query.entity
                )));
            }
            Ok(())
        };
        let result_path = |field: &str| {
            let mut result = path.clone();
            result.push(field.to_owned());
            result
        };
        let rolled_fields: BTreeSet<_> = query
            .object_group_bys
            .iter()
            .map(|group| group.storage_field.as_str())
            .collect();
        for field in &query.group_by {
            let expressions = query
                .expr_projection
                .iter()
                .filter(|item| item.alias == *field)
                .collect::<Vec<_>>();
            if expressions.len() > 1 {
                return Err(StatisticalPlanError(format!(
                    "ambiguous grouping expression alias: {field}"
                )));
            }
            if let Some(projection) = expressions.first() {
                if field.trim().is_empty() || rolled_fields.contains(field.as_str()) {
                    return Err(StatisticalPlanError(
                        "grouping expression cannot replace an intermediate roll-up key".into(),
                    ));
                }
                let (source, result_type) =
                    grouping_expression_source(descriptor, &projection.expr)?;
                self.dimensions.push(DimensionBinding {
                    owner_layer: owner,
                    field: source,
                    output_path: result_path(field),
                    expression: (!matches!(projection.expr, Expr::Column(_)))
                        .then(|| projection.expr.clone()),
                    result_type,
                });
                continue;
            }
            field_exists(field)?;
            // The FK is an intermediate join key, not a final dimension if the
            // parent request declares a different grouping grain.
            if !rolled_fields.contains(field.as_str()) {
                self.dimensions.push(DimensionBinding {
                    owner_layer: owner,
                    field: field.clone(),
                    output_path: result_path(field),
                    expression: None,
                    result_type: descriptor.property_by_name(field).unwrap().data_type,
                });
            }
        }
        // Existing generated group_by_*_as helpers project an additional named
        // view of a grouping key. Preserve that view at every statistical layer;
        // do not treat it as a measure or silently drop it from the output.
        for projection in &query.expr_projection {
            if query.group_by.contains(&projection.alias) {
                continue;
            }
            if let Some(measure) = measure_expression::bind(
                descriptor,
                owner,
                result_path(&projection.alias),
                &projection.alias,
                &projection.expr,
            )? {
                self.measures.push(measure);
                continue;
            }
            let Expr::Column(field) = &projection.expr else {
                return Err(StatisticalPlanError(
                    "statistical expression projection requires further lowering".into(),
                ));
            };
            field_exists(field)?;
            if !query.group_by.contains(field) || rolled_fields.contains(field.as_str()) {
                return Err(StatisticalPlanError(format!(
                    "statistical alias {} must refer to a final grouping key",
                    projection.alias
                )));
            }
            if projection.alias.trim().is_empty() {
                return Err(StatisticalPlanError(
                    "statistical dimension alias must not be empty".into(),
                ));
            }
            if projection.alias != *field {
                self.dimensions.push(DimensionBinding {
                    owner_layer: owner,
                    field: field.clone(),
                    output_path: result_path(&projection.alias),
                    expression: None,
                    result_type: descriptor.property_by_name(field).unwrap().data_type,
                });
            }
        }
        for aggregate in &query.aggregates {
            if aggregate.field != "*" {
                field_exists(&aggregate.field)?;
            } else if aggregate.function != AggregateFunction::Count {
                return Err(StatisticalPlanError(
                    "only COUNT accepts the all-rows operand".to_owned(),
                ));
            }
            if aggregate.alias.trim().is_empty() {
                return Err(StatisticalPlanError(
                    "statistical measure alias must not be empty".to_owned(),
                ));
            }
            self.measures.push(MeasureBinding {
                owner_layer: owner,
                aggregate: aggregate.clone(),
                output_path: result_path(&aggregate.alias),
                operand: None,
                input_field: (aggregate.field != "*").then(|| aggregate.field.clone()),
                input_type: descriptor
                    .property_by_name(&aggregate.field)
                    .map_or(DataType::I64, |property| property.data_type),
            });
        }
        // Ordinary parent selection keeps entity-ID grain. An explicitly
        // aggregate-only parent is instead a global statistical dimension.
        if owner != 0
            && query.group_by.is_empty()
            && query.object_group_bys.is_empty()
            && query.aggregates.is_empty()
            && !self
                .measures
                .iter()
                .any(|measure| measure.owner_layer == owner)
        {
            self.dimensions.push(DimensionBinding {
                owner_layer: owner,
                field: identity.name.clone(),
                output_path: result_path(&identity.name),
                expression: None,
                result_type: identity.data_type,
            });
            let fields = if query.projection.is_empty() {
                descriptor
                    .properties
                    .iter()
                    .map(|property| property.name.as_str())
                    .collect::<Vec<_>>()
            } else {
                query.projection.iter().map(String::as_str).collect()
            };
            for field in fields {
                field_exists(field)?;
                if field != identity.name {
                    self.dimensions.push(DimensionBinding {
                        owner_layer: owner,
                        field: field.into(),
                        output_path: result_path(field),
                        expression: None,
                        result_type: descriptor.property_by_name(field).unwrap().data_type,
                    });
                }
            }
        }
        let mut relation_outputs = BTreeSet::new();
        for group in &query.object_group_bys {
            if !relation_outputs.insert(&group.property_name) {
                return Err(StatisticalPlanError(format!(
                    "duplicate roll-up relation {}.{}",
                    query.entity, group.property_name
                )));
            }
            let relation = descriptor
                .relation_by_name(&group.property_name)
                .ok_or_else(|| {
                    StatisticalPlanError(format!(
                        "unknown roll-up relation {}.{}",
                        query.entity, group.property_name
                    ))
                })?;
            if relation.many
                || relation.target_entity != group.query.entity
                || relation.local_key != group.storage_field
            {
                return Err(StatisticalPlanError(format!(
                    "roll-up relation {}.{} must be a matching to-one relation",
                    query.entity, group.property_name
                )));
            }
            field_exists(&relation.local_key)?;
            let target = lookup(&relation.target_entity).ok_or_else(|| {
                StatisticalPlanError(format!(
                    "missing statistics metadata for {}",
                    relation.target_entity
                ))
            })?;
            if target.id_property().map(|property| property.name.as_str())
                != Some(relation.foreign_key.as_str())
            {
                return Err(StatisticalPlanError(format!(
                    "roll-up relation {}.{} must reference target identity",
                    query.entity, group.property_name
                )));
            }
            self.add_layer(
                &group.query,
                Some(RollupEdge {
                    source_layer: owner,
                    source_field: relation.local_key.clone(),
                    target_field: relation.foreign_key.clone(),
                }),
                result_path(&group.property_name),
                lookup,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataType, Expr, PropertyDescriptor as P, RelationDescriptor as R};

    fn fixture() -> Vec<EntityDescriptor> {
        vec![
            EntityDescriptor::new("School")
                .property(P::new("id", DataType::I64).id())
                .property(P::new("capacity", DataType::I64).column_name("Legacy Capacity"))
                .property(P::new("kind_id", DataType::I64).column_name("Legacy Kind Key"))
                .relation(R::new("kind", "Kind").local_key("kind_id")),
            EntityDescriptor::new("Kind")
                .property(P::new("id", DataType::I64).id())
                .property(P::new("name", DataType::Text).column_name("Legacy Kind Label"))
                .property(P::new("ordinal", DataType::I64))
                .property(P::new("platform_id", DataType::I64))
                .relation(R::new("platform", "Platform").local_key("platform_id")),
            EntityDescriptor::new("Platform")
                .property(P::new("id", DataType::I64).id())
                .property(P::new("name", DataType::Text)),
        ]
    }

    fn build(query: &SelectQuery) -> Result<StatisticalPlan, StatisticalPlanError> {
        let descriptors = fixture();
        StatisticalPlan::build(query, |name| {
            descriptors
                .iter()
                .find(|descriptor| descriptor.name == name)
        })
    }

    #[test]
    fn ordinary_scalar_queries_have_no_nested_statistics() {
        let query = SelectQuery::new("School")
            .filter(Expr::gte("capacity", 10))
            .count("schools");
        assert!(!has_nested_statistics(&query).unwrap());
        assert_eq!(statistical_query_graph(&query).unwrap().len(), 1);
    }

    #[test]
    fn embedded_request_graph_includes_independent_rollup_descendants() {
        let child = SelectQuery::new("School").count("schools").object_group_by(
            "kind",
            "kind_id",
            SelectQuery::new("Kind").object_group_by(
                "platform",
                "platform_id",
                SelectQuery::new("Platform").group_by("name"),
            ),
        );
        let query = SelectQuery::new("School").filter(Expr::in_subquery(
            "kind_id",
            EntityDescriptor::new("School"),
            child,
            "kind.platform.name",
        ));
        assert!(has_nested_statistics(&query).unwrap());
        assert_eq!(
            statistical_query_graph(&query)
                .unwrap()
                .iter()
                .map(|query| query.entity.as_str())
                .collect::<Vec<_>>(),
            ["School", "School", "Kind", "Platform"]
        );
    }

    #[test]
    fn independent_requests_share_the_complete_statement_query_budget() {
        let query = SelectQuery::new("School").filter(Expr::And(
            (0..64)
                .map(|_| {
                    Expr::in_subquery(
                        "kind_id",
                        EntityDescriptor::new("Kind"),
                        SelectQuery::new("Kind").group_by("id"),
                        "id",
                    )
                })
                .collect(),
        ));
        assert!(
            statistical_query_graph(&query)
                .unwrap_err()
                .to_string()
                .contains("64 queries")
        );
        assert!(has_nested_statistics(&query).is_err());
    }

    #[test]
    fn nested_detection_rejects_oversized_expression_trees() {
        let mut expression = Expr::eq("capacity", 1);
        for _ in 0..65 {
            expression = Expr::Not(Box::new(expression));
        }
        let query = SelectQuery::new("School").filter(expression);
        assert!(
            has_nested_statistics(&query)
                .unwrap_err()
                .to_string()
                .contains("64 levels")
        );
        assert!(statistical_query_graph(&query).is_err());
    }

    #[test]
    fn computed_dimensions_bind_their_source_and_result_type() {
        let expression = Expr::soundex(Expr::column("name"));
        let query = SelectQuery::new("Kind")
            .group_by_expr("phonetic", expression.clone())
            .count("n");
        let plan = build(&query).unwrap();
        assert_eq!(plan.dimensions.len(), 1);
        assert_eq!(plan.dimensions[0].field, "name");
        assert_eq!(plan.dimensions[0].output_path, vec!["phonetic"]);
        assert_eq!(plan.dimensions[0].expression, Some(expression));
        assert_eq!(plan.dimensions[0].result_type, DataType::Text);
    }

    #[test]
    fn grouping_functions_reject_aggregates_wrong_types_unknown_fields_and_depth() {
        for expression in [
            Expr::count_all(),
            Expr::soundex(Expr::column("ordinal")),
            Expr::soundex(Expr::column("missing")),
            Expr::gbk(Expr::column("ordinal")),
        ] {
            assert!(
                build(
                    &SelectQuery::new("Kind")
                        .group_by_expr("key", expression)
                        .count("n")
                )
                .is_err()
            );
        }
        let mut expression = Expr::column("name");
        for _ in 0..70 {
            expression = Expr::soundex(expression);
        }
        assert!(
            build(
                &SelectQuery::new("Kind")
                    .group_by_expr("key", expression)
                    .count("n")
            )
            .unwrap_err()
            .to_string()
            .contains("64 levels")
        );
    }

    #[test]
    fn grouped_parent_replaces_intermediate_fk_without_losing_source_measures() {
        let source = SelectQuery::new("School")
            .group_by("kind_id")
            .aggregate(Aggregate::count("n"))
            .aggregate(Aggregate::avg("capacity", "mean"))
            .object_group_by("kind", "kind_id", SelectQuery::new("Kind").group_by("name"));
        let original = source.clone();
        let plan = build(&source).unwrap();
        assert_eq!(source, original);
        assert_eq!(
            plan.dimensions,
            vec![DimensionBinding {
                owner_layer: 1,
                field: "name".into(),
                output_path: vec!["kind".into(), "name".into()],
                expression: None,
                result_type: DataType::Text,
            }]
        );
        assert_eq!(plan.measures.len(), 2);
        assert!(plan.measures.iter().all(|measure| measure.owner_layer == 0));
        assert_eq!(plan.measures[1].aggregate.function, AggregateFunction::Avg);
        assert_eq!(
            plan.layers[0]
                .descriptor
                .property_by_name("kind_id")
                .unwrap()
                .column_name,
            "Legacy Kind Key"
        );
    }

    #[test]
    fn named_group_projection_survives_rollup_without_becoming_a_measure() {
        let query = SelectQuery::new("School")
            .group_by("capacity")
            .project_expr("bucket", Expr::column("capacity"))
            .aggregate(Aggregate::count("schools"))
            .object_group_by("kind", "kind_id", SelectQuery::new("Kind").group_by("name"));
        let plan = build(&query).unwrap();
        assert!(
            plan.dimensions
                .iter()
                .any(|binding| binding.owner_layer == 0
                    && binding.field == "capacity"
                    && binding.output_path == ["bucket"])
        );
        assert_eq!(plan.measures.len(), 1);
        assert_eq!(plan.layers[0].request, query);
    }

    #[test]
    fn non_grouped_expression_projection_is_not_silently_discarded() {
        let query = SelectQuery::new("School")
            .group_by("capacity")
            .project_expr("not_a_dimension", Expr::column("id"))
            .object_group_by("kind", "kind_id", SelectQuery::new("Kind").group_by("name"));
        assert!(build(&query).is_err());
    }

    #[test]
    fn literal_alias_cannot_shadow_a_nested_result_path() {
        let query = SelectQuery::new("School")
            .group_by("capacity")
            .project_expr("kind.name", Expr::column("capacity"))
            .object_group_by("kind", "kind_id", SelectQuery::new("Kind").group_by("name"));
        assert!(build(&query).is_err());
    }

    #[test]
    fn multihop_keeps_population_filters_owner_grains_and_local_measures() {
        let parent = SelectQuery::new("Kind")
            .and_filter(Expr::eq("ordinal", 1))
            .aggregate(Aggregate::sum("ordinal", "total"))
            .group_by("platform_id")
            .object_group_by(
                "platform",
                "platform_id",
                SelectQuery::new("Platform")
                    .group_by("name")
                    .and_filter(Expr::eq("id", 9)),
            );
        let source = SelectQuery::new("School")
            .and_filter(Expr::gt("capacity", 0))
            .aggregate(Aggregate::sum("capacity", "total"))
            .group_by("kind_id")
            .object_group_by("kind", "kind_id", parent);
        let plan = build(&source).unwrap();
        assert_eq!(plan.layers.len(), 3);
        assert_eq!(plan.dimensions[0].owner_layer, 2);
        assert_eq!(plan.dimensions[0].output_path, ["kind", "platform", "name"]);
        assert_eq!(
            plan.measures
                .iter()
                .map(|measure| measure.owner_layer)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(plan.measures[1].output_path, ["kind", "total"]);
        assert!(
            plan.layers
                .iter()
                .all(|layer| layer.request.filter.is_some())
        );
        assert!(plan.layers.iter().all(|layer| layer.identity_field == "id"));
    }

    #[test]
    fn ordinary_parent_selection_retains_id_grain_but_aggregate_only_parent_does_not() {
        let plain =
            SelectQuery::new("School").object_group_by("kind", "kind_id", SelectQuery::new("Kind"));
        assert_eq!(build(&plain).unwrap().dimensions[0].field, "id");
        let global = SelectQuery::new("School")
            .aggregate(Aggregate::count("schools"))
            .object_group_by(
                "kind",
                "kind_id",
                SelectQuery::new("Kind").aggregate(Aggregate::count("kinds")),
            );
        let plan = build(&global).unwrap();
        assert!(plan.dimensions.is_empty());
        assert_eq!(plan.measures[1].owner_layer, 1);
    }

    #[test]
    fn legacy_column_names_are_not_accepted_as_logical_fields() {
        assert!(build(&SelectQuery::new("School").group_by("Legacy Kind Key")).is_err());
        assert!(
            build(&SelectQuery::new("School").aggregate(Aggregate::sum("Legacy Capacity", "n")))
                .is_err()
        );
    }

    #[test]
    fn duplicate_aliases_wrong_targets_and_fanout_fail_before_execution() {
        assert!(
            build(
                &SelectQuery::new("School")
                    .group_by("id")
                    .aggregate(Aggregate::count("id"))
            )
            .is_err()
        );
        assert!(
            build(&SelectQuery::new("School").object_group_by(
                "kind",
                "kind_id",
                SelectQuery::new("Platform")
            ))
            .is_err()
        );
        let mut descriptors = fixture();
        descriptors[0].relations[0].many = true;
        let query =
            SelectQuery::new("School").object_group_by("kind", "kind_id", SelectQuery::new("Kind"));
        assert!(
            StatisticalPlan::build(&query, |name| descriptors
                .iter()
                .find(|descriptor| descriptor.name == name))
            .is_err()
        );
    }

    #[test]
    fn alias_collision_at_different_owner_paths_is_not_a_collision() {
        let plan = build(
            &SelectQuery::new("School")
                .aggregate(Aggregate::count("n"))
                .object_group_by(
                    "kind",
                    "kind_id",
                    SelectQuery::new("Kind").aggregate(Aggregate::count("n")),
                ),
        )
        .unwrap();
        assert_eq!(plan.measures[0].output_path, ["n"]);
        assert_eq!(plan.measures[1].output_path, ["kind", "n"]);
    }

    #[test]
    fn scalar_alias_cannot_overwrite_a_nested_dimension_container() {
        assert!(
            build(
                &SelectQuery::new("School")
                    .aggregate(Aggregate::count("kind"))
                    .object_group_by("kind", "kind_id", SelectQuery::new("Kind").group_by("name"))
            )
            .is_err()
        );
    }

    #[test]
    fn joined_populations_cannot_cross_data_service_boundaries() {
        let mut descriptors = fixture();
        descriptors[1].data_service = Some("external".into());
        let query = SelectQuery::new("School").object_group_by(
            "kind",
            "kind_id",
            SelectQuery::new("Kind").group_by("name"),
        );
        assert!(
            StatisticalPlan::build(&query, |name| descriptors
                .iter()
                .find(|descriptor| descriptor.name == name))
            .is_err()
        );
    }
    #[test]
    fn identity_grouping_retains_every_scalar_metadata_type() {
        for data_type in [
            DataType::I64,
            DataType::U64,
            DataType::F64,
            DataType::Decimal,
            DataType::Bool,
            DataType::Text,
            DataType::LargeText,
            DataType::Date,
            DataType::Timestamp,
            DataType::Binary,
        ] {
            let descriptor = EntityDescriptor::new("Scalar").property(P::new("value", data_type));
            assert_eq!(
                grouping_expression_source(&descriptor, &Expr::identity(Expr::column("value")))
                    .unwrap(),
                ("value".to_owned(), data_type)
            );
        }
    }

    #[test]
    fn identity_grouping_does_not_hide_invalid_arguments_or_nested_aggregates() {
        let descriptor = EntityDescriptor::new("Scalar").property(P::new("value", DataType::I64));
        for expression in [
            Expr::function(ExprFunction::Identity, []),
            Expr::function(
                ExprFunction::Identity,
                [Expr::column("value"), Expr::column("value")],
            ),
            Expr::identity(Expr::column("unknown")),
            Expr::identity(Expr::sum_expr(Expr::column("value"))),
            Expr::identity(Expr::gbk(Expr::column("value"))),
        ] {
            assert!(grouping_expression_source(&descriptor, &expression).is_err());
        }
        let mut expression = Expr::column("value");
        for _ in 0..70 {
            expression = Expr::identity(expression);
        }
        assert!(
            grouping_expression_source(&descriptor, &expression)
                .unwrap_err()
                .to_string()
                .contains("64")
        );
    }
}
