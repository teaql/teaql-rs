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

use crate::{Aggregate, AggregateFunction, EntityDescriptor, SelectQuery};

const MAX_LAYERS: usize = 64;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionBinding {
    pub owner_layer: usize,
    pub field: String,
    pub output_path: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasureBinding {
    pub owner_layer: usize,
    pub aggregate: Aggregate,
    pub output_path: Vec<String>,
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
                existing.starts_with(path) || path.starts_with(existing)
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
            field_exists(field)?;
            // The FK is an intermediate join key, not a final dimension if the
            // parent request declares a different grouping grain.
            if !rolled_fields.contains(field.as_str()) {
                self.dimensions.push(DimensionBinding {
                    owner_layer: owner,
                    field: field.clone(),
                    output_path: result_path(field),
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
            });
        }
        // Ordinary parent selection keeps entity-ID grain. An explicitly
        // aggregate-only parent is instead a global statistical dimension.
        if owner != 0
            && query.group_by.is_empty()
            && query.object_group_bys.is_empty()
            && query.aggregates.is_empty()
        {
            self.dimensions.push(DimensionBinding {
                owner_layer: owner,
                field: identity.name.clone(),
                output_path: result_path(&identity.name),
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
}
