//! SQL lowering for metadata-bound layered statistics. Each measure owner is
//! deduplicated at entity identity + final dimensions before aggregation. In
//! particular AVG is computed from source observations, never group averages.

use std::collections::{BTreeMap, BTreeSet};
use teaql_core::aggregation_plan::StatisticalPlan;
use teaql_core::{
    AggregateFunction, CompactRow, DataType, EntityDescriptor, Expr, PropertyDescriptor,
    SelectQuery, Value,
};

use crate::{CompiledQuery, SqlBindings, SqlCompileError, SqlDialect};

/// Compile a complete population join, then one aggregate CTE per measure
/// owner. Result columns use stable internal labels (`__teaql_dN`,
/// `__teaql_mN`); the plan supplies their logical, possibly nested paths.
///
/// Unsupported post-aggregation layer shapes fail explicitly rather than
/// silently applying pre-aggregation pagination or dropping predicates.
pub fn compile_statistical_plan(
    dialect: &impl SqlDialect,
    plan: &StatisticalPlan,
) -> Result<CompiledQuery, SqlCompileError> {
    let invalid = |message: &str| SqlCompileError::InvalidFunctionArguments(message.to_owned());
    let Some(root) = plan.layers.first() else {
        return Err(invalid("empty statistical plan"));
    };
    if plan.dimensions.is_empty() && plan.measures.is_empty() {
        return Err(invalid(
            "statistical plan has neither dimensions nor measures",
        ));
    }
    let q = |name: &str| dialect.quote_ident(name);
    let mut params = SqlBindings::new();
    let mut ctes = Vec::new();
    for (index, layer) in plan.layers.iter().enumerate() {
        let request = &layer.request;
        if request.partition_by.is_some()
            || request.raw_sql.is_some()
            || !request.raw_projections.is_empty()
            || !request.dynamic_properties.is_empty()
            || request.dynamic_field_selection.is_some()
        {
            return Err(invalid(
                "layered statistics expression projections or entity extensions require further lowering",
            ));
        }
        for projection in &request.expr_projection {
            let Expr::Column(field) = &projection.expr else {
                return Err(invalid(
                    "layered statistics expression requires further lowering",
                ));
            };
            let mut path = layer.output_path.clone();
            path.push(projection.alias.clone());
            if !request.group_by.contains(field)
                || !plan.dimensions.iter().any(|dimension| {
                    dimension.owner_layer == index
                        && dimension.field == *field
                        && dimension.output_path == path
                })
            {
                return Err(invalid(
                    "statistical alias is not bound to a final grouping dimension",
                ));
            }
        }
        let mut population = request.clone();
        population.expr_projection.clear();
        population.group_by.clear();
        population.aggregates.clear();
        population.object_group_bys.clear();
        population.having = None;
        population.order_by.clear();
        population.slice = None;
        let mut fields = BTreeSet::from([layer.identity_field.clone()]);
        fields.extend(
            plan.dimensions
                .iter()
                .filter(|dimension| dimension.owner_layer == index)
                .map(|dimension| dimension.field.clone()),
        );
        fields.extend(
            plan.measures
                .iter()
                .filter(|measure| measure.owner_layer == index && measure.aggregate.field != "*")
                .map(|measure| measure.aggregate.field.clone()),
        );
        for child in &plan.layers {
            if let Some(edge) = &child.incoming {
                if edge.source_layer == index {
                    fields.insert(edge.source_field.clone());
                }
            }
        }
        if let Some(edge) = &layer.incoming {
            fields.insert(edge.target_field.clone());
        }
        population.projection = fields.into_iter().collect();
        let sql = dialect.compile_select_sql(&layer.descriptor, &population, &mut params)?;
        ctes.push(format!("{} AS ({sql})", q(&format!("__teaql_q{index}"))));
    }
    let field = |layer: usize, name: &str| -> Result<String, SqlCompileError> {
        let descriptor = &plan
            .layers
            .get(layer)
            .ok_or_else(|| invalid("invalid statistics owner index"))?
            .descriptor;
        if descriptor.property_by_name(name).is_none() {
            return Err(SqlCompileError::UnknownField(name.into()));
        }
        Ok(format!("{}.{}", q(&format!("l{layer}")), q(name)))
    };
    let mut joins = format!("{} AS {}", q("__teaql_q0"), q("l0"));
    for (index, layer) in plan.layers.iter().enumerate().skip(1) {
        let edge = layer
            .incoming
            .as_ref()
            .ok_or_else(|| invalid("non-source statistics layer requires an incoming edge"))?;
        if edge.source_layer >= index {
            return Err(invalid("statistics layer edges must be acyclic"));
        }
        // Eligibility is represented by each filtered population, not by a
        // second grouped request misused as an entity-ID membership query.
        joins.push_str(&format!(
            " JOIN {} AS {} ON {} = {}",
            q(&format!("__teaql_q{index}")),
            q(&format!("l{index}")),
            field(edge.source_layer, &edge.source_field)?,
            field(index, &edge.target_field)?
        ));
    }
    let dimension_names = (0..plan.dimensions.len())
        .map(|index| q(&format!("__teaql_d{index}")))
        .collect::<Vec<_>>();
    let mut selected = Vec::new();
    for (index, dimension) in plan.dimensions.iter().enumerate() {
        selected.push(format!(
            "{} AS {}",
            field(dimension.owner_layer, &dimension.field)?,
            dimension_names[index]
        ));
    }
    for (index, layer) in plan.layers.iter().enumerate() {
        selected.push(format!(
            "{} AS {}",
            field(index, &layer.identity_field)?,
            q(&format!("__teaql_id{index}"))
        ));
    }
    for (index, measure) in plan.measures.iter().enumerate() {
        if measure.aggregate.field != "*" {
            selected.push(format!(
                "{} AS {}",
                field(measure.owner_layer, &measure.aggregate.field)?,
                q(&format!("__teaql_v{index}"))
            ));
        }
    }
    ctes.push(format!(
        "{} AS (SELECT {} FROM {joins})",
        q("__teaql_base"),
        selected.join(", ")
    ));
    let mut eligibility = Vec::new();
    for (owner, layer) in plan.layers.iter().enumerate().skip(1) {
        if layer.request.having.is_none()
            && layer.request.order_by.is_empty()
            && layer.request.slice.is_none()
        {
            continue;
        }
        let (eligible, labels) =
            compile_parent_cells(dialect, plan, owner, &mut ctes, &mut params)?;
        eligibility.push(format!(
            "EXISTS (SELECT 1 FROM {} AS {} WHERE {})",
            q(&eligible),
            q("e"),
            match_dimensions(dialect, &labels, "p", "e")
        ));
    }
    ctes.push(format!(
        "{} AS (SELECT * FROM {} AS {}{})",
        q("__teaql_population"),
        q("__teaql_base"),
        q("p"),
        if eligibility.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", eligibility.join(" AND "))
        }
    ));
    let dimensions = dimension_names.join(", ");
    let dimension_prefix = if dimensions.is_empty() {
        String::new()
    } else {
        format!("{dimensions}, ")
    };
    let group_clause = if dimensions.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {dimensions}")
    };
    // A scalar aggregate still returns one row for an empty population.
    let cells_sql = if dimensions.is_empty() {
        "SELECT 1 AS __teaql_unit".to_owned()
    } else {
        format!(
            "SELECT DISTINCT {dimensions} FROM {}",
            q("__teaql_population")
        )
    };
    ctes.push(format!("{} AS ({cells_sql})", q("__teaql_cells")));
    let mut result = dimension_names
        .iter()
        .map(|name| format!("{}.{} AS {name}", q("c"), name))
        .collect::<Vec<_>>();
    let mut result_joins = format!("{} AS {}", q("__teaql_cells"), q("c"));
    for (owner, layer) in plan.layers.iter().enumerate() {
        let measures = plan
            .measures
            .iter()
            .enumerate()
            .filter(|(_, measure)| measure.owner_layer == owner)
            .collect::<Vec<_>>();
        if measures.is_empty() {
            continue;
        }
        let grain_name = q(&format!("__teaql_grain{owner}"));
        let id = q(&format!("__teaql_id{owner}"));
        let mut grain_fields = dimension_names.clone();
        grain_fields.push(id.clone());
        grain_fields.extend(
            measures
                .iter()
                .filter(|(_, measure)| measure.aggregate.field != "*")
                .map(|(index, _)| q(&format!("__teaql_v{index}"))),
        );
        ctes.push(format!(
            "{grain_name} AS (SELECT DISTINCT {} FROM {} WHERE {id} IS NOT NULL)",
            grain_fields.join(", "),
            q("__teaql_population")
        ));
        let mut calls = Vec::new();
        for (index, measure) in measures {
            let operand = if measure.aggregate.field == "*" {
                "*".to_owned()
            } else {
                q(&format!("__teaql_v{index}"))
            };
            let call =
                dialect.aggregate_call_sql_for(&layer.descriptor, &measure.aggregate, &operand)?;
            let label = q(&format!("__teaql_m{index}"));
            calls.push(format!("{call} AS {label}"));
            let value = format!("{}.{}", q(&format!("m{owner}")), label);
            let value = if matches!(
                measure.aggregate.function,
                AggregateFunction::Count | AggregateFunction::CountDistinct
            ) {
                format!("COALESCE({value}, 0)")
            } else {
                value
            };
            result.push(format!("{value} AS {label}"));
        }
        let aggregate_name = q(&format!("__teaql_measure{owner}"));
        ctes.push(format!(
            "{aggregate_name} AS (SELECT {dimension_prefix}{} FROM {grain_name}{group_clause})",
            calls.join(", ")
        ));
        let alias = q(&format!("m{owner}"));
        let on = if dimension_names.is_empty() {
            "1 = 1".to_owned()
        } else {
            dimension_names
                .iter()
                .map(|name| {
                    let left = format!("{}.{}", q("c"), name);
                    let right = format!("{alias}.{name}");
                    format!("({left} = {right} OR ({left} IS NULL AND {right} IS NULL))")
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        result_joins.push_str(&format!(" LEFT JOIN {aggregate_name} AS {alias} ON {on}"));
    }
    let mut sql = format!(
        "WITH {} SELECT {} FROM {result_joins}",
        ctes.join(", "),
        result.join(", ")
    );
    if root.request.having.is_some()
        || !root.request.order_by.is_empty()
        || root.request.slice.is_some()
    {
        // Filter and page final cells, never the source population. Aliases
        // bind through a result descriptor, not a second raw SQL substitution.
        let descriptor = result_descriptor(plan)?;
        let mut outer = SelectQuery::new("__teaql_statistical_result");
        outer.projection = descriptor
            .properties
            .iter()
            .map(|property| property.name.clone())
            .collect();
        outer.filter = root.request.having.clone();
        outer.order_by = root.request.order_by.clone();
        outer.slice = root.request.slice;
        let outer_sql = dialect.compile_select_sql(&descriptor, &outer, &mut params)?;
        sql = format!(
            "WITH {} , {} AS (SELECT {} FROM {result_joins}) {outer_sql}",
            ctes.join(", "),
            q("__teaql_statistical_result"),
            result.join(", ")
        );
    }
    Ok(CompiledQuery {
        sql,
        log_context: params.log_context(dialect.kind()),
        params: params.into_values(),
        comment: root.request.comment.clone(),
    })
}

/// Evaluate each parent's bounds at its own descendant dimension grain, using
/// the source-filtered population. Picking parent IDs before grouping would
/// discard other IDs that belong to the same eligible business dimension.
fn compile_parent_cells(
    dialect: &impl SqlDialect,
    plan: &StatisticalPlan,
    owner: usize,
    ctes: &mut Vec<String>,
    params: &mut SqlBindings,
) -> Result<(String, Vec<String>), SqlCompileError> {
    let q = |name: &str| dialect.quote_ident(name);
    let layer = &plan.layers[owner];
    let dimensions = plan
        .dimensions
        .iter()
        .enumerate()
        .filter(|(_, binding)| binding.output_path.starts_with(&layer.output_path))
        .collect::<Vec<_>>();
    let measures = plan
        .measures
        .iter()
        .enumerate()
        .filter(|(_, binding)| binding.owner_layer == owner)
        .collect::<Vec<_>>();
    let labels = dimensions
        .iter()
        .map(|(index, _)| q(&format!("__teaql_d{index}")))
        .collect::<Vec<_>>();
    let dimension_sql = labels.join(", ");
    let body = if measures.is_empty() {
        if dimensions.is_empty() {
            format!("SELECT 1 AS {}", q("__teaql_unit"))
        } else {
            format!("SELECT DISTINCT {dimension_sql} FROM {}", q("__teaql_base"))
        }
    } else {
        let id = q(&format!("__teaql_id{owner}"));
        let grain = q(&format!("__teaql_parent_grain{owner}"));
        let mut fields = labels.clone();
        fields.push(id.clone());
        fields.extend(
            measures
                .iter()
                .filter(|(_, binding)| binding.aggregate.field != "*")
                .map(|(index, _)| q(&format!("__teaql_v{index}"))),
        );
        ctes.push(format!(
            "{grain} AS (SELECT DISTINCT {} FROM {} WHERE {id} IS NOT NULL)",
            fields.join(", "),
            q("__teaql_base")
        ));
        let mut selections = labels.clone();
        for (index, binding) in &measures {
            let operand = if binding.aggregate.field == "*" {
                "*".into()
            } else {
                q(&format!("__teaql_v{index}"))
            };
            let call =
                dialect.aggregate_call_sql_for(&layer.descriptor, &binding.aggregate, &operand)?;
            selections.push(format!("{call} AS {}", q(&format!("__teaql_m{index}"))));
        }
        format!(
            "SELECT {} FROM {grain}{}",
            selections.join(", "),
            if labels.is_empty() {
                String::new()
            } else {
                format!(" GROUP BY {dimension_sql}")
            }
        )
    };
    let raw_name = format!("__teaql_parent_cells{owner}");
    ctes.push(format!("{} AS ({body})", q(&raw_name)));
    let mut descriptor = EntityDescriptor::new(&raw_name).table_name(&raw_name);
    let mut query = SelectQuery::new(&raw_name);
    let complete = result_descriptor(plan)?;
    for (index, binding) in &dimensions {
        let label = format!("__teaql_d{index}");
        let property = complete
            .properties
            .iter()
            .find(|property| property.column_name == label)
            .unwrap();
        let name = binding.output_path[layer.output_path.len()..].join(".");
        descriptor = descriptor
            .property(PropertyDescriptor::new(&name, property.data_type).column_name(&label));
        query = query.project_expr(label, Expr::column(name));
    }
    for (index, binding) in &measures {
        let label = format!("__teaql_m{index}");
        let property = complete
            .properties
            .iter()
            .find(|property| property.column_name == label)
            .unwrap();
        descriptor = descriptor.property(
            PropertyDescriptor::new(&binding.aggregate.alias, property.data_type)
                .column_name(&label),
        );
        query = query.project_expr(label, Expr::column(&binding.aggregate.alias));
    }
    if dimensions.is_empty() && measures.is_empty() {
        descriptor = descriptor.property(PropertyDescriptor::new("__teaql_unit", DataType::I64));
        query = query.project("__teaql_unit");
    }
    query.filter = layer.request.having.clone();
    query.order_by = layer.request.order_by.clone();
    query.slice = layer.request.slice;
    let sql = dialect.compile_select_sql(&descriptor, &query, params)?;
    let eligible = format!("__teaql_eligible{owner}");
    ctes.push(format!("{} AS ({sql})", q(&eligible)));
    Ok((eligible, labels))
}

fn match_dimensions(
    dialect: &impl SqlDialect,
    labels: &[String],
    left: &str,
    right: &str,
) -> String {
    if labels.is_empty() {
        return "1 = 1".into();
    }
    labels
        .iter()
        .map(|label| {
            let a = format!("{}.{label}", dialect.quote_ident(left));
            let b = format!("{}.{label}", dialect.quote_ident(right));
            format!("({a} = {b} OR ({a} IS NULL AND {b} IS NULL))")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn result_descriptor(plan: &StatisticalPlan) -> Result<EntityDescriptor, SqlCompileError> {
    let mut descriptor = EntityDescriptor::new("__teaql_statistical_result")
        .table_name("__teaql_statistical_result");
    for (index, dimension) in plan.dimensions.iter().enumerate() {
        let property = plan.layers[dimension.owner_layer]
            .descriptor
            .property_by_name(&dimension.field)
            .ok_or_else(|| SqlCompileError::UnknownField(dimension.field.clone()))?;
        descriptor = descriptor.property(
            PropertyDescriptor::new(dimension.output_path.join("."), property.data_type)
                .column_name(format!("__teaql_d{index}")),
        );
    }
    for (index, measure) in plan.measures.iter().enumerate() {
        let data_type = if matches!(
            measure.aggregate.function,
            AggregateFunction::Count | AggregateFunction::CountDistinct
        ) {
            DataType::I64
        } else {
            plan.layers[measure.owner_layer]
                .descriptor
                .property_by_name(&measure.aggregate.field)
                .ok_or_else(|| SqlCompileError::UnknownField(measure.aggregate.field.clone()))?
                .data_type
        };
        descriptor = descriptor.property(
            PropertyDescriptor::new(measure.output_path.join("."), data_type)
                .column_name(format!("__teaql_m{index}")),
        );
    }
    Ok(descriptor)
}

/// Restore logical result paths after a driver returns the compiler's neutral
/// column labels. Statistical rows are DTOs, never invented entity identities.
pub fn decode_statistical_rows(
    plan: &StatisticalPlan,
    rows: Vec<CompactRow>,
) -> Result<Vec<CompactRow>, String> {
    fn insert(
        row: &mut BTreeMap<String, Value>,
        path: &[String],
        value: Value,
    ) -> Result<(), String> {
        let Some((name, rest)) = path.split_first() else {
            return Err("empty statistical result path".into());
        };
        if rest.is_empty() {
            if row.insert(name.clone(), value).is_some() {
                return Err(format!("conflicting statistical result path: {path:?}"));
            }
            return Ok(());
        }
        let entry = row
            .entry(name.clone())
            .or_insert_with(|| Value::Object(BTreeMap::new()));
        let Value::Object(object) = entry else {
            return Err(format!("conflicting statistical result path: {path:?}"));
        };
        insert(object, rest, value)
    }
    let descriptor = result_descriptor(plan).map_err(|error| error.to_string())?;
    rows.into_iter()
        .map(|row| {
            let mut result = BTreeMap::new();
            for property in &descriptor.properties {
                let mut value = row
                    .get(&property.column_name)
                    .or_else(|| row.get(&property.name))
                    .cloned()
                    .ok_or_else(|| {
                        format!("missing statistical result column {}", property.column_name)
                    })?;
                if property.data_type == DataType::Decimal
                    && !matches!(value, Value::Null | Value::TypedNull(_))
                {
                    if matches!(value, Value::F64(_)) {
                        return Err(
                            "exact Decimal statistical result arrived as floating point".into()
                        );
                    }
                    value =
                        Value::Decimal(value.try_decimal().ok_or_else(|| {
                            "invalid exact Decimal statistical result".to_owned()
                        })?);
                }
                let path = if let Some(index) = property.column_name.strip_prefix("__teaql_d") {
                    &plan.dimensions[index
                        .parse::<usize>()
                        .map_err(|_| "invalid dimension column")?]
                    .output_path
                } else {
                    let index = property
                        .column_name
                        .strip_prefix("__teaql_m")
                        .ok_or("invalid measure column")?;
                    &plan.measures[index
                        .parse::<usize>()
                        .map_err(|_| "invalid measure column")?]
                    .output_path
                };
                insert(&mut result, path, value)?;
            }
            Ok(CompactRow::from(result))
        })
        .collect()
}
