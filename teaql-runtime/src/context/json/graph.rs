use super::{NativeJsonShapes, UserContext, error, native_row, scalar, scalar_row};
use std::collections::HashMap;
use teaql_core::{CompactRow, Entity, EntityDescriptor, EntityError, Value};

fn has_relation_input(descriptor: &EntityDescriptor, value: &serde_json::Value) -> bool {
    descriptor.relations.iter().any(|relation| {
        value
            .get(&relation.name)
            .is_some_and(|value| value.is_object() || value.is_array() || value.is_null())
    })
}

pub(super) fn decode_value<'a, T: Entity>(
    context: &'a UserContext,
    descriptor: &'a EntityDescriptor,
    value: &'a serde_json::Value,
    shapes: &mut NativeJsonShapes<'a>,
) -> Result<T, EntityError> {
    if !has_relation_input(descriptor, value) {
        return T::from_compact_row(native_row::<T>(descriptor, value, shapes)?);
    }
    let mut plan = GraphJsonPlan::default();
    plan.capabilities.insert(
        descriptor.name.as_str(),
        T::supports_dynamic_property_load(),
    );
    let parsed = parse_graph(context, descriptor, value, shapes, &mut plan, 0)?;
    let root = crate::EntityRuntimeState::default();
    let mut graph = crate::EntityGraphBuilder::for_json_read();
    let row = install_edges(context, parsed, &root, &mut graph)?;
    root.freeze_graph(graph)
        .map_err(|_| error(&descriptor.name, "graph could not be frozen"))?;
    T::from_compact_row_with_context(row, &root)
}

#[derive(Default)]
struct GraphJsonPlan<'a> {
    nodes: usize,
    capabilities: HashMap<&'a str, bool>,
    edges: HashMap<(&'a str, u64, &'a str), &'a serde_json::Value>,
    option_views: HashMap<(&'a str, u64), &'a serde_json::Value>,
    relation_shapes: teaql_core::RelationShapeCache,
}
struct ParsedGraph<'a> {
    descriptor: &'a EntityDescriptor,
    row: CompactRow,
    edges: Vec<ParsedEdge<'a>>,
}
struct ParsedEdge<'a> {
    relation: &'a teaql_core::RelationDescriptor,
    owner_id: u64,
    children: Vec<ParsedGraph<'a>>,
    repeated: bool,
}

fn parse_graph<'a>(
    context: &'a UserContext,
    descriptor: &'a EntityDescriptor,
    value: &'a serde_json::Value,
    shapes: &mut NativeJsonShapes<'a>,
    plan: &mut GraphJsonPlan<'a>,
    depth: usize,
) -> Result<ParsedGraph<'a>, EntityError> {
    plan.nodes += 1;
    if depth > 64 || plan.nodes > 100_000 {
        return Err(error(
            &descriptor.name,
            "graph input exceeds depth or node limit",
        ));
    }
    let supports = if let Some(supports) = plan.capabilities.get(descriptor.name.as_str()) {
        *supports
    } else {
        let supports = context.json_graph_capability(descriptor)?;
        plan.capabilities.insert(&descriptor.name, supports);
        supports
    };
    let mut row = scalar_row(descriptor, value, shapes, supports, true)?;
    let mut edges = Vec::new();
    for relation in &descriptor.relations {
        let Some(payload) = value
            .get(&relation.name)
            .filter(|v| v.is_object() || v.is_array() || v.is_null())
        else {
            continue;
        };
        let identity = descriptor
            .properties
            .iter()
            .find(|property| property.is_id)
            .ok_or_else(|| error(&descriptor.name, "graph owner lacks an identity definition"))?;
        let owner_id = row
            .get(&identity.name)
            .and_then(Value::try_u64)
            .ok_or_else(|| {
                error(
                    &descriptor.name,
                    "graph relation requires an explicitly loaded owner id",
                )
            })?;
        let target = context
            .entity(&relation.target_entity)
            .ok_or_else(|| error(&descriptor.name, "relation target is not installed"))?;
        // Validate target registration even for an empty relation.
        if !plan.capabilities.contains_key(target.name.as_str()) {
            let capability = context.json_graph_capability(target)?;
            plan.capabilities.insert(&target.name, capability);
        }
        let values: Vec<&serde_json::Value> = if relation.many {
            payload
                .as_array()
                .ok_or_else(|| error(&descriptor.name, "to-many relation requires an array"))?
                .iter()
                .collect()
        } else if payload.is_null() {
            Vec::new()
        } else if payload.is_object() {
            vec![payload]
        } else {
            return Err(error(
                &descriptor.name,
                "to-one relation requires object or null",
            ));
        };
        let mut children = Vec::with_capacity(values.len());
        for value in values {
            if !value.is_object() {
                return Err(error(
                    &descriptor.name,
                    "relation item must be an entity object",
                ));
            }
            children.push(parse_graph(
                context,
                target,
                value,
                shapes,
                plan,
                depth + 1,
            )?);
        }
        let local = descriptor
            .properties
            .iter()
            .find(|p| p.name == relation.local_key || p.column_name == relation.local_key)
            .ok_or_else(|| {
                error(
                    &descriptor.name,
                    "relation local key is not a model property",
                )
            })?;
        if !relation.many && !local.is_id {
            if let Some(child) = children.first() {
                let identity = target
                    .properties
                    .iter()
                    .find(|p| p.is_id)
                    .and_then(|p| child.row.get(&p.name))
                    .and_then(Value::try_u64);
                if let Some(id) = identity {
                    let key = (target.name.as_str(), id);
                    if let Some(previous) = plan.option_views.get(&key) {
                        if *previous != payload {
                            return Err(error(
                                &target.name,
                                "incompatible target views require separate JSON root graphs",
                            ));
                        }
                    } else {
                        plan.option_views.insert(key, payload);
                    }
                }
            }
            let supplied = children
                .first()
                .map(|child| {
                    let foreign = target
                        .properties
                        .iter()
                        .find(|p| {
                            p.name == relation.foreign_key || p.column_name == relation.foreign_key
                        })
                        .ok_or_else(|| {
                            error(&target.name, "relation foreign key is not a model property")
                        })?;
                    child
                        .row
                        .get(&foreign.name)
                        .cloned()
                        .ok_or_else(|| error(&target.name, "relation target key was not loaded"))
                })
                .transpose()?
                .unwrap_or(Value::Null);
            if let Some(existing) = row.get(&local.name) {
                if !same_key(existing, &supplied) {
                    return Err(error(
                        &descriptor.name,
                        "relation target conflicts with the loaded foreign key",
                    ));
                }
            } else {
                let supplied = scalar(&descriptor.name, local, &supplied.to_json_value())?;
                row.insert(local.name.clone(), supplied);
            }
        }
        if relation.many || local.is_id {
            let foreign = target
                .properties
                .iter()
                .find(|p| p.name == relation.foreign_key || p.column_name == relation.foreign_key)
                .ok_or_else(|| {
                    error(&target.name, "relation foreign key is not a model property")
                })?;
            if let Some(expected) = row.get(&local.name) {
                for child in &children {
                    if let Some(actual) = child.row.get(&foreign.name)
                        && !same_key(expected, actual)
                    {
                        return Err(error(
                            &descriptor.name,
                            "relation child conflicts with the loaded join key",
                        ));
                    }
                }
            }
        }
        let key = (descriptor.name.as_str(), owner_id, relation.name.as_str());
        let repeated = if let Some(previous) = plan.edges.get(&key) {
            if *previous != payload {
                return Err(error(
                    &descriptor.name,
                    "conflicting views of the same relation in one graph",
                ));
            }
            true
        } else {
            plan.edges.insert(key, payload);
            false
        };
        row.mark_relation_loaded(&relation.name, &mut plan.relation_shapes);
        edges.push(ParsedEdge {
            relation,
            owner_id,
            children,
            repeated,
        });
    }
    Ok(ParsedGraph {
        descriptor,
        row,
        edges,
    })
}

fn same_key(left: &Value, right: &Value) -> bool {
    left == right
        || (matches!(left, Value::I64(_) | Value::U64(_))
            && matches!(right, Value::I64(_) | Value::U64(_))
            && left.try_u64().is_some()
            && left.try_u64() == right.try_u64())
}

fn install_edges(
    context: &UserContext,
    parsed: ParsedGraph<'_>,
    root: &crate::EntityRuntimeState,
    graph: &mut crate::EntityGraphBuilder,
) -> Result<CompactRow, EntityError> {
    for edge in parsed.edges {
        let mut rows = Vec::with_capacity(edge.children.len());
        for child in edge.children {
            rows.push(install_edges(context, child, root, graph)?);
        }
        if edge.repeated {
            continue;
        }
        if edge.relation.many {
            let list = teaql_core::SmartList {
                data: rows,
                is_loaded: true,
                ..Default::default()
            };
            context.decode_compact_smart_list_into_graph(
                &edge.relation.target_entity,
                list,
                root,
                graph,
                &parsed.descriptor.name,
                edge.owner_id,
                &edge.relation.name,
            )?;
        } else {
            context.decode_compact_entity_option_into_graph(
                &edge.relation.target_entity,
                rows,
                root,
                graph,
                &parsed.descriptor.name,
                edge.owner_id,
                &edge.relation.name,
            )?;
        }
    }
    Ok(parsed.row)
}
