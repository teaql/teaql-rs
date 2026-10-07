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
    let (view, row) = install_edges(context, parsed, &root, &mut graph)?;
    root.freeze_graph(graph)
        .map_err(|_| error(&descriptor.name, "graph could not be frozen"))?;
    T::from_compact_row_with_context(row, &root.with_json_view(view))
}

#[derive(Default)]
struct GraphJsonPlan<'a> {
    nodes: usize,
    capabilities: HashMap<&'a str, bool>,
    native_views: HashMap<(&'a str, u64), NativeViews<'a>>,
    relation_shapes: teaql_core::RelationShapeCache,
}
struct NativeViews<'a> {
    first: &'a serde_json::Value,
    // Unique identities need no separate vector allocation.
    additional: Vec<&'a serde_json::Value>,
}
struct ParsedGraph<'a> {
    view: u64,
    descriptor: &'a EntityDescriptor,
    row: CompactRow,
    edges: Vec<ParsedEdge<'a>>,
}
struct ParsedEdge<'a> {
    relation: &'a teaql_core::RelationDescriptor,
    owner_id: u64,
    children: Vec<ParsedGraph<'a>>,
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
    let view = plan.nodes as u64;
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
        let values: &[serde_json::Value] = if relation.many {
            payload
                .as_array()
                .ok_or_else(|| error(&descriptor.name, "to-many relation requires an array"))?
                .as_slice()
        } else if payload.is_null() {
            &[]
        } else if payload.is_object() {
            std::slice::from_ref(payload)
        } else {
            return Err(error(
                &descriptor.name,
                "to-one relation requires object or null",
            ));
        };
        if values.len() > 100_000 - plan.nodes {
            return Err(error(
                &descriptor.name,
                "graph input exceeds depth or node limit",
            ));
        }
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
        row.mark_relation_loaded(&relation.name, &mut plan.relation_shapes);
        edges.push(ParsedEdge {
            relation,
            owner_id,
            children,
        });
    }
    if let Some(id) = descriptor
        .properties
        .iter()
        .find(|p| p.is_id)
        .and_then(|p| row.get(&p.name))
        .and_then(Value::try_u64)
    {
        let entry = plan.native_views.entry((descriptor.name.as_str(), id));
        if let std::collections::hash_map::Entry::Occupied(mut entry) = entry {
            let previous = entry.get_mut();
            for input in std::iter::once(previous.first).chain(previous.additional.iter().copied())
            {
                let prior =
                    native_view_with_inferred_keys(context, descriptor, input, shapes, supports)?;
                for property in &descriptor.properties {
                    if let (Some(left), Some(right)) =
                        (prior.get(&property.name), row.get(&property.name))
                        && !same_key(left, right)
                    {
                        return Err(error(
                            &descriptor.name,
                            "conflicting native values for one identity",
                        ));
                    }
                }
            }
            previous.additional.push(value);
        } else {
            entry.or_insert(NativeViews {
                first: value,
                additional: Vec::new(),
            });
        }
    }
    Ok(ParsedGraph {
        view,
        descriptor,
        row,
        edges,
    })
}

// Only repeated identities take this path. Reconstruct scalar join keys without
// reparsing their nested graph or cloning relation payloads.
fn native_view_with_inferred_keys<'a>(
    context: &'a UserContext,
    descriptor: &'a EntityDescriptor,
    value: &'a serde_json::Value,
    shapes: &mut NativeJsonShapes<'a>,
    supports: bool,
) -> Result<CompactRow, EntityError> {
    let mut row = scalar_row(descriptor, value, shapes, supports, true)?;
    for relation in descriptor.relations.iter().filter(|r| !r.many) {
        let Some(local) = descriptor
            .properties
            .iter()
            .find(|p| p.name == relation.local_key || p.column_name == relation.local_key)
        else {
            continue;
        };
        if local.is_id || row.get(&local.name).is_some() {
            continue;
        }
        let Some(payload) = value.get(&relation.name) else {
            continue;
        };
        if payload.is_null() {
            row.insert(local.name.clone(), Value::Null);
            continue;
        }
        if !payload.is_object() {
            continue;
        }
        let target = context
            .entity(&relation.target_entity)
            .ok_or_else(|| error(&descriptor.name, "relation target is not installed"))?;
        let target_row = scalar_row(
            target,
            payload,
            shapes,
            context.json_graph_capability(target)?,
            true,
        )?;
        if let Some(foreign) = target
            .properties
            .iter()
            .find(|p| p.name == relation.foreign_key || p.column_name == relation.foreign_key)
            && let Some(key) = target_row.get(&foreign.name)
        {
            row.insert(local.name.clone(), key.clone());
        }
    }
    Ok(row)
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
) -> Result<(u64, CompactRow), EntityError> {
    let owner = crate::EntityRuntimeState::fresh_with_weak_graph(root).with_json_view(parsed.view);
    for edge in parsed.edges {
        let mut rows = Vec::with_capacity(edge.children.len());
        for child in edge.children {
            rows.push(install_edges(context, child, root, graph)?);
        }
        context.entity_graph_decoders.decode_json_edge(
            &edge.relation.target_entity,
            rows,
            &owner,
            graph,
            crate::registry::JsonEdgeRequest {
                owner: &parsed.descriptor.name,
                id: edge.owner_id,
                relation: &edge.relation.name,
                many: edge.relation.many,
            },
        )?;
    }
    Ok((parsed.view, parsed.row))
}
