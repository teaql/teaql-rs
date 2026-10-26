use super::{
    Error, JsonFieldNamingProfile, Kind, Limits, Model, ObjectLocation, failure, pointer,
    profile_index, scalar,
};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Number, Value as Json};
use std::collections::{BTreeMap, BTreeSet};

pub(super) enum Node {
    Null,
    Bool(bool),
    Number(Number),
    Text(String),
    Array(Vec<Node>),
    Object(Vec<(String, Node)>),
}
struct Seed<'a> {
    remaining: &'a mut usize,
    depth: usize,
    max_depth: usize,
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Node;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Node, D::Error> {
        if *self.remaining == 0 || self.depth > self.max_depth {
            return Err(serde::de::Error::custom("WIRE_JSON_LIMIT"));
        }
        *self.remaining -= 1;
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Node;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("bounded JSON")
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Node, E> {
        Ok(Node::Bool(value))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Node, E> {
        Ok(Node::Number(value.into()))
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Node, E> {
        Ok(Node::Number(value.into()))
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Node, E> {
        Number::from_f64(value)
            .map(Node::Number)
            .ok_or_else(|| E::custom("WIRE_INVALID_JSON"))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Node, E> {
        Ok(Node::Text(value.to_owned()))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Node, E> {
        Ok(Node::Text(value))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut values: A) -> Result<Node, A::Error> {
        let mut result = Vec::new();
        while let Some(value) = values.next_element_seed(Seed {
            remaining: &mut *self.remaining,
            depth: self.depth + 1,
            max_depth: self.max_depth,
        })? {
            result.push(value)
        }
        Ok(Node::Array(result))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut values: A) -> Result<Node, A::Error> {
        let mut result = Vec::new();
        while let Some(key) = values.next_key::<String>()? {
            let value = values.next_value_seed(Seed {
                remaining: &mut *self.remaining,
                depth: self.depth + 1,
                max_depth: self.max_depth,
            })?;
            result.push((key, value));
        }
        Ok(Node::Object(result))
    }
}
pub(super) fn parse(payload: &str, limits: Limits) -> Result<Node, Error> {
    let root = ObjectLocation::root();
    if payload.len() > limits.max_bytes {
        return Err(failure("WIRE_JSON_LIMIT", &root, ""));
    }
    let mut remaining = limits.max_nodes;
    let mut decoder = serde_json::Deserializer::from_str(payload);
    let value = Seed {
        remaining: &mut remaining,
        depth: 0,
        max_depth: limits.max_depth,
    }
    .deserialize(&mut decoder)
    .map_err(|error| {
        failure(
            if error.to_string().contains("WIRE_JSON_LIMIT") {
                "WIRE_JSON_LIMIT"
            } else {
                "WIRE_INVALID_JSON"
            },
            &root,
            "",
        )
    })?;
    decoder
        .end()
        .map_err(|_| failure("WIRE_INVALID_JSON", &root, ""))?;
    Ok(value)
}
impl Node {
    pub(super) fn into_json(self, location: &ObjectLocation, path: &str) -> Result<Json, Error> {
        Ok(match self {
            Self::Null => Json::Null,
            Self::Bool(value) => Json::Bool(value),
            Self::Text(value) => Json::String(value),
            Self::Number(value) => {
                if value
                    .as_i64()
                    .is_some_and(|v| v.unsigned_abs() > 9_007_199_254_740_991)
                    || value.as_u64().is_some_and(|v| v > 9_007_199_254_740_991)
                    || value
                        .as_f64()
                        .is_some_and(|v| v.fract() == 0.0 && v.abs() > 9_007_199_254_740_991_f64)
                {
                    return Err(failure("WIRE_UNTYPED_INTEGER_PRECISION", location, path));
                }
                Json::Number(value)
            }
            Self::Array(values) => Json::Array(
                values
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| {
                        value
                            .into_json(&location.clone().element(i), &pointer(path, &i.to_string()))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Object(values) => {
                let mut result = serde_json::Map::new();
                for (key, value) in values {
                    let child = location.clone().member(&key);
                    let child_path = pointer(path, &key);
                    if result.contains_key(&key) {
                        return Err(failure("WIRE_FIELD_COLLISION", &child, &child_path));
                    }
                    result.insert(key, value.into_json(&child, &child_path)?);
                }
                Json::Object(result)
            }
        })
    }
}
impl Model {
    pub(super) fn read_entity(
        &self,
        entity: &str,
        node: Node,
        profile: JsonFieldNamingProfile,
        location: &ObjectLocation,
        path: &str,
    ) -> Result<Json, Error> {
        let schema = self.schema(entity, location, path)?;
        let Node::Object(values) = node else {
            return Err(failure("WIRE_ENTITY_OBJECT_REQUIRED", location, path));
        };
        let mut seen = BTreeSet::new();
        let mut result = BTreeMap::new();
        for (spelling, node) in values {
            let child_path = pointer(path, &spelling);
            if super::reserved(&spelling) {
                return Err(failure(
                    "WIRE_RUNTIME_STATE",
                    &location.clone().member(&spelling),
                    &child_path,
                ));
            }
            let field = schema.names[profile_index(profile)]
                .get(&spelling)
                .map(|index| &schema.fields[*index]);
            if field.is_none()
                && spelling.starts_with('_')
                && !matches!(
                    spelling.as_str(),
                    "_" | "_comment"
                        | "_dirty_fields"
                        | "_original_values"
                        | "_is_new"
                        | "_is_deleted"
                        | "__load_state"
                        | "__teaql_runtime_state"
                )
            {
                if !seen.insert(spelling.clone()) {
                    return Err(failure(
                        "WIRE_FIELD_COLLISION",
                        &location.clone().member(&spelling),
                        &child_path,
                    ));
                }
                result.insert(
                    spelling.clone(),
                    node.into_json(&location.clone().member(&spelling), &child_path)?,
                );
                continue;
            }
            let field = field.ok_or_else(|| {
                failure(
                    "WIRE_UNKNOWN_FIELD",
                    &location.clone().member(&spelling),
                    &child_path,
                )
            })?;
            let child = location.clone().member(&field.canonical);
            if !seen.insert(field.canonical.clone()) {
                return Err(failure("WIRE_FIELD_COLLISION", &child, &child_path));
            }
            let value = match &field.kind {
                Kind::Reference { target, detail_key } => {
                    if matches!(node, Node::Object(_)) {
                        let detail =
                            self.read_entity(target, node, profile, &child, &child_path)?;
                        let target_schema = self.schema(target, &child, &child_path)?;
                        let id = target_schema
                            .fields
                            .iter()
                            .find(|field| matches!(field.kind, Kind::Id))
                            .and_then(|field| detail.get(&field.record))
                            .ok_or_else(|| {
                                failure("WIRE_REFERENCE_ID_REQUIRED", &child, &child_path)
                            })?;
                        result.insert(field.record.clone(), id.clone());
                        result.insert(detail_key.clone(), detail);
                        continue;
                    }
                    if matches!(node, Node::Null) && field.nullable {
                        // An explicit wire null describes the association,
                        // not merely an unselected FK storage cell. Preserve
                        // its Loaded(null) marker through graph hydration.
                        result.insert(detail_key.clone(), Json::Null);
                        Json::Null
                    } else {
                        Json::Number(
                            scalar::read_id(node)
                                .ok_or_else(|| failure("WIRE_INVALID_TYPE", &child, &child_path))?
                                .into(),
                        )
                    }
                }
                Kind::Many { target } => {
                    let Node::Array(nodes) = node else {
                        return Err(failure("WIRE_INVALID_TYPE", &child, &child_path));
                    };
                    Json::Array(
                        nodes
                            .into_iter()
                            .enumerate()
                            .map(|(index, node)| {
                                self.read_entity(
                                    target,
                                    node,
                                    profile,
                                    &child.clone().element(index),
                                    &pointer(&child_path, &index.to_string()),
                                )
                            })
                            .collect::<Result<_, _>>()?,
                    )
                }
                _ => scalar::read(field, node, &child, &child_path)?.to_json_value(),
            };
            result.insert(field.record.clone(), value);
        }
        let has_graph_input = schema.fields.iter().any(|field| match &field.kind {
            Kind::Reference { detail_key, .. } => result.contains_key(detail_key),
            Kind::Many { .. } => result.contains_key(&field.record),
            _ => false,
        });
        if has_graph_input {
            let identity = schema
                .fields
                .iter()
                .find(|field| matches!(field.kind, Kind::Id))
                .expect("validated schema identity");
            if result
                .get(&identity.record)
                .and_then(Json::as_u64)
                .is_none()
            {
                return Err(failure(
                    "WIRE_ENTITY_ID_REQUIRED",
                    &location.clone().member(&identity.canonical),
                    &pointer(path, &profile.render(&identity.canonical)),
                ));
            }
        }
        Ok(Json::Object(result.into_iter().collect()))
    }
}
