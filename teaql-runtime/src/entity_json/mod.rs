//! Model-owned entity JSON. Never infer logical types from a JSON value or a
//! field's spelling, and never fall back to the lossy presentation serializer.
mod input;
mod output;
mod scalar;

use crate::{JsonFieldNamingProfile, ObjectLocation, UserContext};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use teaql_core::{Entity, EntityJsonTraversal, EntityJsonTraversalError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Id,
    Version,
    Integer,
    Long,
    Float,
    Double,
    Decimal,
    Text,
    Boolean,
    Date,
    Time,
    DateTime,
    Json,
    Reference { target: String, detail_key: String },
    Many { target: String },
}
impl Kind {
    fn description(&self) -> &'static str {
        match self {
            Self::Id => "unsigned 64-bit ID string",
            Self::Version | Self::Long => "signed 64-bit integer string",
            Self::Integer => "signed 32-bit Integer",
            Self::Float => "finite Float",
            Self::Double => "finite Double",
            Self::Decimal => "exact Decimal string",
            Self::Text => "string",
            Self::Boolean => "boolean",
            Self::Date => "calendar date",
            Self::Time => "wall-clock time",
            Self::DateTime => "millisecond RFC3339 instant",
            Self::Json => "JSON value",
            Self::Reference { .. } => "entity ID or entity object",
            Self::Many { .. } => "entity array",
        }
    }
}
#[derive(Debug, Clone)]
pub struct Field {
    canonical: String,
    record: String,
    kind: Kind,
    nullable: bool,
}
impl Field {
    pub fn new(canonical: impl Into<String>, record: impl Into<String>, kind: Kind) -> Self {
        Self {
            canonical: canonical.into(),
            record: record.into(),
            kind,
            nullable: false,
        }
    }
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}
#[derive(Debug, Clone)]
pub struct Definition {
    name: String,
    fields: Vec<Field>,
}
impl Definition {
    pub fn new(name: impl Into<String>, fields: Vec<Field>) -> Self {
        Self {
            name: name.into(),
            fields,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_bytes: usize,
    pub max_depth: usize,
    pub max_nodes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bytes: 16 * 1024 * 1024,
            max_depth: 64,
            max_nodes: 100_000,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: &'static str,
    pub location: ObjectLocation,
    pub instance_path: String,
    pub expected_kind: Option<&'static str>,
}
impl Error {
    pub fn to_wire(&self) -> serde_json::Value {
        let mut value = serde_json::json!({"code":self.code,"location":self.location.wire_segments(),"instancePath":self.instance_path});
        if let Some(expected) = self.expected_kind {
            value["expectedKind"] = expected.into()
        }
        value
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}", self.code, self.instance_path)?;
        if let Some(expected) = self.expected_kind {
            write!(f, " (expected {expected})")?
        }
        Ok(())
    }
}
impl std::error::Error for Error {}
impl serde::Serialize for Error {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.to_wire(), serializer)
    }
}
#[derive(Debug)]
struct Schema {
    fields: Vec<Field>,
    names: [BTreeMap<String, usize>; 3],
    keys: BTreeSet<String>,
}
#[derive(Debug, Clone)]
pub struct Model {
    schemas: Arc<BTreeMap<String, Schema>>,
    limits: Limits,
}
pub(super) fn failure(code: &'static str, location: &ObjectLocation, path: &str) -> Error {
    Error {
        code,
        location: location.clone(),
        instance_path: path.to_owned(),
        expected_kind: None,
    }
}
pub(super) fn pointer(path: &str, key: &str) -> String {
    format!("{}/{}", path, key.replace('~', "~0").replace('/', "~1"))
}
pub(super) fn reserved(key: &str) -> bool {
    matches!(
        key,
        "_comment"
            | "_dirty_fields"
            | "_original_values"
            | "_is_new"
            | "_is_deleted"
            | "__load_state"
            | "__teaql_runtime_state"
    )
}
pub(super) fn profile_index(profile: JsonFieldNamingProfile) -> usize {
    match profile {
        JsonFieldNamingProfile::CamelCase => 0,
        JsonFieldNamingProfile::SnakeCase => 1,
        JsonFieldNamingProfile::PascalCase => 2,
    }
}
pub(super) const PROFILES: [JsonFieldNamingProfile; 3] = [
    JsonFieldNamingProfile::CamelCase,
    JsonFieldNamingProfile::SnakeCase,
    JsonFieldNamingProfile::PascalCase,
];
impl Model {
    pub fn new(definitions: Vec<Definition>) -> Result<Self, Error> {
        let invalid = || failure("WIRE_MODEL", &ObjectLocation::root(), "");
        let mut schemas = BTreeMap::new();
        for definition in definitions {
            if definition.name.trim().is_empty() || schemas.contains_key(&definition.name) {
                return Err(invalid());
            }
            if definition
                .fields
                .iter()
                .filter(|field| matches!(field.kind, Kind::Id))
                .count()
                != 1
                || definition
                    .fields
                    .iter()
                    .filter(|field| matches!(field.kind, Kind::Version))
                    .count()
                    != 1
            {
                return Err(invalid());
            }
            let mut names: [BTreeMap<String, usize>; 3] = std::array::from_fn(|_| BTreeMap::new());
            let mut keys = BTreeSet::new();
            for (index, field) in definition.fields.iter().enumerate() {
                if field.canonical.is_empty()
                    || field.canonical.starts_with(['_', '#'])
                    || field.record.is_empty()
                    || !keys.insert(field.record.clone())
                {
                    return Err(invalid());
                }
                if let Kind::Reference { target, detail_key } = &field.kind {
                    if target.is_empty()
                        || detail_key.is_empty()
                        || !keys.insert(detail_key.clone())
                    {
                        return Err(invalid());
                    }
                }
                if matches!(field.kind, Kind::Id | Kind::Version | Kind::Many { .. })
                    && field.nullable
                {
                    return Err(invalid());
                }
                for profile in PROFILES {
                    for spelling in [profile.render(&field.canonical), field.canonical.clone()] {
                        if names[profile_index(profile)]
                            .insert(spelling, index)
                            .is_some_and(|old| old != index)
                        {
                            return Err(invalid());
                        }
                    }
                }
            }
            schemas.insert(
                definition.name,
                Schema {
                    fields: definition.fields,
                    names,
                    keys,
                },
            );
        }
        for schema in schemas.values() {
            for field in &schema.fields {
                if let Kind::Reference { target, .. } | Kind::Many { target } = &field.kind {
                    if !schemas.contains_key(target) {
                        return Err(invalid());
                    }
                }
            }
        }
        Ok(Self {
            schemas: Arc::new(schemas),
            limits: Limits::default(),
        })
    }
    pub fn with_limits(mut self, limits: Limits) -> Result<Self, Error> {
        if limits.max_bytes == 0
            || limits.max_depth == 0
            || limits.max_depth > 64
            || limits.max_nodes == 0
        {
            return Err(failure("WIRE_MODEL", &ObjectLocation::root(), ""));
        }
        self.limits = limits;
        Ok(self)
    }
    fn schema(&self, name: &str, location: &ObjectLocation, path: &str) -> Result<&Schema, Error> {
        self.schemas
            .get(name)
            .ok_or_else(|| failure("WIRE_UNKNOWN_ENTITY", location, path))
    }
    pub fn serialize<T: Entity>(
        &self,
        entity: &T,
        profile: JsonFieldNamingProfile,
    ) -> Result<String, Error> {
        let mut traversal =
            EntityJsonTraversal::for_strict_wire(self.limits.max_depth, self.limits.max_nodes);
        let record = entity.borrowed_json_values(&mut traversal);
        if let Some(error) = traversal.failure() {
            return Err(failure(
                match error {
                    EntityJsonTraversalError::Cycle => "WIRE_ENTITY_CYCLE",
                    EntityJsonTraversalError::Limit => "WIRE_JSON_LIMIT",
                },
                &ObjectLocation::root(),
                "",
            ));
        }
        let record = record
            .ok_or_else(|| failure("WIRE_ENTITY_CARRIER_REQUIRED", &ObjectLocation::root(), ""))?;
        let mut budget = self.limits.max_nodes;
        let value = self.write_entity(
            T::ENTITY_NAME,
            &record,
            profile,
            &ObjectLocation::root(),
            "",
            0,
            &mut budget,
        )?;
        let output = serde_json::to_string(&value)
            .map_err(|_| failure("WIRE_INVALID_JSON", &ObjectLocation::root(), ""))?;
        if output.len() > self.limits.max_bytes {
            return Err(failure("WIRE_JSON_LIMIT", &ObjectLocation::root(), ""));
        }
        Ok(output)
    }
    /// Presentation hydration, not proof that caller-supplied values are a
    /// verified editing snapshot. Save authorization remains a server concern.
    pub fn deserialize<T: Entity>(
        &self,
        context: &UserContext,
        payload: &str,
        profile: JsonFieldNamingProfile,
    ) -> Result<T, Error> {
        let node = input::parse(payload, self.limits)?;
        let value = self.read_entity(T::ENTITY_NAME, node, profile, &ObjectLocation::root(), "")?;
        context
            .decode_json_entity::<T>(&value)
            .map_err(|_| failure("WIRE_ENTITY_HYDRATION", &ObjectLocation::root(), ""))
    }
    pub fn deserialize_many<T: Entity>(
        &self,
        context: &UserContext,
        payload: &str,
        profile: JsonFieldNamingProfile,
    ) -> Result<Vec<T>, Error> {
        let input::Node::Array(nodes) = input::parse(payload, self.limits)? else {
            return Err(failure(
                "WIRE_ENTITY_ARRAY_REQUIRED",
                &ObjectLocation::root(),
                "",
            ));
        };
        let values = nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| {
                self.read_entity(
                    T::ENTITY_NAME,
                    node,
                    profile,
                    &ObjectLocation::root().element(index),
                    &pointer("", &index.to_string()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        context
            .decode_json_entities::<T>(&serde_json::Value::Array(values))
            .map_err(|_| failure("WIRE_ENTITY_HYDRATION", &ObjectLocation::root(), ""))
    }
}

impl UserContext {
    pub fn encode_entity_json<T: Entity>(
        &self,
        entity: &T,
        profile: JsonFieldNamingProfile,
    ) -> Result<String, Error> {
        self.get_resource::<Model>()
            .ok_or_else(|| failure("WIRE_MODEL_NOT_INSTALLED", &ObjectLocation::root(), ""))?
            .serialize(entity, profile)
    }
    pub fn decode_entity_json<T: Entity>(
        &self,
        payload: &str,
        profile: JsonFieldNamingProfile,
    ) -> Result<T, Error> {
        self.get_resource::<Model>()
            .ok_or_else(|| failure("WIRE_MODEL_NOT_INSTALLED", &ObjectLocation::root(), ""))?
            .deserialize(self, payload, profile)
    }
}

#[cfg(test)]
mod tests;
