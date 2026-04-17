use std::collections::BTreeMap;

use crate::{EntityDescriptor, Record, Value, record_to_json_value};

pub trait TeaqlEntity {
    fn entity_descriptor() -> EntityDescriptor;

    fn register_into(store: &mut impl EntityDescriptorStore) {
        store.register_descriptor(Self::entity_descriptor());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityError {
    pub entity: String,
    pub message: String,
}

impl EntityError {
    pub fn new(entity: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for EntityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.entity, self.message)
    }
}

impl std::error::Error for EntityError {}

pub trait Entity: TeaqlEntity + Sized {
    fn from_record(record: Record) -> Result<Self, EntityError>;
    fn into_record(self) -> Record;

    fn into_json(self) -> serde_json::Value {
        record_to_json_value(&self.into_record())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BaseEntityData {
    pub id: Option<u64>,
    pub version: i64,
    pub dynamic: BTreeMap<String, serde_json::Value>,
}

impl BaseEntityData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: u64) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_version(mut self, version: i64) -> Self {
        self.version = version;
        self
    }

    pub fn with_dynamic(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.dynamic.insert(key.into(), value.into());
        self
    }

    pub fn dynamic(&self, key: &str) -> Option<&serde_json::Value> {
        self.dynamic.get(key)
    }

    pub fn put_dynamic(
        &mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Option<serde_json::Value> {
        self.dynamic.insert(key.into(), value.into())
    }

    pub fn remove_dynamic(&mut self, key: &str) -> Option<serde_json::Value> {
        self.dynamic.remove(key)
    }

    pub fn to_record(&self) -> Record {
        let mut record = Record::new();
        if let Some(id) = self.id {
            record.insert("id".to_owned(), Value::U64(id));
        }
        record.insert("version".to_owned(), Value::I64(self.version));
        for (key, value) in &self.dynamic {
            record.insert(key.clone(), Value::Json(value.clone()));
        }
        record
    }

    pub fn from_record(record: &Record) -> Result<Self, EntityError> {
        let id = match record.get("id") {
            Some(Value::U64(v)) => Some(*v),
            Some(Value::I64(v)) if *v >= 0 => Some(*v as u64),
            Some(Value::Null) | None => None,
            other => {
                return Err(EntityError::new(
                    "BaseEntity",
                    format!("invalid id field: {other:?}"),
                ));
            }
        };

        let version = match record.get("version") {
            Some(Value::I64(v)) => *v,
            Some(Value::Null) | None => 0,
            other => {
                return Err(EntityError::new(
                    "BaseEntity",
                    format!("invalid version field: {other:?}"),
                ));
            }
        };

        let dynamic = record
            .iter()
            .filter(|(key, _)| key.as_str() != "id" && key.as_str() != "version")
            .map(|(key, value)| (key.clone(), value.to_json_value()))
            .collect();

        Ok(Self {
            id,
            version,
            dynamic,
        })
    }
}

pub trait BaseEntity: Entity {
    fn base(&self) -> &BaseEntityData;
    fn base_mut(&mut self) -> &mut BaseEntityData;

    fn id(&self) -> Option<u64> {
        self.base().id
    }

    fn set_id(&mut self, id: u64) {
        self.base_mut().id = Some(id);
    }

    fn version_value(&self) -> i64 {
        self.base().version
    }

    fn set_version(&mut self, version: i64) {
        self.base_mut().version = version;
    }

    fn dynamic(&self, key: &str) -> Option<&serde_json::Value> {
        self.base().dynamic(key)
    }

    fn put_dynamic(
        &mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Option<serde_json::Value> {
        self.base_mut().put_dynamic(key, value)
    }
}

pub trait IdentifiableEntity: Entity {
    fn id_value(&self) -> Value;
}

pub trait VersionedEntity: Entity {
    fn version(&self) -> i64;
}

pub trait EntityDescriptorStore {
    fn register_descriptor(&mut self, descriptor: EntityDescriptor);
}

#[macro_export]
macro_rules! register_entities {
    ($store:expr, $($entity:ty),+ $(,)?) => {{
        $(
            <$entity as $crate::TeaqlEntity>::register_into($store);
        )+
    }};
}
