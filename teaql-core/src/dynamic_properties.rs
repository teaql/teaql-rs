//! Readonly result-schema descriptors. Definitions do not imply property presence.
use crate::{DataType, Value};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, PartialEq, Eq)]
pub struct DynamicPropertyDefinitions {
    types: BTreeMap<String, DataType>,
    hash: u64,
}

impl DynamicPropertyDefinitions {
    pub fn new(fields: impl IntoIterator<Item = (String, DataType)>) -> Result<Arc<Self>, String> {
        let mut types = BTreeMap::new();
        for (name, data_type) in fields {
            if !name.starts_with('_') || name[1..].trim().is_empty() {
                return Err("dynamic property metadata requires '_' names".into());
            }
            if types.insert(name.clone(), data_type).is_some() {
                return Err(format!("duplicate dynamic property definition: {name}"));
            }
        }
        Ok(Self::from_types(types))
    }
    fn from_types(types: BTreeMap<String, DataType>) -> Arc<Self> {
        use std::hash::{Hash, Hasher};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        for (name, data_type) in &types {
            name.hash(&mut hash);
            std::mem::discriminant(data_type).hash(&mut hash);
        }
        Arc::new(Self {
            types,
            hash: hash.finish(),
        })
    }
    pub fn types(&self) -> &BTreeMap<String, DataType> {
        &self.types
    }
    pub fn data_type(&self, name: &str) -> Option<DataType> {
        self.types.get(name).copied()
    }

    pub fn validate_value(&self, name: &str, value: &Value) -> Result<(), String> {
        let Some(declared) = self.data_type(name) else {
            return Ok(());
        };
        let matches = match value {
            Value::Null => true,
            Value::TypedNull(actual) => *actual == declared,
            Value::Bool(_) => declared == DataType::Bool,
            Value::I64(_) => declared == DataType::I64,
            Value::U64(_) => declared == DataType::U64,
            Value::F64(_) => declared == DataType::F64,
            Value::Decimal(_) => declared == DataType::Decimal,
            Value::Text(_) => matches!(declared, DataType::Text | DataType::LargeText),
            Value::Date(_) => declared == DataType::Date,
            Value::Timestamp(_) => declared == DataType::Timestamp,
            Value::Json(json) => {
                json.is_null()
                    || match declared {
                        DataType::Json => true,
                        DataType::Bool => json.is_boolean(),
                        DataType::I64 => json.as_i64().is_some(),
                        DataType::U64 => json.as_u64().is_some(),
                        DataType::F64 => json.is_number(),
                        DataType::Text | DataType::LargeText => json.is_string(),
                        _ => false,
                    }
            }
            Value::Object(_) | Value::List(_) => declared == DataType::Json,
        };
        if matches {
            Ok(())
        } else {
            Err(format!("invalid dynamic property type: {name}"))
        }
    }

    /// Merge schema information only; conflicting declarations never win by row order.
    pub fn merge(left: &Arc<Self>, right: &Arc<Self>) -> Result<Arc<Self>, String> {
        for (name, data_type) in &right.types {
            if left
                .data_type(name)
                .is_some_and(|previous| previous != *data_type)
            {
                return Err(name.clone());
            }
        }
        if right.types.keys().all(|name| left.types.contains_key(name)) {
            return Ok(left.clone());
        }
        if left.types.keys().all(|name| right.types.contains_key(name)) {
            return Ok(right.clone());
        }
        let mut types = left.types.clone();
        types.extend(right.types.clone());
        Ok(Self::from_types(types))
    }
}

impl std::hash::Hash for DynamicPropertyDefinitions {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        hasher.write_u64(self.hash);
    }
}
