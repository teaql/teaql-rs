use super::UserContext;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use teaql_core::{
    CompactRow, CompactRowLayout, DataType, Decimal, Entity, EntityDescriptor, EntityError,
    PropertyDescriptor, Value,
};

impl UserContext {
    /// Presentation hydration against this context's installed model. This does not authorize a save.
    pub fn decode_json_entity<T: Entity>(
        &self,
        value: &serde_json::Value,
    ) -> Result<T, EntityError> {
        let descriptor = self.json_descriptor::<T>()?;
        decode_value::<T>(self, descriptor, value, &mut NativeJsonShapes::default())
    }

    /// Compatible actual projections share immutable geometry; payloads and mutation state stay private.
    pub fn decode_json_entities<T: Entity>(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<T>, EntityError> {
        let descriptor = self.json_descriptor::<T>()?;
        let array = value
            .as_array()
            .ok_or_else(|| error(T::ENTITY_NAME, "expected entity array"))?;
        let mut shapes = NativeJsonShapes::default();
        array
            .iter()
            .map(|value| decode_value::<T>(self, descriptor, value, &mut shapes))
            .collect()
    }

    fn json_descriptor<T: Entity>(&self) -> Result<&EntityDescriptor, EntityError> {
        let installed = self
            .entity(T::ENTITY_NAME)
            .ok_or_else(|| error(T::ENTITY_NAME, "entity is not installed in this context"))?;
        let expected = T::entity_descriptor();
        if installed.properties.len() != expected.properties.len()
            || expected
                .properties
                .iter()
                .any(|property| !installed.properties.contains(property))
            || installed.relations.len() != expected.relations.len()
            || expected
                .relations
                .iter()
                .any(|relation| !installed.relations.contains(relation))
        {
            return Err(error(
                T::ENTITY_NAME,
                "installed metadata does not match the typed entity",
            ));
        }
        T::field_layout()?.ok_or_else(|| {
            error(
                T::ENTITY_NAME,
                "typed JSON requires generated field indexes",
            )
        })?;
        Ok(installed)
    }
}

fn error(entity: &str, message: &str) -> EntityError {
    EntityError::new(entity, format!("JSON_ENTITY_INPUT: {message}"))
}

#[derive(Default)]
struct NativeJsonShapes<'a> {
    // Operation-local borrowed keys. Only a new actual shape copies names into owned geometry.
    layouts: HashMap<Vec<&'a str>, Arc<CompactRowLayout>>,
}

fn native_row<'a, T: Entity>(
    descriptor: &'a EntityDescriptor,
    value: &'a serde_json::Value,
    shapes: &mut NativeJsonShapes<'a>,
) -> Result<CompactRow, EntityError> {
    scalar_row(
        descriptor,
        value,
        shapes,
        T::supports_dynamic_property_load(),
        false,
    )
}

fn scalar_row<'a>(
    descriptor: &'a EntityDescriptor,
    value: &'a serde_json::Value,
    shapes: &mut NativeJsonShapes<'a>,
    supports_dynamic: bool,
    graph: bool,
) -> Result<CompactRow, EntityError> {
    let object = value
        .as_object()
        .ok_or_else(|| error(&descriptor.name, "expected entity object"))?;
    let mut fields = Vec::with_capacity(object.len());
    for (name, value) in object {
        if matches!(
            name.as_str(),
            "_comment"
                | "_dirty_fields"
                | "_original_values"
                | "_is_new"
                | "_is_deleted"
                | "__load_state"
                | "__teaql_runtime_state"
        ) {
            return Err(error(
                &descriptor.name,
                "incoming runtime state is forbidden",
            ));
        }
        if name.starts_with('#') {
            return Err(error(
                &descriptor.name,
                "persistent fields require trusted definitions and storage provenance",
            ));
        }
        if name.starts_with('_') {
            if name.len() == 1 || !supports_dynamic {
                return Err(error(
                    &descriptor.name,
                    "entity lacks a readonly dynamic-property carrier",
                ));
            }
            fields.push((
                name.as_str(),
                if value.is_null() {
                    Value::Null
                } else {
                    Value::Json(value.clone())
                },
            ));
            continue;
        }
        if descriptor
            .relations
            .iter()
            .any(|relation| relation.name == *name)
            && (value.is_object() || value.is_array() || value.is_null())
        {
            if graph {
                continue;
            }
            return Err(error(
                &descriptor.name,
                "relation input requires graph-aware hydration",
            ));
        }
        let property = descriptor
            .properties
            .iter()
            .find(|property| property.name == *name || property.column_name == *name)
            .ok_or_else(|| error(&descriptor.name, &format!("unknown model field: {name}")))?;
        let converted = scalar(&descriptor.name, property, value)?;
        fields.push((property.name.as_str(), converted));
    }
    fields.sort_by(|left, right| left.0.cmp(right.0));
    for pair in fields.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(error(
                &descriptor.name,
                &format!("duplicate aliases for field: {}", pair[0].0),
            ));
        }
    }
    let (columns, values): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
    let layout = if let Some(layout) = shapes.layouts.get(&columns) {
        layout.clone()
    } else {
        let owned: Vec<String> = columns.iter().map(|name| (*name).to_owned()).collect();
        let layout = CompactRowLayout::new(owned.into());
        shapes.layouts.insert(columns, layout.clone());
        layout
    };
    Ok(CompactRow::with_layout(layout, values))
}

#[path = "json/graph.rs"]
mod graph;
use graph::decode_value;

fn scalar(
    entity: &str,
    property: &PropertyDescriptor,
    value: &serde_json::Value,
) -> Result<Value, EntityError> {
    if value.is_null() && property.nullable {
        return Ok(Value::Null);
    }
    let invalid = || error(entity, &format!("invalid field type: {}", property.name));
    Ok(match property.data_type {
        DataType::Bool => Value::Bool(value.as_bool().ok_or_else(invalid)?),
        DataType::I64 => Value::I64(value.as_i64().ok_or_else(invalid)?),
        DataType::U64 => Value::U64(value.as_u64().ok_or_else(invalid)?),
        DataType::F64 => Value::F64(
            value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(invalid)?,
        ),
        DataType::Decimal => {
            let text = match value {
                serde_json::Value::String(text) => text.clone(),
                serde_json::Value::Number(number) => number.to_string(),
                _ => return Err(invalid()),
            };
            Value::Decimal(Decimal::from_str(&text).map_err(|_| invalid())?)
        }
        DataType::Text | DataType::LargeText => {
            Value::Text(value.as_str().ok_or_else(invalid)?.to_owned())
        }
        DataType::Json => {
            if value.is_null() {
                return Err(invalid());
            }
            Value::Json(value.clone())
        }
        DataType::Date => Value::Date(
            chrono::NaiveDate::parse_from_str(value.as_str().ok_or_else(invalid)?, "%Y-%m-%d")
                .map_err(|_| invalid())?,
        ),
        DataType::Timestamp => {
            let millis = if let Some(millis) = value.as_i64() {
                millis
            } else {
                chrono::DateTime::parse_from_rfc3339(value.as_str().ok_or_else(invalid)?)
                    .map_err(|_| invalid())?
                    .timestamp_millis()
            };
            Value::Timestamp(teaql_core::time::Timestamp(millis))
        }
    })
}

#[cfg(test)]
#[path = "json/tests.rs"]
mod tests;
