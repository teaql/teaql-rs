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
        T::from_compact_row(native_row::<T>(
            descriptor,
            value,
            &mut NativeJsonShapes::default(),
        )?)
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
        let rows = array
            .iter()
            .map(|value| native_row::<T>(descriptor, value, &mut shapes))
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter().map(T::from_compact_row).collect()
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
            if name.len() == 1 || !T::supports_dynamic_property_load() {
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
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use teaql_core::{TeaqlEntity, eval::LoadState};

    #[teaql_macros::teaql_entity]
    #[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
    #[teaql(entity = "JsonProbe", indexed_layout)]
    struct JsonProbe {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        name: Option<String>,
        address: Option<String>,
        count: i64,
        active: bool,
        ratio: f64,
        price: Decimal,
        birthday: chrono::NaiveDate,
        happened_at: teaql_core::time::Timestamp,
        raw: serde_json::Value,
        #[teaql(column = "base_url")]
        display_name: Option<String>,
        #[teaql(relation(target = "JsonProbe", local_key = "id", foreign_key = "id"))]
        parent: Option<Box<JsonProbe>>,
        #[teaql(dynamic)]
        properties: BTreeMap<String, Value>,
        #[teaql(skip)]
        __load_state: LoadState,
    }

    impl JsonProbe {
        const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "json-probe-v1";
        const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
            ("id", 0),
            ("version", 1),
            ("name", 2),
            ("address", 3),
            ("count", 4),
            ("active", 5),
            ("ratio", 6),
            ("price", 7),
            ("birthday", 8),
            ("happened_at", 9),
            ("raw", 10),
            ("base_url", 11),
        ];
        const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(
            &'static str,
            &'static str,
            &'static str,
        )] = &[
            ("id", "id", "id"),
            ("version", "version", "version"),
            ("name", "name", "name"),
            ("address", "address", "address"),
            ("count", "count", "count"),
            ("active", "active", "active"),
            ("ratio", "ratio", "ratio"),
            ("price", "price", "price"),
            ("birthday", "birthday", "birthday"),
            ("happened_at", "happened_at", "happened_at"),
            ("raw", "raw", "raw"),
            ("base_url", "display_name", "base_url"),
        ];
    }

    fn context() -> UserContext {
        crate::RuntimeModule::new()
            .entity::<JsonProbe>()
            .into_context()
    }

    #[test]
    fn native_json_keeps_null_omission_falsy_dates_decimal_and_no_mutation() {
        let input = serde_json::json!({"id":1,"version":7,"name":null,"count":0,"active":false,
            "ratio":3.5,"price":"123.450","birthday":"2024-02-29","happened_at":1700000000123_i64,
            "raw":{"literal":"_comment"},"base_url":"","_name":"derived","_nil":null,"_count":0});
        let row = context().decode_json_entity::<JsonProbe>(&input).unwrap();
        assert!(row.is_field_loaded("name"));
        assert!(row.name.is_none());
        assert!(!row.is_field_loaded("address"));
        assert!(row.address.is_none());
        assert_eq!(row.count, 0);
        assert!(!row.active);
        assert_eq!(row.ratio, 3.5);
        assert_eq!(row.price.to_string(), "123.450");
        assert_eq!(row.birthday.to_string(), "2024-02-29");
        assert_eq!(row.happened_at.0, 1700000000123);
        assert_eq!(row.display_name.as_deref(), Some(""));
        assert!(row.dirty_fields().is_none());
        assert!(!row.is_new());
        assert!(!row.is_marked_as_delete());
        assert!(!row.has_pending_dynamic_mutations());
        let json = row.clone().into_json();
        assert_eq!(json["_name"], "derived");
        assert!(json["_nil"].is_null());
        assert_eq!(json["_count"], 0);
        assert!(json.get("address").is_none());
        assert_eq!(json["name"], serde_json::Value::Null);
        let restored = context().decode_json_entity::<JsonProbe>(&json).unwrap();
        assert_eq!(json, restored.into_json());
    }

    #[test]
    fn batch_actual_shapes_share_snapshots_and_keep_values_and_ledgers_private() {
        let rows = context().decode_json_entities::<JsonProbe>(&serde_json::json!([
            {"id":1,"version":7,"name":null},{"version":7,"name":"private","id":1},{"id":2,"version":7}
        ])).unwrap();
        assert!(Arc::ptr_eq(
            &rows[0].loaded_state_snapshot().unwrap(),
            &rows[1].loaded_state_snapshot().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            &rows[0].loaded_state_snapshot().unwrap(),
            &rows[2].loaded_state_snapshot().unwrap()
        ));
        assert!(rows[0].name.is_none());
        assert_eq!(rows[1].name.as_deref(), Some("private"));
        assert!(!rows[2].is_field_loaded("name"));
        let state = rows[0]
            .__teaql_runtime_state_any()
            .unwrap()
            .downcast_ref::<crate::EntityRuntimeState>()
            .unwrap();
        state.set(
            crate::EntityKey::new("JsonProbe", 1),
            "name",
            Value::Text("one private mutation".into()),
        );
        assert!(rows[0].dirty_fields().is_some());
        assert!(rows[1].dirty_fields().is_none());
        assert!(rows[2].dirty_fields().is_none());
    }

    #[test]
    fn aliases_and_unsupported_input_reject_without_exposing_values() {
        let ctx = context();
        for input in [
            serde_json::json!({"unknown":"PRIVATE_LITERAL"}),
            serde_json::json!({"name":42}),
            serde_json::json!({"active":null}),
            serde_json::json!({"count":1.5}),
            serde_json::json!({"id":-1}),
            serde_json::json!({"birthday":"PRIVATE_LITERAL"}),
            serde_json::json!({"_original_values":{}}),
            serde_json::json!({"#name":"PRIVATE_LITERAL"}),
            serde_json::json!({"display_name":"A","base_url":"B"}),
            serde_json::json!({"parent":{"id":1,"name":"PRIVATE_LITERAL"}}),
        ] {
            let error = ctx.decode_json_entity::<JsonProbe>(&input).unwrap_err();
            assert!(!error.to_string().contains("PRIVATE_LITERAL"));
        }
        assert!(
            UserContext::new()
                .decode_json_entity::<JsonProbe>(&serde_json::json!({"id":1}))
                .is_err()
        );
        let mut descriptor = JsonProbe::entity_descriptor();
        descriptor.properties[0].data_type = DataType::Text;
        let wrong = UserContext::new()
            .with_metadata(crate::InMemoryMetadataStore::new().with_entity(descriptor));
        assert!(
            wrong
                .decode_json_entity::<JsonProbe>(&serde_json::json!({"id":"PRIVATE_LITERAL"}))
                .is_err()
        );
    }

    #[test]
    fn native_batches_construct_geometry_per_shape_not_per_row() {
        let descriptor = JsonProbe::entity_descriptor();
        let null = serde_json::json!({"id":1,"version":7,"display_name":null});
        let value = serde_json::json!({"base_url":"private","version":7,"id":2});
        let minimal = serde_json::json!({"id":3,"version":7});
        for count in [1, 100, 10_000] {
            let mut shapes = NativeJsonShapes::default();
            let mut rows = Vec::with_capacity(count);
            for index in 0..count {
                let row = native_row::<JsonProbe>(
                    &descriptor,
                    if index % 2 == 0 { &null } else { &value },
                    &mut shapes,
                )
                .unwrap();
                if let Some(first) = rows.first() {
                    assert!(Arc::ptr_eq(
                        &row.shared_layout(),
                        &CompactRow::shared_layout(first)
                    ));
                }
                rows.push(row);
            }
            assert_eq!(shapes.layouts.len(), 1);
            let other = native_row::<JsonProbe>(&descriptor, &minimal, &mut shapes).unwrap();
            assert_eq!(shapes.layouts.len(), 2);
            assert!(!Arc::ptr_eq(
                &rows[0].shared_layout(),
                &other.shared_layout()
            ));
            drop(shapes);
            let first = JsonProbe::from_compact_row(rows.remove(0)).unwrap();
            assert!(first.is_field_loaded("display_name"));
            assert!(first.display_name.is_none());
            assert!(
                !JsonProbe::from_compact_row(other)
                    .unwrap()
                    .is_field_loaded("display_name")
            );
        }
    }
}
