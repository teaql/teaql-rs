use super::{
    Error, Field, JsonFieldNamingProfile, Kind, Model, ObjectLocation, failure, pointer, scalar,
};
use serde_json::Value as Json;
use std::collections::BTreeMap;
use teaql_core::Value;
fn key(value: &Value) -> Option<u64> {
    match value {
        Value::U64(v) => Some(*v),
        Value::I64(v) if *v >= 0 => Some(*v as u64),
        _ => None,
    }
}
impl Model {
    fn tick(
        &self,
        depth: usize,
        budget: &mut usize,
        location: &ObjectLocation,
        path: &str,
    ) -> Result<(), Error> {
        if depth > self.limits.max_depth || *budget == 0 {
            return Err(failure("WIRE_JSON_LIMIT", location, path));
        }
        *budget -= 1;
        Ok(())
    }
    pub(super) fn write_entity(
        &self,
        entity: &str,
        record: &BTreeMap<String, Value>,
        profile: JsonFieldNamingProfile,
        location: &ObjectLocation,
        path: &str,
        depth: usize,
        budget: &mut usize,
    ) -> Result<Json, Error> {
        self.tick(depth, budget, location, path)?;
        let schema = self.schema(entity, location, path)?;
        let mut output = serde_json::Map::new();
        for name in record.keys() {
            if super::reserved(name) {
                return Err(failure(
                    "WIRE_RUNTIME_STATE",
                    &location.clone().member(name),
                    &pointer(path, name),
                ));
            }
            if !schema.keys.contains(name) && (!name.starts_with('_') || name.len() == 1) {
                return Err(failure(
                    "WIRE_UNKNOWN_FIELD",
                    &location.clone().member(name),
                    &pointer(path, name),
                ));
            }
        }
        for field in &schema.fields {
            let spelling = profile.render(&field.canonical);
            let child = location.clone().member(&field.canonical);
            let child_path = pointer(path, &spelling);
            let mut value = record.get(&field.record);
            if let Kind::Reference { target, detail_key } = &field.kind {
                if let Some(detail) = record.get(detail_key) {
                    match detail {
                        Value::Object(detail) => {
                            let target_schema = self.schema(target, &child, &child_path)?;
                            let id = target_schema
                                .fields
                                .iter()
                                .find(|field| matches!(field.kind, Kind::Id))
                                .and_then(|field| detail.get(&field.record))
                                .and_then(key)
                                .ok_or_else(|| {
                                    failure("WIRE_REFERENCE_ID_REQUIRED", &child, &child_path)
                                })?;
                            if value.is_some_and(|value| key(value) != Some(id)) {
                                return Err(failure(
                                    "WIRE_REFERENCE_ID_MISMATCH",
                                    &child,
                                    &child_path,
                                ));
                            }
                            output.insert(
                                spelling,
                                self.write_entity(
                                    target,
                                    detail,
                                    profile,
                                    &child,
                                    &child_path,
                                    depth + 1,
                                    budget,
                                )?,
                            );
                            continue;
                        }
                        Value::Null => {
                            if value.is_none() {
                                value = Some(detail)
                            }
                        }
                        _ => return Err(failure("WIRE_INVALID_TYPE", &child, &child_path)),
                    }
                }
            }
            let Some(value) = value else { continue };
            let json = if let Kind::One { target } = &field.kind {
                match value {
                    Value::Null if field.nullable => {
                        self.tick(depth + 1, budget, &child, &child_path)?;
                        Json::Null
                    }
                    Value::Object(record) => self.write_entity(
                        target,
                        record,
                        profile,
                        &child,
                        &child_path,
                        depth + 1,
                        budget,
                    )?,
                    _ => return Err(failure("WIRE_ENTITY_OBJECT_REQUIRED", &child, &child_path)),
                }
            } else if let Kind::Many { target } = &field.kind {
                self.tick(depth + 1, budget, &child, &child_path)?;
                let Value::List(values) = value else {
                    return Err(failure("WIRE_INVALID_TYPE", &child, &child_path));
                };
                let mut items = Vec::with_capacity(values.len().min(*budget));
                for (index, value) in values.iter().enumerate() {
                    let index_location = child.clone().element(index);
                    let index_path = pointer(&child_path, &index.to_string());
                    let Value::Object(record) = value else {
                        return Err(failure(
                            "WIRE_ENTITY_OBJECT_REQUIRED",
                            &index_location,
                            &index_path,
                        ));
                    };
                    items.push(self.write_entity(
                        target,
                        record,
                        profile,
                        &index_location,
                        &index_path,
                        depth + 2,
                        budget,
                    )?);
                }
                Json::Array(items)
            } else if matches!(field.kind, Kind::Json) {
                let json = scalar::write(field, value, &child, &child_path)?;
                self.check_json(&json, &child, &child_path, depth + 1, budget)?;
                json
            } else {
                self.tick(depth + 1, budget, &child, &child_path)?;
                if let Kind::Reference { .. } = &field.kind {
                    let reference = Field::new(&field.canonical, &field.record, Kind::Id)
                        .nullable(field.nullable);
                    scalar::write(&reference, value, &child, &child_path)?
                } else {
                    scalar::write(field, value, &child, &child_path)?
                }
            };
            output.insert(spelling, json);
        }
        for (name, value) in record.iter().filter(|(name, _)| name.starts_with('_')) {
            let location = location.clone().member(name);
            let path = pointer(path, name);
            let json = self.dynamic(value, &location, &path, depth + 1, budget)?;
            output.insert(name.clone(), json);
        }
        Ok(Json::Object(output))
    }
    fn check_json(
        &self,
        value: &Json,
        location: &ObjectLocation,
        path: &str,
        depth: usize,
        budget: &mut usize,
    ) -> Result<(), Error> {
        self.tick(depth, budget, location, path)?;
        match value {
            Json::Number(value)
                if value
                    .as_f64()
                    .is_some_and(|v| v.fract() == 0.0 && v.abs() > 9_007_199_254_740_991_f64) =>
            {
                return Err(failure("WIRE_UNTYPED_INTEGER_PRECISION", location, path));
            }
            Json::String(value) if value.len() > self.limits.max_bytes => {
                return Err(failure("WIRE_JSON_LIMIT", location, path));
            }
            Json::Array(values) => {
                for (i, value) in values.iter().enumerate() {
                    self.check_json(
                        value,
                        &location.clone().element(i),
                        &pointer(path, &i.to_string()),
                        depth + 1,
                        budget,
                    )?
                }
            }
            Json::Object(values) => {
                for (name, value) in values {
                    self.check_json(
                        value,
                        &location.clone().member(name),
                        &pointer(path, name),
                        depth + 1,
                        budget,
                    )?
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn dynamic(
        &self,
        value: &Value,
        location: &ObjectLocation,
        path: &str,
        depth: usize,
        budget: &mut usize,
    ) -> Result<Json, Error> {
        match value {
            Value::Null
            | Value::Bool(_)
            | Value::Text(_)
            | Value::I64(_)
            | Value::U64(_)
            | Value::Json(_) => {
                let json = value.to_json_value();
                self.check_json(&json, location, path, depth, budget)?;
                Ok(json)
            }
            Value::F64(value) if value.is_finite() => {
                let json = Json::Number(
                    serde_json::Number::from_f64(*value)
                        .ok_or_else(|| failure("WIRE_INVALID_TYPE", location, path))?,
                );
                self.check_json(&json, location, path, depth, budget)?;
                Ok(json)
            }
            Value::List(values) => {
                self.tick(depth, budget, location, path)?;
                Ok(Json::Array(
                    values
                        .iter()
                        .enumerate()
                        .map(|(i, value)| {
                            self.dynamic(
                                value,
                                &location.clone().element(i),
                                &pointer(path, &i.to_string()),
                                depth + 1,
                                budget,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                ))
            }
            Value::Object(values) => {
                self.tick(depth, budget, location, path)?;
                Ok(Json::Object(
                    values
                        .iter()
                        .map(|(name, value)| {
                            Ok((
                                name.clone(),
                                self.dynamic(
                                    value,
                                    &location.clone().member(name),
                                    &pointer(path, name),
                                    depth + 1,
                                    budget,
                                )?,
                            ))
                        })
                        .collect::<Result<_, Error>>()?,
                ))
            }
            _ => Err(failure("WIRE_DYNAMIC_TYPE_REQUIRED", location, path)),
        }
    }
}
