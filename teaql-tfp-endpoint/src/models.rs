use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use teaql_core::{
    Aggregate, AggregateFunction, DeleteCommand, Expr, InsertCommand, OrderBy, Record,
    RecoverCommand, SelectQuery, SortDirection, TraceNode, UpdateCommand, Value,
};
use teaql_data_service::MutationRequest;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TfpOrderBy {
    #[serde(alias = "f")]
    pub field: String,
    #[serde(default)]
    pub expr: Option<JsonValue>,
    #[serde(alias = "d")]
    pub direction: String,
}

impl TfpOrderBy {
    pub fn to_core(&self) -> Result<OrderBy, String> {
        let dir = match self.direction.as_str() {
            value if value.eq_ignore_ascii_case("asc") => SortDirection::Asc,
            value if value.eq_ignore_ascii_case("desc") => SortDirection::Desc,
            _ => return Err(format!("Unsupported order direction: {}", self.direction)),
        };
        Ok(OrderBy {
            field: self.field.clone(),
            expr: None, // TODO: parse expression if needed
            direction: dir,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TfpSelectQuery {
    pub entity: String,
    pub filter_condition: Option<JsonValue>,
    #[serde(default, rename = "_filters")]
    pub filters: Vec<JsonValue>,
    #[serde(alias = "_limit")]
    pub limit_value: Option<usize>,
    #[serde(alias = "_offset")]
    pub offset_value: Option<usize>,
    #[serde(default, alias = "_orderBy")]
    pub order_items: Vec<TfpOrderBy>,
    #[serde(default)]
    pub select_items: Vec<String>,
    #[serde(default, alias = "_groupBy")]
    pub group_by_items: Vec<String>,
    #[serde(default, alias = "_aggregates")]
    pub aggregate_items: Vec<TfpAggregateItem>,
    pub comment_text: Option<String>,
    #[serde(default, rename = "_comment")]
    pub generated_comment: Option<String>,
    #[serde(default)]
    pub purpose_text: Option<String>,
    #[serde(default, rename = "_purpose")]
    pub generated_purpose: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TfpAggregateItem {
    #[serde(alias = "func")]
    pub function: String,
    pub field: String,
    #[serde(alias = "retName")]
    pub alias: String,
}

impl TfpSelectQuery {
    pub fn map_fields(
        &mut self,
        fields: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        if let Some(filter) = &mut self.filter_condition {
            map_filter_fields(filter, fields)?;
        }
        for filter in &mut self.filters {
            map_filter_fields(filter, fields)?;
        }
        for order in &mut self.order_items {
            order.field = fields
                .get(&order.field)
                .ok_or_else(|| format!("Unknown field: {}", order.field))?
                .clone();
        }
        for field in &mut self.group_by_items {
            *field = fields
                .get(field)
                .ok_or_else(|| format!("Unknown field: {field}"))?
                .clone();
        }
        for aggregate in &mut self.aggregate_items {
            if aggregate.field != "*" {
                aggregate.field = fields
                    .get(&aggregate.field)
                    .ok_or_else(|| format!("Unknown field: {}", aggregate.field))?
                    .clone();
            }
        }
        Ok(())
    }

    pub fn to_core(&self) -> Result<SelectQuery, String> {
        let mut filters = Vec::new();
        if let Some(filter) = &self.filter_condition {
            filters.push(parse_json_filter(filter)?);
        }
        for filter in &self.filters {
            filters.push(parse_json_filter(filter)?);
        }

        let mut q = SelectQuery::new(&self.entity);
        q.filter = combine_and(filters);

        if let Some(l) = self.limit_value
            && l > 0
        {
            q = q.limit(l as u64);
        }

        if let Some(o) = self.offset_value
            && o > 0
        {
            q = q.offset(o as u64);
        }

        for o in &self.order_items {
            q.order_by.push(o.to_core()?);
        }

        if !self.select_items.is_empty() {
            q.projection.extend(self.select_items.iter().cloned());
        }

        if !self.group_by_items.is_empty() {
            q.group_by = self.group_by_items.clone();
        }

        for item in &self.aggregate_items {
            let function = match item.function.to_ascii_lowercase().as_str() {
                "count" => AggregateFunction::Count,
                "sum" => AggregateFunction::Sum,
                "avg" => AggregateFunction::Avg,
                "min" => AggregateFunction::Min,
                "max" => AggregateFunction::Max,
                "stddev" => AggregateFunction::Stddev,
                "stddevpop" | "stddev_pop" => AggregateFunction::StddevPop,
                "varsamp" | "var_samp" => AggregateFunction::VarSamp,
                "varpop" | "var_pop" => AggregateFunction::VarPop,
                other => return Err(format!("Unsupported aggregate function: {other}")),
            };
            q.aggregates
                .push(Aggregate::new(function, &item.field, &item.alias));
        }

        Ok(q)
    }
}

fn map_filter_fields(
    value: &mut JsonValue,
    fields: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    let object = value.as_object_mut().ok_or("Filter must be an object")?;
    let logical_key = if object.contains_key("$and") {
        Some("$and")
    } else if object.contains_key("$or") {
        Some("$or")
    } else {
        None
    };
    if let Some(key) = logical_key {
        let items = object.get_mut(key).unwrap();
        for item in items
            .as_array_mut()
            .ok_or("Logical filter must be an array")?
        {
            map_filter_fields(item, fields)?;
        }
        return Ok(());
    }
    let old = std::mem::take(object);
    for (field, predicate) in old {
        let mapped = fields
            .get(&field)
            .ok_or_else(|| format!("Unknown or forbidden field: {field}"))?;
        object.insert(mapped.clone(), predicate);
    }
    Ok(())
}

fn combine_and(mut expressions: Vec<Expr>) -> Option<Expr> {
    match expressions.len() {
        0 => None,
        1 => expressions.pop(),
        _ => Some(Expr::And(expressions)),
    }
}

fn json_value(value: &JsonValue) -> Result<Value, String> {
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::Number(value) if value.is_i64() => Value::I64(value.as_i64().unwrap()),
        JsonValue::Number(value) if value.is_u64() => Value::U64(value.as_u64().unwrap()),
        JsonValue::Number(value) => Value::F64(value.as_f64().ok_or("Invalid number")?),
        JsonValue::String(value) => Value::Text(value.clone()),
        JsonValue::Array(values) => {
            Value::List(values.iter().map(json_value).collect::<Result<_, _>>()?)
        }
        JsonValue::Object(value) if value.len() == 1 && value.contains_key("id") => {
            json_value(value.get("id").unwrap())?
        }
        JsonValue::Object(_) => {
            return Err("Object values are forbidden except entity references".into());
        }
    })
}

pub fn parse_json_filter(value: &JsonValue) -> Result<Expr, String> {
    let object = value.as_object().ok_or("Filter must be an object")?;
    if let Some(items) = object.get("$and") {
        let items = items.as_array().ok_or("$and must be an array")?;
        if items.is_empty() {
            return Err("$and must not be empty".into());
        }
        return Ok(Expr::And(
            items
                .iter()
                .map(parse_json_filter)
                .collect::<Result<_, _>>()?,
        ));
    }
    if let Some(items) = object.get("$or") {
        let items = items.as_array().ok_or("$or must be an array")?;
        if items.is_empty() {
            return Err("$or must not be empty".into());
        }
        return Ok(Expr::Or(
            items
                .iter()
                .map(parse_json_filter)
                .collect::<Result<_, _>>()?,
        ));
    }
    if object.is_empty() {
        return Err("Filter must not be empty".into());
    }
    let mut expressions = Vec::new();
    for (field, predicate) in object {
        if field.starts_with('$') || field.contains('.') {
            return Err(format!("Unknown or deep filter field: {field}"));
        }
        let predicates = predicate
            .as_object()
            .ok_or_else(|| format!("Predicate for {field} must be an object"))?;
        if predicates.len() != 1 {
            return Err(format!(
                "Predicate for {field} must contain exactly one operator"
            ));
        }
        let (operator, operand) = predicates.iter().next().unwrap();
        let expression = match operator.as_str() {
            "$eq" => Expr::eq(field, non_null_value(operator, operand)?),
            "$ne" => Expr::ne(field, non_null_value(operator, operand)?),
            "$gt" => Expr::gt(field, non_null_value(operator, operand)?),
            "$gte" => Expr::gte(field, json_value(operand)?),
            "$lt" => Expr::lt(field, non_null_value(operator, operand)?),
            "$lte" => Expr::lte(field, json_value(operand)?),
            "$contains" => Expr::contain(
                field,
                operand.as_str().ok_or("$contains requires a string")?,
            ),
            "$notContains" => Expr::not_contain(
                field,
                operand.as_str().ok_or("$notContains requires a string")?,
            ),
            "$startsWith" => Expr::begin_with(
                field,
                operand.as_str().ok_or("$startsWith requires a string")?,
            ),
            "$notStartsWith" => Expr::not_begin_with(
                field,
                operand.as_str().ok_or("$notStartsWith requires a string")?,
            ),
            "$endsWith" => Expr::end_with(
                field,
                operand.as_str().ok_or("$endsWith requires a string")?,
            ),
            "$notEndsWith" => Expr::not_end_with(
                field,
                operand.as_str().ok_or("$notEndsWith requires a string")?,
            ),
            "$in" => Expr::in_list(field, bounded_list(operator, operand)?),
            "$notIn" => Expr::not_in_list(field, bounded_list(operator, operand)?),
            "$between" => {
                let values = operand
                    .as_array()
                    .filter(|values| values.len() == 2)
                    .ok_or("$between requires exactly two values")?;
                Expr::between(field, json_value(&values[0])?, json_value(&values[1])?)
            }
            "$isKnown" => {
                require_true(operator, operand)?;
                Expr::is_not_null(field)
            }
            "$isUnknown" => {
                require_true(operator, operand)?;
                Expr::is_null(field)
            }
            _ => return Err(format!("Unsupported predicate operator: {operator}")),
        };
        expressions.push(expression);
    }
    Ok(combine_and(expressions).unwrap())
}

fn non_null_value(operator: &str, operand: &JsonValue) -> Result<Value, String> {
    if operand.is_null() {
        return Err(format!(
            "{operator} does not accept null; use $isKnown or $isUnknown"
        ));
    }
    json_value(operand)
}

fn bounded_list(operator: &str, operand: &JsonValue) -> Result<Vec<Value>, String> {
    let values = operand
        .as_array()
        .ok_or_else(|| format!("{operator} requires an array"))?;
    if values.is_empty() || values.len() > 100 {
        return Err(format!("{operator} size must be between 1 and 100"));
    }
    values.iter().map(json_value).collect()
}

fn require_true(operator: &str, operand: &JsonValue) -> Result<(), String> {
    if operand == &JsonValue::Bool(true) {
        return Ok(());
    }
    Err(format!("{operator} requires true"))
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TfpMutationQuery {
    pub entity: String,
    pub action: String, // "Create" | "Update" | "Delete" | "Recover"
    pub payload: JsonValue,
    pub id: Option<JsonValue>,
    #[serde(default, rename = "expectedVersion")]
    pub expected_version: Option<i64>,
    pub comment: Option<String>,
}

impl TfpMutationQuery {
    pub fn map_writable_fields(
        &mut self,
        fields: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        let object = self
            .payload
            .as_object_mut()
            .ok_or("Mutation payload must be an object")?;
        let original = std::mem::take(object);
        for (field, value) in original {
            let mapped = fields
                .get(&field)
                .ok_or_else(|| format!("Field is not writable by federation policy: {field}"))?;
            object.insert(mapped.clone(), value);
        }
        Ok(())
    }

    pub fn to_core(&self) -> Result<MutationRequest, String> {
        let comment = self
            .comment
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or("Mutation audit reason is required")?;
        let trace = vec![TraceNode {
            entity_type: self.entity.clone(),
            entity_id: None, // Can be populated if needed
            comment: comment.to_owned(),
        }];

        let id_val = self
            .id
            .as_ref()
            .map(json_value)
            .transpose()?
            .unwrap_or(teaql_core::Value::Null);

        match self.action.as_str() {
            "Create" => {
                let mut record = Record::new();
                if let JsonValue::Object(map) = &self.payload {
                    for (k, v) in map {
                        record.insert(k.clone(), json_value(v)?);
                    }
                } else {
                    return Err("Mutation payload must be an object".into());
                }
                Ok(MutationRequest::Insert(InsertCommand {
                    entity: self.entity.clone(),
                    values: record.into(),
                    trace_chain: trace,
                }))
            }
            "Update" => {
                let mut record = Record::new();
                if let JsonValue::Object(map) = &self.payload {
                    for (k, v) in map {
                        record.insert(k.clone(), json_value(v)?);
                    }
                } else {
                    return Err("Mutation payload must be an object".into());
                }
                Ok(MutationRequest::Update(UpdateCommand {
                    entity: self.entity.clone(),
                    id: id_val,
                    values: record.into(),
                    expected_version: self.expected_version,
                    old_values: None,
                    trace_chain: trace,
                }))
            }
            "Delete" => Ok(MutationRequest::Delete(DeleteCommand {
                entity: self.entity.clone(),
                id: id_val,
                expected_version: self.expected_version,
                soft_delete: true,
                trace_chain: trace,
            })),
            "Recover" => Ok(MutationRequest::Recover(RecoverCommand {
                entity: self.entity.clone(),
                id: id_val,
                expected_version: self.expected_version.unwrap_or(0),
                trace_chain: trace,
            })),
            _ => Err("Unknown mutation action".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn translates_generated_typescript_query_without_broadening() {
        let payload = json!({
            "entity": "CustomerOrder",
            "_filters": [{"$and": [
                {"commercePlatform": {"$eq": 1}},
                {"orderNumber": {"$contains": "ORD-00"}},
                {"totalAmount": {"$gte": 130}},
                {"status": {"$in": [1001, 1002]}}
            ]}],
            "_limit": 10,
            "_offset": 5,
            "_orderBy": [{"f":"orderNumber","d":"asc"}],
            "_groupBy": ["status"],
            "_aggregates": [{"func":"count","field":"id","retName":"record_count"}],
            "_comment": "federated query",
            "_purpose": "requested purpose"
        });
        let mut query: TfpSelectQuery = serde_json::from_value(payload).unwrap();
        let fields = BTreeMap::from([
            ("id".into(), "id".into()),
            ("commercePlatform".into(), "commerce_platform_id".into()),
            ("orderNumber".into(), "order_number".into()),
            ("totalAmount".into(), "total_amount".into()),
            ("status".into(), "status_id".into()),
        ]);
        query.map_fields(&fields).unwrap();
        let core = query.to_core().unwrap();
        assert!(core.filter.is_some());
        assert_eq!(core.slice.unwrap().limit, Some(10));
        assert_eq!(core.group_by, vec!["status_id"]);
        assert_eq!(core.aggregates[0].field, "id");
        assert_eq!(core.aggregates[0].alias, "record_count");
        assert_eq!(core.order_by[0].field, "order_number");
        assert!(core.continuous_page_fetch.is_none());
        assert!(core.id_set_pagination.is_none());
    }

    #[test]
    fn rejects_unknown_operator_field_deep_path_and_excessive_in() {
        assert!(parse_json_filter(&json!({"id":{"$wat":1}})).is_err());
        assert!(parse_json_filter(&json!({"customer.email":{"$eq":"masked"}})).is_err());
        assert!(parse_json_filter(&json!({"id":{"$in":[]}})).is_err());
        let values: Vec<_> = (0..101).collect();
        assert!(parse_json_filter(&json!({"id":{"$in":values}})).is_err());
        let mut query: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder", "_filters":[{"unknown":{"$eq":1}}]
        }))
        .unwrap();
        assert!(
            query
                .map_fields(&BTreeMap::from([("id".into(), "id".into())]))
                .is_err()
        );
    }

    #[test]
    fn parses_extended_portable_predicates_and_nullable_boolean() {
        for filter in [
            json!({"id":{"$ne":8}}),
            json!({"id":{"$notIn":[8,9]}}),
            json!({"id":{"$gt":6}}),
            json!({"id":{"$lt":8}}),
            json!({"id":{"$between":[7,9]}}),
            json!({"orderNumber":{"$notContains":"BAD"}}),
            json!({"orderNumber":{"$startsWith":"ORD"}}),
            json!({"orderNumber":{"$notStartsWith":"BAD"}}),
            json!({"orderNumber":{"$endsWith":"007"}}),
            json!({"orderNumber":{"$notEndsWith":"999"}}),
            json!({"reviewed":{"$isKnown":true}}),
            json!({"reviewed":{"$isUnknown":true}}),
            json!({"reviewed":{"$eq":true}}),
            json!({"reviewed":{"$eq":false}}),
        ] {
            parse_json_filter(&filter).unwrap_or_else(|error| {
                panic!("failed to parse {filter}: {error}")
            });
        }
        for filter in [
            json!({"id":{"$between":[7]}}),
            json!({"id":{"$notIn":[]}}),
            json!({"reviewed":{"$isKnown":false}}),
            json!({"reviewed":{"$isUnknown":null}}),
            json!({"reviewed":{"$eq":null}}),
        ] {
            assert!(parse_json_filter(&filter).is_err(), "accepted {filter}");
        }
    }

    #[test]
    fn mutation_converts_json_scalars_and_requires_audit_reason() {
        let mutation = TfpMutationQuery {
            entity: "OrderSearchPreset".into(),
            action: "Update".into(),
            payload: json!({"name":"Swift verified", "version":2, "active":true}),
            id: Some(json!(900001)),
            expected_version: Some(2),
            comment: Some("Verify Swift audited mutation".into()),
        };
        let request = mutation.to_core().unwrap();
        let teaql_data_service::MutationRequest::Update(command) = request else {
            panic!("expected update command")
        };
        assert_eq!(command.id, Value::I64(900001));
        assert_eq!(command.expected_version, Some(2));
        assert_eq!(
            command.values.get("name"),
            Some(&Value::Text("Swift verified".into()))
        );
        assert_eq!(command.values.get("version"), Some(&Value::I64(2)));
        assert_eq!(command.values.get("active"), Some(&Value::Bool(true)));
        assert_eq!(
            command.trace_chain[0].comment,
            "Verify Swift audited mutation"
        );

        let missing_reason = TfpMutationQuery {
            comment: Some(" ".into()),
            ..mutation
        };
        assert_eq!(
            missing_reason.to_core().unwrap_err(),
            "Mutation audit reason is required"
        );
    }
}
