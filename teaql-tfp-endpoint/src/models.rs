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
        if self.expr.is_some() {
            return Err("Order expressions are not supported by canonical TFP v1".to_string());
        }
        let dir = match self.direction.as_str() {
            value if value.eq_ignore_ascii_case("asc") => SortDirection::Asc,
            value if value.eq_ignore_ascii_case("desc") => SortDirection::Desc,
            _ => return Err(format!("Unsupported order direction: {}", self.direction)),
        };
        Ok(OrderBy {
            field: self.field.clone(),
            expr: None,
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
    #[serde(default)]
    pub facets: Vec<TfpFacetRequest>,
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
pub struct TfpFacetRequest {
    pub facet_name: String,
    pub relation_name: String,
    pub query: Box<TfpSelectQuery>,
    #[serde(default = "default_include_all_facets")]
    pub include_all_facets: bool,
}

fn default_include_all_facets() -> bool {
    true
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

const MAX_LOGICAL_FILTER_CHILDREN: usize = 100;
const MAX_FILTER_DEPTH: usize = 16;
const MAX_FILTER_PREDICATES: usize = 256;
pub const MAX_SELECT_ITEMS: usize = 256;
pub const MAX_ORDER_ITEMS: usize = 32;
pub const MAX_GROUP_BY_ITEMS: usize = 64;
pub const MAX_AGGREGATE_ITEMS: usize = 64;
pub const MAX_GOVERNANCE_EVIDENCE_BYTES: usize = 1024;

impl TfpSelectQuery {
    pub(crate) fn validate_request_shape(&self) -> Result<(), String> {
        self.validate_filter_shape()?;
        self.validate_query_item_counts()?;
        self.validate_limit_shape()?;
        self.resolved_comment()?;
        self.resolved_purpose()?;
        Ok(())
    }

    pub(crate) fn validate_query_item_counts(&self) -> Result<(), String> {
        for (name, actual, maximum) in [
            ("selectItems", self.select_items.len(), MAX_SELECT_ITEMS),
            ("orderItems", self.order_items.len(), MAX_ORDER_ITEMS),
            (
                "groupByItems",
                self.group_by_items.len(),
                MAX_GROUP_BY_ITEMS,
            ),
            (
                "aggregateItems",
                self.aggregate_items.len(),
                MAX_AGGREGATE_ITEMS,
            ),
        ] {
            if actual > maximum {
                return Err(format!(
                    "A TFP query may contain at most {maximum} {name} entries"
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_limit_shape(&self) -> Result<(), String> {
        if !self.limit_value.is_some_and(|limit| limit > 0) {
            return Err("A TFP query requires an explicit positive limit".into());
        }
        Ok(())
    }

    pub(crate) fn resolved_comment(&self) -> Result<&str, String> {
        resolve_query_evidence(
            "commentText",
            self.comment_text.as_deref(),
            "_comment",
            self.generated_comment.as_deref(),
        )
    }

    pub(crate) fn resolved_purpose(&self) -> Result<&str, String> {
        resolve_query_evidence(
            "purposeText",
            self.purpose_text.as_deref(),
            "_purpose",
            self.generated_purpose.as_deref(),
        )
    }

    pub fn validate_filter_shape(&self) -> Result<(), String> {
        let mut predicates = 0;
        if let Some(filter) = &self.filter_condition {
            validate_filter_tree(filter, 0, &mut predicates)?;
        }
        for filter in &self.filters {
            validate_filter_tree(filter, 0, &mut predicates)?;
        }
        Ok(())
    }

    pub fn map_fields(
        &mut self,
        fields: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        self.validate_filter_shape()?;
        if let Some(filter) = &mut self.filter_condition {
            map_filter_fields(filter, fields)?;
        }
        for filter in &mut self.filters {
            map_filter_fields(filter, fields)?;
        }
        let mut mapped_order_fields = std::collections::BTreeSet::new();
        for order in &mut self.order_items {
            let mapped = fields
                .get(&order.field)
                .ok_or_else(|| format!("Unknown field: {}", order.field))?
                .clone();
            if !mapped_order_fields.insert(mapped.clone()) {
                return Err(format!(
                    "Query field is duplicated after mapping in orderItems: {mapped}"
                ));
            }
            order.field = mapped;
        }
        let mut mapped_select_fields = std::collections::BTreeSet::new();
        for field in &mut self.select_items {
            let mapped = fields
                .get(field)
                .ok_or_else(|| format!("Unknown field: {field}"))?
                .clone();
            if !mapped_select_fields.insert(mapped.clone()) {
                return Err(format!(
                    "Query field is duplicated after mapping in selectItems: {mapped}"
                ));
            }
            *field = mapped;
        }
        let mut mapped_group_fields = std::collections::BTreeSet::new();
        for field in &mut self.group_by_items {
            let mapped = fields
                .get(field)
                .ok_or_else(|| format!("Unknown field: {field}"))?
                .clone();
            if !mapped_group_fields.insert(mapped.clone()) {
                return Err(format!(
                    "Query field is duplicated after mapping in groupByItems: {mapped}"
                ));
            }
            *field = mapped;
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
        self.validate_request_shape()?;
        if !self.facets.is_empty() {
            return Err(
                "Facet execution requires TfpEndpoint and cannot use direct query translation"
                    .into(),
            );
        }
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

fn resolve_query_evidence<'a>(
    canonical_name: &str,
    canonical_value: Option<&'a str>,
    legacy_name: &str,
    legacy_value: Option<&'a str>,
) -> Result<&'a str, String> {
    if canonical_value.is_some() && legacy_value.is_some() {
        return Err(format!(
            "A TFP query cannot provide both {canonical_name} and {legacy_name}"
        ));
    }
    let value = canonical_value
        .or(legacy_value)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            format!("A TFP query requires non-blank {canonical_name} or {legacy_name}")
        })?;
    validate_governance_evidence(value)
        .map_err(|reason| format!("A TFP query {canonical_name} {reason}"))?;
    Ok(value)
}

fn validate_governance_evidence(value: &str) -> Result<(), String> {
    if value.len() > MAX_GOVERNANCE_EVIDENCE_BYTES {
        return Err(format!(
            "must be at most {MAX_GOVERNANCE_EVIDENCE_BYTES} UTF-8 bytes"
        ));
    }
    if value.chars().any(char::is_control) {
        return Err("must not contain control characters".into());
    }
    Ok(())
}

fn validate_filter_tree(
    value: &JsonValue,
    depth: usize,
    predicates: &mut usize,
) -> Result<(), String> {
    if depth > MAX_FILTER_DEPTH {
        return Err(format!("Filter nesting depth exceeds {MAX_FILTER_DEPTH}"));
    }
    let object = value.as_object().ok_or("Filter must be an object")?;
    let has_and = object.contains_key("$and");
    let has_or = object.contains_key("$or");
    if has_and || has_or {
        if object.len() != 1 || has_and == has_or {
            return Err("Logical filter must contain exactly one $and or $or key".into());
        }
        let key = if has_and { "$and" } else { "$or" };
        let children = object[key]
            .as_array()
            .ok_or_else(|| format!("{key} must be an array"))?;
        if children.is_empty() || children.len() > MAX_LOGICAL_FILTER_CHILDREN {
            return Err(format!(
                "Logical filter child count must be between 1 and {MAX_LOGICAL_FILTER_CHILDREN}"
            ));
        }
        for child in children {
            validate_filter_tree(child, depth + 1, predicates)?;
        }
        return Ok(());
    }
    if object.is_empty() {
        return Err("Filter must not be empty".into());
    }
    *predicates = predicates
        .checked_add(object.len())
        .ok_or("Filter predicate count overflow")?;
    if *predicates > MAX_FILTER_PREDICATES {
        return Err(format!(
            "Filter predicate count exceeds {MAX_FILTER_PREDICATES}"
        ));
    }
    Ok(())
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
        if object.contains_key(mapped) {
            return Err(format!(
                "Filter field aliases collide after mapping: {mapped}"
            ));
        }
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
    let has_and = object.contains_key("$and");
    let has_or = object.contains_key("$or");
    if (has_and || has_or) && (object.len() != 1 || has_and == has_or) {
        return Err("Logical filter must contain exactly one $and or $or key".into());
    }
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
            "$eq" => Expr::eq(field, non_null_scalar_value(operator, operand)?),
            "$ne" => Expr::ne(field, non_null_scalar_value(operator, operand)?),
            "$gt" => Expr::gt(field, non_null_scalar_value(operator, operand)?),
            "$gte" => Expr::gte(field, non_null_scalar_value(operator, operand)?),
            "$lt" => Expr::lt(field, non_null_scalar_value(operator, operand)?),
            "$lte" => Expr::lte(field, non_null_scalar_value(operator, operand)?),
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
                Expr::between(
                    field,
                    non_null_scalar_value(operator, &values[0])?,
                    non_null_scalar_value(operator, &values[1])?,
                )
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

fn non_null_scalar_value(operator: &str, operand: &JsonValue) -> Result<Value, String> {
    match json_value(operand)? {
        Value::Null => Err(format!(
            "{operator} does not accept null; use $isKnown or $isUnknown"
        )),
        value @ (Value::Bool(_)
        | Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::Text(_)) => Ok(value),
        _ => Err(format!("{operator} requires a non-null scalar")),
    }
}

fn bounded_list(operator: &str, operand: &JsonValue) -> Result<Vec<Value>, String> {
    let values = operand
        .as_array()
        .ok_or_else(|| format!("{operator} requires an array"))?;
    if values.is_empty() || values.len() > 100 {
        return Err(format!("{operator} size must be between 1 and 100"));
    }
    values
        .iter()
        .map(|value| non_null_scalar_value(operator, value))
        .collect()
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
    pub(crate) fn validate_request_shape(&self) -> Result<(), String> {
        let payload = self
            .payload
            .as_object()
            .ok_or("Mutation payload must be an object")?;
        if matches!(self.action.as_str(), "Delete" | "Recover") && !payload.is_empty() {
            return Err(format!(
                "Invalid mutation request: {} requires an empty payload",
                self.action
            ));
        }

        let has_id = self.id.as_ref().is_some_and(|id| !id.is_null());
        let require_valid_id = || -> Result<(), String> {
            let id =
                self.id.as_ref().filter(|id| !id.is_null()).ok_or_else(|| {
                    format!("Invalid mutation request: {} requires id", self.action)
                })?;
            let normalized = json_value(id).map_err(|_| {
                format!(
                    "Invalid mutation request: {} requires an integer or text id",
                    self.action
                )
            })?;
            if matches!(normalized, Value::I64(_) | Value::U64(_) | Value::Text(_)) {
                return Ok(());
            }
            Err(format!(
                "Invalid mutation request: {} requires an integer or text id",
                self.action
            ))
        };
        match self.action.as_str() {
            "Create" => {
                if has_id {
                    return Err("Invalid mutation request: Create must not carry id".into());
                }
                if self.expected_version.is_some() {
                    return Err(
                        "Invalid mutation request: Create must not carry expectedVersion".into(),
                    );
                }
            }
            "Update" | "Delete" => {
                require_valid_id()?;
                if !self.expected_version.is_some_and(|version| version > 0) {
                    return Err(format!(
                        "Invalid mutation request: {} requires a positive expectedVersion",
                        self.action
                    ));
                }
            }
            "Recover" => {
                require_valid_id()?;
                if !self.expected_version.is_some_and(|version| version < 0) {
                    return Err(
                        "Invalid mutation request: Recover requires a negative expectedVersion"
                            .into(),
                    );
                }
            }
            _ => {}
        }
        self.resolved_comment()?;
        Ok(())
    }

    fn resolved_comment(&self) -> Result<&str, String> {
        let comment = self
            .comment
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or("Mutation audit reason is required")?;
        validate_governance_evidence(comment)
            .map_err(|reason| format!("Invalid mutation request: comment {reason}"))?;
        Ok(comment)
    }

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
            if object.contains_key(mapped) {
                return Err(format!(
                    "Mutation field aliases collide after mapping: {mapped}"
                ));
            }
            object.insert(mapped.clone(), value);
        }
        Ok(())
    }

    pub fn to_core(&self) -> Result<MutationRequest, String> {
        self.validate_request_shape()?;
        let comment = self.resolved_comment()?;
        let trace = vec![TraceNode {
            kind: teaql_core::TraceKind::AuditReason,
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
    fn direct_translation_requires_an_explicit_positive_limit() {
        for limit_value in [None, Some(0)] {
            let query = TfpSelectQuery {
                entity: "CustomerOrder".into(),
                filter_condition: None,
                filters: Vec::new(),
                limit_value,
                offset_value: None,
                order_items: Vec::new(),
                select_items: Vec::new(),
                group_by_items: Vec::new(),
                aggregate_items: Vec::new(),
                facets: Vec::new(),
                comment_text: Some("bounded query".into()),
                generated_comment: None,
                purpose_text: Some("verify direct translation".into()),
                generated_purpose: None,
            };
            assert_eq!(
                query.to_core().unwrap_err(),
                "A TFP query requires an explicit positive limit"
            );
        }

        let bounded = TfpSelectQuery {
            entity: "CustomerOrder".into(),
            filter_condition: None,
            filters: Vec::new(),
            limit_value: Some(25),
            offset_value: None,
            order_items: Vec::new(),
            select_items: Vec::new(),
            group_by_items: Vec::new(),
            aggregate_items: Vec::new(),
            facets: Vec::new(),
            comment_text: Some("bounded query".into()),
            generated_comment: None,
            purpose_text: Some("verify direct translation".into()),
            generated_purpose: None,
        }
        .to_core()
        .expect("positive limit");
        assert_eq!(bounded.slice.and_then(|slice| slice.limit), Some(25));
    }

    #[test]
    fn canonical_query_item_counts_accept_exact_boundaries_and_reject_one_more() {
        let base = TfpSelectQuery {
            entity: "CustomerOrder".into(),
            filter_condition: None,
            filters: Vec::new(),
            limit_value: Some(10),
            offset_value: None,
            order_items: Vec::new(),
            select_items: Vec::new(),
            group_by_items: Vec::new(),
            aggregate_items: Vec::new(),
            facets: Vec::new(),
            comment_text: Some("load bounded query shape".into()),
            generated_comment: None,
            purpose_text: Some("verify canonical item budgets".into()),
            generated_purpose: None,
        };

        let mut select = base.clone();
        select.select_items = (0..MAX_SELECT_ITEMS)
            .map(|index| format!("field_{index}"))
            .collect();
        select.clone().to_core().expect("exact select boundary");
        select.select_items.push("one_too_many".into());
        assert!(select.to_core().unwrap_err().contains("selectItems"));

        let mut order = base.clone();
        order.order_items = (0..MAX_ORDER_ITEMS)
            .map(|index| TfpOrderBy {
                field: format!("field_{index}"),
                expr: None,
                direction: "asc".into(),
            })
            .collect();
        order.clone().to_core().expect("exact order boundary");
        order.order_items.push(TfpOrderBy {
            field: "one_too_many".into(),
            expr: None,
            direction: "asc".into(),
        });
        assert!(order.to_core().unwrap_err().contains("orderItems"));

        let mut group = base.clone();
        group.group_by_items = (0..MAX_GROUP_BY_ITEMS)
            .map(|index| format!("field_{index}"))
            .collect();
        group.clone().to_core().expect("exact group boundary");
        group.group_by_items.push("one_too_many".into());
        assert!(group.to_core().unwrap_err().contains("groupByItems"));

        let mut aggregate = base;
        aggregate.aggregate_items = (0..MAX_AGGREGATE_ITEMS)
            .map(|index| TfpAggregateItem {
                function: "count".into(),
                field: "id".into(),
                alias: format!("count_{index}"),
            })
            .collect();
        aggregate
            .clone()
            .to_core()
            .expect("exact aggregate boundary");
        aggregate.aggregate_items.push(TfpAggregateItem {
            function: "count".into(),
            field: "id".into(),
            alias: "one_too_many".into(),
        });
        assert!(aggregate.to_core().unwrap_err().contains("aggregateItems"));
    }

    #[test]
    fn direct_translation_rejects_facets_instead_of_discarding_them() {
        let query: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder",
            "limitValue":10,
            "commentText":"load orders",
            "purposeText":"render orders",
            "facets":[{
                "facetName":"statusFacet",
                "relationName":"status",
                "query":{
                    "entity":"OrderStatus",
                    "limitValue":10,
                    "commentText":"load statuses",
                    "purposeText":"render order filters",
                    "selectItems":["id", "name"],
                    "aggregateItems":[{
                        "function":"count", "field":"id", "alias":"recordCount"
                    }]
                }
            }]
        }))
        .expect("canonical facet query");

        assert_eq!(
            query.to_core().unwrap_err(),
            "Facet execution requires TfpEndpoint and cannot use direct query translation"
        );
    }

    #[test]
    fn direct_translation_requires_unambiguous_comment_and_purpose() {
        for payload in [
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":" ", "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"load orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"load orders", "_comment":"legacy load",
                "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"load orders", "purposeText":"render orders",
                "_purpose":"legacy render"
            }),
        ] {
            let query: TfpSelectQuery = serde_json::from_value(payload).unwrap();
            assert!(query.to_core().is_err(), "accepted {query:?}");
        }

        for payload in [
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"load orders", "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "_limit":10,
                "_comment":"load orders", "_purpose":"render orders"
            }),
        ] {
            let query: TfpSelectQuery = serde_json::from_value(payload).unwrap();
            query.to_core().expect("one supported evidence spelling");
        }
    }

    #[test]
    fn governance_evidence_is_utf8_bounded_and_rejects_control_characters() {
        let exact = "🧱".repeat(MAX_GOVERNANCE_EVIDENCE_BYTES / 4);
        assert_eq!(exact.len(), MAX_GOVERNANCE_EVIDENCE_BYTES);
        let query: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder", "limitValue":10,
            "commentText":exact, "purposeText":"验证支付审批"
        }))
        .expect("bounded Unicode evidence");
        query.to_core().expect("exact byte boundary remains valid");

        for (field, value) in [
            (
                "commentText",
                format!("{}x", "a".repeat(MAX_GOVERNANCE_EVIDENCE_BYTES)),
            ),
            ("purposeText", "render\nforged-log-line".into()),
        ] {
            let mut payload = json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"load orders", "purposeText":"render orders"
            });
            payload[field] = JsonValue::String(value);
            let query: TfpSelectQuery = serde_json::from_value(payload).unwrap();
            assert!(query.to_core().is_err(), "accepted invalid {field}");
        }

        let exact_mutation = TfpMutationQuery {
            entity: "CustomerOrder".into(),
            action: "Create".into(),
            payload: json!({"order_number":"O-1"}),
            id: None,
            expected_version: None,
            comment: Some("x".repeat(MAX_GOVERNANCE_EVIDENCE_BYTES)),
        };
        exact_mutation
            .to_core()
            .expect("exact mutation evidence boundary remains valid");

        for comment in [
            "x".repeat(MAX_GOVERNANCE_EVIDENCE_BYTES + 1),
            "audit\rforged".into(),
        ] {
            let mut mutation = exact_mutation.clone();
            mutation.comment = Some(comment);
            assert!(mutation.to_core().is_err());
        }
    }

    #[test]
    fn rejects_order_expression_instead_of_silently_dropping_it() {
        let order = TfpOrderBy {
            field: "id".into(),
            expr: Some(json!({"function":"lower", "arguments":["name"]})),
            direction: "asc".into(),
        };
        assert_eq!(
            order.to_core().unwrap_err(),
            "Order expressions are not supported by canonical TFP v1"
        );

        let field_order = TfpOrderBy {
            field: "order_number".into(),
            expr: None,
            direction: "desc".into(),
        }
        .to_core()
        .expect("ordinary field order");
        assert_eq!(field_order.field, "order_number");
        assert!(field_order.expr.is_none());
        assert_eq!(field_order.direction, SortDirection::Desc);
    }

    #[test]
    fn rejects_unknown_operator_field_deep_path_and_excessive_in() {
        assert!(parse_json_filter(&json!({"id":{"$wat":1}})).is_err());
        assert!(parse_json_filter(&json!({"customer.email":{"$eq":"masked"}})).is_err());
        assert!(parse_json_filter(&json!({"id":{"$in":[]}})).is_err());
        for filter in [
            json!({"id":{"$gte":null}}),
            json!({"id":{"$lte":null}}),
            json!({"id":{"$between":[null, 10]}}),
            json!({"id":{"$between":[1, null]}}),
            json!({"id":{"$eq":{"id":null}}}),
            json!({"id":{"$ne":{"id":null}}}),
            json!({"id":{"$gte":{"id":null}}}),
            json!({"id":{"$between":[{"id":null}, 10]}}),
            json!({"id":{"$eq":[1, 2]}}),
            json!({"id":{"$gte":[1]}}),
            json!({"id":{"$between":[[1], 10]}}),
            json!({"id":{"$in":[1, null]}}),
            json!({"id":{"$in":[[1, 2]]}}),
            json!({"id":{"$notIn":[{"id":null}]}}),
        ] {
            assert!(parse_json_filter(&filter).is_err(), "accepted {filter}");
        }
        assert!(parse_json_filter(&json!({"id":{"$eq":{"id":42}}})).is_ok());
        assert!(parse_json_filter(&json!({"id":{"$in":[{"id":42}, 7]}})).is_ok());
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

        let mut collision: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder",
            "filterCondition":{
                "order_number":{"$startsWith":"SAFE"},
                "orderNumber":{"$contains":"OTHER"}
            }
        }))
        .unwrap();
        assert_eq!(
            collision
                .map_fields(&BTreeMap::from([
                    ("order_number".into(), "order_number".into()),
                    ("orderNumber".into(), "order_number".into()),
                ]))
                .unwrap_err(),
            "Filter field aliases collide after mapping: order_number"
        );
    }

    #[test]
    fn query_mapping_rejects_duplicate_mapped_projection_group_and_order_fields() {
        let mappings = BTreeMap::from([
            ("id".into(), "id".into()),
            ("order_number".into(), "order_number".into()),
            ("orderNumber".into(), "order_number".into()),
        ]);
        for (payload, expected_error) in [
            (
                json!({
                    "entity":"CustomerOrder",
                    "selectItems":["order_number", "orderNumber"]
                }),
                "Query field is duplicated after mapping in selectItems: order_number",
            ),
            (
                json!({
                    "entity":"CustomerOrder",
                    "groupByItems":["order_number", "orderNumber"]
                }),
                "Query field is duplicated after mapping in groupByItems: order_number",
            ),
            (
                json!({
                    "entity":"CustomerOrder",
                    "orderItems":[
                        {"field":"order_number", "direction":"asc"},
                        {"field":"orderNumber", "direction":"desc"}
                    ]
                }),
                "Query field is duplicated after mapping in orderItems: order_number",
            ),
        ] {
            let mut query: TfpSelectQuery = serde_json::from_value(payload).unwrap();
            assert_eq!(query.map_fields(&mappings).unwrap_err(), expected_error);
        }

        let mut aggregates: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder",
            "aggregateItems":[
                {"function":"sum", "field":"id", "alias":"idSum"},
                {"function":"max", "field":"id", "alias":"idMax"}
            ]
        }))
        .unwrap();
        aggregates
            .map_fields(&mappings)
            .expect("same-field aggregates have distinct semantics");
    }

    #[test]
    fn rejects_ambiguous_or_unbounded_filter_trees() {
        for filter in [
            json!({"$and":[{"id":{"$eq":1}}], "id":{"$eq":2}}),
            json!({
                "$and":[{"id":{"$eq":1}}],
                "$or":[{"id":{"$eq":2}}]
            }),
        ] {
            assert!(parse_json_filter(&filter).is_err(), "accepted {filter}");
        }

        let broad = json!({"$or": (0..=MAX_LOGICAL_FILTER_CHILDREN)
            .map(|id| json!({"id":{"$eq":id}}))
            .collect::<Vec<_>>()});
        let broad_query: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder", "filterCondition":broad
        }))
        .unwrap();
        assert!(broad_query.validate_filter_shape().is_err());

        let mut deep = json!({"id":{"$eq":1}});
        for _ in 0..=MAX_FILTER_DEPTH {
            deep = json!({"$and":[deep]});
        }
        let deep_query: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder", "filterCondition":deep
        }))
        .unwrap();
        assert!(deep_query.validate_filter_shape().is_err());

        let filters = (0..=MAX_FILTER_PREDICATES)
            .map(|id| json!({"id":{"$eq":id}}))
            .collect::<Vec<_>>();
        let excessive_query: TfpSelectQuery = serde_json::from_value(json!({
            "entity":"CustomerOrder", "_filters":filters
        }))
        .unwrap();
        assert!(excessive_query.validate_filter_shape().is_err());
    }

    #[test]
    fn retains_valid_nested_logic_and_ordinary_implicit_and() {
        let filter = json!({"$or":[
            {"$and":[{"id":{"$gt":1}}, {"id":{"$lt":10}}]},
            {"reviewed":{"$eq":true}}
        ]});
        assert!(parse_json_filter(&filter).is_ok());
        assert!(
            parse_json_filter(&json!({
                "id":{"$gt":1},
                "reviewed":{"$eq":true}
            }))
            .is_ok()
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
            parse_json_filter(&filter)
                .unwrap_or_else(|error| panic!("failed to parse {filter}: {error}"));
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

    #[test]
    fn mutation_mapping_rejects_alias_collisions_before_value_overwrite() {
        let mut mutation = TfpMutationQuery {
            entity: "CustomerOrder".into(),
            action: "Update".into(),
            payload: json!({
                "order_number":"SAFE-42",
                "orderNumber":"OTHER-42"
            }),
            id: Some(json!(42)),
            expected_version: Some(3),
            comment: Some("update order".into()),
        };
        assert_eq!(
            mutation
                .map_writable_fields(&BTreeMap::from([
                    ("order_number".into(), "order_number".into()),
                    ("orderNumber".into(), "order_number".into()),
                ]))
                .unwrap_err(),
            "Mutation field aliases collide after mapping: order_number"
        );

        let mut single_alias = TfpMutationQuery {
            payload: json!({"orderNumber":"SAFE-42"}),
            ..mutation
        };
        single_alias
            .map_writable_fields(&BTreeMap::from([(
                "orderNumber".into(),
                "order_number".into(),
            )]))
            .expect("one configured alias");
        assert_eq!(single_alias.payload, json!({"order_number":"SAFE-42"}));
    }

    #[test]
    fn lifecycle_mutations_reject_payload_values_in_direct_translation() {
        for (action, expected_version) in [("Delete", 2), ("Recover", -2)] {
            let mutation = TfpMutationQuery {
                entity: "CustomerOrder".into(),
                action: action.into(),
                payload: json!({"order_number":"must not be ignored"}),
                id: Some(json!(42)),
                expected_version: Some(expected_version),
                comment: Some(format!("{action} order")),
            };

            assert_eq!(
                mutation.to_core().unwrap_err(),
                format!("Invalid mutation request: {action} requires an empty payload")
            );
        }
    }

    #[test]
    fn lifecycle_metadata_is_enforced_in_direct_translation() {
        let cases = [
            (
                "Create",
                Some(json!(42)),
                None,
                "Invalid mutation request: Create must not carry id",
            ),
            (
                "Create",
                None,
                Some(1),
                "Invalid mutation request: Create must not carry expectedVersion",
            ),
            (
                "Update",
                None,
                Some(1),
                "Invalid mutation request: Update requires id",
            ),
            (
                "Delete",
                Some(json!(42)),
                Some(-1),
                "Invalid mutation request: Delete requires a positive expectedVersion",
            ),
            (
                "Recover",
                Some(json!(42)),
                Some(1),
                "Invalid mutation request: Recover requires a negative expectedVersion",
            ),
        ];

        for (action, id, expected_version, expected_error) in cases {
            let mutation = TfpMutationQuery {
                entity: "CustomerOrder".into(),
                action: action.into(),
                payload: json!({}),
                id,
                expected_version,
                comment: Some(format!("{action} order")),
            };

            assert_eq!(mutation.to_core().unwrap_err(), expected_error);
        }

        for (action, id, expected_version) in [
            ("Update", json!({"id":null}), 1),
            ("Delete", json!([42]), 1),
            ("Recover", json!(true), -1),
            ("Update", json!(1.5), 1),
        ] {
            let mutation = TfpMutationQuery {
                entity: "CustomerOrder".into(),
                action: action.into(),
                payload: json!({}),
                id: Some(id),
                expected_version: Some(expected_version),
                comment: Some(format!("{action} order")),
            };
            assert_eq!(
                mutation.to_core().unwrap_err(),
                format!("Invalid mutation request: {action} requires an integer or text id")
            );
        }

        let valid_reference = TfpMutationQuery {
            entity: "CustomerOrder".into(),
            action: "Update".into(),
            payload: json!({"name":"updated"}),
            id: Some(json!({"id":42})),
            expected_version: Some(1),
            comment: Some("update order".into()),
        }
        .to_core()
        .expect("non-null entity reference id");
        let MutationRequest::Update(command) = valid_reference else {
            panic!("expected update command")
        };
        assert_eq!(command.id, Value::I64(42));
    }
}
