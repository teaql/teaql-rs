//! Local UI search input. This is not a permissive federation decoder.
use crate::{Expr, OrderBy, SelectQuery};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct SearchModel {
    pub fields: BTreeMap<String, String>,
    pub relations: BTreeMap<String, String>,
}
pub type SearchModels = BTreeMap<String, SearchModel>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicSearchWarning {
    pub code: &'static str,
    pub entity: String,
    pub clause: &'static str,
    pub field_path: String,
}
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicSearchFilter {
    pub field_path: String,
    pub operator: String,
    pub value: Value,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicSearchOrder {
    pub field_path: String,
    pub descending: bool,
}
#[derive(Debug, Clone)]
pub struct NormalizedDynamicSearch {
    pub filters: Vec<DynamicSearchFilter>,
    pub orders: Vec<DynamicSearchOrder>,
    pub warnings: Vec<DynamicSearchWarning>,
}
#[derive(Debug, Clone)]
pub struct DynamicSearchResult {
    pub query: SelectQuery,
    pub warnings: Vec<DynamicSearchWarning>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicSearchError(pub &'static str);
impl std::fmt::Display for DynamicSearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for DynamicSearchError {}
type Result<T> = std::result::Result<T, DynamicSearchError>;

/// Metadata and bounds must come from trusted application setup, never from source.
/// Passing no warning sink uses structured stderr logging without submitted values.
pub fn normalize_dynamic_search(
    source: &str,
    entity: &str,
    models: &SearchModels,
    max_clauses: usize,
    warn: Option<&mut dyn FnMut(&DynamicSearchWarning)>,
) -> Result<NormalizedDynamicSearch> {
    if max_clauses == 0 || !models.contains_key(entity) {
        return Err(DynamicSearchError("Invalid trusted search setup"));
    }
    let value: Value = serde_json::from_str(source)
        .map_err(|_| DynamicSearchError("Dynamic search requires valid JSON"))?;
    let root = value
        .as_object()
        .ok_or(DynamicSearchError("Expected search object"))?;
    if root.keys().any(|k| k != "filter" && k != "orderBy") {
        return Err(DynamicSearchError("Unsupported dynamic search control"));
    }
    let empty_filters = Map::new();
    let empty_orders = Vec::new();
    let filters = match root.get("filter") {
        None => &empty_filters,
        Some(value) => value
            .as_object()
            .ok_or(DynamicSearchError("Invalid search filter"))?,
    };
    let orders = match root.get("orderBy") {
        None => &empty_orders,
        Some(value) => value
            .as_array()
            .ok_or(DynamicSearchError("Invalid search ordering"))?,
    };
    if filters.len().saturating_add(orders.len()) > max_clauses {
        return Err(DynamicSearchError("Dynamic search exceeds clause limit"));
    }
    let mut result = NormalizedDynamicSearch {
        filters: vec![],
        orders: vec![],
        warnings: vec![],
    };
    for (path, predicate) in filters {
        let (operator, value) = if let Some(parts) = predicate.as_object() {
            if parts.len() != 1 {
                return Err(DynamicSearchError("Malformed dynamic search operator"));
            }
            let (op, val) = parts.iter().next().unwrap();
            if ![
                "$eq",
                "$ne",
                "$gt",
                "$gte",
                "$lt",
                "$lte",
                "$in",
                "$notIn",
                "$contains",
            ]
            .contains(&op.as_str())
            {
                return Err(DynamicSearchError("Unsupported dynamic search operator"));
            }
            (op.as_str(), val)
        } else {
            ("$eq", predicate)
        };
        if matches!(operator, "$in" | "$notIn")
            && !value.as_array().is_some_and(|values| values.len() <= 1000)
        {
            return Err(DynamicSearchError("Invalid or oversized search value list"));
        }
        let Some(kind) = field_type(path, entity, models)? else {
            result.warnings.push(warning(entity, "FILTER", path));
            continue;
        };
        if operator == "$contains" && kind != "string" {
            return Err(DynamicSearchError(
                "String operator requires a string field",
            ));
        }
        if let Some(items) = value.as_array() {
            if !matches!(operator, "$in" | "$notIn") {
                return Err(DynamicSearchError("Unexpected search value list"));
            }
            for item in items {
                validate_scalar(item, kind)?;
            }
        } else {
            validate_scalar(value, kind)?;
        }
        result.filters.push(DynamicSearchFilter {
            field_path: path.clone(),
            operator: operator.into(),
            value: value.clone(),
        });
    }
    for value in orders {
        let order = value
            .as_object()
            .ok_or(DynamicSearchError("Invalid search ordering"))?;
        let path = order
            .get("field")
            .and_then(Value::as_str)
            .ok_or(DynamicSearchError("Invalid ordering field"))?;
        let direction = order
            .get("direction")
            .and_then(Value::as_str)
            .ok_or(DynamicSearchError("Invalid ordering direction"))?;
        if order.len() != 2 || !matches!(direction, "asc" | "desc") {
            return Err(DynamicSearchError("Invalid dynamic search ordering"));
        }
        if field_type(path, entity, models)?.is_none() {
            result.warnings.push(warning(entity, "ORDER_BY", path));
        } else {
            result.orders.push(DynamicSearchOrder {
                field_path: path.into(),
                descending: direction == "desc",
            });
        }
    }
    emit(&result.warnings, warn);
    Ok(result)
}

/// Compile through trusted native bindings and retain the existing scoped query.
/// Bindings own canonical names and authorization inside related queries.
pub fn merge_dynamic_search(
    base: &SelectQuery,
    source: &str,
    models: &SearchModels,
    mut filter_binding: impl FnMut(&DynamicSearchFilter) -> Result<Expr>,
    mut order_binding: impl FnMut(&DynamicSearchOrder) -> Result<OrderBy>,
    warn: Option<&mut dyn FnMut(&DynamicSearchWarning)>,
) -> Result<DynamicSearchResult> {
    let search = normalize_dynamic_search(source, &base.entity, models, 100, Some(&mut |_| {}))?;
    let filters = search
        .filters
        .iter()
        .map(&mut filter_binding)
        .collect::<Result<Vec<_>>>()?;
    let orders = search
        .orders
        .iter()
        .map(&mut order_binding)
        .collect::<Result<Vec<_>>>()?;
    let mut query = base.clone();
    for filter in filters {
        query = query.and_filter(filter);
    }
    query.order_by.extend(orders);
    emit(&search.warnings, warn);
    Ok(DynamicSearchResult {
        query,
        warnings: search.warnings,
    })
}

fn field_type<'a>(path: &str, entity: &str, models: &'a SearchModels) -> Result<Option<&'a str>> {
    let parts: Vec<_> = path.split('.').collect();
    if parts.len() > 16
        || parts.iter().any(|p| {
            p.is_empty()
                || p.starts_with('$')
                || matches!(*p, "__proto__" | "prototype" | "constructor")
        })
    {
        return Err(DynamicSearchError("Invalid search field path"));
    }
    let mut model = &models[entity];
    for part in &parts[..parts.len() - 1] {
        let Some(target) = model.relations.get(*part) else {
            return Ok(None);
        };
        model = models.get(target).ok_or(DynamicSearchError(
            "Invalid trusted search relation metadata",
        ))?;
    }
    Ok(model.fields.get(*parts.last().unwrap()).map(String::as_str))
}

fn validate_scalar(value: &Value, kind: &str) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }
    let number = value.as_f64().filter(|n| n.is_finite());
    let valid = match kind {
        "integer" | "timestamp" => {
            number.is_some_and(|n| n.abs() <= 9_007_199_254_740_991.0 && n.fract() == 0.0)
        }
        "number" => number.is_some(),
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "decimal" => number.is_some() || value.as_str().is_some_and(decimal_text),
        "date" => value.as_str().is_some_and(|s| {
            s.len() == 10
                && chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok_and(|date| {
                    date.format("%Y-%m-%d").to_string() == s && !s.starts_with("0000")
                })
        }),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(DynamicSearchError("Invalid value for known search field"))
    }
}
fn decimal_text(text: &str) -> bool {
    let text = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);
    let parts: Vec<_> = text.split('.').collect();
    parts.len() <= 2
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|c| c.is_ascii_digit()))
}
fn warning(entity: &str, clause: &'static str, path: &str) -> DynamicSearchWarning {
    DynamicSearchWarning {
        code: "DYNAMIC_SEARCH_UNKNOWN_FIELD",
        entity: entity.into(),
        clause,
        field_path: path.into(),
    }
}
fn emit(
    warnings: &[DynamicSearchWarning],
    mut warn: Option<&mut dyn FnMut(&DynamicSearchWarning)>,
) {
    for warning in warnings {
        if let Some(ref mut sink) = warn {
            sink(warning);
        } else if let Ok(json) = serde_json::to_string(warning) {
            eprintln!("{json}");
        }
    }
}
