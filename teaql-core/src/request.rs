//! Query builder layer types for TeaQL.
//!
//! This module contains the builder-side query types that were previously generated
//! by the code generator's StringTemplate. They are the static, domain-independent
//! parts shared by every generated TeaQL crate.
//!
//! Several types here intentionally shadow names from the parent crate (e.g.
//! [`RelationAggregate`], [`ObjectGroupBy`], [`RawProjection`]). The builder
//! versions carry a [`QuerySelection`] while the core/query versions carry a
//! [`SelectQuery`]. The conversion happens in [`QuerySelection::into_query`] and
//! [`apply_runtime_metadata`].

use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::{
    BinaryOp, Expr, ObjectGroupBy as CoreObjectGroupBy, RawSqlProjection as CoreRawSqlProjection,
    Record, RelationAggregate as RuntimeRelationAggregate, SelectQuery, SmartList, Value,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const COUNT_ALIAS: &str = "count";
pub const TYPE_FIELD: &str = "internal_type";
pub const TYPE_GROUP_FIELD: &str = "type_group";

// ---------------------------------------------------------------------------
// FieldOperator
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Between,
    In,
    NotIn,
    Contain,
    NotContain,
    BeginWith,
    NotBeginWith,
    EndWith,
    NotEndWith,
    SoundsLike,
    IsNull,
    IsNotNull,
}

// ---------------------------------------------------------------------------
// DateRange
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct DateRange<T> {
    pub start: T,
    pub end: T,
}

impl<T> DateRange<T> {
    pub fn new(start: T, end: T) -> Self {
        Self { start, end }
    }
}

// ---------------------------------------------------------------------------
// EntityReference
// ---------------------------------------------------------------------------

pub trait EntityReference {
    fn entity_id_value(self) -> Value;
}

impl EntityReference for Value {
    fn entity_id_value(self) -> Value {
        self
    }
}

impl EntityReference for u64 {
    fn entity_id_value(self) -> Value {
        Value::U64(self)
    }
}

// ---------------------------------------------------------------------------
// QuerySelection
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct QuerySelection {
    pub query: SelectQuery,
    pub relation_selections: Vec<RelationSelection>,
    pub relation_filters: Vec<RelationFilter>,
    pub child_enhancements: Vec<QuerySelection>,
    pub query_options: QueryOptions,
}

impl QuerySelection {
    pub fn new(query: impl Into<SelectQuery>) -> Self {
        Self {
            query: query.into(),
            relation_selections: Vec::new(),
            relation_filters: Vec::new(),
            child_enhancements: Vec::new(),
            query_options: QueryOptions::default(),
        }
    }

    pub fn into_query(self) -> SelectQuery {
        let query = apply_relation_selections(self.query, self.relation_selections);
        apply_runtime_metadata(query, &self.query_options, &self.child_enhancements)
    }
}

impl From<SelectQuery> for QuerySelection {
    fn from(query: SelectQuery) -> Self {
        QuerySelection::new(query)
    }
}

// ---------------------------------------------------------------------------
// RelationSelection
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct RelationSelection {
    pub name: String,
    pub query: SelectQuery,
    pub relation_selections: Vec<RelationSelection>,
    pub relation_filters: Vec<RelationFilter>,
    pub child_enhancements: Vec<QuerySelection>,
    pub query_options: QueryOptions,
}

impl RelationSelection {
    pub fn new(name: impl Into<String>, selection: impl Into<QuerySelection>) -> Self {
        let selection = selection.into();
        Self {
            name: name.into(),
            query: selection.query,
            relation_selections: selection.relation_selections,
            relation_filters: selection.relation_filters,
            child_enhancements: selection.child_enhancements,
            query_options: selection.query_options,
        }
    }

    pub fn into_query(self) -> SelectQuery {
        let query = apply_relation_selections(self.query, self.relation_selections);
        apply_runtime_metadata(query, &self.query_options, &self.child_enhancements)
    }
}

// ---------------------------------------------------------------------------
// RelationFilter
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct RelationFilter {
    pub name: String,
    pub query: SelectQuery,
    pub relation_selections: Vec<RelationSelection>,
    pub relation_filters: Vec<RelationFilter>,
    pub child_enhancements: Vec<QuerySelection>,
    pub query_options: QueryOptions,
}

impl RelationFilter {
    pub fn new(name: impl Into<String>, selection: impl Into<QuerySelection>) -> Self {
        let selection = selection.into();
        Self {
            name: name.into(),
            query: selection.query,
            relation_selections: selection.relation_selections,
            relation_filters: selection.relation_filters,
            child_enhancements: selection.child_enhancements,
            query_options: selection.query_options,
        }
    }
}

// ---------------------------------------------------------------------------
// QueryOptions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryOptions {
    pub comment: Option<String>,
    pub raw_sql: Option<String>,
    pub raw_sql_search_criteria: Vec<String>,
    pub dynamic_properties: Vec<RawDynamicProperty>,
    pub raw_projections: Vec<RawProjection>,
    pub relation_aggregates: Vec<RelationAggregate>,
    pub object_group_bys: Vec<ObjectGroupBy>,
    pub facets: Vec<FacetRequest>,
}

// ---------------------------------------------------------------------------
// UnsafeRawSqlSegment
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsafeRawSqlSegment {
    sql: String,
}

impl UnsafeRawSqlSegment {
    pub fn trusted(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }

    pub fn into_sql(self) -> String {
        self.sql
    }
}

// ---------------------------------------------------------------------------
// RawDynamicProperty
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawDynamicProperty {
    pub property_name: String,
    pub raw_sql_segment: String,
}

impl RawDynamicProperty {
    pub fn new(property_name: impl Into<String>, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        Self {
            property_name: property_name.into(),
            raw_sql_segment: raw_sql_segment.into_sql(),
        }
    }
}

// ---------------------------------------------------------------------------
// RawProjection (builder version — distinct from crate::RawSqlProjection)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawProjection {
    pub property_name: String,
    pub raw_sql_segment: String,
}

impl RawProjection {
    pub fn new(property_name: impl Into<String>, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        Self {
            property_name: property_name.into(),
            raw_sql_segment: raw_sql_segment.into_sql(),
        }
    }
}

// ---------------------------------------------------------------------------
// RelationAggregate (builder version — carries QuerySelection, not SelectQuery)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct RelationAggregate {
    pub relation_name: String,
    pub alias: String,
    pub query: QuerySelection,
    pub single_result: bool,
}

impl RelationAggregate {
    pub fn new(
        relation_name: impl Into<String>,
        alias: impl Into<String>,
        query: impl Into<QuerySelection>,
        single_result: bool,
    ) -> Self {
        Self {
            relation_name: relation_name.into(),
            alias: alias.into(),
            query: query.into(),
            single_result,
        }
    }
}

// ---------------------------------------------------------------------------
// FacetRequest
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct FacetRequest {
    pub facet_name: String,
    pub relation_name: String,
    pub query: QuerySelection,
    pub include_all_facets: bool,
}

impl FacetRequest {
    pub fn new(
        facet_name: impl Into<String>,
        relation_name: impl Into<String>,
        query: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        Self {
            facet_name: facet_name.into(),
            relation_name: relation_name.into(),
            query: query.into(),
            include_all_facets,
        }
    }
}

// ---------------------------------------------------------------------------
// ObjectGroupBy (builder version — carries QuerySelection, not SelectQuery)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct ObjectGroupBy {
    pub property_name: String,
    pub storage_field: String,
    pub query: QuerySelection,
}

impl ObjectGroupBy {
    pub fn new(
        property_name: impl Into<String>,
        storage_field: impl Into<String>,
        query: impl Into<QuerySelection>,
    ) -> Self {
        Self {
            property_name: property_name.into(),
            storage_field: storage_field.into(),
            query: query.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Relation selection / runtime metadata helpers
// ---------------------------------------------------------------------------

pub fn apply_relation_selections(
    mut query: SelectQuery,
    relation_selections: Vec<RelationSelection>,
) -> SelectQuery {
    for selection in relation_selections {
        query = query.relation_query(selection.name.clone(), selection.into_query());
    }
    query
}

pub fn apply_runtime_metadata(
    mut query: SelectQuery,
    options: &QueryOptions,
    child_enhancements: &[QuerySelection],
) -> SelectQuery {
    if let Some(c) = options.comment.clone() {
        query = query.comment(c);
    }
    query.raw_sql = options.raw_sql.clone();
    query.raw_sql_search_criteria = options.raw_sql_search_criteria.clone();
    query.dynamic_properties = options
        .dynamic_properties
        .iter()
        .map(|projection| {
            CoreRawSqlProjection::new(
                projection.property_name.clone(),
                projection.raw_sql_segment.clone(),
            )
        })
        .collect();
    query.raw_projections = options
        .raw_projections
        .iter()
        .map(|projection| {
            CoreRawSqlProjection::new(
                projection.property_name.clone(),
                projection.raw_sql_segment.clone(),
            )
        })
        .collect();
    query.object_group_bys = options
        .object_group_bys
        .iter()
        .map(|group_by| {
            CoreObjectGroupBy::new(
                group_by.property_name.clone(),
                group_by.storage_field.clone(),
                group_by.query.clone().into_query(),
            )
        })
        .collect();
    query.child_enhancements = child_enhancements
        .iter()
        .cloned()
        .map(QuerySelection::into_query)
        .collect();
    query
}

// ---------------------------------------------------------------------------
// runtime_relation_aggregates — converts builder → core RelationAggregate
// ---------------------------------------------------------------------------

pub fn runtime_relation_aggregates(options: &QueryOptions) -> Vec<RuntimeRelationAggregate> {
    options
        .relation_aggregates
        .iter()
        .map(|aggregate| {
            RuntimeRelationAggregate::new(
                aggregate.relation_name.clone(),
                aggregate.alias.clone(),
                aggregate.query.clone().into_query(),
                aggregate.single_result,
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Facet helpers
// ---------------------------------------------------------------------------

pub fn merge_outer_filter_into_facet_aggregates(
    selection: &mut QuerySelection,
    outer_query: &SelectQuery,
) {
    let Some(filter) = outer_query.filter.clone() else {
        return;
    };
    for aggregate in &mut selection.query_options.relation_aggregates {
        if aggregate.query.query.entity == outer_query.entity {
            aggregate.query.query = aggregate.query.query.clone().and_filter(filter.clone());
        }
    }
}

pub fn attach_facets<T>(rows: &mut SmartList<T>, facets: BTreeMap<String, SmartList<Record>>) {
    for (name, facet) in facets {
        rows.add_facet(name, facet);
    }
}

// ---------------------------------------------------------------------------
// field_operator_expr / field_operator_column_expr
// ---------------------------------------------------------------------------

pub fn field_operator_expr(field: &str, operator: FieldOperator, values: Vec<Value>) -> Expr {
    match operator {
        FieldOperator::Equal => Expr::eq(field, required_value(operator, &values, 0)),
        FieldOperator::NotEqual => Expr::ne(field, required_value(operator, &values, 0)),
        FieldOperator::GreaterThan => Expr::gt(field, required_value(operator, &values, 0)),
        FieldOperator::GreaterThanOrEqual => Expr::gte(field, required_value(operator, &values, 0)),
        FieldOperator::LessThan => Expr::lt(field, required_value(operator, &values, 0)),
        FieldOperator::LessThanOrEqual => Expr::lte(field, required_value(operator, &values, 0)),
        FieldOperator::Between => Expr::between(
            field,
            required_value(operator, &values, 0),
            required_value(operator, &values, 1),
        ),
        FieldOperator::In => Expr::in_list(field, values),
        FieldOperator::NotIn => Expr::not_in_list(field, values),
        FieldOperator::Contain => Expr::contain(field, required_text(operator, &values, 0)),
        FieldOperator::NotContain => Expr::not_contain(field, required_text(operator, &values, 0)),
        FieldOperator::BeginWith => Expr::begin_with(field, required_text(operator, &values, 0)),
        FieldOperator::NotBeginWith => {
            Expr::not_begin_with(field, required_text(operator, &values, 0))
        }
        FieldOperator::EndWith => Expr::end_with(field, required_text(operator, &values, 0)),
        FieldOperator::NotEndWith => Expr::not_end_with(field, required_text(operator, &values, 0)),
        FieldOperator::SoundsLike => Expr::sound_like(field, required_value(operator, &values, 0)),
        FieldOperator::IsNull => Expr::is_null(field),
        FieldOperator::IsNotNull => Expr::is_not_null(field),
    }
}

pub fn field_operator_column_expr(field: &str, operator: FieldOperator, other_field: &str) -> Expr {
    let binary_op = match operator {
        FieldOperator::Equal => BinaryOp::Eq,
        FieldOperator::NotEqual => BinaryOp::Ne,
        FieldOperator::GreaterThan => BinaryOp::Gt,
        FieldOperator::GreaterThanOrEqual => BinaryOp::Gte,
        FieldOperator::LessThan => BinaryOp::Lt,
        FieldOperator::LessThanOrEqual => BinaryOp::Lte,
        FieldOperator::Contain => BinaryOp::Like,
        FieldOperator::NotContain => BinaryOp::NotLike,
        FieldOperator::BeginWith => BinaryOp::Like,
        FieldOperator::NotBeginWith => BinaryOp::NotLike,
        FieldOperator::EndWith => BinaryOp::Like,
        FieldOperator::NotEndWith => BinaryOp::NotLike,
        unsupported => panic!("{unsupported:?} is not supported for property-to-property filters"),
    };
    Expr::compare_columns(field, binary_op, other_field)
}

// ---------------------------------------------------------------------------
// required_value / required_text
// ---------------------------------------------------------------------------

pub fn required_value(operator: FieldOperator, values: &[Value], index: usize) -> Value {
    values
        .get(index)
        .cloned()
        .unwrap_or_else(|| panic!("{operator:?} requires value at index {index}"))
}

pub fn required_text(operator: FieldOperator, values: &[Value], index: usize) -> String {
    match required_value(operator, values, index) {
        Value::Text(value) => value,
        value => panic!("{operator:?} requires text value, got {value:?}"),
    }
}

// ---------------------------------------------------------------------------
// remove_default_live_filter / remove_filter_expr
// ---------------------------------------------------------------------------

pub fn remove_default_live_filter(filter: Option<Expr>) -> Option<Expr> {
    let default_filter = Expr::gt("version", 0_i64);
    remove_filter_expr(filter?, &default_filter)
}

pub fn remove_filter_expr(filter: Expr, target: &Expr) -> Option<Expr> {
    if &filter == target {
        return None;
    }
    match filter {
        Expr::And(parts) => {
            let mut retained = parts
                .into_iter()
                .filter_map(|part| remove_filter_expr(part, target))
                .collect::<Vec<_>>();
            match retained.len() {
                0 => None,
                1 => retained.pop(),
                _ => Some(Expr::And(retained)),
            }
        }
        other => Some(other),
    }
}

// ---------------------------------------------------------------------------
// Dynamic JSON helpers
// ---------------------------------------------------------------------------

pub fn dynamic_json_value_to_teaql_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::Number(value) => value
            .as_i64()
            .map(Value::I64)
            .or_else(|| value.as_u64().map(Value::U64))
            .or_else(|| value.as_f64().map(Value::F64))
            .unwrap_or(Value::Null),
        JsonValue::String(value) => Value::Text(value.trim().to_owned()),
        JsonValue::Array(values) => Value::List(
            values
                .iter()
                .map(dynamic_json_value_to_teaql_value)
                .collect(),
        ),
        JsonValue::Object(object) => object
            .get("id")
            .map(dynamic_json_value_to_teaql_value)
            .unwrap_or(Value::Null),
    }
}

pub fn dynamic_json_values(value: &JsonValue) -> Vec<Value> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .map(dynamic_json_value_to_teaql_value)
            .collect(),
        value => vec![dynamic_json_value_to_teaql_value(value)],
    }
}

pub fn dynamic_json_operator(value: &JsonValue) -> FieldOperator {
    match value {
        JsonValue::String(value) if value.eq_ignore_ascii_case("__is_null__") => {
            FieldOperator::IsNull
        }
        JsonValue::String(value) if value.eq_ignore_ascii_case("__is_not_null__") => {
            FieldOperator::IsNotNull
        }
        JsonValue::String(_) => FieldOperator::Contain,
        JsonValue::Number(_) | JsonValue::Bool(_) => FieldOperator::Equal,
        JsonValue::Array(values) if values.first().map(JsonValue::is_string).unwrap_or(false) => {
            FieldOperator::In
        }
        JsonValue::Array(values) if values.first().map(JsonValue::is_object).unwrap_or(false) => {
            FieldOperator::In
        }
        JsonValue::Array(values) if values.len() == 2 => FieldOperator::Between,
        _ => FieldOperator::Equal,
    }
}

pub fn dynamic_json_filter_expr(field: &str, value: &JsonValue) -> Expr {
    let operator = dynamic_json_operator(value);
    field_operator_expr(field, operator, dynamic_json_values(value))
}

pub fn dynamic_json_u64_field(
    object: &serde_json::Map<String, JsonValue>,
    field: &str,
) -> Option<u64> {
    object.get(field).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
    })
}
