use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{Expr, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[cfg(test)]
mod hard_limit_tests {
    use super::*;

    #[test]
    fn list_limit_defaults_rejects_and_allows_explicit_override() {
        assert_eq!(
            SelectQuery::new("Order")
                .prepare_for_list()
                .unwrap()
                .slice
                .unwrap()
                .limit,
            Some(10_000)
        );
        assert!(
            SelectQuery::new("Order")
                .limit(10_001)
                .prepare_for_list()
                .is_err()
        );
        assert!(
            SelectQuery::new("Order")
                .limit(10_001)
                .hard_limit(20_000)
                .prepare_for_list()
                .is_ok()
        );
    }

    #[test]
    fn continuous_page_fetch_is_explicit_and_validated() {
        assert!(SelectQuery::new("Order").continuous_page_fetch.is_none());
        let query =
            SelectQuery::new("Order").optimize_for_continuous_page_fetch_with("recent-orders", 30);
        let options = query.continuous_page_fetch.unwrap();
        assert_eq!(options.namespace, "recent-orders");
        assert_eq!(options.ttl_seconds, 30);
    }

    #[test]
    #[should_panic(expected = "continuous page namespace must not be empty")]
    fn continuous_page_fetch_rejects_empty_namespace() {
        let _ = SelectQuery::new("Order").optimize_for_continuous_page_fetch_with(" ", 30);
    }

    #[test]
    fn id_set_pagination_is_explicit_and_validated() {
        assert!(SelectQuery::new("Order").id_set_pagination.is_none());
        let query = SelectQuery::new("Order").optimize_pagination_with_id_set_config(
            "recent-orders",
            30,
            5_000,
        );
        let options = query.id_set_pagination.expect("ID set options");
        assert_eq!(options.namespace, "recent-orders");
        assert_eq!(options.ttl_seconds, 30);
        assert_eq!(options.max_ids, 5_000);
    }

    #[test]
    #[should_panic(expected = "ID set pagination max_ids must be positive")]
    fn id_set_pagination_rejects_zero_limit() {
        let _ = SelectQuery::new("Order").optimize_pagination_with_id_set_config("orders", 30, 0);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedExpr {
    pub alias: String,
    pub expr: Expr,
}

impl NamedExpr {
    pub fn new(alias: impl Into<String>, expr: Expr) -> Self {
        Self {
            alias: alias.into(),
            expr,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub field: String,
    pub expr: Option<Expr>,
    pub direction: SortDirection,
}

impl OrderBy {
    pub fn new(field: impl Into<String>, direction: SortDirection) -> Self {
        Self {
            field: field.into(),
            expr: None,
            direction,
        }
    }

    pub fn expr(expr: Expr, direction: SortDirection) -> Self {
        Self {
            field: String::new(),
            expr: Some(expr),
            direction,
        }
    }

    pub fn asc(field: impl Into<String>) -> Self {
        Self::new(field, SortDirection::Asc)
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self::new(field, SortDirection::Desc)
    }

    pub fn asc_expr(expr: Expr) -> Self {
        Self::expr(expr, SortDirection::Asc)
    }

    pub fn desc_expr(expr: Expr) -> Self {
        Self::expr(expr, SortDirection::Desc)
    }

    pub fn asc_gbk(field: impl Into<String>) -> Self {
        Self::asc_expr(Expr::gbk(Expr::column(field)))
    }

    pub fn desc_gbk(field: impl Into<String>) -> Self {
        Self::desc_expr(Expr::gbk(Expr::column(field)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Stddev,
    StddevPop,
    VarSamp,
    VarPop,
    BitAnd,
    BitOr,
    BitXor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub function: AggregateFunction,
    pub field: String,
    pub alias: String,
}

impl Aggregate {
    pub fn new(
        function: AggregateFunction,
        field: impl Into<String>,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            function,
            field: field.into(),
            alias: alias.into(),
        }
    }

    pub fn count(alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Count, "*", alias)
    }

    pub fn count_field(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Count, field, alias)
    }

    pub fn sum(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Sum, field, alias)
    }

    pub fn avg(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Avg, field, alias)
    }

    pub fn min(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Min, field, alias)
    }

    pub fn max(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Max, field, alias)
    }

    pub fn stddev(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Stddev, field, alias)
    }

    pub fn stddev_pop(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::StddevPop, field, alias)
    }

    pub fn var_samp(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::VarSamp, field, alias)
    }

    pub fn var_pop(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::VarPop, field, alias)
    }

    pub fn bit_and(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::BitAnd, field, alias)
    }

    pub fn bit_or(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::BitOr, field, alias)
    }

    pub fn bit_xor(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::BitXor, field, alias)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slice {
    pub limit: Option<u64>,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationLoad {
    pub name: String,
    pub query: Option<Box<SelectQuery>>,
}

impl RelationLoad {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: None,
        }
    }

    pub fn with_query(name: impl Into<String>, query: SelectQuery) -> Self {
        Self {
            name: name.into(),
            query: Some(Box::new(query)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationAggregate {
    pub relation_name: String,
    pub alias: String,
    pub query: SelectQuery,
    pub single_result: bool,
}

impl RelationAggregate {
    pub fn new(
        relation_name: impl Into<String>,
        alias: impl Into<String>,
        query: SelectQuery,
        single_result: bool,
    ) -> Self {
        Self {
            relation_name: relation_name.into(),
            alias: alias.into(),
            query,
            single_result,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSqlProjection {
    pub property_name: String,
    pub raw_sql_segment: String,
}

impl RawSqlProjection {
    pub fn new(property_name: impl Into<String>, raw_sql_segment: impl Into<String>) -> Self {
        Self {
            property_name: property_name.into(),
            raw_sql_segment: raw_sql_segment.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectGroupBy {
    pub property_name: String,
    pub storage_field: String,
    pub query: SelectQuery,
}

impl ObjectGroupBy {
    pub fn new(
        property_name: impl Into<String>,
        storage_field: impl Into<String>,
        query: SelectQuery,
    ) -> Self {
        Self {
            property_name: property_name.into(),
            storage_field: storage_field.into(),
            query,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregationCacheOptions {
    pub enabled: bool,
    pub cache_expired_millis: u64,
    pub propagate: bool,
    pub propagate_cache_expired_millis: u64,
}

impl AggregationCacheOptions {
    pub fn enabled(cache_expired_millis: u64) -> Self {
        Self {
            enabled: true,
            cache_expired_millis,
            propagate: false,
            propagate_cache_expired_millis: 0,
        }
    }

    pub fn propagate(mut self, cache_expired_millis: u64) -> Self {
        self.propagate = true;
        self.propagate_cache_expired_millis = cache_expired_millis;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamConfig {
    pub chunk_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuousPageFetchOptions {
    pub namespace: String,
    pub ttl_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdSetPaginationOptions {
    pub namespace: String,
    pub ttl_seconds: u64,
    pub max_ids: u64,
}

impl IdSetPaginationOptions {
    pub const DEFAULT_TTL_SECONDS: u64 = 600;
    pub const DEFAULT_MAX_IDS: u64 = 3_000_000;

    pub fn new(namespace: impl Into<String>, ttl_seconds: u64, max_ids: u64) -> Self {
        let namespace = namespace.into();
        assert!(
            !namespace.trim().is_empty(),
            "ID set pagination namespace must not be empty"
        );
        assert!(
            ttl_seconds > 0,
            "ID set pagination ttl_seconds must be positive"
        );
        assert!(max_ids > 0, "ID set pagination max_ids must be positive");
        Self {
            namespace,
            ttl_seconds,
            max_ids,
        }
    }
}

impl ContinuousPageFetchOptions {
    pub const DEFAULT_TTL_SECONDS: u64 = 600;

    pub fn new(namespace: impl Into<String>, ttl_seconds: u64) -> Self {
        let namespace = namespace.into();
        assert!(
            !namespace.trim().is_empty(),
            "continuous page namespace must not be empty"
        );
        assert!(
            ttl_seconds > 0,
            "continuous page ttl_seconds must be positive"
        );
        Self {
            namespace,
            ttl_seconds,
        }
    }
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self { chunk_size: 1000 }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    /// Safety ceiling for a fully materialized outer query.
    pub hard_limit: u64,
    pub entity: String,
    pub projection: Vec<String>,
    pub expr_projection: Vec<NamedExpr>,
    pub search_with_text: Option<String>,
    pub filter: Option<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub slice: Option<Slice>,
    /// Apply `slice` independently inside each value of this property.
    pub partition_by: Option<String>,
    pub aggregates: Vec<Aggregate>,
    pub group_by: Vec<String>,
    pub relations: Vec<RelationLoad>,
    pub aggregation_cache: Option<AggregationCacheOptions>,
    pub comment: Option<String>,
    pub trace_chain: Vec<crate::TraceNode>,
    pub raw_sql: Option<String>,
    pub raw_sql_search_criteria: Vec<String>,
    pub dynamic_properties: Vec<RawSqlProjection>,
    pub raw_projections: Vec<RawSqlProjection>,
    pub object_group_bys: Vec<ObjectGroupBy>,
    pub child_enhancements: Vec<SelectQuery>,
    pub stream_config: Option<StreamConfig>,
    /// Explicit, process-local hint for transparent seek pagination of outer list queries.
    pub continuous_page_fetch: Option<ContinuousPageFetchOptions>,
    /// Explicit hint to retain the complete ordered ID sequence for pagination.
    pub id_set_pagination: Option<IdSetPaginationOptions>,
}

impl SelectQuery {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            hard_limit: 10_000,
            entity: entity.into(),
            projection: Vec::new(),
            expr_projection: Vec::new(),
            search_with_text: None,
            filter: None,
            having: None,
            order_by: Vec::new(),
            slice: None,
            partition_by: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            relations: Vec::new(),
            aggregation_cache: None,
            comment: None,
            trace_chain: Vec::new(),
            raw_sql: None,
            raw_sql_search_criteria: Vec::new(),
            dynamic_properties: Vec::new(),
            raw_projections: Vec::new(),
            object_group_bys: Vec::new(),
            child_enhancements: Vec::new(),
            stream_config: None,
            continuous_page_fetch: None,
            id_set_pagination: None,
        }
    }

    pub fn project(mut self, field: impl Into<String>) -> Self {
        self.projection.push(field.into());
        self
    }

    pub fn projects(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.projection.extend(fields.into_iter().map(Into::into));
        self
    }

    pub fn project_expr(mut self, alias: impl Into<String>, expr: Expr) -> Self {
        self.expr_projection.push(NamedExpr::new(alias, expr));
        self
    }

    pub fn project_raw(
        mut self,
        alias: impl Into<String>,
        raw_sql_segment: impl Into<String>,
    ) -> Self {
        self.raw_projections
            .push(RawSqlProjection::new(alias, raw_sql_segment));
        self
    }

    pub fn dynamic_property_raw(
        mut self,
        alias: impl Into<String>,
        raw_sql_segment: impl Into<String>,
    ) -> Self {
        self.dynamic_properties
            .push(RawSqlProjection::new(alias, raw_sql_segment));
        self
    }

    pub fn search_with_text(mut self, text: impl Into<String>) -> Self {
        self.search_with_text = Some(text.into());
        self
    }

    pub fn filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn and_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(existing) => existing.and_expr(filter),
            None => filter,
        });
        self
    }

    pub fn or_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(existing) => existing.or_expr(filter),
            None => filter,
        });
        self
    }

    pub fn having(mut self, having: Expr) -> Self {
        self.having = Some(having);
        self
    }

    pub fn and_having(mut self, having: Expr) -> Self {
        self.having = Some(match self.having.take() {
            Some(existing) => existing.and_expr(having),
            None => having,
        });
        self
    }

    pub fn or_having(mut self, having: Expr) -> Self {
        self.having = Some(match self.having.take() {
            Some(existing) => existing.or_expr(having),
            None => having,
        });
        self
    }

    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    pub fn order_asc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::asc(field))
    }

    pub fn order_desc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::desc(field))
    }

    pub fn order_expr_asc(self, expr: Expr) -> Self {
        self.order_by(OrderBy::asc_expr(expr))
    }

    pub fn order_expr_desc(self, expr: Expr) -> Self {
        self.order_by(OrderBy::desc_expr(expr))
    }

    pub fn order_gbk_asc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::asc_gbk(field))
    }

    pub fn order_gbk_desc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::desc_gbk(field))
    }

    pub fn group_by(mut self, field: impl Into<String>) -> Self {
        self.group_by.push(field.into());
        self
    }

    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregates.push(aggregate);
        self
    }

    pub fn count(self, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::count(alias))
    }

    pub fn count_field(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::count_field(field, alias))
    }

    pub fn sum(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::sum(field, alias))
    }

    pub fn avg(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::avg(field, alias))
    }

    pub fn min(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::min(field, alias))
    }

    pub fn max(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::max(field, alias))
    }

    pub fn stddev(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::stddev(field, alias))
    }

    pub fn stddev_pop(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::stddev_pop(field, alias))
    }

    pub fn var_samp(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::var_samp(field, alias))
    }

    pub fn var_pop(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::var_pop(field, alias))
    }

    pub fn bit_and(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::bit_and(field, alias))
    }

    pub fn bit_or(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::bit_or(field, alias))
    }

    pub fn bit_xor(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::bit_xor(field, alias))
    }

    pub fn enable_aggregation_cache(self) -> Self {
        self.enable_aggregation_cache_for(0)
    }

    pub fn enable_aggregation_cache_for(mut self, cache_expired_millis: u64) -> Self {
        self.aggregation_cache = Some(AggregationCacheOptions::enabled(cache_expired_millis));
        self
    }

    pub fn propagate_aggregation_cache(mut self, cache_expired_millis: u64) -> Self {
        self.aggregation_cache = Some(
            self.aggregation_cache
                .unwrap_or_else(|| AggregationCacheOptions::enabled(0))
                .propagate(cache_expired_millis),
        );
        self
    }

    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        let comment_str = comment.into();
        self.comment = Some(comment_str.clone());
        self.trace_chain.push(crate::TraceNode {
            entity_type: self.entity.clone(),
            entity_id: None,
            comment: comment_str,
        });
        self
    }

    pub fn raw_sql(mut self, raw_sql: impl Into<String>) -> Self {
        self.raw_sql = Some(raw_sql.into());
        self
    }

    pub fn raw_sql_search_criteria(mut self, raw_sql: impl Into<String>) -> Self {
        self.raw_sql_search_criteria.push(raw_sql.into());
        self
    }

    pub fn object_group_by(
        mut self,
        property_name: impl Into<String>,
        storage_field: impl Into<String>,
        query: SelectQuery,
    ) -> Self {
        self.object_group_bys
            .push(ObjectGroupBy::new(property_name, storage_field, query));
        self
    }

    pub fn child_enhancement(mut self, query: SelectQuery) -> Self {
        self.child_enhancements.push(query);
        self
    }

    pub fn relation(mut self, name: impl Into<String>) -> Self {
        self.relations.push(RelationLoad::new(name));
        self
    }

    pub fn relation_query(mut self, name: impl Into<String>, query: SelectQuery) -> Self {
        self.relations.push(RelationLoad::with_query(name, query));
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        let slice = self.slice.get_or_insert(Slice {
            limit: None,
            offset: 0,
        });
        slice.limit = Some(limit);
        self
    }

    /// Override the outer materialized-list ceiling. Most callers should keep 10,000.
    pub fn hard_limit(mut self, hard_limit: u64) -> Self {
        assert!(hard_limit > 0, "hard_limit must be positive");
        self.hard_limit = hard_limit;
        self
    }

    /// Apply and validate list-materialization limits. This is intentionally not
    /// used by streaming execution.
    pub fn prepare_for_list(mut self) -> Result<Self, String> {
        self.apply_list_limit(self.hard_limit, true)?;
        Ok(self)
    }

    fn apply_list_limit(&mut self, ceiling: u64, outer: bool) -> Result<(), String> {
        let slice = self.slice.get_or_insert(Slice {
            limit: None,
            offset: 0,
        });
        match slice.limit {
            Some(limit) if limit > ceiling => {
                return Err(format!(
                    "QUERY_HARD_LIMIT_EXCEEDED: requested limit {limit} exceeds hard limit {ceiling}"
                ));
            }
            None => slice.limit = Some(ceiling),
            _ => {}
        }
        for relation in &mut self.relations {
            if let Some(query) = relation.query.as_mut() {
                query.apply_list_limit(10_000, false)?;
            }
        }
        for query in &mut self.child_enhancements {
            query.apply_list_limit(10_000, false)?;
        }
        let _ = outer;
        Ok(())
    }

    pub fn offset(mut self, offset: u64) -> Self {
        let slice = self.slice.get_or_insert(Slice {
            limit: None,
            offset: 0,
        });
        slice.offset = offset;
        self
    }

    pub fn page(self, offset: u64, limit: u64) -> Self {
        self.offset(offset).limit(limit)
    }

    pub fn optimize_for_continuous_page_fetch(mut self) -> Self {
        self.continuous_page_fetch = Some(ContinuousPageFetchOptions::new(
            "default",
            ContinuousPageFetchOptions::DEFAULT_TTL_SECONDS,
        ));
        self
    }

    pub fn optimize_for_continuous_page_fetch_with(
        mut self,
        namespace: impl Into<String>,
        ttl_seconds: u64,
    ) -> Self {
        self.continuous_page_fetch = Some(ContinuousPageFetchOptions::new(namespace, ttl_seconds));
        self
    }

    pub fn optimize_pagination_with_id_set(mut self) -> Self {
        self.id_set_pagination = Some(IdSetPaginationOptions::new(
            "default",
            IdSetPaginationOptions::DEFAULT_TTL_SECONDS,
            IdSetPaginationOptions::DEFAULT_MAX_IDS,
        ));
        self
    }

    pub fn optimize_pagination_with_id_set_config(
        mut self,
        namespace: impl Into<String>,
        ttl_seconds: u64,
        max_ids: u64,
    ) -> Self {
        self.id_set_pagination = Some(IdSetPaginationOptions::new(namespace, ttl_seconds, max_ids));
        self
    }

    /// Scope pagination to each distinct value of `field`.
    ///
    /// Relation loading sets this automatically. Most application queries
    /// should use a generated relation selector instead of calling this
    /// method directly.
    pub fn partition_by(mut self, field: impl Into<String>) -> Self {
        self.partition_by = Some(field.into());
        self
    }

    /// Enable streaming mode with the given chunk size.
    /// When streaming, rows are fetched and enhanced in batches rather than all at once.
    pub fn stream(mut self, chunk_size: usize) -> Self {
        self.stream_config = Some(StreamConfig { chunk_size });
        self
    }

    /// Enable streaming mode with default chunk size (1000).
    pub fn stream_default(mut self) -> Self {
        self.stream_config = Some(StreamConfig::default());
        self
    }
}

pub type Record = BTreeMap<String, Value>;

/// A database result row whose column names are shared by the whole result set.
///
/// Providers use this representation for typed decoding so a 100-row result does
/// not allocate 100 copies of every projected column name (or 100 B-trees).
#[derive(Debug, Clone, PartialEq)]
pub struct CompactRow {
    columns: Arc<[String]>,
    values: Vec<Value>,
}

impl CompactRow {
    pub fn new(columns: Arc<[String]>, values: Vec<Value>) -> Self {
        debug_assert_eq!(columns.len(), values.len());
        Self { columns, values }
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|column| column == name)
            .and_then(|index| self.values.get(index))
    }

    pub fn shared_columns(&self) -> Arc<[String]> {
        self.columns.clone()
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.columns
            .iter()
            .position(|column| column == name)
            .and_then(|index| self.values.get_mut(index))
    }

    /// Adds or replaces a projected value. Column layouts remain shared until
    /// an enhancement actually changes the shape of this row.
    pub fn insert(&mut self, name: String, value: Value) -> Option<Value> {
        if let Some(index) = self.columns.iter().position(|column| column == &name) {
            return Some(std::mem::replace(&mut self.values[index], value));
        }
        let mut columns = self.columns.to_vec();
        columns.push(name);
        self.columns = columns.into();
        self.values.push(value);
        None
    }

    pub fn remove(&mut self, name: &str) -> Option<Value> {
        let index = self.columns.iter().position(|column| column == name)?;
        let mut columns = self.columns.to_vec();
        columns.remove(index);
        self.columns = columns.into();
        Some(self.values.remove(index))
    }

    pub fn extend(&mut self, other: CompactRow) {
        for (name, value) in other.columns.iter().cloned().zip(other.values) {
            self.insert(name, value);
        }
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.columns.iter().any(|column| column == name)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.columns.iter().zip(self.values.iter())
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.columns.iter()
    }

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    pub fn into_map(self) -> BTreeMap<String, Value> {
        self.columns.iter().cloned().zip(self.values).collect()
    }

    /// Transitional boundary adapter. Core query providers should construct
    /// compact rows directly instead of routing through this function.
    pub fn from_map(values_by_name: BTreeMap<String, Value>) -> Self {
        let (columns, values): (Vec<_>, Vec<_>) = values_by_name.into_iter().unzip();
        Self::new(columns.into(), values)
    }
}

impl From<BTreeMap<String, Value>> for CompactRow {
    fn from(values: BTreeMap<String, Value>) -> Self {
        Self::from_map(values)
    }
}

/// Internal projection used to implement per-parent pagination for relation
/// loads. Runtime relation attachment removes it before exposing child rows.
pub const PARTITION_RANK_PROPERTY: &str = "__teaql_partition_rank";

pub fn record_to_json_value(record: &Record) -> serde_json::Value {
    serde_json::Value::Object(
        record
            .iter()
            .map(|(key, value)| (key.clone(), value.to_json_value()))
            .collect(),
    )
}

pub fn compact_row_to_json_value(row: &CompactRow) -> serde_json::Value {
    serde_json::Value::Object(
        row.iter()
            .map(|(key, value)| (key.clone(), value.to_json_value()))
            .collect(),
    )
}
