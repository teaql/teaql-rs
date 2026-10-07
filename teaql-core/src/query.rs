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

    #[test]
    fn topn_001_002_003_probe_threshold_is_explicit_and_bounded() {
        assert!(
            SelectQuery::new("Trip")
                .top_n_probe_parent_threshold
                .is_none()
        );
        assert_eq!(
            SelectQuery::new("Trip")
                .top_n_probe_parent_threshold(32)
                .top_n_probe_parent_threshold,
            Some(32)
        );
        assert_eq!(
            SelectQuery::new("Trip")
                .top_n_probe_parent_threshold(0)
                .top_n_probe_parent_threshold,
            Some(0)
        );
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
    /// Optional bounded-probe override for per-parent Top-N relation loading.
    /// Zero forces the provider's window plan; a positive value permits probes
    /// only when the already-loaded parent count does not exceed the value.
    pub top_n_probe_parent_threshold: Option<usize>,
    pub aggregates: Vec<Aggregate>,
    pub group_by: Vec<String>,
    pub relations: Vec<RelationLoad>,
    /// Query-only Facet selections retained across ordinary relation planning.
    /// The relation loader executes these, never the physical SQL compiler.
    pub facets: Vec<crate::request::FacetRequest>,
    /// Runtime-assembled related metrics retained by nested typed selections.
    /// These are not physical SQL aggregates or relation projection requests.
    pub relation_aggregates: Vec<RelationAggregate>,
    pub aggregation_cache: Option<AggregationCacheOptions>,
    pub comment: Option<String>,
    /// Explicit request purpose; trace nodes are diagnostic lineage, not intent.
    pub purpose: Option<String>,
    pub trace_chain: Vec<crate::TraceNode>,
    pub raw_sql: Option<String>,
    pub raw_sql_search_criteria: Vec<String>,
    pub dynamic_properties: Vec<RawSqlProjection>,
    /// Persistent extensions loaded by the context-owned provider, never compiled as physical SQL fields.
    pub dynamic_field_selection:
        Option<std::sync::Arc<crate::dynamic_fields::DynamicFieldSelection>>,
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
            top_n_probe_parent_threshold: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            relations: Vec::new(),
            facets: Vec::new(),
            relation_aggregates: Vec::new(),
            aggregation_cache: None,
            comment: None,
            purpose: None,
            trace_chain: Vec::new(),
            raw_sql: None,
            raw_sql_search_criteria: Vec::new(),
            dynamic_properties: Vec::new(),
            dynamic_field_selection: None,
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

    pub fn select_dynamic_fields(
        mut self,
        selection: crate::dynamic_fields::DynamicFieldSelection,
    ) -> Self {
        self.dynamic_field_selection = Some(std::sync::Arc::new(selection));
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
            kind: crate::TraceKind::Comment,
            entity_type: self.entity.clone(),
            entity_id: None,
            comment: comment_str,
        });
        self
    }

    pub fn purpose(mut self, purpose: impl Into<String>) -> Self {
        self.purpose = Some(purpose.into());
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

    /// Override the provider's per-parent Top-N execution policy.
    ///
    /// `0` selects the window plan. A positive threshold selects bounded
    /// probes only when the parent rows already in memory are at or below it.
    pub fn top_n_probe_parent_threshold(mut self, threshold: usize) -> Self {
        self.top_n_probe_parent_threshold = Some(threshold);
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

// Kept behind one optional pointer so rows without query-result metadata stay
// compact. The result carrier is separate from projected/persistent values.
#[derive(Debug, Clone, Default, PartialEq)]
struct LoadedRelationResults {
    lists: BTreeMap<String, crate::SmartList<CompactRow>>,
    dynamic_fields: Option<crate::dynamic_fields::DynamicFieldValues>,
}

/// A database result row whose column names are shared by the whole result set.
///
/// Providers use this representation for typed decoding so a 100-row result does
/// not allocate 100 copies of every projected column name (or 100 B-trees).
/// Result-shape metadata shared by rows. Cached availability never contains values or ledgers.
#[derive(Debug)]
pub struct CompactRowLayout {
    names: Arc<[String]>,
    relation_names: Arc<[String]>,
    primary: std::sync::OnceLock<Arc<crate::LoadedSnapshot>>,
    other_types:
        std::sync::Mutex<std::collections::HashMap<usize, std::sync::Weak<crate::LoadedSnapshot>>>,
}

impl CompactRowLayout {
    pub fn new(names: Arc<[String]>) -> Arc<Self> {
        Self::with_relations(names, Arc::from([]))
    }

    fn with_relations(names: Arc<[String]>, relation_names: Arc<[String]>) -> Arc<Self> {
        Arc::new(Self {
            names,
            relation_names,
            primary: Default::default(),
            other_types: Default::default(),
        })
    }

    fn load_state(&self, layout: Arc<crate::FieldLayout>) -> crate::eval::LoadState {
        let primary = self.primary.get_or_init(|| {
            crate::LoadedSnapshot::projection(
                layout.clone(),
                self.names
                    .iter()
                    .chain(self.relation_names.iter())
                    .map(String::as_str),
            )
            .into_shared()
        });
        if Arc::ptr_eq(primary.layout(), &layout) {
            return crate::eval::LoadState::Indexed(primary.clone());
        }
        // Unusual case: more than one typed decoder consumes one result shape.
        let key = Arc::as_ptr(&layout) as usize;
        let mut states = self
            .other_types
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        states.retain(|_, state| state.strong_count() > 0);
        if let Some(state) = states.get(&key).and_then(std::sync::Weak::upgrade) {
            return crate::eval::LoadState::Indexed(state);
        }
        let state = crate::LoadedSnapshot::projection(
            layout,
            self.names
                .iter()
                .chain(self.relation_names.iter())
                .map(String::as_str),
        )
        .into_shared();
        states.insert(key, Arc::downgrade(&state));
        crate::eval::LoadState::Indexed(state)
    }
}

impl std::ops::Deref for CompactRowLayout {
    type Target = [String];
    fn deref(&self) -> &Self::Target {
        &self.names
    }
}

impl PartialEq for CompactRowLayout {
    fn eq(&self, other: &Self) -> bool {
        self.names == other.names && self.relation_names == other.relation_names
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompactRow {
    columns: Arc<CompactRowLayout>,
    values: Vec<Value>,
    // Read-result metadata only. Not a projected field, serialized Value, or
    // mutation snapshot. Ordinary provider rows allocate no sidecar.
    loaded_relations: Option<Box<LoadedRelationResults>>,
}

/// Bounded to one hydration pass. Retaining the source shape prevents address
/// reuse from confusing projections while no entity values are retained.
#[doc(hidden)]
#[derive(Default)]
pub struct RelationShapeCache {
    relation: Option<String>,
    shapes: std::collections::HashMap<usize, (Arc<CompactRowLayout>, Arc<CompactRowLayout>)>,
}

impl CompactRow {
    pub fn new(columns: Arc<[String]>, values: Vec<Value>) -> Self {
        Self::with_layout(CompactRowLayout::new(columns), values)
    }

    pub fn with_layout(columns: Arc<CompactRowLayout>, values: Vec<Value>) -> Self {
        assert_eq!(
            columns.len(),
            values.len(),
            "row values must match the actual projection layout"
        );
        Self {
            columns,
            values,
            loaded_relations: None,
        }
    }

    pub fn loaded_relation(&self, name: &str) -> Option<&crate::SmartList<CompactRow>> {
        self.loaded_relations.as_ref()?.lists.get(name)
    }

    pub fn set_loaded_relation(&mut self, name: String, list: crate::SmartList<CompactRow>) {
        self.loaded_relations
            .get_or_insert_with(Default::default)
            .lists
            .insert(name, list);
    }

    pub fn take_loaded_relation(&mut self, name: &str) -> Option<crate::SmartList<CompactRow>> {
        self.loaded_relations.as_mut()?.lists.remove(name)
    }

    /// Framework read metadata, never an SQL column or mutation payload.
    #[doc(hidden)]
    pub fn set_loaded_dynamic_fields(&mut self, values: crate::dynamic_fields::DynamicFieldValues) {
        self.loaded_relations
            .get_or_insert_with(Default::default)
            .dynamic_fields = Some(values);
    }

    #[doc(hidden)]
    pub fn merge_loaded_dynamic_fields(
        &mut self,
        values: crate::dynamic_fields::DynamicFieldValues,
        shapes: &mut crate::dynamic_fields::DynamicFieldMergeShapes,
    ) -> Result<(), crate::dynamic_fields::DynamicFieldError> {
        let metadata = self.loaded_relations.get_or_insert_with(Default::default);
        if let Some(current) = metadata.dynamic_fields.as_mut() {
            current.merge_loaded(values, shapes)?;
        } else {
            metadata.dynamic_fields = Some(values);
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn take_loaded_dynamic_fields(
        &mut self,
    ) -> Option<crate::dynamic_fields::DynamicFieldValues> {
        self.loaded_relations.as_mut()?.dynamic_fields.take()
    }

    pub fn has_loaded_relations(&self) -> bool {
        self.loaded_relations
            .as_ref()
            .is_some_and(|relations| !relations.lists.is_empty())
    }

    /// Drop read-only relation metadata before retaining a persistence snapshot.
    pub fn clear_loaded_relations(&mut self) {
        self.loaded_relations = None;
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|column| column == name)
            .and_then(|index| self.values.get(index))
    }

    pub fn shared_columns(&self) -> Arc<[String]> {
        self.columns.names.clone()
    }

    pub fn shared_layout(&self) -> Arc<CompactRowLayout> {
        self.columns.clone()
    }

    pub fn indexed_load_state(&self, layout: Arc<crate::FieldLayout>) -> crate::eval::LoadState {
        self.columns.load_state(layout)
    }

    /// Query-only edge availability lives in the shared shape, never a row value.
    #[doc(hidden)]
    pub fn mark_relation_loaded(&mut self, name: &str, shapes: &mut RelationShapeCache) {
        if self.is_loaded_relation(name) {
            return;
        }
        let key = Arc::as_ptr(&self.columns) as usize;
        if shapes.relation.as_deref() != Some(name) {
            shapes.shapes.clear();
            shapes.relation = Some(name.to_owned());
        }
        self.columns = shapes
            .shapes
            .entry(key)
            .or_insert_with(|| {
                let mut relations = self.columns.relation_names.to_vec();
                relations.push(name.to_owned());
                (
                    self.columns.clone(),
                    CompactRowLayout::with_relations(self.columns.names.clone(), relations.into()),
                )
            })
            .1
            .clone();
    }

    #[doc(hidden)]
    pub fn is_loaded_relation(&self, name: &str) -> bool {
        self.columns
            .relation_names
            .iter()
            .any(|relation| relation == name)
    }

    /// Share actual shapes after relation hydration. The temporary pool cannot
    /// retain historical projections, entity values or mutation ownership.
    pub fn share_layouts(rows: &mut [Self]) {
        let mut shapes = std::collections::HashMap::<
            (Arc<[String]>, Arc<[String]>),
            Arc<CompactRowLayout>,
        >::new();
        let mut previous: Option<Arc<CompactRowLayout>> = None;
        for row in rows {
            if previous
                .as_ref()
                .is_some_and(|layout| Arc::ptr_eq(layout, &row.columns))
            {
                continue;
            }
            let layout = shapes
                .entry((row.shared_columns(), row.columns.relation_names.clone()))
                .or_insert_with(|| row.columns.clone())
                .clone();
            row.columns = layout.clone();
            previous = Some(layout);
        }
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
        self.columns =
            CompactRowLayout::with_relations(columns.into(), self.columns.relation_names.clone());
        self.values.push(value);
        None
    }

    pub fn remove(&mut self, name: &str) -> Option<Value> {
        self.take_loaded_relation(name);
        let index = self.columns.iter().position(|column| column == name)?;
        let mut columns = self.columns.to_vec();
        columns.remove(index);
        self.columns =
            CompactRowLayout::with_relations(columns.into(), self.columns.relation_names.clone());
        Some(self.values.remove(index))
    }

    pub fn extend(&mut self, other: CompactRow) {
        self.try_extend(other, &mut Default::default())
            .expect("incompatible CompactRow read metadata");
    }

    /// Checked enhancement merge. Validate provenance before changing any
    /// native value, relation or read payload; callers retain a failed row.
    #[doc(hidden)]
    pub fn try_extend(
        &mut self,
        other: CompactRow,
        shapes: &mut crate::dynamic_fields::DynamicFieldMergeShapes,
    ) -> Result<(), crate::dynamic_fields::DynamicFieldError> {
        if let (Some(left), Some(right)) = (
            self.loaded_relations
                .as_ref()
                .and_then(|metadata| metadata.dynamic_fields.as_ref()),
            other
                .loaded_relations
                .as_ref()
                .and_then(|metadata| metadata.dynamic_fields.as_ref()),
        ) {
            left.validate_merge(right)?;
        }
        let mut relation_names: Option<Vec<String>> = None;
        for name in other.columns.relation_names.iter() {
            let existing = relation_names
                .as_deref()
                .unwrap_or(&self.columns.relation_names);
            if !existing.contains(name) {
                relation_names
                    .get_or_insert_with(|| self.columns.relation_names.to_vec())
                    .push(name.clone());
            }
        }
        if let Some(relations) = other.loaded_relations {
            if let Some(values) = relations.dynamic_fields {
                self.merge_loaded_dynamic_fields(values, shapes)?;
            }
            self.loaded_relations
                .get_or_insert_with(Default::default)
                .lists
                .extend(relations.lists);
        }
        let mut columns: Option<Vec<String>> = None;
        for (name, value) in other.columns.iter().zip(other.values) {
            let names = columns.as_deref().unwrap_or(&self.columns.names);
            if let Some(index) = names.iter().position(|column| column == name) {
                self.values[index] = value;
            } else {
                columns
                    .get_or_insert_with(|| self.columns.names.to_vec())
                    .push(name.clone());
                self.values.push(value);
            }
        }
        if columns.is_some() || relation_names.is_some() {
            self.columns = CompactRowLayout::with_relations(
                columns
                    .map(Arc::from)
                    .unwrap_or_else(|| self.columns.names.clone()),
                relation_names
                    .map(Arc::from)
                    .unwrap_or_else(|| self.columns.relation_names.clone()),
            );
        }
        Ok(())
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
