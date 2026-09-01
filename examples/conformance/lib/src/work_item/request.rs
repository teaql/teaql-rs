use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use teaql_core::{Aggregate, AggregateFunction, EntityDescriptor, Expr, SelectQuery, SmartList};
use teaql_runtime::{DataServiceError, RuntimeError};

use crate::request_support::*;

impl EntityReference for crate::WorkItem {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(&self)
    }
}

impl EntityReference for &crate::WorkItem {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(self)
    }
}

// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/work_item
#[derive(Debug)]
pub struct WorkItemRequest<R = crate::WorkItem> {
    query: SelectQuery,
    relation_selections: Vec<RelationSelection>,
    relation_filters: Vec<RelationFilter>,
    child_enhancements: Vec<QuerySelection>,
    query_options: QueryOptions,
    marker: PhantomData<R>,
}

impl<R> Clone for WorkItemRequest<R> {
    fn clone(&self) -> Self {
        Self {
            query: self.query.clone(),
            relation_selections: self.relation_selections.clone(),
            relation_filters: self.relation_filters.clone(),
            child_enhancements: self.child_enhancements.clone(),
            query_options: self.query_options.clone(),
            marker: PhantomData,
        }
    }
}

impl<R> WorkItemRequest<R> {
    pub(crate) fn new() -> Self {
        Self {
            query: SelectQuery::new("WorkItem")
                .project("id")
                .project("version"),
            relation_selections: Vec::new(),
            relation_filters: Vec::new(),
            child_enhancements: Vec::new(),
            query_options: QueryOptions::default(),
            marker: PhantomData,
        }
    }

    pub fn return_type<T>(self) -> WorkItemRequest<T> {
        WorkItemRequest {
            query: self.query,
            relation_selections: self.relation_selections,
            relation_filters: self.relation_filters,
            child_enhancements: self.child_enhancements,
            query_options: self.query_options,
            marker: PhantomData,
        }
    }

    pub fn query(&self) -> &SelectQuery {
        &self.query
    }

    pub fn relation_selections(&self) -> &[RelationSelection] {
        &self.relation_selections
    }

    pub fn relation_filters(&self) -> &[RelationFilter] {
        &self.relation_filters
    }

    pub fn child_enhancements(&self) -> &[QuerySelection] {
        &self.child_enhancements
    }

    pub fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }

    pub fn into_query(self) -> SelectQuery {
        self.query
    }


    pub fn purpose(self, purpose: impl Into<String>) -> crate::PurposedQuery<Self> {
        crate::PurposedQuery::new(self, purpose)
    }

    pub(crate) async fn _execute_for_list<'a, C>(
        self,
        context: &'a C,
    ) -> Result<SmartList<R>, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
        R: teaql_core::Entity,
    {
        let repository = context
            .work_item_repository()
            .map_err(|err| DataServiceError::Runtime(RuntimeError::Graph(err.to_string())))?;
        let query_options = self.query_options.clone();
        let relation_aggregates = runtime_relation_aggregates(&query_options);
        let query = authorize_query(apply_runtime_metadata(
            self.query,
            &query_options,
            &self.child_enhancements,
        )).map_err(DataServiceError::Runtime)?;
        let (mut rows, facets) = if query_options.facets.is_empty() {
            let rows = repository.fetch_enhanced_entities_with_relation_aggregates_owned::<R>(
                query,
                &relation_aggregates,
            ).await?;
            (rows, std::collections::BTreeMap::new())
        } else {
            let rows = repository.fetch_enhanced_entities_with_relation_aggregates::<R>(
                &query,
                &relation_aggregates,
            ).await?;
            let facets = execute_facets(context, query.as_query(), &query_options)
                .await
                .map_err(DataServiceError::Runtime)?;
            (rows, facets)
        };
        attach_facets(&mut rows, facets);
        Ok(rows)
    }

    pub(crate) async fn _execute_for_rows<'a, C>(
        self,
        context: &'a C,
    ) -> Result<SmartList<teaql_core::CompactRow>, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
    {
        let repository = context
            .work_item_repository()
            .map_err(|err| DataServiceError::Runtime(RuntimeError::Graph(err.to_string())))?;
        let query = authorize_query(apply_runtime_metadata(
            self.query,
            &self.query_options,
            &self.child_enhancements,
        )).map_err(DataServiceError::Runtime)?;
        repository.fetch_smart_list(&query).await
    }

    pub(crate) async fn _execute_for_stream<'a, C>(
        self,
        context: &'a C,
    ) -> Result<TeaqlEntityStream<'a, R, TeaqlDataServiceError<C::WorkItemRepository<'a>>>, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
        R: teaql_core::Entity + 'a,
    {
        Ok(Box::pin(async_stream::try_stream! {
            use futures_util::StreamExt;
            let repository = context
                .work_item_repository()
                .map_err(|err| DataServiceError::Runtime(RuntimeError::Graph(err.to_string())))?;
            let query_options = self.query_options.clone();
            let query = authorize_query(apply_runtime_metadata(
                self.query,
                &query_options,
                &self.child_enhancements,
            )).map_err(DataServiceError::Runtime)?;
            let mut chunks = repository.fetch_stream(&query).await?;
            while let Some(chunk) = chunks.next().await {
                for row in chunk?.rows {
                    yield R::from_compact_row(row).map_err(DataServiceError::Entity)?;
                }
            }
        }))
    }

    pub(crate) async fn _execute_for_first<'a, C>(
        self,
        context: &'a C,
    ) -> Result<Option<R>, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
        R: teaql_core::Entity,
    {
        let rows = self.limit(1)._execute_for_list(context).await?;
        Ok(rows.into_iter().next())
    }

    pub(crate) async fn _execute_for_one<'a, C>(
        self,
        context: &'a C,
    ) -> Result<Option<R>, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
        R: teaql_core::Entity,
    {
        self._execute_for_first(context).await
    }


    pub(crate) async fn _execute_for_page<'a, C>(
        self,
        context: &'a C,
        offset: u64,
        limit: u64,
    ) -> Result<SmartList<R>, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
        R: teaql_core::Entity,
    {
        if self.query.id_set_pagination.is_some() {
            let mut rows = self
                .clone()
                .page_offset(offset, limit)
                ._execute_for_list(context)
                .await?;
            if rows.total_count.is_none() {
                rows.total_count = Some(self._execute_for_count(context).await?);
            }
            return Ok(rows);
        }
        let total_count = self.clone()._execute_for_count(context).await?;
        let mut rows = self.page_offset(offset, limit)._execute_for_list(context).await?;
        rows.total_count = Some(total_count);
        Ok(rows)
    }

    pub(crate) async fn _execute_for_count<'a, C>(
        self,
        context: &'a C,
    ) -> Result<u64, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
    {
        let repository = context
            .work_item_repository()
            .map_err(|err| DataServiceError::Runtime(RuntimeError::Graph(err.to_string())))?;
        let query_options = self.query_options.clone();
        let mut query = apply_runtime_metadata(
            self.query,
            &query_options,
            &self.child_enhancements,
        );
        query.projection.clear();
        query.expr_projection.clear();
        query.order_by.clear();
        query.slice = None;
        query.relations.clear();
        query = query.count(COUNT_ALIAS);
        let query = authorize_query(query).map_err(DataServiceError::Runtime)?;
        let rows = repository.fetch_all(&query).await?;
        rows.first()
            .and_then(|row| row.get(COUNT_ALIAS))
            .and_then(teaql_core::Value::try_u64)
            .ok_or_else(|| DataServiceError::Runtime(RuntimeError::Graph(format!("count result for WorkItem is missing or not numeric"))))
    }

    pub(crate) async fn _execute_for_exists<'a, C>(
        self,
        context: &'a C,
    ) -> Result<bool, TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: TeaqlRepositoryProvider + ?Sized,
    {
        let repository = context
            .work_item_repository()
            .map_err(|err| DataServiceError::Runtime(RuntimeError::Graph(err.to_string())))?;
        let mut query = self.query.limit(1);
        query.relations.clear();
        let query = authorize_query(query).map_err(DataServiceError::Runtime)?;
        let rows = repository.fetch_all(&query).await?;
        Ok(!rows.is_empty())
    }

    pub fn search_with_text(mut self, text: impl Into<String>) -> Self {
        self.query = self.query.search_with_text(text);
        self
    }

    pub fn filter(mut self, filter: Expr) -> Self {
        self.query = self.query.filter(filter);
        self
    }

    pub fn and_filter(mut self, filter: Expr) -> Self {
        self.query = self.query.and_filter(filter);
        self
    }

    pub fn or_filter(mut self, filter: Expr) -> Self {
        self.query = self.query.or_filter(filter);
        self
    }

    pub fn append_search_criteria(self, criteria: Expr) -> Self {
        self.and_filter(criteria)
    }

    pub fn filter_property(
        mut self,
        property1: impl AsRef<str>,
        operator: FieldOperator,
        property2: impl AsRef<str>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_column_expr(
            property1.as_ref(),
            operator,
            property2.as_ref(),
        ));
        self
    }

    pub fn with_deleted_rows(mut self) -> Self {
        self.query.filter = remove_default_live_filter(self.query.filter);
        self
    }

    pub fn deleted_rows_only(mut self) -> Self {
        self.query.filter = remove_default_live_filter(self.query.filter);
        self.query = self.query.and_filter(Expr::lte("version", 0_i64));
        self
    }

    pub fn match_types(
        mut self,
        types: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(TYPE_FIELD, types.into_iter().map(Into::into)));
        self
    }


    pub fn with_type_group(mut self) -> Self {
        self.query = self.query.project(TYPE_GROUP_FIELD);
        self
    }

    pub fn matching_any_of(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        let entity = EntityDescriptor::new(selection.query.entity.clone());
        self.query = self.query.and_filter(Expr::in_subquery("id", entity, selection.query.clone(), "id"));
        self
    }

    pub fn match_any_of(self, request: impl Into<QuerySelection>) -> Self {
        self.matching_any_of(request)
    }

    pub fn enhance_child(mut self, request: impl Into<QuerySelection>) -> Self {
        self.child_enhancements.push(request.into());
        self
    }

    pub fn enhance_children_if_needed(self) -> Self {
        let request = self;
        request
    }


    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.query_options.comment = Some(comment.into());
        self
    }

    pub fn raw_sql(self, raw_sql: impl Into<String>) -> Self {
        self.unsafe_raw_sql(UnsafeRawSqlSegment::trusted(raw_sql))
    }

    pub fn unsafe_raw_sql(mut self, raw_sql: UnsafeRawSqlSegment) -> Self {
        self.query_options.raw_sql = Some(raw_sql.into_sql());
        self
    }

    pub fn raw_sql_filter(self, raw_sql: impl Into<String>) -> Self {
        self.unsafe_raw_sql_filter(UnsafeRawSqlSegment::trusted(raw_sql))
    }

    pub fn unsafe_raw_sql_filter(mut self, raw_sql: UnsafeRawSqlSegment) -> Self {
        self.query_options.raw_sql_search_criteria.push(raw_sql.into_sql());
        self
    }
    pub fn filter_with_json(self, json_expr: impl Into<String>) -> Self {
        self.merge_dynamic_json_expr(json_expr.into())
    }

    fn merge_dynamic_json_expr(self, json_expr: String) -> Self {
        let json = serde_json::from_str::<JsonValue>(&json_expr)
            .unwrap_or_else(|_| panic!("Input JSON format error: {json_expr}"));
        self.merge_dynamic_json(&json)
    }

    fn merge_dynamic_json(mut self, json: &JsonValue) -> Self {
        let Some(object) = json.as_object() else {
            return self;
        };

        for (field, value) in object {
            if field.starts_with('_') {
                continue;
            }
            self = self.apply_dynamic_json_filter(field, value);
        }

        self = self.apply_dynamic_json_order_by(object.get("_orderBy"));

        if let Some(offset) = dynamic_json_u64_field(object, "_start") {
            self = self.skip(offset);
        }
        if let Some(size) = dynamic_json_u64_field(object, "_size") {
            self = self.limit(size);
        }

        if let Some(page_size) = dynamic_json_u64_field(object, "_pageSize") {
            self = self.limit(page_size);
        }
        if let Some(page_number) = dynamic_json_u64_field(object, "_page") {
            if page_number > 0 {
                let size = dynamic_json_u64_field(object, "_pageSize")
                    .or_else(|| self.query.slice.as_ref().and_then(|slice| slice.limit))
                    .unwrap_or(10);
                let offset = page_number.saturating_sub(1).saturating_mul(size);
                self = self.page_offset(offset, size);
            }
        }

        self
    }

    pub(crate) fn apply_dynamic_json_filter(self, field: &str, value: &JsonValue) -> Self {
        if let Some((head, tail)) = field.split_once('.') {
            self.apply_dynamic_json_chain_filter(head, tail, value)
        } else if let Some(storage_field) = Self::dynamic_json_self_field(field) {
            self.and_filter(dynamic_json_filter_expr(storage_field, value))
        } else {
            self
        }
    }

    fn apply_dynamic_json_order_by(mut self, order_by: Option<&JsonValue>) -> Self {
        match order_by {
            Some(JsonValue::String(field)) => {
                if let Some(storage_field) = Self::dynamic_json_self_field(field) {
                    self.query = self.query.order_desc(storage_field);
                }
            }
            Some(JsonValue::Object(order_by)) => {
                self = self.apply_dynamic_json_single_order_by(order_by);
            }
            Some(JsonValue::Array(order_bys)) => {
                for order_by in order_bys {
                    if let Some(order_by) = order_by.as_object() {
                        self = self.apply_dynamic_json_single_order_by(order_by);
                    }
                }
            }
            _ => {}
        }
        self
    }

    fn apply_dynamic_json_single_order_by(
        mut self,
        order_by: &serde_json::Map<String, JsonValue>,
    ) -> Self {
        let Some(field) = order_by.get("field").and_then(JsonValue::as_str) else {
            return self;
        };
        let Some(storage_field) = Self::dynamic_json_self_field(field) else {
            return self;
        };
        if order_by
            .get("useAsc")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false)
        {
            self.query = self.query.order_asc(storage_field);
        } else {
            self.query = self.query.order_desc(storage_field);
        }
        self
    }

    fn dynamic_json_self_field(field: &str) -> Option<&'static str> {
        match field {
            "id" => Some("id"),
            "title" => Some("title"),
            "description" => Some("description"),
            "version" => Some("version"),
            "platform" | "platform_id" => Some("platform_id"),
            _ => None,
        }
    }

    fn apply_dynamic_json_chain_filter(self, head: &str, tail: &str, value: &JsonValue) -> Self {
        let _ = (tail, value);
        match head {
            "platform" => {
                self.with_platform_matching(
                    crate::Q::platforms_minimal()
                        .apply_dynamic_json_filter(tail, value),
                )
            }
            _ => self,
        }
    }

    pub fn create_property_as(
        self,
        property_name: impl Into<String>,
        raw_sql_segment: impl Into<String>,
    ) -> Self {
        self.unsafe_create_property_as(property_name, UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn unsafe_create_property_as(
        mut self,
        property_name: impl Into<String>,
        raw_sql_segment: UnsafeRawSqlSegment,
    ) -> Self {
        self.query_options
            .dynamic_properties
            .push(RawDynamicProperty::new(property_name, raw_sql_segment));
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    pub fn stream(mut self, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "stream chunk size must be positive");
        self.query = self.query.stream(chunk_size);
        self
    }

    pub fn stream_default(mut self) -> Self {
        self.query = self.query.stream_default();
        self
    }

    pub fn skip(mut self, offset: u64) -> Self {
        self.query = self.query.offset(offset);
        self
    }

    pub fn offset_only(self, offset: u64) -> Self {
        self.skip(offset)
    }

    pub fn offset(self, offset: u64, size: u64) -> Self {
        self.page_offset(offset, size)
    }

    pub fn page_offset(mut self, offset: u64, limit: u64) -> Self {
        self.query = self.query.page(offset, limit);
        self
    }

    pub fn optimize_for_continuous_page_fetch(mut self) -> Self {
        self.query = self.query.optimize_for_continuous_page_fetch();
        self
    }

    pub fn optimize_for_continuous_page_fetch_with(
        mut self,
        namespace: impl Into<String>,
        ttl_seconds: u64,
    ) -> Self {
        self.query = self
            .query
            .optimize_for_continuous_page_fetch_with(namespace, ttl_seconds);
        self
    }

    pub fn optimize_pagination_with_id_set(mut self) -> Self {
        self.query = self.query.optimize_pagination_with_id_set();
        self
    }

    pub fn optimize_pagination_with_id_set_config(
        mut self,
        namespace: impl Into<String>,
        ttl_seconds: u64,
        max_ids: u64,
    ) -> Self {
        self.query = self
            .query
            .optimize_pagination_with_id_set_config(namespace, ttl_seconds, max_ids);
        self
    }

    /// Select bounded indexed probes for a per-parent Top-N relation only
    /// when the already-loaded parent count is at or below `threshold`.
    /// Passing zero explicitly selects the provider window plan.
    pub fn top_n_probe_parent_threshold(mut self, threshold: usize) -> Self {
        self.query = self.query.top_n_probe_parent_threshold(threshold);
        self
    }

    pub fn top(self, top_n: u64) -> Self {
        self.limit(top_n)
    }

    pub fn offset_size(self, offset: u64, size: u64) -> Self {
        self.offset(offset, size)
    }

    pub fn unlimited(mut self) -> Self {
        self.query.slice = None;
        self
    }

    pub fn page_number(self, page_number: u64, page_size: u64) -> Self {
        let offset = page_number.saturating_sub(1).saturating_mul(page_size);
        self.page_offset(offset, page_size)
    }

    pub fn page_number_default(self, page_number: u64) -> Self {
        self.page_number(page_number, 10)
    }

    pub fn page(self, page_number: u64, page_size: u64) -> Self {
        self.page_number(page_number, page_size)
    }

    pub fn page_default(self, page_number: u64) -> Self {
        self.page_number_default(page_number)
    }

    pub fn select_self(mut self) -> Self {
        self.query = self.query.project("id");
        self.query = self.query.project("title");
        self.query = self.query.project("description");
        self.query = self.query.project("version");
        self.query = self.query.project("platform_id");
        self
    }

    pub fn select_self_fields(self) -> Self {
        self.select_self()
    }

    pub fn select_self_without_parent(self) -> Self {
        self.select_self_fields()
    }

    pub fn select_all(self) -> Self {
        let mut request = self.select_self();
        request = request.select_platform();
        request
    }

    pub fn select_children(self) -> Self {
        self.select_all()
    }

    pub fn select_any(self) -> Self {
        self.select_children()
    }

    pub fn group_by(mut self, field: impl Into<String>) -> Self {
        self.query = self.query.group_by(field);
        self
    }

    pub fn count(self) -> Self {
        self.count_as("count")
    }

    pub fn count_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count(alias)
    }

    pub fn aggregate_count(mut self, alias: impl Into<String>) -> Self {
        self.query = self.query.count(alias);
        self
    }

    pub fn aggregate_count_field(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.count_field(field, alias);
        self
    }

    pub fn aggregate_with_function(
        mut self,
        field: impl Into<String>,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.query = self.query.aggregate(Aggregate::new(function, field, alias));
        self
    }

    pub fn aggregate_sum(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.sum(field, alias);
        self
    }

    pub fn aggregate_avg(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.avg(field, alias);
        self
    }

    pub fn aggregate_min(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.min(field, alias);
        self
    }

    pub fn aggregate_max(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.max(field, alias);
        self
    }

    pub fn aggregate_stddev(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.stddev(field, alias);
        self
    }

    pub fn aggregate_stddev_pop(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.stddev_pop(field, alias);
        self
    }

    pub fn aggregate_var_samp(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.var_samp(field, alias);
        self
    }

    pub fn aggregate_var_pop(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.var_pop(field, alias);
        self
    }

    pub fn aggregate_bit_and(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.bit_and(field, alias);
        self
    }

    pub fn aggregate_bit_or(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.bit_or(field, alias);
        self
    }

    pub fn aggregate_bit_xor(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.bit_xor(field, alias);
        self
    }

    pub fn enable_aggregation_cache(mut self) -> Self {
        self.query = self.query.enable_aggregation_cache();
        self
    }

    pub fn enable_aggregation_cache_for(mut self, cache_expired_millis: u64) -> Self {
        self.query = self.query.enable_aggregation_cache_for(cache_expired_millis);
        self
    }

    pub fn propagate_aggregation_cache(mut self, cache_expired_millis: u64) -> Self {
        self.query = self.query.propagate_aggregation_cache(cache_expired_millis);
        self
    }

    pub fn group_by_id(self) -> Self {
        self.group_by("id")
    }

    pub fn group_by_id_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("id");
        request.query = request
            .query
            .project_expr(alias, Expr::column("id"));
        request
    }

    pub fn group_by_id_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("id")
            .aggregate_with_function("id", alias, function)
    }

    pub fn count_id(self) -> Self {
        self.count_id_as("id_count")
    }

    pub fn count_id_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("id", alias)
    }

    pub fn sum_id(self) -> Self {
        self.sum_id_as("sum_id")
    }

    pub fn sum_id_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("id", alias)
    }

    pub fn avg_id(self) -> Self {
        self.avg_id_as("avg_id")
    }

    pub fn avg_id_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("id", alias)
    }

    pub fn min_id(self) -> Self {
        self.min_id_as("min_id")
    }

    pub fn min_id_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("id", alias)
    }

    pub fn max_id(self) -> Self {
        self.max_id_as("max_id")
    }

    pub fn max_id_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("id", alias)
    }


    pub fn with_id(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "id",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_id_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "id",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_id_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("id", value));
        self
    }



    pub fn with_id_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("id", value));
        self
    }

    pub fn with_id_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "id",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_id_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "id",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn order_by_id_asc(mut self) -> Self {
        self.query = self.query.order_asc("id");
        self
    }

    pub fn order_by_id_desc(mut self) -> Self {
        self.query = self.query.order_desc("id");
        self
    }

    pub fn order_by_id_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("id");
        self
    }

    pub fn order_by_id_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("id");
        self
    }


    pub fn select_title(mut self) -> Self {
        self.query = self.query.project("title");
        self
    }

    pub fn project_title(self) -> Self {
        self.select_title()
    }

    pub fn select_title_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_title_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_title_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("title", raw_sql_segment));
        self
    }

    pub fn group_by_title(self) -> Self {
        self.group_by("title")
    }

    pub fn group_by_title_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("title");
        request.query = request
            .query
            .project_expr(alias, Expr::column("title"));
        request
    }

    pub fn group_by_title_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("title")
            .aggregate_with_function("title", alias, function)
    }

    pub fn count_title(self) -> Self {
        self.count_title_as("title_count")
    }

    pub fn count_title_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("title", alias)
    }

    pub fn sum_title(self) -> Self {
        self.sum_title_as("sum_title")
    }

    pub fn sum_title_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("title", alias)
    }

    pub fn avg_title(self) -> Self {
        self.avg_title_as("avg_title")
    }

    pub fn avg_title_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("title", alias)
    }

    pub fn min_title(self) -> Self {
        self.min_title_as("min_title")
    }

    pub fn min_title_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("title", alias)
    }

    pub fn max_title(self) -> Self {
        self.max_title_as("max_title")
    }

    pub fn max_title_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("title", alias)
    }

    pub fn unselect_title(mut self) -> Self {
        self.query.projection.retain(|field| field != "title");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "title");
        self
    }


    pub fn with_title(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "title",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_title_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "title",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_title_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("title", value));
        self
    }



    pub fn with_title_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("title", value));
        self
    }

    pub fn with_title_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("title", value));
        self
    }

    pub fn with_title_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("title", value));
        self
    }

    pub fn with_title_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("title", value));
        self
    }

    pub fn with_title_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("title", value));
        self
    }

    pub fn with_title_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("title", lower, upper));
        self
    }

    pub fn with_title_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "title",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_title_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "title",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_title_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "title",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_title_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("title", value));
        self
    }

    pub fn with_title_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("title", value));
        self
    }

    pub fn with_title_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("title", value));
        self
    }

    pub fn with_title_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("title", value));
        self
    }

    pub fn with_title_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("title", value));
        self
    }

    pub fn with_title_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("title", value));
        self
    }

    pub fn with_title_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("title", value));
        self
    }
    pub fn with_title_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("title", value));
        self
    }

    pub fn with_title_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("title", value));
        self
    }

    pub fn with_title_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("title"));
        self
    }



    pub fn with_title_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("title"));
        self
    }


    pub fn order_by_title_asc(mut self) -> Self {
        self.query = self.query.order_asc("title");
        self
    }

    pub fn order_by_title_desc(mut self) -> Self {
        self.query = self.query.order_desc("title");
        self
    }

    pub fn order_by_title_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("title");
        self
    }

    pub fn order_by_title_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("title");
        self
    }


    pub fn select_description(mut self) -> Self {
        self.query = self.query.project("description");
        self
    }

    pub fn project_description(self) -> Self {
        self.select_description()
    }

    pub fn select_description_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_description_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_description_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("description", raw_sql_segment));
        self
    }

    pub fn group_by_description(self) -> Self {
        self.group_by("description")
    }

    pub fn group_by_description_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("description");
        request.query = request
            .query
            .project_expr(alias, Expr::column("description"));
        request
    }

    pub fn group_by_description_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("description")
            .aggregate_with_function("description", alias, function)
    }

    pub fn count_description(self) -> Self {
        self.count_description_as("description_count")
    }

    pub fn count_description_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("description", alias)
    }

    pub fn sum_description(self) -> Self {
        self.sum_description_as("sum_description")
    }

    pub fn sum_description_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("description", alias)
    }

    pub fn avg_description(self) -> Self {
        self.avg_description_as("avg_description")
    }

    pub fn avg_description_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("description", alias)
    }

    pub fn min_description(self) -> Self {
        self.min_description_as("min_description")
    }

    pub fn min_description_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("description", alias)
    }

    pub fn max_description(self) -> Self {
        self.max_description_as("max_description")
    }

    pub fn max_description_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("description", alias)
    }

    pub fn unselect_description(mut self) -> Self {
        self.query.projection.retain(|field| field != "description");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "description");
        self
    }


    pub fn with_description(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "description",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_description_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "description",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_description_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("description", value));
        self
    }



    pub fn with_description_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("description", value));
        self
    }

    pub fn with_description_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("description", value));
        self
    }

    pub fn with_description_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("description", value));
        self
    }

    pub fn with_description_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("description", value));
        self
    }

    pub fn with_description_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("description", value));
        self
    }

    pub fn with_description_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("description", lower, upper));
        self
    }

    pub fn with_description_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "description",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_description_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "description",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_description_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "description",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_description_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("description", value));
        self
    }

    pub fn with_description_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("description", value));
        self
    }

    pub fn with_description_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("description", value));
        self
    }

    pub fn with_description_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("description", value));
        self
    }

    pub fn with_description_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("description", value));
        self
    }

    pub fn with_description_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("description", value));
        self
    }

    pub fn with_description_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("description", value));
        self
    }
    pub fn with_description_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("description", value));
        self
    }

    pub fn with_description_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("description", value));
        self
    }

    pub fn with_description_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("description"));
        self
    }



    pub fn with_description_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("description"));
        self
    }


    pub fn order_by_description_asc(mut self) -> Self {
        self.query = self.query.order_asc("description");
        self
    }

    pub fn order_by_description_desc(mut self) -> Self {
        self.query = self.query.order_desc("description");
        self
    }

    pub fn order_by_description_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("description");
        self
    }

    pub fn order_by_description_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("description");
        self
    }

    pub fn group_by_version(self) -> Self {
        self.group_by("version")
    }

    pub fn group_by_version_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("version");
        request.query = request
            .query
            .project_expr(alias, Expr::column("version"));
        request
    }

    pub fn group_by_version_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("version")
            .aggregate_with_function("version", alias, function)
    }

    pub fn count_version(self) -> Self {
        self.count_version_as("version_count")
    }

    pub fn count_version_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("version", alias)
    }

    pub fn sum_version(self) -> Self {
        self.sum_version_as("sum_version")
    }

    pub fn sum_version_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("version", alias)
    }

    pub fn avg_version(self) -> Self {
        self.avg_version_as("avg_version")
    }

    pub fn avg_version_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("version", alias)
    }

    pub fn min_version(self) -> Self {
        self.min_version_as("min_version")
    }

    pub fn min_version_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("version", alias)
    }

    pub fn max_version(self) -> Self {
        self.max_version_as("max_version")
    }

    pub fn max_version_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("version", alias)
    }

    pub fn order_by_version_asc(mut self) -> Self {
        self.query = self.query.order_asc("version");
        self
    }

    pub fn order_by_version_desc(mut self) -> Self {
        self.query = self.query.order_desc("version");
        self
    }

    pub fn order_by_version_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("version");
        self
    }

    pub fn order_by_version_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("version");
        self
    }
    pub fn filter_by_platform(mut self, value: impl EntityReference) -> Self {
        self.query = self.query.and_filter(Expr::eq("platform_id", value.entity_id_value()));
        self
    }

    pub fn with_platform_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "platform_id",
            <crate::Platform as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.query.clone(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("platform", selection));
        self
    }


    pub fn without_platform_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "platform_id",
            <crate::Platform as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.query.clone(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("platform", selection));
        self
    }


    pub fn have_platform(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("platform_id"));
        self
    }

    pub fn have_no_platform(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("platform_id"));
        self
    }


    pub fn group_by_platform(self) -> Self {
        self.group_by("platform_id")
    }

    pub fn group_by_platform_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("platform_id");
        request.query = request
            .query
            .project_expr(alias, Expr::column("platform_id"));
        request
    }

    pub fn group_by_platform_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("platform_id")
            .aggregate_with_function("platform_id", alias, function)
    }

    pub fn group_by_platform_with(mut self, request: impl Into<QuerySelection>) -> Self {
        self.query = self.query.group_by("platform_id");
        self.query_options.object_group_bys.push(ObjectGroupBy::new(
            "platform",
            "platform_id",
            request,
        ));
        self
    }

    pub fn group_by_platform_with_details(self) -> Self {
        self.group_by_platform_with_details_from(crate::Q::platforms().unlimited())
    }

    pub fn group_by_platform_with_details_from(self, request: impl Into<QuerySelection>) -> Self {
        self.group_by_platform_with(request)
    }


    pub fn roll_up_to_platform(self) -> Self {
        self.roll_up_to_platform_with(crate::Q::platforms().unlimited())
    }

    pub fn roll_up_to_platform_with(self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.with_platform_matching(selection.clone())
            .group_by_platform_with(selection)
    }

    pub fn count_platform(self) -> Self {
        self.count_platform_as("platform_count")
    }

    pub fn count_platform_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("platform_id", alias)
    }

    pub fn unselect_platform(mut self) -> Self {
        self.query.projection.retain(|field| field != "platform_id");
        self.query.relations.retain(|relation| relation.name != "platform");
        self
    }
    pub fn select_platform(mut self) -> Self {
        self.query = self.query.relation("platform");
        self
    }

    pub fn select_platform_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.relation_query("platform", selection.into_query());
        self
}

    pub fn facet_by_platform_as(self, facet_name: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        self.facet_by_platform_as_with_options(facet_name, request, true)
    }

    pub fn facet_by_platform_as_with_options(
        mut self,
        facet_name: impl Into<String>,
        request: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        self.query_options.facets.push(FacetRequest::new(
            facet_name,
            "platform",
            request,
            include_all_facets,
        ));
        self
    }
}

impl<R> Default for WorkItemRequest<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> From< WorkItemRequest<R> > for SelectQuery {
    fn from(request: WorkItemRequest<R>) -> Self {
        QuerySelection::from(request).into_query()
    }
}

impl<R> From< WorkItemRequest<R> > for QuerySelection {
    fn from(request: WorkItemRequest<R>) -> Self {
        Self {
            query: request.query,
            relation_selections: request.relation_selections,
            relation_filters: request.relation_filters,
            child_enhancements: request.child_enhancements,
            query_options: request.query_options,
        }
    }
}


impl<'a, C> crate::request_support::AuditedSave<'a, C> for teaql_core::Audited<crate::WorkItem> 
where C: crate::request_support::TeaqlRepositoryProvider + ?Sized + 'a
{
    type Error = crate::TeaqlDataServiceError<C::WorkItemRepository<'a>>;
    type Entity = crate::WorkItem;
    fn save(self, context: &'a C) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Entity, Self::Error>> + '_>> {
        Box::pin(async move {
            teaql_runtime::save_audited_ledger_entity(self, context.user_context())
                .await
                .map_err(DataServiceError::Runtime)
        })
    }
}

impl<R: teaql_core::Entity> crate::PurposedQuery<WorkItemRequest<R>> {
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.inner.query_options.comment = Some(comment.into());
        self
    }

    pub fn new_entity<C>(&self, context: &C) -> crate::WorkItem
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.require_comment();
        let mut entity = crate::WorkItem::runtime_new(context.user_context().entity_runtime_state());
        if let Ok(id) = context.user_context().next_id(crate::WorkItem::ENTITY_NAME) {
            entity.update_id(id);
        }
        teaql_core::Entity::mark_as_new(&mut entity);
        entity
    }

    fn into_inner_with_trace(mut self) -> WorkItemRequest<R> {
        self.require_comment();
        self.inner.query.trace_chain.push(teaql_core::TraceNode::typed(
            teaql_core::TraceKind::Purpose,
            self.inner.query.entity.clone(),
            None,
            self.purpose,
        ));
        self.inner
    }

    fn require_comment(&self) {
        assert!(
            self.inner
                .query_options
                .comment
                .as_deref()
                .is_some_and(|comment| !comment.trim().is_empty()),
            "query comment must not be empty"
        );
    }

    pub async fn execute_for_page<'a, C>(
        self,
        context: &'a C,
        offset: u64,
        limit: u64,
    ) -> Result<teaql_core::SmartList<R>, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_page(context, offset, limit).await
    }

    pub async fn execute_for_exists<'a, C>(
        self,
        context: &'a C,
    ) -> Result<bool, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_exists(context).await
    }

    pub async fn execute_for_list<'a, C>(self, context: &'a C) -> Result<teaql_core::SmartList<R>, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_list(context).await
    }

    pub async fn execute_for_rows<'a, C>(self, context: &'a C) -> Result<teaql_core::SmartList<teaql_core::CompactRow>, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_rows(context).await
    }

    /// Execute query as a lazy entity stream without materializing the result set.
    /// Set chunk size via .stream(chunk_size) or .stream_default() on the query.
    pub async fn execute_for_stream<'a, C>(self, context: &'a C) -> Result<crate::request_support::TeaqlEntityStream<'a, R, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
        R: teaql_core::Entity + 'a,
    {
        self.into_inner_with_trace()._execute_for_stream(context).await
    }

    pub async fn execute_for_first<'a, C>(self, context: &'a C) -> Result<Option<R>, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_first(context).await
    }

    pub async fn execute_for_one<'a, C>(self, context: &'a C) -> Result<Option<R>, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_one(context).await
    }


    pub async fn execute_for_count<'a, C>(self, context: &'a C) -> Result<u64, crate::request_support::TeaqlDataServiceError<C::WorkItemRepository<'a>>>
    where
        C: crate::request_support::TeaqlRepositoryProvider + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_count(context).await
    }
}
