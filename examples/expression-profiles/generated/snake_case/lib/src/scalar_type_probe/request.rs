use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use teaql_core::{Aggregate, AggregateFunction, EntityDescriptor, Expr, SelectQuery, SmartList};
use teaql_runtime::RuntimeError;

use crate::request_support::*;

impl EntityReference for crate::ScalarTypeProbe {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(&self)
    }
}

impl EntityReference for &crate::ScalarTypeProbe {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(self)
    }
}

// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/scalar_type_probe
#[derive(Debug)]
pub struct ScalarTypeProbeRequest<R = crate::ScalarTypeProbe> {
    query: SelectQuery,
    relation_selections: Vec<RelationSelection>,
    relation_filters: Vec<RelationFilter>,
    child_enhancements: Vec<QuerySelection>,
    query_options: QueryOptions,
    marker: PhantomData<R>,
}

impl<R> Clone for ScalarTypeProbeRequest<R> {
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

impl<R> ScalarTypeProbeRequest<R> {
    pub(crate) fn new() -> Self {
        Self {
            query: SelectQuery::new("ScalarTypeProbe")
                .project("id")
                .project("version"),
            relation_selections: Vec::new(),
            relation_filters: Vec::new(),
            child_enhancements: Vec::new(),
            query_options: QueryOptions::default(),
            marker: PhantomData,
        }
    }

    pub fn return_type<T>(self) -> ScalarTypeProbeRequest<T> {
        ScalarTypeProbeRequest {
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
    ) -> Result<SmartList<R>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        let query_options = self.query_options.clone();
        let relation_aggregates = runtime_relation_aggregates(&query_options);
        let mut query = authorize_query(apply_runtime_metadata(
            self.query,
            &query_options,
            &self.child_enhancements,
        ))?;
        let requested_count = query.take_requested_list_count(&query_options);
        let use_id_set = query.as_query().id_set_pagination.is_some();
        context.user_context().observe_generated_list("ScalarTypeProbe", async {
        let mut total = None;
        if !use_id_set {
            if let Some((alias, count_query)) = &requested_count {
                let count_rows = context.fetch_compact_rows("ScalarTypeProbe", count_query).await?;
                total = Some(count_rows.first().and_then(|row| row.get(alias))
                    .and_then(teaql_core::Value::try_u64)
                    .ok_or_else(|| RuntimeError::Graph("requested list count is missing or not numeric".into()))?);
            }
        }
        let (mut rows, facets) = if query_options.facets.is_empty() {
            let rows = context.fetch_entity_smart_list::<R>(
                "ScalarTypeProbe",
                query,
                relation_aggregates,
            ).await?;
            (rows, std::collections::BTreeMap::new())
        } else {
            let rows = context.fetch_entity_smart_list::<R>(
                "ScalarTypeProbe",
                query.clone(),
                relation_aggregates,
            ).await?;
            let facets = execute_facets(context, query.as_query(), &query_options)
                .await?;
            (rows, facets)
        };
        if let Some((alias, count_query)) = requested_count {
            if total.is_none() { total = rows.total_count; }
            if total.is_none() {
                let count_rows = context.fetch_compact_rows("ScalarTypeProbe", &count_query).await?;
                total = Some(count_rows.first().and_then(|row| row.get(&alias))
                    .and_then(teaql_core::Value::try_u64)
                    .ok_or_else(|| RuntimeError::Graph("requested list count is missing or not numeric".into()))?);
            }
            rows.total_count = total;
            rows.aggregations.insert(alias, teaql_core::Value::U64(total.expect("requested count resolved")));
        }
        attach_facets(&mut rows, facets);
        Ok(rows)
        }).await
    }

    pub(crate) async fn _execute_for_rows<'a, C>(
        self,
        context: &'a C,
    ) -> Result<SmartList<teaql_core::CompactRow>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
    {
        let query = authorize_query(apply_runtime_metadata(
            self.query,
            &self.query_options,
            &self.child_enhancements,
        ))?.with_facet_diagnostics(&self.query_options);
        context.fetch_compact_smart_list("ScalarTypeProbe", &query).await
    }

    pub(crate) async fn _execute_for_stream<'a, C>(
        self,
        context: &'a C,
    ) -> Result<TeaqlEntityStream<'a, R, RuntimeError>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        let query = authorize_query(apply_runtime_metadata(
            self.query,
            &self.query_options,
            &self.child_enhancements,
        ))?;
        Ok(context.user_context().observe_generated_stream(
            "ScalarTypeProbe", context.fetch_entity_stream("ScalarTypeProbe", query),
        ))
    }

    pub(crate) async fn _execute_for_first<'a, C>(
        self,
        context: &'a C,
    ) -> Result<Option<R>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        let rows = self.limit(1)._execute_for_list(context).await?;
        Ok(rows.into_iter().next())
    }

    pub(crate) async fn _execute_for_one<'a, C>(
        self,
        context: &'a C,
    ) -> Result<Option<R>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        self._execute_for_first(context).await
    }


    pub(crate) async fn _execute_for_count<'a, C>(
        self,
        context: &'a C,
    ) -> Result<u64, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
    {
        let query_options = self.query_options.clone();
        let query = apply_runtime_metadata(
            self.query,
            &query_options,
            &self.child_enhancements,
        );
        let operation = query_options.page_count_operation.clone().unwrap_or_else(||
            QueryOperation::new(QueryOperationKind::NamedCount, "execute_for_count"));
        let alias = operation.alias().unwrap_or(COUNT_ALIAS).to_owned();
        let query = authorize_query(query)?.with_facet_diagnostics(&query_options)
            .for_exact_count_with_operation(&alias, operation);
        let rows = context.fetch_compact_rows("ScalarTypeProbe", &query).await?;
        rows.first()
            .and_then(|row| row.get(&alias))
            .and_then(teaql_core::Value::try_u64)
            .ok_or_else(|| RuntimeError::Graph(format!("count result for ScalarTypeProbe is missing or not numeric")))
    }

    pub(crate) async fn _execute_for_exists<'a, C>(
        self,
        context: &'a C,
    ) -> Result<bool, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
    {
        let mut query = apply_runtime_metadata(
            self.query,
            &self.query_options,
            &self.child_enhancements,
        ).limit(1);
        query.relations.clear();
        let query = authorize_query(query)?.with_facet_diagnostics(&self.query_options);
        let rows = context.fetch_compact_rows("ScalarTypeProbe", &query).await?;
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
            "required_string" => Some("required_string"),
            "optional_string" => Some("optional_string"),
            "large_text" => Some("large_text"),
            "required_boolean" => Some("required_boolean"),
            "optional_boolean" => Some("optional_boolean"),
            "required_integer" => Some("required_integer"),
            "optional_integer" => Some("optional_integer"),
            "required_long" => Some("required_long"),
            "optional_long" => Some("optional_long"),
            "required_decimal" => Some("required_decimal"),
            "optional_decimal" => Some("optional_decimal"),
            "required_float" => Some("required_float"),
            "optional_float" => Some("optional_float"),
            "required_double" => Some("required_double"),
            "optional_double" => Some("optional_double"),
            "required_date" => Some("required_date"),
            "optional_date" => Some("optional_date"),
            "required_time" => Some("required_time"),
            "optional_time" => Some("optional_time"),
            "required_datetime" => Some("required_datetime"),
            "optional_datetime" => Some("optional_datetime"),
            "create_time" => Some("create_time"),
            "update_time" => Some("update_time"),
            "version" => Some("version"),
            "type_platform" | "type_platform_id" => Some("type_platform_id"),
            _ => None,
        }
    }

    fn apply_dynamic_json_chain_filter(self, head: &str, tail: &str, value: &JsonValue) -> Self {
        let _ = (tail, value);
        match head {
            "type_platform" => {
                self.with_type_platform_matching(
                    crate::Q::type_platforms_minimal()
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
        self.query = self.query.project("required_string");
        self.query = self.query.project("optional_string");
        self.query = self.query.project("large_text");
        self.query = self.query.project("required_boolean");
        self.query = self.query.project("optional_boolean");
        self.query = self.query.project("required_integer");
        self.query = self.query.project("optional_integer");
        self.query = self.query.project("required_long");
        self.query = self.query.project("optional_long");
        self.query = self.query.project("required_decimal");
        self.query = self.query.project("optional_decimal");
        self.query = self.query.project("required_float");
        self.query = self.query.project("optional_float");
        self.query = self.query.project("required_double");
        self.query = self.query.project("optional_double");
        self.query = self.query.project("required_date");
        self.query = self.query.project("optional_date");
        self.query = self.query.project("required_time");
        self.query = self.query.project("optional_time");
        self.query = self.query.project("required_datetime");
        self.query = self.query.project("optional_datetime");
        self.query = self.query.project("create_time");
        self.query = self.query.project("update_time");
        self.query = self.query.project("version");
        self.query = self.query.project("type_platform_id");
        self
    }

    pub fn select_dynamic_fields_with(mut self, selection: teaql_core::dynamic_fields::DynamicFieldSelection) -> Self {
        self.query = self.query.select_dynamic_fields(selection);
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
        request = request.select_type_platform();
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

    pub fn count(mut self) -> Self {
        self.query_options.page_count_operation = Some(QueryOperation::new(QueryOperationKind::NamedCount, "count"));
        self.aggregate_count("count")
    }

    pub fn count_as(mut self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        self.query_options.page_count_operation = Some(QueryOperation::new(QueryOperationKind::NamedCount, "count_as").with_alias(&alias));
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


    pub fn select_required_string(mut self) -> Self {
        self.query = self.query.project("required_string");
        self
    }

    pub fn project_required_string(self) -> Self {
        self.select_required_string()
    }

    pub fn select_required_string_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_string_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_string_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_string", raw_sql_segment));
        self
    }

    pub fn group_by_required_string(self) -> Self {
        self.group_by("required_string")
    }

    pub fn group_by_required_string_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_string");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_string"));
        request
    }

    pub fn group_by_required_string_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_string")
            .aggregate_with_function("required_string", alias, function)
    }

    pub fn count_required_string(self) -> Self {
        self.count_required_string_as("required_string_count")
    }

    pub fn count_required_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_string", alias)
    }

    pub fn sum_required_string(self) -> Self {
        self.sum_required_string_as("sum_required_string")
    }

    pub fn sum_required_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_string", alias)
    }

    pub fn avg_required_string(self) -> Self {
        self.avg_required_string_as("avg_required_string")
    }

    pub fn avg_required_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_string", alias)
    }

    pub fn min_required_string(self) -> Self {
        self.min_required_string_as("min_required_string")
    }

    pub fn min_required_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_string", alias)
    }

    pub fn max_required_string(self) -> Self {
        self.max_required_string_as("max_required_string")
    }

    pub fn max_required_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_string", alias)
    }

    pub fn unselect_required_string(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_string");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_string");
        self
    }


    pub fn with_required_string(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_string",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_string_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_string",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_string_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_string", value));
        self
    }



    pub fn with_required_string_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_string", value));
        self
    }

    pub fn with_required_string_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_string", value));
        self
    }

    pub fn with_required_string_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_string", value));
        self
    }

    pub fn with_required_string_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_string", value));
        self
    }

    pub fn with_required_string_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_string", value));
        self
    }

    pub fn with_required_string_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_string", lower, upper));
        self
    }

    pub fn with_required_string_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_string",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_string_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_string",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_string_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_string",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_string_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("required_string", value));
        self
    }

    pub fn with_required_string_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("required_string", value));
        self
    }

    pub fn with_required_string_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("required_string", value));
        self
    }

    pub fn with_required_string_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("required_string", value));
        self
    }

    pub fn with_required_string_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("required_string", value));
        self
    }

    pub fn with_required_string_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("required_string", value));
        self
    }

    pub fn with_required_string_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("required_string", value));
        self
    }
    pub fn with_required_string_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_string", value));
        self
    }

    pub fn with_required_string_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_string", value));
        self
    }

    pub fn with_required_string_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_string"));
        self
    }



    pub fn with_required_string_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_string"));
        self
    }


    pub fn order_by_required_string_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_string");
        self
    }

    pub fn order_by_required_string_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_string");
        self
    }

    pub fn order_by_required_string_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_string");
        self
    }

    pub fn order_by_required_string_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_string");
        self
    }


    pub fn select_optional_string(mut self) -> Self {
        self.query = self.query.project("optional_string");
        self
    }

    pub fn project_optional_string(self) -> Self {
        self.select_optional_string()
    }

    pub fn select_optional_string_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_string_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_string_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_string", raw_sql_segment));
        self
    }

    pub fn group_by_optional_string(self) -> Self {
        self.group_by("optional_string")
    }

    pub fn group_by_optional_string_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_string");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_string"));
        request
    }

    pub fn group_by_optional_string_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_string")
            .aggregate_with_function("optional_string", alias, function)
    }

    pub fn count_optional_string(self) -> Self {
        self.count_optional_string_as("optional_string_count")
    }

    pub fn count_optional_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_string", alias)
    }

    pub fn sum_optional_string(self) -> Self {
        self.sum_optional_string_as("sum_optional_string")
    }

    pub fn sum_optional_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_string", alias)
    }

    pub fn avg_optional_string(self) -> Self {
        self.avg_optional_string_as("avg_optional_string")
    }

    pub fn avg_optional_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_string", alias)
    }

    pub fn min_optional_string(self) -> Self {
        self.min_optional_string_as("min_optional_string")
    }

    pub fn min_optional_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_string", alias)
    }

    pub fn max_optional_string(self) -> Self {
        self.max_optional_string_as("max_optional_string")
    }

    pub fn max_optional_string_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_string", alias)
    }

    pub fn unselect_optional_string(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_string");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_string");
        self
    }


    pub fn with_optional_string(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_string",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_string_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_string",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_string_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_string", value));
        self
    }



    pub fn with_optional_string_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_string", value));
        self
    }

    pub fn with_optional_string_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_string", value));
        self
    }

    pub fn with_optional_string_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_string", value));
        self
    }

    pub fn with_optional_string_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_string", value));
        self
    }

    pub fn with_optional_string_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_string", value));
        self
    }

    pub fn with_optional_string_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_string", lower, upper));
        self
    }

    pub fn with_optional_string_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_string",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_string_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_string",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_string_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_string",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_string_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("optional_string", value));
        self
    }

    pub fn with_optional_string_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("optional_string", value));
        self
    }

    pub fn with_optional_string_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("optional_string", value));
        self
    }

    pub fn with_optional_string_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("optional_string", value));
        self
    }

    pub fn with_optional_string_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("optional_string", value));
        self
    }

    pub fn with_optional_string_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("optional_string", value));
        self
    }

    pub fn with_optional_string_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("optional_string", value));
        self
    }
    pub fn with_optional_string_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_string", value));
        self
    }

    pub fn with_optional_string_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_string", value));
        self
    }

    pub fn with_optional_string_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_string"));
        self
    }



    pub fn with_optional_string_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_string"));
        self
    }


    pub fn order_by_optional_string_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_string");
        self
    }

    pub fn order_by_optional_string_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_string");
        self
    }

    pub fn order_by_optional_string_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_string");
        self
    }

    pub fn order_by_optional_string_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_string");
        self
    }


    pub fn select_large_text(mut self) -> Self {
        self.query = self.query.project("large_text");
        self
    }

    pub fn project_large_text(self) -> Self {
        self.select_large_text()
    }

    pub fn select_large_text_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_large_text_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_large_text_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("large_text", raw_sql_segment));
        self
    }

    pub fn group_by_large_text(self) -> Self {
        self.group_by("large_text")
    }

    pub fn group_by_large_text_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("large_text");
        request.query = request
            .query
            .project_expr(alias, Expr::column("large_text"));
        request
    }

    pub fn group_by_large_text_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("large_text")
            .aggregate_with_function("large_text", alias, function)
    }

    pub fn count_large_text(self) -> Self {
        self.count_large_text_as("large_text_count")
    }

    pub fn count_large_text_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("large_text", alias)
    }

    pub fn sum_large_text(self) -> Self {
        self.sum_large_text_as("sum_large_text")
    }

    pub fn sum_large_text_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("large_text", alias)
    }

    pub fn avg_large_text(self) -> Self {
        self.avg_large_text_as("avg_large_text")
    }

    pub fn avg_large_text_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("large_text", alias)
    }

    pub fn min_large_text(self) -> Self {
        self.min_large_text_as("min_large_text")
    }

    pub fn min_large_text_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("large_text", alias)
    }

    pub fn max_large_text(self) -> Self {
        self.max_large_text_as("max_large_text")
    }

    pub fn max_large_text_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("large_text", alias)
    }

    pub fn unselect_large_text(mut self) -> Self {
        self.query.projection.retain(|field| field != "large_text");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "large_text");
        self
    }


    pub fn with_large_text(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "large_text",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_large_text_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "large_text",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_large_text_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("large_text", value));
        self
    }



    pub fn with_large_text_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("large_text", value));
        self
    }

    pub fn with_large_text_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("large_text", value));
        self
    }

    pub fn with_large_text_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("large_text", value));
        self
    }

    pub fn with_large_text_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("large_text", value));
        self
    }

    pub fn with_large_text_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("large_text", value));
        self
    }

    pub fn with_large_text_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("large_text", lower, upper));
        self
    }

    pub fn with_large_text_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "large_text",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_large_text_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "large_text",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_large_text_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "large_text",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_large_text_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("large_text", value));
        self
    }

    pub fn with_large_text_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("large_text", value));
        self
    }

    pub fn with_large_text_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("large_text"));
        self
    }



    pub fn with_large_text_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("large_text"));
        self
    }


    pub fn order_by_large_text_asc(mut self) -> Self {
        self.query = self.query.order_asc("large_text");
        self
    }

    pub fn order_by_large_text_desc(mut self) -> Self {
        self.query = self.query.order_desc("large_text");
        self
    }

    pub fn order_by_large_text_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("large_text");
        self
    }

    pub fn order_by_large_text_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("large_text");
        self
    }


    pub fn select_required_boolean(mut self) -> Self {
        self.query = self.query.project("required_boolean");
        self
    }

    pub fn project_required_boolean(self) -> Self {
        self.select_required_boolean()
    }

    pub fn select_required_boolean_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_boolean_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_boolean_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_boolean", raw_sql_segment));
        self
    }

    pub fn group_by_required_boolean(self) -> Self {
        self.group_by("required_boolean")
    }

    pub fn group_by_required_boolean_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_boolean");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_boolean"));
        request
    }

    pub fn group_by_required_boolean_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_boolean")
            .aggregate_with_function("required_boolean", alias, function)
    }

    pub fn count_required_boolean(self) -> Self {
        self.count_required_boolean_as("required_boolean_count")
    }

    pub fn count_required_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_boolean", alias)
    }

    pub fn sum_required_boolean(self) -> Self {
        self.sum_required_boolean_as("sum_required_boolean")
    }

    pub fn sum_required_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_boolean", alias)
    }

    pub fn avg_required_boolean(self) -> Self {
        self.avg_required_boolean_as("avg_required_boolean")
    }

    pub fn avg_required_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_boolean", alias)
    }

    pub fn min_required_boolean(self) -> Self {
        self.min_required_boolean_as("min_required_boolean")
    }

    pub fn min_required_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_boolean", alias)
    }

    pub fn max_required_boolean(self) -> Self {
        self.max_required_boolean_as("max_required_boolean")
    }

    pub fn max_required_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_boolean", alias)
    }

    pub fn unselect_required_boolean(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_boolean");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_boolean");
        self
    }

    pub fn which_are_required_boolean(mut self) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_boolean", true));
        self
    }

    pub fn which_are_not_required_boolean(mut self) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_boolean", false));
        self
    }

    pub fn with_required_boolean_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_boolean"));
        self
    }

    pub fn with_required_boolean_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_boolean"));
        self
    }
    pub fn order_by_required_boolean_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_boolean");
        self
    }

    pub fn order_by_required_boolean_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_boolean");
        self
    }

    pub fn order_by_required_boolean_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_boolean");
        self
    }

    pub fn order_by_required_boolean_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_boolean");
        self
    }


    pub fn select_optional_boolean(mut self) -> Self {
        self.query = self.query.project("optional_boolean");
        self
    }

    pub fn project_optional_boolean(self) -> Self {
        self.select_optional_boolean()
    }

    pub fn select_optional_boolean_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_boolean_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_boolean_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_boolean", raw_sql_segment));
        self
    }

    pub fn group_by_optional_boolean(self) -> Self {
        self.group_by("optional_boolean")
    }

    pub fn group_by_optional_boolean_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_boolean");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_boolean"));
        request
    }

    pub fn group_by_optional_boolean_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_boolean")
            .aggregate_with_function("optional_boolean", alias, function)
    }

    pub fn count_optional_boolean(self) -> Self {
        self.count_optional_boolean_as("optional_boolean_count")
    }

    pub fn count_optional_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_boolean", alias)
    }

    pub fn sum_optional_boolean(self) -> Self {
        self.sum_optional_boolean_as("sum_optional_boolean")
    }

    pub fn sum_optional_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_boolean", alias)
    }

    pub fn avg_optional_boolean(self) -> Self {
        self.avg_optional_boolean_as("avg_optional_boolean")
    }

    pub fn avg_optional_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_boolean", alias)
    }

    pub fn min_optional_boolean(self) -> Self {
        self.min_optional_boolean_as("min_optional_boolean")
    }

    pub fn min_optional_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_boolean", alias)
    }

    pub fn max_optional_boolean(self) -> Self {
        self.max_optional_boolean_as("max_optional_boolean")
    }

    pub fn max_optional_boolean_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_boolean", alias)
    }

    pub fn unselect_optional_boolean(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_boolean");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_boolean");
        self
    }

    pub fn which_are_optional_boolean(mut self) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_boolean", true));
        self
    }

    pub fn which_are_not_optional_boolean(mut self) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_boolean", false));
        self
    }

    pub fn with_optional_boolean_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_boolean"));
        self
    }

    pub fn with_optional_boolean_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_boolean"));
        self
    }
    pub fn order_by_optional_boolean_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_boolean");
        self
    }

    pub fn order_by_optional_boolean_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_boolean");
        self
    }

    pub fn order_by_optional_boolean_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_boolean");
        self
    }

    pub fn order_by_optional_boolean_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_boolean");
        self
    }


    pub fn select_required_integer(mut self) -> Self {
        self.query = self.query.project("required_integer");
        self
    }

    pub fn project_required_integer(self) -> Self {
        self.select_required_integer()
    }

    pub fn select_required_integer_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_integer_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_integer_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_integer", raw_sql_segment));
        self
    }

    pub fn select_required_integer_with_function(self, function: AggregateFunction) -> Self {
        self.select_required_integer_as_with_function("required_integer", function)
    }

    pub fn select_required_integer_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("required_integer", alias, function)
    }

    pub fn group_by_required_integer(self) -> Self {
        self.group_by("required_integer")
    }

    pub fn group_by_required_integer_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_integer");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_integer"));
        request
    }

    pub fn group_by_required_integer_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_integer")
            .aggregate_with_function("required_integer", alias, function)
    }

    pub fn count_required_integer(self) -> Self {
        self.count_required_integer_as("required_integer_count")
    }

    pub fn count_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_integer", alias)
    }

    pub fn sum_required_integer(self) -> Self {
        self.sum_required_integer_as("sum_required_integer")
    }

    pub fn sum_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_integer", alias)
    }

    pub fn avg_required_integer(self) -> Self {
        self.avg_required_integer_as("avg_required_integer")
    }

    pub fn avg_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_integer", alias)
    }

    pub fn min_required_integer(self) -> Self {
        self.min_required_integer_as("min_required_integer")
    }

    pub fn min_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_integer", alias)
    }

    pub fn max_required_integer(self) -> Self {
        self.max_required_integer_as("max_required_integer")
    }

    pub fn max_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_integer", alias)
    }

    pub fn standard_deviation_required_integer(self) -> Self {
        self.standard_deviation_required_integer_as("stdDev_required_integer")
    }

    pub fn standard_deviation_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("required_integer", alias)
    }

    pub fn square_root_of_population_standard_deviation_required_integer(self) -> Self {
        self.square_root_of_population_standard_deviation_required_integer_as("stdDevPop_required_integer")
    }

    pub fn square_root_of_population_standard_deviation_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("required_integer", alias)
    }

    pub fn sample_variance_required_integer(self) -> Self {
        self.sample_variance_required_integer_as("varSamp_required_integer")
    }

    pub fn sample_variance_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("required_integer", alias)
    }

    pub fn sample_population_variance_required_integer(self) -> Self {
        self.sample_population_variance_required_integer_as("varPop_required_integer")
    }

    pub fn sample_population_variance_required_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("required_integer", alias)
    }

    pub fn unselect_required_integer(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_integer");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_integer");
        self
    }


    pub fn with_required_integer(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_integer",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_integer_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_integer",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_integer_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_integer", value));
        self
    }



    pub fn with_required_integer_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_integer", value));
        self
    }

    pub fn with_required_integer_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_integer", value));
        self
    }

    pub fn with_required_integer_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_integer", value));
        self
    }

    pub fn with_required_integer_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_integer", value));
        self
    }

    pub fn with_required_integer_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_integer", value));
        self
    }

    pub fn with_required_integer_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_integer", lower, upper));
        self
    }

    pub fn with_required_integer_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_integer",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_integer_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_integer",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_integer_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_integer",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_integer_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_integer", value));
        self
    }

    pub fn with_required_integer_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_integer", value));
        self
    }

    pub fn with_required_integer_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_integer"));
        self
    }



    pub fn with_required_integer_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_integer"));
        self
    }


    pub fn order_by_required_integer_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_integer");
        self
    }

    pub fn order_by_required_integer_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_integer");
        self
    }

    pub fn order_by_required_integer_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_integer");
        self
    }

    pub fn order_by_required_integer_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_integer");
        self
    }


    pub fn select_optional_integer(mut self) -> Self {
        self.query = self.query.project("optional_integer");
        self
    }

    pub fn project_optional_integer(self) -> Self {
        self.select_optional_integer()
    }

    pub fn select_optional_integer_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_integer_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_integer_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_integer", raw_sql_segment));
        self
    }

    pub fn select_optional_integer_with_function(self, function: AggregateFunction) -> Self {
        self.select_optional_integer_as_with_function("optional_integer", function)
    }

    pub fn select_optional_integer_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("optional_integer", alias, function)
    }

    pub fn group_by_optional_integer(self) -> Self {
        self.group_by("optional_integer")
    }

    pub fn group_by_optional_integer_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_integer");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_integer"));
        request
    }

    pub fn group_by_optional_integer_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_integer")
            .aggregate_with_function("optional_integer", alias, function)
    }

    pub fn count_optional_integer(self) -> Self {
        self.count_optional_integer_as("optional_integer_count")
    }

    pub fn count_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_integer", alias)
    }

    pub fn sum_optional_integer(self) -> Self {
        self.sum_optional_integer_as("sum_optional_integer")
    }

    pub fn sum_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_integer", alias)
    }

    pub fn avg_optional_integer(self) -> Self {
        self.avg_optional_integer_as("avg_optional_integer")
    }

    pub fn avg_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_integer", alias)
    }

    pub fn min_optional_integer(self) -> Self {
        self.min_optional_integer_as("min_optional_integer")
    }

    pub fn min_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_integer", alias)
    }

    pub fn max_optional_integer(self) -> Self {
        self.max_optional_integer_as("max_optional_integer")
    }

    pub fn max_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_integer", alias)
    }

    pub fn standard_deviation_optional_integer(self) -> Self {
        self.standard_deviation_optional_integer_as("stdDev_optional_integer")
    }

    pub fn standard_deviation_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("optional_integer", alias)
    }

    pub fn square_root_of_population_standard_deviation_optional_integer(self) -> Self {
        self.square_root_of_population_standard_deviation_optional_integer_as("stdDevPop_optional_integer")
    }

    pub fn square_root_of_population_standard_deviation_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("optional_integer", alias)
    }

    pub fn sample_variance_optional_integer(self) -> Self {
        self.sample_variance_optional_integer_as("varSamp_optional_integer")
    }

    pub fn sample_variance_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("optional_integer", alias)
    }

    pub fn sample_population_variance_optional_integer(self) -> Self {
        self.sample_population_variance_optional_integer_as("varPop_optional_integer")
    }

    pub fn sample_population_variance_optional_integer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("optional_integer", alias)
    }

    pub fn unselect_optional_integer(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_integer");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_integer");
        self
    }


    pub fn with_optional_integer(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_integer",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_integer_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_integer",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_integer_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_integer", value));
        self
    }



    pub fn with_optional_integer_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_integer", value));
        self
    }

    pub fn with_optional_integer_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_integer", value));
        self
    }

    pub fn with_optional_integer_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_integer", value));
        self
    }

    pub fn with_optional_integer_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_integer", value));
        self
    }

    pub fn with_optional_integer_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_integer", value));
        self
    }

    pub fn with_optional_integer_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_integer", lower, upper));
        self
    }

    pub fn with_optional_integer_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_integer",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_integer_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_integer",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_integer_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_integer",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_integer_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_integer", value));
        self
    }

    pub fn with_optional_integer_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_integer", value));
        self
    }

    pub fn with_optional_integer_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_integer"));
        self
    }



    pub fn with_optional_integer_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_integer"));
        self
    }


    pub fn order_by_optional_integer_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_integer");
        self
    }

    pub fn order_by_optional_integer_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_integer");
        self
    }

    pub fn order_by_optional_integer_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_integer");
        self
    }

    pub fn order_by_optional_integer_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_integer");
        self
    }


    pub fn select_required_long(mut self) -> Self {
        self.query = self.query.project("required_long");
        self
    }

    pub fn project_required_long(self) -> Self {
        self.select_required_long()
    }

    pub fn select_required_long_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_long_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_long_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_long", raw_sql_segment));
        self
    }

    pub fn select_required_long_with_function(self, function: AggregateFunction) -> Self {
        self.select_required_long_as_with_function("required_long", function)
    }

    pub fn select_required_long_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("required_long", alias, function)
    }

    pub fn group_by_required_long(self) -> Self {
        self.group_by("required_long")
    }

    pub fn group_by_required_long_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_long");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_long"));
        request
    }

    pub fn group_by_required_long_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_long")
            .aggregate_with_function("required_long", alias, function)
    }

    pub fn count_required_long(self) -> Self {
        self.count_required_long_as("required_long_count")
    }

    pub fn count_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_long", alias)
    }

    pub fn sum_required_long(self) -> Self {
        self.sum_required_long_as("sum_required_long")
    }

    pub fn sum_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_long", alias)
    }

    pub fn avg_required_long(self) -> Self {
        self.avg_required_long_as("avg_required_long")
    }

    pub fn avg_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_long", alias)
    }

    pub fn min_required_long(self) -> Self {
        self.min_required_long_as("min_required_long")
    }

    pub fn min_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_long", alias)
    }

    pub fn max_required_long(self) -> Self {
        self.max_required_long_as("max_required_long")
    }

    pub fn max_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_long", alias)
    }

    pub fn standard_deviation_required_long(self) -> Self {
        self.standard_deviation_required_long_as("stdDev_required_long")
    }

    pub fn standard_deviation_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("required_long", alias)
    }

    pub fn square_root_of_population_standard_deviation_required_long(self) -> Self {
        self.square_root_of_population_standard_deviation_required_long_as("stdDevPop_required_long")
    }

    pub fn square_root_of_population_standard_deviation_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("required_long", alias)
    }

    pub fn sample_variance_required_long(self) -> Self {
        self.sample_variance_required_long_as("varSamp_required_long")
    }

    pub fn sample_variance_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("required_long", alias)
    }

    pub fn sample_population_variance_required_long(self) -> Self {
        self.sample_population_variance_required_long_as("varPop_required_long")
    }

    pub fn sample_population_variance_required_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("required_long", alias)
    }

    pub fn unselect_required_long(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_long");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_long");
        self
    }


    pub fn with_required_long(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_long",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_long_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_long",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_long_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_long", value));
        self
    }



    pub fn with_required_long_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_long", value));
        self
    }

    pub fn with_required_long_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_long", value));
        self
    }

    pub fn with_required_long_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_long", value));
        self
    }

    pub fn with_required_long_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_long", value));
        self
    }

    pub fn with_required_long_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_long", value));
        self
    }

    pub fn with_required_long_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_long", lower, upper));
        self
    }

    pub fn with_required_long_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_long",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_long_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_long",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_long_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_long",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_long_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_long", value));
        self
    }

    pub fn with_required_long_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_long", value));
        self
    }

    pub fn with_required_long_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_long"));
        self
    }



    pub fn with_required_long_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_long"));
        self
    }


    pub fn order_by_required_long_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_long");
        self
    }

    pub fn order_by_required_long_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_long");
        self
    }

    pub fn order_by_required_long_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_long");
        self
    }

    pub fn order_by_required_long_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_long");
        self
    }


    pub fn select_optional_long(mut self) -> Self {
        self.query = self.query.project("optional_long");
        self
    }

    pub fn project_optional_long(self) -> Self {
        self.select_optional_long()
    }

    pub fn select_optional_long_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_long_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_long_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_long", raw_sql_segment));
        self
    }

    pub fn select_optional_long_with_function(self, function: AggregateFunction) -> Self {
        self.select_optional_long_as_with_function("optional_long", function)
    }

    pub fn select_optional_long_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("optional_long", alias, function)
    }

    pub fn group_by_optional_long(self) -> Self {
        self.group_by("optional_long")
    }

    pub fn group_by_optional_long_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_long");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_long"));
        request
    }

    pub fn group_by_optional_long_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_long")
            .aggregate_with_function("optional_long", alias, function)
    }

    pub fn count_optional_long(self) -> Self {
        self.count_optional_long_as("optional_long_count")
    }

    pub fn count_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_long", alias)
    }

    pub fn sum_optional_long(self) -> Self {
        self.sum_optional_long_as("sum_optional_long")
    }

    pub fn sum_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_long", alias)
    }

    pub fn avg_optional_long(self) -> Self {
        self.avg_optional_long_as("avg_optional_long")
    }

    pub fn avg_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_long", alias)
    }

    pub fn min_optional_long(self) -> Self {
        self.min_optional_long_as("min_optional_long")
    }

    pub fn min_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_long", alias)
    }

    pub fn max_optional_long(self) -> Self {
        self.max_optional_long_as("max_optional_long")
    }

    pub fn max_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_long", alias)
    }

    pub fn standard_deviation_optional_long(self) -> Self {
        self.standard_deviation_optional_long_as("stdDev_optional_long")
    }

    pub fn standard_deviation_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("optional_long", alias)
    }

    pub fn square_root_of_population_standard_deviation_optional_long(self) -> Self {
        self.square_root_of_population_standard_deviation_optional_long_as("stdDevPop_optional_long")
    }

    pub fn square_root_of_population_standard_deviation_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("optional_long", alias)
    }

    pub fn sample_variance_optional_long(self) -> Self {
        self.sample_variance_optional_long_as("varSamp_optional_long")
    }

    pub fn sample_variance_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("optional_long", alias)
    }

    pub fn sample_population_variance_optional_long(self) -> Self {
        self.sample_population_variance_optional_long_as("varPop_optional_long")
    }

    pub fn sample_population_variance_optional_long_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("optional_long", alias)
    }

    pub fn unselect_optional_long(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_long");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_long");
        self
    }


    pub fn with_optional_long(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_long",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_long_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_long",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_long_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_long", value));
        self
    }



    pub fn with_optional_long_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_long", value));
        self
    }

    pub fn with_optional_long_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_long", value));
        self
    }

    pub fn with_optional_long_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_long", value));
        self
    }

    pub fn with_optional_long_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_long", value));
        self
    }

    pub fn with_optional_long_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_long", value));
        self
    }

    pub fn with_optional_long_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_long", lower, upper));
        self
    }

    pub fn with_optional_long_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_long",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_long_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_long",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_long_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_long",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_long_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_long", value));
        self
    }

    pub fn with_optional_long_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_long", value));
        self
    }

    pub fn with_optional_long_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_long"));
        self
    }



    pub fn with_optional_long_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_long"));
        self
    }


    pub fn order_by_optional_long_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_long");
        self
    }

    pub fn order_by_optional_long_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_long");
        self
    }

    pub fn order_by_optional_long_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_long");
        self
    }

    pub fn order_by_optional_long_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_long");
        self
    }


    pub fn select_required_decimal(mut self) -> Self {
        self.query = self.query.project("required_decimal");
        self
    }

    pub fn project_required_decimal(self) -> Self {
        self.select_required_decimal()
    }

    pub fn select_required_decimal_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_decimal_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_decimal_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_decimal", raw_sql_segment));
        self
    }

    pub fn select_required_decimal_with_function(self, function: AggregateFunction) -> Self {
        self.select_required_decimal_as_with_function("required_decimal", function)
    }

    pub fn select_required_decimal_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("required_decimal", alias, function)
    }

    pub fn group_by_required_decimal(self) -> Self {
        self.group_by("required_decimal")
    }

    pub fn group_by_required_decimal_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_decimal");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_decimal"));
        request
    }

    pub fn group_by_required_decimal_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_decimal")
            .aggregate_with_function("required_decimal", alias, function)
    }

    pub fn count_required_decimal(self) -> Self {
        self.count_required_decimal_as("required_decimal_count")
    }

    pub fn count_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_decimal", alias)
    }

    pub fn sum_required_decimal(self) -> Self {
        self.sum_required_decimal_as("sum_required_decimal")
    }

    pub fn sum_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_decimal", alias)
    }

    pub fn avg_required_decimal(self) -> Self {
        self.avg_required_decimal_as("avg_required_decimal")
    }

    pub fn avg_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_decimal", alias)
    }

    pub fn min_required_decimal(self) -> Self {
        self.min_required_decimal_as("min_required_decimal")
    }

    pub fn min_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_decimal", alias)
    }

    pub fn max_required_decimal(self) -> Self {
        self.max_required_decimal_as("max_required_decimal")
    }

    pub fn max_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_decimal", alias)
    }

    pub fn standard_deviation_required_decimal(self) -> Self {
        self.standard_deviation_required_decimal_as("stdDev_required_decimal")
    }

    pub fn standard_deviation_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("required_decimal", alias)
    }

    pub fn square_root_of_population_standard_deviation_required_decimal(self) -> Self {
        self.square_root_of_population_standard_deviation_required_decimal_as("stdDevPop_required_decimal")
    }

    pub fn square_root_of_population_standard_deviation_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("required_decimal", alias)
    }

    pub fn sample_variance_required_decimal(self) -> Self {
        self.sample_variance_required_decimal_as("varSamp_required_decimal")
    }

    pub fn sample_variance_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("required_decimal", alias)
    }

    pub fn sample_population_variance_required_decimal(self) -> Self {
        self.sample_population_variance_required_decimal_as("varPop_required_decimal")
    }

    pub fn sample_population_variance_required_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("required_decimal", alias)
    }

    pub fn unselect_required_decimal(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_decimal");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_decimal");
        self
    }


    pub fn with_required_decimal(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_decimal",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_decimal_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_decimal",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_decimal_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_decimal", value));
        self
    }



    pub fn with_required_decimal_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_decimal", value));
        self
    }

    pub fn with_required_decimal_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_decimal", value));
        self
    }

    pub fn with_required_decimal_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_decimal", value));
        self
    }

    pub fn with_required_decimal_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_decimal", value));
        self
    }

    pub fn with_required_decimal_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_decimal", value));
        self
    }

    pub fn with_required_decimal_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_decimal", lower, upper));
        self
    }

    pub fn with_required_decimal_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_decimal",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_decimal_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_decimal",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_decimal_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_decimal",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_decimal_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_decimal", value));
        self
    }

    pub fn with_required_decimal_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_decimal", value));
        self
    }

    pub fn with_required_decimal_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_decimal"));
        self
    }



    pub fn with_required_decimal_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_decimal"));
        self
    }


    pub fn order_by_required_decimal_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_decimal");
        self
    }

    pub fn order_by_required_decimal_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_decimal");
        self
    }

    pub fn order_by_required_decimal_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_decimal");
        self
    }

    pub fn order_by_required_decimal_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_decimal");
        self
    }


    pub fn select_optional_decimal(mut self) -> Self {
        self.query = self.query.project("optional_decimal");
        self
    }

    pub fn project_optional_decimal(self) -> Self {
        self.select_optional_decimal()
    }

    pub fn select_optional_decimal_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_decimal_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_decimal_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_decimal", raw_sql_segment));
        self
    }

    pub fn select_optional_decimal_with_function(self, function: AggregateFunction) -> Self {
        self.select_optional_decimal_as_with_function("optional_decimal", function)
    }

    pub fn select_optional_decimal_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("optional_decimal", alias, function)
    }

    pub fn group_by_optional_decimal(self) -> Self {
        self.group_by("optional_decimal")
    }

    pub fn group_by_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_decimal");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_decimal"));
        request
    }

    pub fn group_by_optional_decimal_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_decimal")
            .aggregate_with_function("optional_decimal", alias, function)
    }

    pub fn count_optional_decimal(self) -> Self {
        self.count_optional_decimal_as("optional_decimal_count")
    }

    pub fn count_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_decimal", alias)
    }

    pub fn sum_optional_decimal(self) -> Self {
        self.sum_optional_decimal_as("sum_optional_decimal")
    }

    pub fn sum_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_decimal", alias)
    }

    pub fn avg_optional_decimal(self) -> Self {
        self.avg_optional_decimal_as("avg_optional_decimal")
    }

    pub fn avg_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_decimal", alias)
    }

    pub fn min_optional_decimal(self) -> Self {
        self.min_optional_decimal_as("min_optional_decimal")
    }

    pub fn min_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_decimal", alias)
    }

    pub fn max_optional_decimal(self) -> Self {
        self.max_optional_decimal_as("max_optional_decimal")
    }

    pub fn max_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_decimal", alias)
    }

    pub fn standard_deviation_optional_decimal(self) -> Self {
        self.standard_deviation_optional_decimal_as("stdDev_optional_decimal")
    }

    pub fn standard_deviation_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("optional_decimal", alias)
    }

    pub fn square_root_of_population_standard_deviation_optional_decimal(self) -> Self {
        self.square_root_of_population_standard_deviation_optional_decimal_as("stdDevPop_optional_decimal")
    }

    pub fn square_root_of_population_standard_deviation_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("optional_decimal", alias)
    }

    pub fn sample_variance_optional_decimal(self) -> Self {
        self.sample_variance_optional_decimal_as("varSamp_optional_decimal")
    }

    pub fn sample_variance_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("optional_decimal", alias)
    }

    pub fn sample_population_variance_optional_decimal(self) -> Self {
        self.sample_population_variance_optional_decimal_as("varPop_optional_decimal")
    }

    pub fn sample_population_variance_optional_decimal_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("optional_decimal", alias)
    }

    pub fn unselect_optional_decimal(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_decimal");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_decimal");
        self
    }


    pub fn with_optional_decimal(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_decimal",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_decimal_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_decimal",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_decimal_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_decimal", value));
        self
    }



    pub fn with_optional_decimal_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_decimal", value));
        self
    }

    pub fn with_optional_decimal_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_decimal", value));
        self
    }

    pub fn with_optional_decimal_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_decimal", value));
        self
    }

    pub fn with_optional_decimal_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_decimal", value));
        self
    }

    pub fn with_optional_decimal_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_decimal", value));
        self
    }

    pub fn with_optional_decimal_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_decimal", lower, upper));
        self
    }

    pub fn with_optional_decimal_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_decimal",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_decimal_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_decimal",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_decimal_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_decimal",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_decimal_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_decimal", value));
        self
    }

    pub fn with_optional_decimal_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_decimal", value));
        self
    }

    pub fn with_optional_decimal_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_decimal"));
        self
    }



    pub fn with_optional_decimal_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_decimal"));
        self
    }


    pub fn order_by_optional_decimal_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_decimal");
        self
    }

    pub fn order_by_optional_decimal_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_decimal");
        self
    }

    pub fn order_by_optional_decimal_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_decimal");
        self
    }

    pub fn order_by_optional_decimal_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_decimal");
        self
    }


    pub fn select_required_float(mut self) -> Self {
        self.query = self.query.project("required_float");
        self
    }

    pub fn project_required_float(self) -> Self {
        self.select_required_float()
    }

    pub fn select_required_float_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_float_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_float_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_float", raw_sql_segment));
        self
    }

    pub fn select_required_float_with_function(self, function: AggregateFunction) -> Self {
        self.select_required_float_as_with_function("required_float", function)
    }

    pub fn select_required_float_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("required_float", alias, function)
    }

    pub fn group_by_required_float(self) -> Self {
        self.group_by("required_float")
    }

    pub fn group_by_required_float_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_float");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_float"));
        request
    }

    pub fn group_by_required_float_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_float")
            .aggregate_with_function("required_float", alias, function)
    }

    pub fn count_required_float(self) -> Self {
        self.count_required_float_as("required_float_count")
    }

    pub fn count_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_float", alias)
    }

    pub fn sum_required_float(self) -> Self {
        self.sum_required_float_as("sum_required_float")
    }

    pub fn sum_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_float", alias)
    }

    pub fn avg_required_float(self) -> Self {
        self.avg_required_float_as("avg_required_float")
    }

    pub fn avg_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_float", alias)
    }

    pub fn min_required_float(self) -> Self {
        self.min_required_float_as("min_required_float")
    }

    pub fn min_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_float", alias)
    }

    pub fn max_required_float(self) -> Self {
        self.max_required_float_as("max_required_float")
    }

    pub fn max_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_float", alias)
    }

    pub fn standard_deviation_required_float(self) -> Self {
        self.standard_deviation_required_float_as("stdDev_required_float")
    }

    pub fn standard_deviation_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("required_float", alias)
    }

    pub fn square_root_of_population_standard_deviation_required_float(self) -> Self {
        self.square_root_of_population_standard_deviation_required_float_as("stdDevPop_required_float")
    }

    pub fn square_root_of_population_standard_deviation_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("required_float", alias)
    }

    pub fn sample_variance_required_float(self) -> Self {
        self.sample_variance_required_float_as("varSamp_required_float")
    }

    pub fn sample_variance_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("required_float", alias)
    }

    pub fn sample_population_variance_required_float(self) -> Self {
        self.sample_population_variance_required_float_as("varPop_required_float")
    }

    pub fn sample_population_variance_required_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("required_float", alias)
    }

    pub fn unselect_required_float(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_float");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_float");
        self
    }


    pub fn with_required_float(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_float",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_float_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_float",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_float_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_float", value));
        self
    }



    pub fn with_required_float_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_float", value));
        self
    }

    pub fn with_required_float_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_float", value));
        self
    }

    pub fn with_required_float_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_float", value));
        self
    }

    pub fn with_required_float_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_float", value));
        self
    }

    pub fn with_required_float_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_float", value));
        self
    }

    pub fn with_required_float_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_float", lower, upper));
        self
    }

    pub fn with_required_float_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_float",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_float_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_float",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_float_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_float",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_float_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_float", value));
        self
    }

    pub fn with_required_float_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_float", value));
        self
    }

    pub fn with_required_float_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_float"));
        self
    }



    pub fn with_required_float_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_float"));
        self
    }


    pub fn order_by_required_float_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_float");
        self
    }

    pub fn order_by_required_float_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_float");
        self
    }

    pub fn order_by_required_float_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_float");
        self
    }

    pub fn order_by_required_float_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_float");
        self
    }


    pub fn select_optional_float(mut self) -> Self {
        self.query = self.query.project("optional_float");
        self
    }

    pub fn project_optional_float(self) -> Self {
        self.select_optional_float()
    }

    pub fn select_optional_float_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_float_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_float_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_float", raw_sql_segment));
        self
    }

    pub fn select_optional_float_with_function(self, function: AggregateFunction) -> Self {
        self.select_optional_float_as_with_function("optional_float", function)
    }

    pub fn select_optional_float_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("optional_float", alias, function)
    }

    pub fn group_by_optional_float(self) -> Self {
        self.group_by("optional_float")
    }

    pub fn group_by_optional_float_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_float");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_float"));
        request
    }

    pub fn group_by_optional_float_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_float")
            .aggregate_with_function("optional_float", alias, function)
    }

    pub fn count_optional_float(self) -> Self {
        self.count_optional_float_as("optional_float_count")
    }

    pub fn count_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_float", alias)
    }

    pub fn sum_optional_float(self) -> Self {
        self.sum_optional_float_as("sum_optional_float")
    }

    pub fn sum_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_float", alias)
    }

    pub fn avg_optional_float(self) -> Self {
        self.avg_optional_float_as("avg_optional_float")
    }

    pub fn avg_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_float", alias)
    }

    pub fn min_optional_float(self) -> Self {
        self.min_optional_float_as("min_optional_float")
    }

    pub fn min_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_float", alias)
    }

    pub fn max_optional_float(self) -> Self {
        self.max_optional_float_as("max_optional_float")
    }

    pub fn max_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_float", alias)
    }

    pub fn standard_deviation_optional_float(self) -> Self {
        self.standard_deviation_optional_float_as("stdDev_optional_float")
    }

    pub fn standard_deviation_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("optional_float", alias)
    }

    pub fn square_root_of_population_standard_deviation_optional_float(self) -> Self {
        self.square_root_of_population_standard_deviation_optional_float_as("stdDevPop_optional_float")
    }

    pub fn square_root_of_population_standard_deviation_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("optional_float", alias)
    }

    pub fn sample_variance_optional_float(self) -> Self {
        self.sample_variance_optional_float_as("varSamp_optional_float")
    }

    pub fn sample_variance_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("optional_float", alias)
    }

    pub fn sample_population_variance_optional_float(self) -> Self {
        self.sample_population_variance_optional_float_as("varPop_optional_float")
    }

    pub fn sample_population_variance_optional_float_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("optional_float", alias)
    }

    pub fn unselect_optional_float(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_float");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_float");
        self
    }


    pub fn with_optional_float(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_float",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_float_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_float",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_float_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_float", value));
        self
    }



    pub fn with_optional_float_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_float", value));
        self
    }

    pub fn with_optional_float_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_float", value));
        self
    }

    pub fn with_optional_float_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_float", value));
        self
    }

    pub fn with_optional_float_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_float", value));
        self
    }

    pub fn with_optional_float_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_float", value));
        self
    }

    pub fn with_optional_float_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_float", lower, upper));
        self
    }

    pub fn with_optional_float_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_float",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_float_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_float",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_float_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_float",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_float_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_float", value));
        self
    }

    pub fn with_optional_float_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_float", value));
        self
    }

    pub fn with_optional_float_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_float"));
        self
    }



    pub fn with_optional_float_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_float"));
        self
    }


    pub fn order_by_optional_float_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_float");
        self
    }

    pub fn order_by_optional_float_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_float");
        self
    }

    pub fn order_by_optional_float_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_float");
        self
    }

    pub fn order_by_optional_float_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_float");
        self
    }


    pub fn select_required_double(mut self) -> Self {
        self.query = self.query.project("required_double");
        self
    }

    pub fn project_required_double(self) -> Self {
        self.select_required_double()
    }

    pub fn select_required_double_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_double_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_double_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_double", raw_sql_segment));
        self
    }

    pub fn select_required_double_with_function(self, function: AggregateFunction) -> Self {
        self.select_required_double_as_with_function("required_double", function)
    }

    pub fn select_required_double_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("required_double", alias, function)
    }

    pub fn group_by_required_double(self) -> Self {
        self.group_by("required_double")
    }

    pub fn group_by_required_double_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_double");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_double"));
        request
    }

    pub fn group_by_required_double_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_double")
            .aggregate_with_function("required_double", alias, function)
    }

    pub fn count_required_double(self) -> Self {
        self.count_required_double_as("required_double_count")
    }

    pub fn count_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_double", alias)
    }

    pub fn sum_required_double(self) -> Self {
        self.sum_required_double_as("sum_required_double")
    }

    pub fn sum_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_double", alias)
    }

    pub fn avg_required_double(self) -> Self {
        self.avg_required_double_as("avg_required_double")
    }

    pub fn avg_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_double", alias)
    }

    pub fn min_required_double(self) -> Self {
        self.min_required_double_as("min_required_double")
    }

    pub fn min_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_double", alias)
    }

    pub fn max_required_double(self) -> Self {
        self.max_required_double_as("max_required_double")
    }

    pub fn max_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_double", alias)
    }

    pub fn standard_deviation_required_double(self) -> Self {
        self.standard_deviation_required_double_as("stdDev_required_double")
    }

    pub fn standard_deviation_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("required_double", alias)
    }

    pub fn square_root_of_population_standard_deviation_required_double(self) -> Self {
        self.square_root_of_population_standard_deviation_required_double_as("stdDevPop_required_double")
    }

    pub fn square_root_of_population_standard_deviation_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("required_double", alias)
    }

    pub fn sample_variance_required_double(self) -> Self {
        self.sample_variance_required_double_as("varSamp_required_double")
    }

    pub fn sample_variance_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("required_double", alias)
    }

    pub fn sample_population_variance_required_double(self) -> Self {
        self.sample_population_variance_required_double_as("varPop_required_double")
    }

    pub fn sample_population_variance_required_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("required_double", alias)
    }

    pub fn unselect_required_double(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_double");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_double");
        self
    }


    pub fn with_required_double(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_double",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_double_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_double",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_double_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_double", value));
        self
    }



    pub fn with_required_double_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_double", value));
        self
    }

    pub fn with_required_double_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_double", value));
        self
    }

    pub fn with_required_double_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_double", value));
        self
    }

    pub fn with_required_double_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_double", value));
        self
    }

    pub fn with_required_double_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_double", value));
        self
    }

    pub fn with_required_double_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_double", lower, upper));
        self
    }

    pub fn with_required_double_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_double",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_double_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_double",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_double_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_double",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_double_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_double", value));
        self
    }

    pub fn with_required_double_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_double", value));
        self
    }

    pub fn with_required_double_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_double"));
        self
    }



    pub fn with_required_double_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_double"));
        self
    }


    pub fn order_by_required_double_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_double");
        self
    }

    pub fn order_by_required_double_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_double");
        self
    }

    pub fn order_by_required_double_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_double");
        self
    }

    pub fn order_by_required_double_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_double");
        self
    }


    pub fn select_optional_double(mut self) -> Self {
        self.query = self.query.project("optional_double");
        self
    }

    pub fn project_optional_double(self) -> Self {
        self.select_optional_double()
    }

    pub fn select_optional_double_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_double_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_double_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_double", raw_sql_segment));
        self
    }

    pub fn select_optional_double_with_function(self, function: AggregateFunction) -> Self {
        self.select_optional_double_as_with_function("optional_double", function)
    }

    pub fn select_optional_double_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("optional_double", alias, function)
    }

    pub fn group_by_optional_double(self) -> Self {
        self.group_by("optional_double")
    }

    pub fn group_by_optional_double_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_double");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_double"));
        request
    }

    pub fn group_by_optional_double_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_double")
            .aggregate_with_function("optional_double", alias, function)
    }

    pub fn count_optional_double(self) -> Self {
        self.count_optional_double_as("optional_double_count")
    }

    pub fn count_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_double", alias)
    }

    pub fn sum_optional_double(self) -> Self {
        self.sum_optional_double_as("sum_optional_double")
    }

    pub fn sum_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_double", alias)
    }

    pub fn avg_optional_double(self) -> Self {
        self.avg_optional_double_as("avg_optional_double")
    }

    pub fn avg_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_double", alias)
    }

    pub fn min_optional_double(self) -> Self {
        self.min_optional_double_as("min_optional_double")
    }

    pub fn min_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_double", alias)
    }

    pub fn max_optional_double(self) -> Self {
        self.max_optional_double_as("max_optional_double")
    }

    pub fn max_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_double", alias)
    }

    pub fn standard_deviation_optional_double(self) -> Self {
        self.standard_deviation_optional_double_as("stdDev_optional_double")
    }

    pub fn standard_deviation_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("optional_double", alias)
    }

    pub fn square_root_of_population_standard_deviation_optional_double(self) -> Self {
        self.square_root_of_population_standard_deviation_optional_double_as("stdDevPop_optional_double")
    }

    pub fn square_root_of_population_standard_deviation_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("optional_double", alias)
    }

    pub fn sample_variance_optional_double(self) -> Self {
        self.sample_variance_optional_double_as("varSamp_optional_double")
    }

    pub fn sample_variance_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("optional_double", alias)
    }

    pub fn sample_population_variance_optional_double(self) -> Self {
        self.sample_population_variance_optional_double_as("varPop_optional_double")
    }

    pub fn sample_population_variance_optional_double_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("optional_double", alias)
    }

    pub fn unselect_optional_double(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_double");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_double");
        self
    }


    pub fn with_optional_double(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_double",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_double_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_double",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_double_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_double", value));
        self
    }



    pub fn with_optional_double_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_double", value));
        self
    }

    pub fn with_optional_double_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_double", value));
        self
    }

    pub fn with_optional_double_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_double", value));
        self
    }

    pub fn with_optional_double_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_double", value));
        self
    }

    pub fn with_optional_double_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_double", value));
        self
    }

    pub fn with_optional_double_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_double", lower, upper));
        self
    }

    pub fn with_optional_double_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_double",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_double_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_double",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_double_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_double",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_double_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_double", value));
        self
    }

    pub fn with_optional_double_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_double", value));
        self
    }

    pub fn with_optional_double_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_double"));
        self
    }



    pub fn with_optional_double_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_double"));
        self
    }


    pub fn order_by_optional_double_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_double");
        self
    }

    pub fn order_by_optional_double_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_double");
        self
    }

    pub fn order_by_optional_double_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_double");
        self
    }

    pub fn order_by_optional_double_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_double");
        self
    }


    pub fn select_required_date(mut self) -> Self {
        self.query = self.query.project("required_date");
        self
    }

    pub fn project_required_date(self) -> Self {
        self.select_required_date()
    }

    pub fn select_required_date_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_date_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_date_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_date", raw_sql_segment));
        self
    }

    pub fn group_by_required_date(self) -> Self {
        self.group_by("required_date")
    }

    pub fn group_by_required_date_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_date");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_date"));
        request
    }

    pub fn group_by_required_date_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_date")
            .aggregate_with_function("required_date", alias, function)
    }

    pub fn count_required_date(self) -> Self {
        self.count_required_date_as("required_date_count")
    }

    pub fn count_required_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_date", alias)
    }

    pub fn sum_required_date(self) -> Self {
        self.sum_required_date_as("sum_required_date")
    }

    pub fn sum_required_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_date", alias)
    }

    pub fn avg_required_date(self) -> Self {
        self.avg_required_date_as("avg_required_date")
    }

    pub fn avg_required_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_date", alias)
    }

    pub fn min_required_date(self) -> Self {
        self.min_required_date_as("min_required_date")
    }

    pub fn min_required_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_date", alias)
    }

    pub fn max_required_date(self) -> Self {
        self.max_required_date_as("max_required_date")
    }

    pub fn max_required_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_date", alias)
    }

    pub fn unselect_required_date(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_date");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_date");
        self
    }


    pub fn with_required_date(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_date",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_date_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_date",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_date_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_date", value));
        self
    }



    pub fn with_required_date_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_date", value));
        self
    }

    pub fn with_required_date_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_date", value));
        self
    }

    pub fn with_required_date_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_date", value));
        self
    }

    pub fn with_required_date_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_date", value));
        self
    }

    pub fn with_required_date_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_date", value));
        self
    }

    pub fn with_required_date_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_date", lower, upper));
        self
    }

    pub fn with_required_date_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_date",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_date_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_date",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_date_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_date",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_date_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_date", value));
        self
    }

    pub fn with_required_date_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_date", value));
        self
    }

    pub fn with_required_date_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_date"));
        self
    }



    pub fn with_required_date_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_date"));
        self
    }


    pub fn order_by_required_date_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_date");
        self
    }

    pub fn order_by_required_date_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_date");
        self
    }

    pub fn order_by_required_date_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_date");
        self
    }

    pub fn order_by_required_date_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_date");
        self
    }


    pub fn select_optional_date(mut self) -> Self {
        self.query = self.query.project("optional_date");
        self
    }

    pub fn project_optional_date(self) -> Self {
        self.select_optional_date()
    }

    pub fn select_optional_date_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_date_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_date_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_date", raw_sql_segment));
        self
    }

    pub fn group_by_optional_date(self) -> Self {
        self.group_by("optional_date")
    }

    pub fn group_by_optional_date_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_date");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_date"));
        request
    }

    pub fn group_by_optional_date_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_date")
            .aggregate_with_function("optional_date", alias, function)
    }

    pub fn count_optional_date(self) -> Self {
        self.count_optional_date_as("optional_date_count")
    }

    pub fn count_optional_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_date", alias)
    }

    pub fn sum_optional_date(self) -> Self {
        self.sum_optional_date_as("sum_optional_date")
    }

    pub fn sum_optional_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_date", alias)
    }

    pub fn avg_optional_date(self) -> Self {
        self.avg_optional_date_as("avg_optional_date")
    }

    pub fn avg_optional_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_date", alias)
    }

    pub fn min_optional_date(self) -> Self {
        self.min_optional_date_as("min_optional_date")
    }

    pub fn min_optional_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_date", alias)
    }

    pub fn max_optional_date(self) -> Self {
        self.max_optional_date_as("max_optional_date")
    }

    pub fn max_optional_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_date", alias)
    }

    pub fn unselect_optional_date(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_date");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_date");
        self
    }


    pub fn with_optional_date(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_date",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_date_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_date",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_date_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_date", value));
        self
    }



    pub fn with_optional_date_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_date", value));
        self
    }

    pub fn with_optional_date_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_date", value));
        self
    }

    pub fn with_optional_date_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_date", value));
        self
    }

    pub fn with_optional_date_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_date", value));
        self
    }

    pub fn with_optional_date_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_date", value));
        self
    }

    pub fn with_optional_date_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_date", lower, upper));
        self
    }

    pub fn with_optional_date_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_date",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_date_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_date",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_date_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_date",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_date_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_date", value));
        self
    }

    pub fn with_optional_date_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_date", value));
        self
    }

    pub fn with_optional_date_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_date"));
        self
    }



    pub fn with_optional_date_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_date"));
        self
    }


    pub fn order_by_optional_date_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_date");
        self
    }

    pub fn order_by_optional_date_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_date");
        self
    }

    pub fn order_by_optional_date_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_date");
        self
    }

    pub fn order_by_optional_date_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_date");
        self
    }


    pub fn select_required_time(mut self) -> Self {
        self.query = self.query.project("required_time");
        self
    }

    pub fn project_required_time(self) -> Self {
        self.select_required_time()
    }

    pub fn select_required_time_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_time_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_time_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_time", raw_sql_segment));
        self
    }

    pub fn group_by_required_time(self) -> Self {
        self.group_by("required_time")
    }

    pub fn group_by_required_time_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_time");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_time"));
        request
    }

    pub fn group_by_required_time_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_time")
            .aggregate_with_function("required_time", alias, function)
    }

    pub fn count_required_time(self) -> Self {
        self.count_required_time_as("required_time_count")
    }

    pub fn count_required_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_time", alias)
    }

    pub fn sum_required_time(self) -> Self {
        self.sum_required_time_as("sum_required_time")
    }

    pub fn sum_required_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_time", alias)
    }

    pub fn avg_required_time(self) -> Self {
        self.avg_required_time_as("avg_required_time")
    }

    pub fn avg_required_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_time", alias)
    }

    pub fn min_required_time(self) -> Self {
        self.min_required_time_as("min_required_time")
    }

    pub fn min_required_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_time", alias)
    }

    pub fn max_required_time(self) -> Self {
        self.max_required_time_as("max_required_time")
    }

    pub fn max_required_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_time", alias)
    }

    pub fn unselect_required_time(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_time");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_time");
        self
    }


    pub fn with_required_time(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_time_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_time_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_time", value));
        self
    }



    pub fn with_required_time_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_time", value));
        self
    }

    pub fn with_required_time_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_time", value));
        self
    }

    pub fn with_required_time_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_time", value));
        self
    }

    pub fn with_required_time_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_time", value));
        self
    }

    pub fn with_required_time_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_time", value));
        self
    }

    pub fn with_required_time_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_time", lower, upper));
        self
    }

    pub fn with_required_time_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_time",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_time_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_time_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_time_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_time", value));
        self
    }

    pub fn with_required_time_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_time", value));
        self
    }

    pub fn with_required_time_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_time"));
        self
    }



    pub fn with_required_time_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_time"));
        self
    }


    pub fn order_by_required_time_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_time");
        self
    }

    pub fn order_by_required_time_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_time");
        self
    }

    pub fn order_by_required_time_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_time");
        self
    }

    pub fn order_by_required_time_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_time");
        self
    }


    pub fn select_optional_time(mut self) -> Self {
        self.query = self.query.project("optional_time");
        self
    }

    pub fn project_optional_time(self) -> Self {
        self.select_optional_time()
    }

    pub fn select_optional_time_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_time_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_time_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_time", raw_sql_segment));
        self
    }

    pub fn group_by_optional_time(self) -> Self {
        self.group_by("optional_time")
    }

    pub fn group_by_optional_time_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_time");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_time"));
        request
    }

    pub fn group_by_optional_time_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_time")
            .aggregate_with_function("optional_time", alias, function)
    }

    pub fn count_optional_time(self) -> Self {
        self.count_optional_time_as("optional_time_count")
    }

    pub fn count_optional_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_time", alias)
    }

    pub fn sum_optional_time(self) -> Self {
        self.sum_optional_time_as("sum_optional_time")
    }

    pub fn sum_optional_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_time", alias)
    }

    pub fn avg_optional_time(self) -> Self {
        self.avg_optional_time_as("avg_optional_time")
    }

    pub fn avg_optional_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_time", alias)
    }

    pub fn min_optional_time(self) -> Self {
        self.min_optional_time_as("min_optional_time")
    }

    pub fn min_optional_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_time", alias)
    }

    pub fn max_optional_time(self) -> Self {
        self.max_optional_time_as("max_optional_time")
    }

    pub fn max_optional_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_time", alias)
    }

    pub fn unselect_optional_time(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_time");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_time");
        self
    }


    pub fn with_optional_time(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_time_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_time_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_time", value));
        self
    }



    pub fn with_optional_time_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_time", value));
        self
    }

    pub fn with_optional_time_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_time", value));
        self
    }

    pub fn with_optional_time_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_time", value));
        self
    }

    pub fn with_optional_time_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_time", value));
        self
    }

    pub fn with_optional_time_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_time", value));
        self
    }

    pub fn with_optional_time_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_time", lower, upper));
        self
    }

    pub fn with_optional_time_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_time",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_time_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_time_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_time_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_time", value));
        self
    }

    pub fn with_optional_time_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_time", value));
        self
    }

    pub fn with_optional_time_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_time"));
        self
    }



    pub fn with_optional_time_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_time"));
        self
    }


    pub fn order_by_optional_time_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_time");
        self
    }

    pub fn order_by_optional_time_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_time");
        self
    }

    pub fn order_by_optional_time_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_time");
        self
    }

    pub fn order_by_optional_time_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_time");
        self
    }


    pub fn select_required_datetime(mut self) -> Self {
        self.query = self.query.project("required_datetime");
        self
    }

    pub fn project_required_datetime(self) -> Self {
        self.select_required_datetime()
    }

    pub fn select_required_datetime_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_required_datetime_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_required_datetime_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("required_datetime", raw_sql_segment));
        self
    }

    pub fn group_by_required_datetime(self) -> Self {
        self.group_by("required_datetime")
    }

    pub fn group_by_required_datetime_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("required_datetime");
        request.query = request
            .query
            .project_expr(alias, Expr::column("required_datetime"));
        request
    }

    pub fn group_by_required_datetime_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("required_datetime")
            .aggregate_with_function("required_datetime", alias, function)
    }

    pub fn count_required_datetime(self) -> Self {
        self.count_required_datetime_as("required_datetime_count")
    }

    pub fn count_required_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("required_datetime", alias)
    }

    pub fn sum_required_datetime(self) -> Self {
        self.sum_required_datetime_as("sum_required_datetime")
    }

    pub fn sum_required_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("required_datetime", alias)
    }

    pub fn avg_required_datetime(self) -> Self {
        self.avg_required_datetime_as("avg_required_datetime")
    }

    pub fn avg_required_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("required_datetime", alias)
    }

    pub fn min_required_datetime(self) -> Self {
        self.min_required_datetime_as("min_required_datetime")
    }

    pub fn min_required_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("required_datetime", alias)
    }

    pub fn max_required_datetime(self) -> Self {
        self.max_required_datetime_as("max_required_datetime")
    }

    pub fn max_required_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("required_datetime", alias)
    }

    pub fn unselect_required_datetime(mut self) -> Self {
        self.query.projection.retain(|field| field != "required_datetime");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "required_datetime");
        self
    }


    pub fn with_required_datetime(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "required_datetime",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_required_datetime_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "required_datetime",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_required_datetime_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("required_datetime", value));
        self
    }



    pub fn with_required_datetime_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("required_datetime", value));
        self
    }

    pub fn with_required_datetime_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_datetime", value));
        self
    }

    pub fn with_required_datetime_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("required_datetime", value));
        self
    }

    pub fn with_required_datetime_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_datetime", value));
        self
    }

    pub fn with_required_datetime_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("required_datetime", value));
        self
    }

    pub fn with_required_datetime_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("required_datetime", lower, upper));
        self
    }

    pub fn with_required_datetime_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "required_datetime",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_required_datetime_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "required_datetime",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_datetime_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "required_datetime",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_required_datetime_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("required_datetime", value));
        self
    }

    pub fn with_required_datetime_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("required_datetime", value));
        self
    }

    pub fn with_required_datetime_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("required_datetime"));
        self
    }



    pub fn with_required_datetime_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("required_datetime"));
        self
    }


    pub fn order_by_required_datetime_asc(mut self) -> Self {
        self.query = self.query.order_asc("required_datetime");
        self
    }

    pub fn order_by_required_datetime_desc(mut self) -> Self {
        self.query = self.query.order_desc("required_datetime");
        self
    }

    pub fn order_by_required_datetime_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("required_datetime");
        self
    }

    pub fn order_by_required_datetime_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("required_datetime");
        self
    }


    pub fn select_optional_datetime(mut self) -> Self {
        self.query = self.query.project("optional_datetime");
        self
    }

    pub fn project_optional_datetime(self) -> Self {
        self.select_optional_datetime()
    }

    pub fn select_optional_datetime_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_optional_datetime_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_optional_datetime_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("optional_datetime", raw_sql_segment));
        self
    }

    pub fn group_by_optional_datetime(self) -> Self {
        self.group_by("optional_datetime")
    }

    pub fn group_by_optional_datetime_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("optional_datetime");
        request.query = request
            .query
            .project_expr(alias, Expr::column("optional_datetime"));
        request
    }

    pub fn group_by_optional_datetime_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("optional_datetime")
            .aggregate_with_function("optional_datetime", alias, function)
    }

    pub fn count_optional_datetime(self) -> Self {
        self.count_optional_datetime_as("optional_datetime_count")
    }

    pub fn count_optional_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("optional_datetime", alias)
    }

    pub fn sum_optional_datetime(self) -> Self {
        self.sum_optional_datetime_as("sum_optional_datetime")
    }

    pub fn sum_optional_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("optional_datetime", alias)
    }

    pub fn avg_optional_datetime(self) -> Self {
        self.avg_optional_datetime_as("avg_optional_datetime")
    }

    pub fn avg_optional_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("optional_datetime", alias)
    }

    pub fn min_optional_datetime(self) -> Self {
        self.min_optional_datetime_as("min_optional_datetime")
    }

    pub fn min_optional_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("optional_datetime", alias)
    }

    pub fn max_optional_datetime(self) -> Self {
        self.max_optional_datetime_as("max_optional_datetime")
    }

    pub fn max_optional_datetime_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("optional_datetime", alias)
    }

    pub fn unselect_optional_datetime(mut self) -> Self {
        self.query.projection.retain(|field| field != "optional_datetime");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "optional_datetime");
        self
    }


    pub fn with_optional_datetime(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "optional_datetime",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_optional_datetime_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "optional_datetime",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_optional_datetime_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("optional_datetime", value));
        self
    }



    pub fn with_optional_datetime_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("optional_datetime", value));
        self
    }

    pub fn with_optional_datetime_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_datetime", value));
        self
    }

    pub fn with_optional_datetime_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("optional_datetime", value));
        self
    }

    pub fn with_optional_datetime_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_datetime", value));
        self
    }

    pub fn with_optional_datetime_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("optional_datetime", value));
        self
    }

    pub fn with_optional_datetime_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("optional_datetime", lower, upper));
        self
    }

    pub fn with_optional_datetime_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "optional_datetime",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_optional_datetime_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "optional_datetime",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_datetime_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "optional_datetime",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_optional_datetime_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("optional_datetime", value));
        self
    }

    pub fn with_optional_datetime_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("optional_datetime", value));
        self
    }

    pub fn with_optional_datetime_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("optional_datetime"));
        self
    }



    pub fn with_optional_datetime_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("optional_datetime"));
        self
    }


    pub fn order_by_optional_datetime_asc(mut self) -> Self {
        self.query = self.query.order_asc("optional_datetime");
        self
    }

    pub fn order_by_optional_datetime_desc(mut self) -> Self {
        self.query = self.query.order_desc("optional_datetime");
        self
    }

    pub fn order_by_optional_datetime_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("optional_datetime");
        self
    }

    pub fn order_by_optional_datetime_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("optional_datetime");
        self
    }


    pub fn select_create_time(mut self) -> Self {
        self.query = self.query.project("create_time");
        self
    }

    pub fn project_create_time(self) -> Self {
        self.select_create_time()
    }

    pub fn select_create_time_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_create_time_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_create_time_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("create_time", raw_sql_segment));
        self
    }

    pub fn group_by_create_time(self) -> Self {
        self.group_by("create_time")
    }

    pub fn group_by_create_time_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("create_time");
        request.query = request
            .query
            .project_expr(alias, Expr::column("create_time"));
        request
    }

    pub fn group_by_create_time_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("create_time")
            .aggregate_with_function("create_time", alias, function)
    }

    pub fn count_create_time(self) -> Self {
        self.count_create_time_as("create_time_count")
    }

    pub fn count_create_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("create_time", alias)
    }

    pub fn sum_create_time(self) -> Self {
        self.sum_create_time_as("sum_create_time")
    }

    pub fn sum_create_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("create_time", alias)
    }

    pub fn avg_create_time(self) -> Self {
        self.avg_create_time_as("avg_create_time")
    }

    pub fn avg_create_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("create_time", alias)
    }

    pub fn min_create_time(self) -> Self {
        self.min_create_time_as("min_create_time")
    }

    pub fn min_create_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("create_time", alias)
    }

    pub fn max_create_time(self) -> Self {
        self.max_create_time_as("max_create_time")
    }

    pub fn max_create_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("create_time", alias)
    }

    pub fn unselect_create_time(mut self) -> Self {
        self.query.projection.retain(|field| field != "create_time");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "create_time");
        self
    }


    pub fn with_create_time(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "create_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_create_time_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "create_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_create_time_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("create_time", value));
        self
    }



    pub fn with_create_time_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("create_time", value));
        self
    }

    pub fn with_create_time_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("create_time", value));
        self
    }

    pub fn with_create_time_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("create_time", value));
        self
    }

    pub fn with_create_time_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("create_time", value));
        self
    }

    pub fn with_create_time_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("create_time", value));
        self
    }

    pub fn with_create_time_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("create_time", lower, upper));
        self
    }

    pub fn with_create_time_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "create_time",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_create_time_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "create_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_create_time_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "create_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_create_time_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("create_time", value));
        self
    }

    pub fn with_create_time_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("create_time", value));
        self
    }

    pub fn with_create_time_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("create_time"));
        self
    }



    pub fn with_create_time_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("create_time"));
        self
    }


    pub fn order_by_create_time_asc(mut self) -> Self {
        self.query = self.query.order_asc("create_time");
        self
    }

    pub fn order_by_create_time_desc(mut self) -> Self {
        self.query = self.query.order_desc("create_time");
        self
    }

    pub fn order_by_create_time_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("create_time");
        self
    }

    pub fn order_by_create_time_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("create_time");
        self
    }


    pub fn select_update_time(mut self) -> Self {
        self.query = self.query.project("update_time");
        self
    }

    pub fn project_update_time(self) -> Self {
        self.select_update_time()
    }

    pub fn select_update_time_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_update_time_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_update_time_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("update_time", raw_sql_segment));
        self
    }

    pub fn group_by_update_time(self) -> Self {
        self.group_by("update_time")
    }

    pub fn group_by_update_time_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("update_time");
        request.query = request
            .query
            .project_expr(alias, Expr::column("update_time"));
        request
    }

    pub fn group_by_update_time_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("update_time")
            .aggregate_with_function("update_time", alias, function)
    }

    pub fn count_update_time(self) -> Self {
        self.count_update_time_as("update_time_count")
    }

    pub fn count_update_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("update_time", alias)
    }

    pub fn sum_update_time(self) -> Self {
        self.sum_update_time_as("sum_update_time")
    }

    pub fn sum_update_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("update_time", alias)
    }

    pub fn avg_update_time(self) -> Self {
        self.avg_update_time_as("avg_update_time")
    }

    pub fn avg_update_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("update_time", alias)
    }

    pub fn min_update_time(self) -> Self {
        self.min_update_time_as("min_update_time")
    }

    pub fn min_update_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("update_time", alias)
    }

    pub fn max_update_time(self) -> Self {
        self.max_update_time_as("max_update_time")
    }

    pub fn max_update_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("update_time", alias)
    }

    pub fn unselect_update_time(mut self) -> Self {
        self.query.projection.retain(|field| field != "update_time");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "update_time");
        self
    }


    pub fn with_update_time(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "update_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn create_update_time_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "update_time",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_update_time_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("update_time", value));
        self
    }



    pub fn with_update_time_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("update_time", value));
        self
    }

    pub fn with_update_time_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("update_time", value));
        self
    }

    pub fn with_update_time_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("update_time", value));
        self
    }

    pub fn with_update_time_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("update_time", value));
        self
    }

    pub fn with_update_time_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("update_time", value));
        self
    }

    pub fn with_update_time_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("update_time", lower, upper));
        self
    }

    pub fn with_update_time_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "update_time",
            range.start,
            range.end,
        ));
        self
    }

    pub fn with_update_time_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "update_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_update_time_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "update_time",
            values.into_iter().map(Into::into),
        ));
        self
    }

    pub fn with_update_time_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("update_time", value));
        self
    }

    pub fn with_update_time_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("update_time", value));
        self
    }

    pub fn with_update_time_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("update_time"));
        self
    }



    pub fn with_update_time_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("update_time"));
        self
    }


    pub fn order_by_update_time_asc(mut self) -> Self {
        self.query = self.query.order_asc("update_time");
        self
    }

    pub fn order_by_update_time_desc(mut self) -> Self {
        self.query = self.query.order_desc("update_time");
        self
    }

    pub fn order_by_update_time_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("update_time");
        self
    }

    pub fn order_by_update_time_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("update_time");
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
    pub fn filter_by_type_platform(mut self, value: impl EntityReference) -> Self {
        self.query = self.query.and_filter(Expr::eq("type_platform_id", value.entity_id_value()));
        self
    }

    pub fn with_type_platform_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "type_platform_id",
            <crate::TypePlatform as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.query.clone(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("type_platform", selection));
        self
    }


    pub fn without_type_platform_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "type_platform_id",
            <crate::TypePlatform as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.query.clone(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("type_platform", selection));
        self
    }


    pub fn have_type_platform(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("type_platform_id"));
        self
    }

    pub fn have_no_type_platform(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("type_platform_id"));
        self
    }


    pub fn group_by_type_platform(self) -> Self {
        self.group_by("type_platform_id")
    }

    pub fn group_by_type_platform_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let mut request = self.group_by("type_platform_id");
        request.query = request
            .query
            .project_expr(alias, Expr::column("type_platform_id"));
        request
    }

    pub fn group_by_type_platform_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.group_by("type_platform_id")
            .aggregate_with_function("type_platform_id", alias, function)
    }

    pub fn group_by_type_platform_with(mut self, request: impl Into<QuerySelection>) -> Self {
        self.query = self.query.group_by("type_platform_id");
        self.query_options.object_group_bys.push(ObjectGroupBy::new(
            "type_platform",
            "type_platform_id",
            request,
        ));
        self
    }

    pub fn group_by_type_platform_with_details(self) -> Self {
        self.group_by_type_platform_with_details_from(crate::Q::type_platforms().unlimited())
    }

    pub fn group_by_type_platform_with_details_from(self, request: impl Into<QuerySelection>) -> Self {
        self.group_by_type_platform_with(request)
    }


    pub fn roll_up_to_type_platform(self) -> Self {
        self.roll_up_to_type_platform_with(crate::Q::type_platforms().unlimited())
    }

    pub fn roll_up_to_type_platform_with(self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.with_type_platform_matching(selection.clone())
            .group_by_type_platform_with(selection)
    }

    pub fn count_type_platform(self) -> Self {
        self.count_type_platform_as("type_platform_count")
    }

    pub fn count_type_platform_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("type_platform_id", alias)
    }

    pub fn unselect_type_platform(mut self) -> Self {
        self.query.projection.retain(|field| field != "type_platform_id");
        self.query.relations.retain(|relation| relation.name != "type_platform");
        self
    }
    pub fn select_type_platform(mut self) -> Self {
        self.query.relations.push(teaql_core::RelationLoad::new("type_platform")
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_type_platform").with_relation("type_platform")));
        self
    }

    pub fn select_type_platform_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query.relations.push(teaql_core::RelationLoad::with_query("type_platform", selection.into_query())
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_type_platform_with").with_relation("type_platform")));
        self
}

    pub fn facet_by_type_platform_as(self, facet_name: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        self.facet_by_type_platform_internal(facet_name, request, true, "facet_by_type_platform_as")
    }

    pub fn facet_by_type_platform_as_with_options(
        self,
        facet_name: impl Into<String>,
        request: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        self.facet_by_type_platform_internal(facet_name, request, include_all_facets, "facet_by_type_platform_as_with_options")
    }

    fn facet_by_type_platform_internal(mut self, facet_name: impl Into<String>, request: impl Into<QuerySelection>, include_all_facets: bool, method: &'static str) -> Self {
        let facet_name = facet_name.into();
        let operation = QueryOperation::new(QueryOperationKind::Facet, method)
            .with_alias(&facet_name).with_relation("type_platform");
        self.query_options.facets.push(FacetRequest::new(
            facet_name,
            "type_platform",
            request,
            include_all_facets,
        ).with_operation(operation));
        self
    }
}

impl<R> Default for ScalarTypeProbeRequest<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> From< ScalarTypeProbeRequest<R> > for SelectQuery {
    fn from(request: ScalarTypeProbeRequest<R>) -> Self {
        QuerySelection::from(request).into_query()
    }
}

impl<R> From< ScalarTypeProbeRequest<R> > for QuerySelection {
    fn from(request: ScalarTypeProbeRequest<R>) -> Self {
        Self {
            query: request.query,
            relation_selections: request.relation_selections,
            relation_filters: request.relation_filters,
            child_enhancements: request.child_enhancements,
            query_options: request.query_options,
        }
    }
}


impl<'a, C> crate::request_support::AuditedSave<'a, C> for teaql_core::Audited<crate::ScalarTypeProbe> 
where C: crate::TeaqlRuntime + ?Sized + 'a
{
    type Error = teaql_runtime::RuntimeError;
    type Entity = crate::ScalarTypeProbe;
    fn save(self, context: &'a C) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Entity, Self::Error>> + Send + '_>> {
        Box::pin(async move {
            context.save_audited_entity(self).await
        })
    }
}

impl<R: teaql_core::Entity> crate::PurposedQuery<ScalarTypeProbeRequest<R>> {
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.inner.query_options.comment = Some(comment.into());
        self
    }

    pub fn new_entity<C>(&self, context: &C) -> crate::ScalarTypeProbe
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.require_comment();
        let mut entity = crate::ScalarTypeProbe::runtime_new(context.user_context().entity_runtime_state());
        if let Ok(id) = context.user_context().next_id(crate::ScalarTypeProbe::ENTITY_NAME) {
            entity.update_id(id);
        }
        teaql_core::Entity::mark_as_new(&mut entity);
        entity
    }

    fn into_inner_with_trace(mut self) -> ScalarTypeProbeRequest<R> {
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

    pub async fn execute_for_exists<'a, C>(
        self,
        context: &'a C,
    ) -> Result<bool, teaql_runtime::RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_exists(context).await
    }

    pub async fn execute_for_list<'a, C>(self, context: &'a C) -> Result<teaql_core::SmartList<R>, teaql_runtime::RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        self.into_inner_with_trace()._execute_for_list(context).await
    }

    pub async fn execute_for_rows<'a, C>(self, context: &'a C) -> Result<teaql_core::SmartList<teaql_core::CompactRow>, teaql_runtime::RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_rows(context).await
    }

    /// Execute query as a lazy entity stream without materializing the result set.
    /// Set chunk size via .stream(chunk_size) or .stream_default() on the query.
    pub async fn execute_for_stream<'a, C>(self, context: &'a C) -> Result<crate::request_support::TeaqlEntityStream<'a, R, teaql_runtime::RuntimeError>, teaql_runtime::RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        self.into_inner_with_trace()._execute_for_stream(context).await
    }

    pub async fn execute_for_first<'a, C>(self, context: &'a C) -> Result<Option<R>, teaql_runtime::RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        self.into_inner_with_trace()._execute_for_first(context).await
    }

    pub async fn execute_for_one<'a, C>(self, context: &'a C) -> Result<Option<R>, teaql_runtime::RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        self.into_inner_with_trace()._execute_for_one(context).await
    }


    pub async fn execute_for_count<'a, C>(self, context: &'a C) -> Result<u64, teaql_runtime::RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.into_inner_with_trace()._execute_for_count(context).await
    }
}
