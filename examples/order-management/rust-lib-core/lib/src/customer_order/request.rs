use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use teaql_core::{Aggregate, AggregateFunction, EntityDescriptor, Expr, SelectQuery, SmartList};
use teaql_runtime::RuntimeError;

use crate::request_support::*;

impl EntityReference for crate::CustomerOrder {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(&self)
    }
}

impl EntityReference for &crate::CustomerOrder {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(self)
    }
}

// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/customer_order
#[derive(Debug)]
pub struct CustomerOrderRequest<R = crate::CustomerOrder> {
    query: SelectQuery,
    relation_selections: Vec<RelationSelection>,
    relation_filters: Vec<RelationFilter>,
    child_enhancements: Vec<QuerySelection>,
    query_options: QueryOptions,
    roll_up_links: Vec<RollupLink>,
    roll_up_publication: Option<RollupTarget>,
    marker: PhantomData<R>,
}

impl<R> Clone for CustomerOrderRequest<R> {
    fn clone(&self) -> Self {
        if !self.roll_up_links.is_empty() {
            let snapshot = self.__resolved_roll_up_selection().expect("invalid roll-up configuration");
            return Self {
                query: snapshot.query,
                relation_selections: snapshot.relation_selections,
                relation_filters: snapshot.relation_filters,
                child_enhancements: snapshot.child_enhancements,
                query_options: snapshot.query_options,
                roll_up_links: Vec::new(),
                roll_up_publication: None,
                marker: PhantomData,
            };
        }
        Self {
            query: self.query.clone(),
            relation_selections: self.relation_selections.clone(),
            relation_filters: self.relation_filters.clone(),
            child_enhancements: self.child_enhancements.clone(),
            query_options: self.query_options.clone(),
            roll_up_links: Vec::new(),
            roll_up_publication: None,
            marker: PhantomData,
        }
    }
}

impl<R> CustomerOrderRequest<R> {
    pub(crate) fn new() -> Self {
        Self {
            query: SelectQuery::new("CustomerOrder")
                .project("id")
                .project("version"),
            relation_selections: Vec::new(),
            relation_filters: Vec::new(),
            child_enhancements: Vec::new(),
            query_options: QueryOptions::default(),
            roll_up_links: Vec::new(),
            roll_up_publication: None,
            marker: PhantomData,
        }
    }

    pub fn return_type<T>(self) -> CustomerOrderRequest<T> {
        CustomerOrderRequest {
            query: self.query,
            relation_selections: self.relation_selections,
            relation_filters: self.relation_filters,
            child_enhancements: self.child_enhancements,
            query_options: self.query_options,
            roll_up_links: self.roll_up_links,
            roll_up_publication: self.roll_up_publication,
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
        QuerySelection::from(self).into_query()
    }

    fn __base_roll_up_selection(&self) -> QuerySelection {
        QuerySelection {
            query: self.query.clone(),
            relation_selections: self.relation_selections.clone(),
            relation_filters: self.relation_filters.clone(),
            child_enhancements: self.child_enhancements.clone(),
            query_options: self.query_options.clone(),
        }
    }

    fn __resolved_roll_up_selection(&self) -> Result<QuerySelection, RuntimeError> {
        resolve_rollup_links(self.__base_roll_up_selection(), &self.roll_up_links)
    }

    fn __publish_roll_up(&self) {
        if let Some(target) = &self.roll_up_publication {
            *target.lock().expect("poisoned roll-up configuration") = RollupState {
                selection: self.__base_roll_up_selection(),
                links: self.roll_up_links.clone(),
            };
        }
    }

    fn __synced(self) -> Self {
        self.__publish_roll_up();
        self
    }

    pub(crate) fn __roll_up_target(&mut self) -> RollupTarget {
        if self.roll_up_publication.is_none() {
            self.roll_up_publication = Some(std::sync::Arc::new(std::sync::Mutex::new(RollupState {
                selection: self.__base_roll_up_selection(), links: self.roll_up_links.clone(),
            })));
        }
        self.__publish_roll_up();
        self.roll_up_publication.as_ref().unwrap().clone()
    }

    fn __resolve_roll_ups(&mut self) -> Result<(), RuntimeError> {
        if self.roll_up_links.is_empty() { return Ok(()); }
        let selection = self.__resolved_roll_up_selection()?;
        self.query = selection.query;
        self.relation_selections = selection.relation_selections;
        self.relation_filters = selection.relation_filters;
        self.child_enhancements = selection.child_enhancements;
        self.query_options = selection.query_options;
        self.roll_up_links.clear();
        Ok(())
    }


    pub fn purpose(self, purpose: impl Into<String>) -> crate::PurposedQuery<Self> {
        crate::PurposedQuery::new(self, purpose)
    }

    pub(crate) async fn _execute_for_list<'a, C>(
        mut self,
        context: &'a C,
    ) -> Result<SmartList<R>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        self.__resolve_roll_ups()?;
        let query_options = self.query_options.clone();
        let relation_aggregates = runtime_relation_aggregates(&query_options);
        let mut query = authorize_query(apply_runtime_metadata(
            self.query,
            &query_options,
            &self.child_enhancements,
        ))?;
        let requested_count = query.take_requested_list_count(&query_options);
        let use_id_set = query.as_query().id_set_pagination.is_some();
        context.user_context().observe_generated_list("CustomerOrder", async {
        let mut total = None;
        if !use_id_set {
            if let Some((alias, count_query)) = &requested_count {
                let count_rows = context.fetch_compact_rows("CustomerOrder", count_query).await?;
                total = Some(exact_list_count(&count_rows, alias)?);
            }
        }
        if !query.as_query().group_by.is_empty() || !query.as_query().object_group_bys.is_empty() {
            return Err(RuntimeError::Graph("grouped projections require execute_for_rows".into()));
        }
        let mut rows = if !query.as_query().aggregates.is_empty() {
            let records = context.fetch_compact_rows("CustomerOrder", &query).await?;
            let mut rows = SmartList::new(Vec::new());
            if let Some(record) = records.into_iter().next() { rows.aggregations = record.into_map(); }
            rows
        } else {
            context.fetch_entity_smart_list::<R>(
                "CustomerOrder", query.clone(), relation_aggregates,
            ).await?
        };
        let facets = if query_options.facets.is_empty() { std::collections::BTreeMap::new() }
            else { execute_facets(context, query.as_query(), &query_options).await? };
        if let Some((alias, count_query)) = requested_count {
            if total.is_none() { total = rows.total_count; }
            if total.is_none() {
                let count_rows = context.fetch_compact_rows("CustomerOrder", &count_query).await?;
                total = Some(exact_list_count(&count_rows, &alias)?);
            }
            rows.total_count = total;
            rows.aggregations.insert(alias, teaql_core::Value::U64(total.expect("requested count resolved")));
        }
        attach_facets(&mut rows, facets);
        Ok(rows)
        }).await
    }

    pub(crate) async fn _execute_for_rows<'a, C>(
        mut self,
        context: &'a C,
    ) -> Result<SmartList<teaql_core::CompactRow>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
    {
        self.__resolve_roll_ups()?;
        let query = authorize_query(apply_runtime_metadata(
            self.query,
            &self.query_options,
            &self.child_enhancements,
        ))?.with_facet_diagnostics(&self.query_options);
        context.fetch_compact_smart_list("CustomerOrder", &query).await
    }

    pub(crate) async fn _execute_for_stream<'a, C>(
        mut self,
        context: &'a C,
    ) -> Result<TeaqlEntityStream<'a, R, RuntimeError>, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
        R: teaql_core::Entity + Send + 'a,
    {
        self.__resolve_roll_ups()?;
        let query = authorize_query(apply_runtime_metadata(
            self.query,
            &self.query_options,
            &self.child_enhancements,
        ))?;
        let shape = query.as_query();
        if !shape.aggregates.is_empty() || !shape.group_by.is_empty() || !shape.object_group_bys.is_empty() {
            return Err(RuntimeError::Graph("Entity streams require entity rows; use a record or aggregation query".to_owned()));
        }
        Ok(context.user_context().observe_generated_stream(
            "CustomerOrder", context.fetch_entity_stream("CustomerOrder", query),
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
        mut self,
        context: &'a C,
    ) -> Result<u64, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
    {
        self.__resolve_roll_ups()?;
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
        let rows = context.fetch_compact_rows("CustomerOrder", &query).await?;
        rows.first()
            .and_then(|row| row.get(&alias))
            .and_then(teaql_core::Value::try_u64)
            .ok_or_else(|| RuntimeError::Graph(format!("count result for CustomerOrder is missing or not numeric")))
    }

    pub(crate) async fn _execute_for_exists<'a, C>(
        mut self,
        context: &'a C,
    ) -> Result<bool, RuntimeError>
    where
        C: TeaqlRuntime + ?Sized,
    {
        self.__resolve_roll_ups()?;
        let mut query = apply_runtime_metadata(
            self.query,
            &self.query_options,
            &self.child_enhancements,
        ).limit(1);
        query.relations.clear();
        let query = authorize_query(query)?.with_facet_diagnostics(&self.query_options);
        let rows = context.fetch_compact_rows("CustomerOrder", &query).await?;
        Ok(!rows.is_empty())
    }

    pub fn search_with_text(mut self, text: impl Into<String>) -> Self {
        self.query = self.query.search_with_text(text);
        self.__synced()
    }

    pub fn filter(mut self, filter: Expr) -> Self {
        self.query = self.query.filter(filter);
        self.__synced()
    }

    pub fn and_filter(mut self, filter: Expr) -> Self {
        self.query = self.query.and_filter(filter);
        self.__synced()
    }

    pub fn or_filter(mut self, filter: Expr) -> Self {
        self.query = self.query.or_filter(filter);
        self.__synced()
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
        self.__synced()
    }

    pub fn with_deleted_rows(mut self) -> Self {
        self.query.filter = remove_default_live_filter(self.query.filter);
        self.__synced()
    }

    pub fn deleted_rows_only(mut self) -> Self {
        self.query.filter = remove_default_live_filter(self.query.filter);
        self.query = self.query.and_filter(Expr::lte("version", 0_i64));
        self.__synced()
    }

    pub fn match_types(
        mut self,
        types: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(TYPE_FIELD, types.into_iter().map(Into::into)));
        self.__synced()
    }


    pub fn with_type_group(mut self) -> Self {
        self.query = self.query.project(TYPE_GROUP_FIELD);
        self.__synced()
    }

    pub fn matching_any_of<O>(mut self, request: CustomerOrderRequest<O>) -> Self {
        let filter = QuerySelection::from(request).into_query().filter;
        if let Some(criteria) = crate::request_support::criteria_alternatives_excluding_version(filter) {
            self.query = self.query.and_filter(criteria);
        }
        self.__synced()
    }

    pub fn match_any_of<O>(self, request: CustomerOrderRequest<O>) -> Self {
        self.matching_any_of(request)
    }

    pub fn matching_ids_of<O>(mut self, request: CustomerOrderRequest<O>) -> Self {
        let entity = <crate::CustomerOrder as teaql_core::TeaqlEntity>::entity_descriptor();
        self.query = self.query.and_filter(Expr::in_subquery("id", entity, QuerySelection::from(request).into_query(), "id"));
        self.__synced()
    }

    pub fn enhance_child(mut self, request: impl Into<QuerySelection>) -> Self {
        self.child_enhancements.push(request.into());
        self.__synced()
    }

    pub fn enhance_children_if_needed(self) -> Self {
        let request = self;
        request
    }


    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.query_options.comment = Some(comment.into());
        self.__synced()
    }

    pub fn raw_sql(self, raw_sql: impl Into<String>) -> Self {
        self.unsafe_raw_sql(UnsafeRawSqlSegment::trusted(raw_sql))
    }

    pub fn unsafe_raw_sql(mut self, raw_sql: UnsafeRawSqlSegment) -> Self {
        self.query_options.raw_sql = Some(raw_sql.into_sql());
        self.__synced()
    }

    pub fn raw_sql_filter(self, raw_sql: impl Into<String>) -> Self {
        self.unsafe_raw_sql_filter(UnsafeRawSqlSegment::trusted(raw_sql))
    }

    pub fn unsafe_raw_sql_filter(mut self, raw_sql: UnsafeRawSqlSegment) -> Self {
        self.query_options.raw_sql_search_criteria.push(raw_sql.into_sql());
        self.__synced()
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

        self.__synced()
    }

    pub(crate) fn apply_dynamic_json_filter(self, field: &str, value: &JsonValue) -> Self {
        if let Some((head, tail)) = field.split_once('.') {
            self.apply_dynamic_json_chain_filter(head, tail, value)
        } else if let Some(storage_field) = Self::dynamic_json_self_field(field) {
            self.and_filter(dynamic_json_filter_expr(storage_field, value))
        } else {
            self.__synced()
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
        self.__synced()
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
        self.__synced()
    }

    fn dynamic_json_self_field(field: &str) -> Option<&'static str> {
        match field {
            "id" => Some("id"),
            "order_number" => Some("order_number"),
            "order_date" => Some("order_date"),
            "total_amount" => Some("total_amount"),
            "create_time" => Some("create_time"),
            "update_time" => Some("update_time"),
            "version" => Some("version"),
            "status" | "status_id" => Some("status_id"),
            "customer" | "customer_id" => Some("customer_id"),
            "commerce_platform" | "commerce_platform_id" => Some("commerce_platform_id"),
            _ => None,
        }
    }

    fn apply_dynamic_json_chain_filter(self, head: &str, tail: &str, value: &JsonValue) -> Self {
        let _ = (tail, value);
        match head {
            "status" => {
                self.with_status_matching(
                    crate::Q::order_statuses_minimal()
                        .apply_dynamic_json_filter(tail, value),
                )
            }
            "customer" => {
                self.with_customer_matching(
                    crate::Q::customers_minimal()
                        .apply_dynamic_json_filter(tail, value),
                )
            }
            "commerce_platform" => {
                self.with_commerce_platform_matching(
                    crate::Q::commerce_platforms_minimal()
                        .apply_dynamic_json_filter(tail, value),
                )
            }
            "order_line_list" => {
                self.with_order_line_list_matching(
                    crate::Q::order_lines_minimal()
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
        self.__synced()
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.query = self.query.limit(limit);
        self.__synced()
    }

    pub fn stream(mut self, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "stream chunk size must be positive");
        self.query = self.query.stream(chunk_size);
        self.__synced()
    }

    pub fn stream_default(mut self) -> Self {
        self.query = self.query.stream_default();
        self.__synced()
    }

    pub fn skip(mut self, offset: u64) -> Self {
        self.query = self.query.offset(offset);
        self.__synced()
    }

    pub fn offset_only(self, offset: u64) -> Self {
        self.skip(offset)
    }

    pub fn offset(self, offset: u64, size: u64) -> Self {
        self.page_offset(offset, size)
    }

    pub fn page_offset(mut self, offset: u64, limit: u64) -> Self {
        self.query = self.query.page(offset, limit);
        self.__synced()
    }

    pub fn optimize_for_continuous_page_fetch(mut self) -> Self {
        self.query = self.query.optimize_for_continuous_page_fetch();
        self.__synced()
    }

    pub fn optimize_for_continuous_page_fetch_with(
        mut self,
        namespace: impl Into<String>,
        ttl_seconds: u64,
    ) -> Self {
        self.query = self
            .query
            .optimize_for_continuous_page_fetch_with(namespace, ttl_seconds);
        self.__synced()
    }

    pub fn optimize_pagination_with_id_set(mut self) -> Self {
        self.query = self.query.optimize_pagination_with_id_set();
        self.__synced()
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
        self.__synced()
    }

    /// Select bounded indexed probes for a per-parent Top-N relation only
    /// when the already-loaded parent count is at or below `threshold`.
    /// Passing zero explicitly selects the provider window plan.
    pub fn top_n_probe_parent_threshold(mut self, threshold: usize) -> Self {
        self.query = self.query.top_n_probe_parent_threshold(threshold);
        self.__synced()
    }

    pub fn top(self, top_n: u64) -> Self {
        self.limit(top_n)
    }

    pub fn offset_size(self, offset: u64, size: u64) -> Self {
        self.offset(offset, size)
    }

    pub fn unlimited(mut self) -> Self {
        self.query.slice = None;
        self.__synced()
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
        self.query = self.query.project("order_number");
        self.query = self.query.project("order_date");
        self.query = self.query.project("total_amount");
        self.query = self.query.project("create_time");
        self.query = self.query.project("update_time");
        self.query = self.query.project("version");
        self.query = self.query.project("status_id");
        self.query = self.query.project("customer_id");
        self.query = self.query.project("commerce_platform_id");
        self.__synced()
    }

    pub fn select_dynamic_fields_with(mut self, selection: teaql_core::dynamic_fields::DynamicFieldSelection) -> Self {
        self.query = self.query.select_dynamic_fields(selection);
        self.__synced()
    }

    pub fn select_self_fields(self) -> Self {
        self.select_self()
    }

    pub fn select_self_without_parent(self) -> Self {
        self.select_self_fields()
    }

    pub fn select_all(self) -> Self {
        let mut request = self.select_self();
        request = request.select_status();
        request = request.select_customer();
        request = request.select_commerce_platform();
        request
    }

    pub fn select_children(self) -> Self {
        let mut request = self.select_all();
        request = request.select_order_line_list();
        request
    }

    pub fn select_any(self) -> Self {
        self.select_children()
    }

    pub fn group_by(mut self, field: impl Into<String>) -> Self {
        self.query = self.query.group_by(field);
        self.__synced()
    }

    /// Order a selected statistical alias or nested result path, matching Java.
    pub fn add_order_by_ascending(mut self, result_key: impl Into<String>) -> Self {
        self.query = self.query.order_asc(result_key);
        self.__synced()
    }

    pub fn add_order_by_descending(mut self, result_key: impl Into<String>) -> Self {
        self.query = self.query.order_desc(result_key);
        self.__synced()
    }

    pub fn add_order_by_ascending_using_gbk(mut self, result_key: impl Into<String>) -> Self {
        self.query = self.query.order_gbk_asc(result_key);
        self.__synced()
    }

    pub fn add_order_by_descending_using_gbk(mut self, result_key: impl Into<String>) -> Self {
        self.query = self.query.order_gbk_desc(result_key);
        self.__synced()
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
        self.__synced()
    }
    /// Java addAggregate(SimpleNamedExpression): bind a typed aggregate AST.
    pub fn add_aggregate(mut self, aggregate: teaql_core::NamedExpr) -> Self {
        self.query = self.query.aggregate_expression(aggregate);
        self.__synced()
    }

    /// Filter selected statistical results. Repeated conditions compose with AND.
    pub fn having(mut self, condition: Expr) -> Self {
        let condition = match self.query.having.take() {
            Some(previous) => Expr::and([previous, condition]),
            None => condition,
        };
        self.query = self.query.having(condition);
        self.__synced()
    }

    pub fn aggregate_count_field(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.count_field(field, alias);
        self.__synced()
    }

    pub fn aggregate_with_function(
        mut self,
        field: impl Into<String>,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.query = self.query.aggregate(Aggregate::new(function, field, alias));
        self.__synced()
    }

    pub fn aggregate_sum(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.sum(field, alias);
        self.__synced()
    }

    pub fn aggregate_avg(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.avg(field, alias);
        self.__synced()
    }

    pub fn aggregate_min(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.min(field, alias);
        self.__synced()
    }

    pub fn aggregate_max(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.max(field, alias);
        self.__synced()
    }

    pub fn aggregate_stddev(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.stddev(field, alias);
        self.__synced()
    }

    pub fn aggregate_stddev_pop(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.stddev_pop(field, alias);
        self.__synced()
    }

    pub fn aggregate_var_samp(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.var_samp(field, alias);
        self.__synced()
    }

    pub fn aggregate_var_pop(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.var_pop(field, alias);
        self.__synced()
    }

    pub fn aggregate_bit_and(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.bit_and(field, alias);
        self.__synced()
    }

    pub fn aggregate_bit_or(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.bit_or(field, alias);
        self.__synced()
    }

    pub fn aggregate_bit_xor(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.query = self.query.bit_xor(field, alias);
        self.__synced()
    }

    pub fn enable_aggregation_cache(mut self) -> Self {
        self.query = self.query.enable_aggregation_cache();
        self.__synced()
    }

    pub fn enable_aggregation_cache_for(mut self, cache_expired_millis: u64) -> Self {
        self.query = self.query.enable_aggregation_cache_for(cache_expired_millis);
        self.__synced()
    }

    pub fn propagate_aggregation_cache(mut self, cache_expired_millis: u64) -> Self {
        self.query = self.query.propagate_aggregation_cache(cache_expired_millis);
        self.__synced()
    }

    pub fn group_by_id(self) -> Self {
        self.group_by("id")
    }

    pub fn group_by_id_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("id"));
        request.__synced()
    }

    pub fn group_by_id_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("id")]));
        request.__synced()
    }

    pub fn count_id(self) -> Self {
        self.count_id_as("id_count")
    }

    pub fn count_id_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("id", alias)
    }
    pub fn count_distinct_id(self) -> Self {
        self.count_distinct_id_as("id_distinct_count")
    }

    pub fn count_distinct_id_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("id", alias, AggregateFunction::CountDistinct)
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
        self.__synced()
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
        self.__synced()
    }



    pub fn with_id_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("id", value));
        self.__synced()
    }

    pub fn with_id_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "id",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_id_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "id",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn order_by_id_asc(mut self) -> Self {
        self.query = self.query.order_asc("id");
        self.__synced()
    }

    pub fn order_by_id_desc(mut self) -> Self {
        self.query = self.query.order_desc("id");
        self.__synced()
    }

    pub fn order_by_id_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("id");
        self.__synced()
    }

    pub fn order_by_id_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("id");
        self.__synced()
    }


    pub fn select_order_number(mut self) -> Self {
        self.query = self.query.project("order_number");
        self.__synced()
    }

    pub fn project_order_number(self) -> Self {
        self.select_order_number()
    }

    pub fn select_order_number_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_order_number_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_order_number_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("order_number", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_order_number(self) -> Self {
        self.group_by("order_number")
    }

    pub fn group_by_order_number_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("order_number"));
        request.__synced()
    }

    pub fn group_by_order_number_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("order_number")]));
        request.__synced()
    }

    pub fn count_order_number(self) -> Self {
        self.count_order_number_as("order_number_count")
    }

    pub fn count_order_number_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("order_number", alias)
    }
    pub fn count_distinct_order_number(self) -> Self {
        self.count_distinct_order_number_as("order_number_distinct_count")
    }

    pub fn count_distinct_order_number_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("order_number", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_order_number(self) -> Self {
        self.sum_order_number_as("sum_order_number")
    }

    pub fn sum_order_number_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("order_number", alias)
    }

    pub fn avg_order_number(self) -> Self {
        self.avg_order_number_as("avg_order_number")
    }

    pub fn avg_order_number_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("order_number", alias)
    }

    pub fn min_order_number(self) -> Self {
        self.min_order_number_as("min_order_number")
    }

    pub fn min_order_number_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("order_number", alias)
    }

    pub fn max_order_number(self) -> Self {
        self.max_order_number_as("max_order_number")
    }

    pub fn max_order_number_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("order_number", alias)
    }

    pub fn unselect_order_number(mut self) -> Self {
        self.query.projection.retain(|field| field != "order_number");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "order_number");
        self.__synced()
    }


    pub fn with_order_number(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "order_number",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_order_number_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "order_number",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_order_number_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("order_number", value));
        self.__synced()
    }



    pub fn with_order_number_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("order_number", lower, upper));
        self.__synced()
    }

    pub fn with_order_number_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "order_number",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_order_number_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "order_number",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_order_number_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "order_number",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_order_number_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("order_number", value));
        self.__synced()
    }
    pub fn with_order_number_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("order_number", value));
        self.__synced()
    }

    pub fn with_order_number_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("order_number"));
        self.__synced()
    }



    pub fn with_order_number_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("order_number"));
        self.__synced()
    }


    pub fn order_by_order_number_asc(mut self) -> Self {
        self.query = self.query.order_asc("order_number");
        self.__synced()
    }

    pub fn order_by_order_number_desc(mut self) -> Self {
        self.query = self.query.order_desc("order_number");
        self.__synced()
    }

    pub fn order_by_order_number_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("order_number");
        self.__synced()
    }

    pub fn order_by_order_number_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("order_number");
        self.__synced()
    }


    pub fn select_order_date(mut self) -> Self {
        self.query = self.query.project("order_date");
        self.__synced()
    }

    pub fn project_order_date(self) -> Self {
        self.select_order_date()
    }

    pub fn select_order_date_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_order_date_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_order_date_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("order_date", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_order_date(self) -> Self {
        self.group_by("order_date")
    }

    pub fn group_by_order_date_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("order_date"));
        request.__synced()
    }

    pub fn group_by_order_date_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("order_date")]));
        request.__synced()
    }

    pub fn count_order_date(self) -> Self {
        self.count_order_date_as("order_date_count")
    }

    pub fn count_order_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("order_date", alias)
    }
    pub fn count_distinct_order_date(self) -> Self {
        self.count_distinct_order_date_as("order_date_distinct_count")
    }

    pub fn count_distinct_order_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("order_date", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_order_date(self) -> Self {
        self.sum_order_date_as("sum_order_date")
    }

    pub fn sum_order_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("order_date", alias)
    }

    pub fn avg_order_date(self) -> Self {
        self.avg_order_date_as("avg_order_date")
    }

    pub fn avg_order_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("order_date", alias)
    }

    pub fn min_order_date(self) -> Self {
        self.min_order_date_as("min_order_date")
    }

    pub fn min_order_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("order_date", alias)
    }

    pub fn max_order_date(self) -> Self {
        self.max_order_date_as("max_order_date")
    }

    pub fn max_order_date_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("order_date", alias)
    }

    pub fn unselect_order_date(mut self) -> Self {
        self.query.projection.retain(|field| field != "order_date");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "order_date");
        self.__synced()
    }


    pub fn with_order_date(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "order_date",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_order_date_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "order_date",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_order_date_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("order_date", value));
        self.__synced()
    }



    pub fn with_order_date_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("order_date", value));
        self.__synced()
    }

    pub fn with_order_date_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("order_date", value));
        self.__synced()
    }

    pub fn with_order_date_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("order_date", value));
        self.__synced()
    }

    pub fn with_order_date_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("order_date", value));
        self.__synced()
    }

    pub fn with_order_date_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("order_date", value));
        self.__synced()
    }

    pub fn with_order_date_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("order_date", lower, upper));
        self.__synced()
    }

    pub fn with_order_date_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "order_date",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_order_date_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "order_date",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_order_date_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "order_date",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_order_date_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("order_date", value));
        self.__synced()
    }

    pub fn with_order_date_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("order_date", value));
        self.__synced()
    }

    pub fn with_order_date_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("order_date"));
        self.__synced()
    }



    pub fn with_order_date_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("order_date"));
        self.__synced()
    }


    pub fn order_by_order_date_asc(mut self) -> Self {
        self.query = self.query.order_asc("order_date");
        self.__synced()
    }

    pub fn order_by_order_date_desc(mut self) -> Self {
        self.query = self.query.order_desc("order_date");
        self.__synced()
    }

    pub fn order_by_order_date_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("order_date");
        self.__synced()
    }

    pub fn order_by_order_date_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("order_date");
        self.__synced()
    }


    pub fn select_total_amount(mut self) -> Self {
        self.query = self.query.project("total_amount");
        self.__synced()
    }

    pub fn project_total_amount(self) -> Self {
        self.select_total_amount()
    }

    pub fn select_total_amount_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_total_amount_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_total_amount_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("total_amount", raw_sql_segment));
        self.__synced()
    }

    pub fn select_total_amount_with_function(self, function: AggregateFunction) -> Self {
        self.select_total_amount_as_with_function("total_amount", function)
    }

    pub fn select_total_amount_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("total_amount", alias, function)
    }

    pub fn group_by_total_amount(self) -> Self {
        self.group_by("total_amount")
    }

    pub fn group_by_total_amount_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("total_amount"));
        request.__synced()
    }

    pub fn group_by_total_amount_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("total_amount")]));
        request.__synced()
    }

    pub fn count_total_amount(self) -> Self {
        self.count_total_amount_as("total_amount_count")
    }

    pub fn count_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("total_amount", alias)
    }
    pub fn count_distinct_total_amount(self) -> Self {
        self.count_distinct_total_amount_as("total_amount_distinct_count")
    }

    pub fn count_distinct_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("total_amount", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_total_amount(self) -> Self {
        self.sum_total_amount_as("sum_total_amount")
    }

    pub fn sum_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("total_amount", alias)
    }

    pub fn avg_total_amount(self) -> Self {
        self.avg_total_amount_as("avg_total_amount")
    }

    pub fn avg_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("total_amount", alias)
    }

    pub fn min_total_amount(self) -> Self {
        self.min_total_amount_as("min_total_amount")
    }

    pub fn min_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("total_amount", alias)
    }

    pub fn max_total_amount(self) -> Self {
        self.max_total_amount_as("max_total_amount")
    }

    pub fn max_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("total_amount", alias)
    }

    pub fn standard_deviation_total_amount(self) -> Self {
        self.standard_deviation_total_amount_as("stdDev_total_amount")
    }

    pub fn standard_deviation_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("total_amount", alias)
    }

    pub fn square_root_of_population_standard_deviation_total_amount(self) -> Self {
        self.square_root_of_population_standard_deviation_total_amount_as("stdDevPop_total_amount")
    }

    pub fn square_root_of_population_standard_deviation_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("total_amount", alias)
    }

    pub fn sample_variance_total_amount(self) -> Self {
        self.sample_variance_total_amount_as("varSamp_total_amount")
    }

    pub fn sample_variance_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("total_amount", alias)
    }

    pub fn sample_population_variance_total_amount(self) -> Self {
        self.sample_population_variance_total_amount_as("varPop_total_amount")
    }

    pub fn sample_population_variance_total_amount_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("total_amount", alias)
    }

    pub fn unselect_total_amount(mut self) -> Self {
        self.query.projection.retain(|field| field != "total_amount");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "total_amount");
        self.__synced()
    }


    pub fn with_total_amount(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "total_amount",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_total_amount_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "total_amount",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_total_amount_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("total_amount", value));
        self.__synced()
    }



    pub fn with_total_amount_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("total_amount", value));
        self.__synced()
    }

    pub fn with_total_amount_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("total_amount", value));
        self.__synced()
    }

    pub fn with_total_amount_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("total_amount", value));
        self.__synced()
    }

    pub fn with_total_amount_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("total_amount", value));
        self.__synced()
    }

    pub fn with_total_amount_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("total_amount", value));
        self.__synced()
    }

    pub fn with_total_amount_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("total_amount", lower, upper));
        self.__synced()
    }

    pub fn with_total_amount_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "total_amount",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_total_amount_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "total_amount",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_total_amount_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "total_amount",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_total_amount_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("total_amount", value));
        self.__synced()
    }

    pub fn with_total_amount_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("total_amount", value));
        self.__synced()
    }

    pub fn with_total_amount_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("total_amount"));
        self.__synced()
    }



    pub fn with_total_amount_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("total_amount"));
        self.__synced()
    }


    pub fn order_by_total_amount_asc(mut self) -> Self {
        self.query = self.query.order_asc("total_amount");
        self.__synced()
    }

    pub fn order_by_total_amount_desc(mut self) -> Self {
        self.query = self.query.order_desc("total_amount");
        self.__synced()
    }

    pub fn order_by_total_amount_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("total_amount");
        self.__synced()
    }

    pub fn order_by_total_amount_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("total_amount");
        self.__synced()
    }


    pub fn select_create_time(mut self) -> Self {
        self.query = self.query.project("create_time");
        self.__synced()
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
        self.__synced()
    }

    pub fn group_by_create_time(self) -> Self {
        self.group_by("create_time")
    }

    pub fn group_by_create_time_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("create_time"));
        request.__synced()
    }

    pub fn group_by_create_time_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("create_time")]));
        request.__synced()
    }

    pub fn count_create_time(self) -> Self {
        self.count_create_time_as("create_time_count")
    }

    pub fn count_create_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("create_time", alias)
    }
    pub fn count_distinct_create_time(self) -> Self {
        self.count_distinct_create_time_as("create_time_distinct_count")
    }

    pub fn count_distinct_create_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("create_time", alias, AggregateFunction::CountDistinct)
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
        self.__synced()
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
        self.__synced()
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
        self.__synced()
    }



    pub fn with_create_time_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("create_time", value));
        self.__synced()
    }

    pub fn with_create_time_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("create_time", value));
        self.__synced()
    }

    pub fn with_create_time_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("create_time", value));
        self.__synced()
    }

    pub fn with_create_time_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("create_time", value));
        self.__synced()
    }

    pub fn with_create_time_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("create_time", value));
        self.__synced()
    }

    pub fn with_create_time_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("create_time", lower, upper));
        self.__synced()
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
        self.__synced()
    }

    pub fn with_create_time_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "create_time",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_create_time_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "create_time",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_create_time_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("create_time", value));
        self.__synced()
    }

    pub fn with_create_time_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("create_time", value));
        self.__synced()
    }

    pub fn with_create_time_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("create_time"));
        self.__synced()
    }



    pub fn with_create_time_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("create_time"));
        self.__synced()
    }


    pub fn order_by_create_time_asc(mut self) -> Self {
        self.query = self.query.order_asc("create_time");
        self.__synced()
    }

    pub fn order_by_create_time_desc(mut self) -> Self {
        self.query = self.query.order_desc("create_time");
        self.__synced()
    }

    pub fn order_by_create_time_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("create_time");
        self.__synced()
    }

    pub fn order_by_create_time_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("create_time");
        self.__synced()
    }


    pub fn select_update_time(mut self) -> Self {
        self.query = self.query.project("update_time");
        self.__synced()
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
        self.__synced()
    }

    pub fn group_by_update_time(self) -> Self {
        self.group_by("update_time")
    }

    pub fn group_by_update_time_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("update_time"));
        request.__synced()
    }

    pub fn group_by_update_time_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("update_time")]));
        request.__synced()
    }

    pub fn count_update_time(self) -> Self {
        self.count_update_time_as("update_time_count")
    }

    pub fn count_update_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("update_time", alias)
    }
    pub fn count_distinct_update_time(self) -> Self {
        self.count_distinct_update_time_as("update_time_distinct_count")
    }

    pub fn count_distinct_update_time_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("update_time", alias, AggregateFunction::CountDistinct)
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
        self.__synced()
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
        self.__synced()
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
        self.__synced()
    }



    pub fn with_update_time_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("update_time", value));
        self.__synced()
    }

    pub fn with_update_time_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("update_time", value));
        self.__synced()
    }

    pub fn with_update_time_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("update_time", value));
        self.__synced()
    }

    pub fn with_update_time_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("update_time", value));
        self.__synced()
    }

    pub fn with_update_time_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("update_time", value));
        self.__synced()
    }

    pub fn with_update_time_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("update_time", lower, upper));
        self.__synced()
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
        self.__synced()
    }

    pub fn with_update_time_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "update_time",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_update_time_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "update_time",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_update_time_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("update_time", value));
        self.__synced()
    }

    pub fn with_update_time_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("update_time", value));
        self.__synced()
    }

    pub fn with_update_time_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("update_time"));
        self.__synced()
    }



    pub fn with_update_time_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("update_time"));
        self.__synced()
    }


    pub fn order_by_update_time_asc(mut self) -> Self {
        self.query = self.query.order_asc("update_time");
        self.__synced()
    }

    pub fn order_by_update_time_desc(mut self) -> Self {
        self.query = self.query.order_desc("update_time");
        self.__synced()
    }

    pub fn order_by_update_time_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("update_time");
        self.__synced()
    }

    pub fn order_by_update_time_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("update_time");
        self.__synced()
    }

    pub fn group_by_version(self) -> Self {
        self.group_by("version")
    }

    pub fn group_by_version_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("version"));
        request.__synced()
    }

    pub fn group_by_version_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("version")]));
        request.__synced()
    }

    pub fn count_version(self) -> Self {
        self.count_version_as("version_count")
    }

    pub fn count_version_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("version", alias)
    }
    pub fn count_distinct_version(self) -> Self {
        self.count_distinct_version_as("version_distinct_count")
    }

    pub fn count_distinct_version_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("version", alias, AggregateFunction::CountDistinct)
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
        self.__synced()
    }

    pub fn order_by_version_desc(mut self) -> Self {
        self.query = self.query.order_desc("version");
        self.__synced()
    }

    pub fn order_by_version_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("version");
        self.__synced()
    }

    pub fn order_by_version_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("version");
        self.__synced()
    }
    /// Please use `with_status_is` instead
    pub(crate) fn filter_by_status(mut self, value: impl EntityReference) -> Self {
        self.query = self.query.and_filter(Expr::eq("status_id", value.entity_id_value()));
        self.__synced()
    }
    /// Complex relation filter for `status`.
    ///
    /// **Usage Priority:**
    ///
    /// 1. **Preferred**: If you only want to filter by specific known constants, please **prefer** the generated semantic shortcut methods, such as:
    ///    - [`Self::with_status_is_xxx`]
    ///
    ///    This gives the best code readability.
    ///
    /// 2. **Advanced**: Only use this method when you need to perform advanced searches, dynamic subqueries, or filter based on complex relation conditions.
    ///
    /// # Example
    /// ```text
    /// // Only use when building dynamic queries
    /// let dynamic_query = crate::Q::order_statuses_minimal().filter(...);
    /// let request = crate::Q::customer_orders().with_status_matching(dynamic_query);
    /// ```
    pub fn with_status_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "status_id",
            <crate::OrderStatus as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("status", selection));
        self.__synced()
    }


    /// Complex relation filter for `status`.
    ///
    /// **Usage Priority:**
    ///
    /// 1. **Preferred**: If you only want to filter by specific known constants, please **prefer** the generated semantic shortcut methods, such as:
    ///    - [`Self::with_status_is_not_xxx`]
    ///
    ///    This gives the best code readability.
    ///
    /// 2. **Advanced**: Only use this method when you need to perform advanced searches, dynamic subqueries, or filter based on complex relation conditions.
    ///
    /// # Example
    /// ```text
    /// // Only use when building dynamic queries
    /// let dynamic_query = crate::Q::order_statuses_minimal().filter(...);
    /// let request = crate::Q::customer_orders().without_status_matching(dynamic_query);
    /// ```
    pub fn without_status_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "status_id",
            <crate::OrderStatus as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("status", selection));
        self.__synced()
    }


    pub fn have_status(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("status_id"));
        self.__synced()
    }

    pub fn have_no_status(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("status_id"));
        self.__synced()
    }

    pub fn order_by_status_asc(mut self) -> Self {
        self.query = self.query.order_asc("status_id");
        self.__synced()
    }

    pub fn order_by_status_desc(mut self) -> Self {
        self.query = self.query.order_desc("status_id");
        self.__synced()
    }


    pub fn group_by_status(self) -> Self {
        self.group_by("status_id")
    }

    pub fn group_by_status_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("status_id"));
        request.__synced()
    }

    pub fn group_by_status_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("status_id")]));
        request.__synced()
    }

    pub fn group_by_status_with(mut self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_status_with_operation(request, "group_by_status_with")
    }

    fn __group_by_status_with_operation(mut self, request: impl Into<QuerySelection>, method: &str) -> Self {
        self.query = self.query.group_by("status_id");
        self.query_options.object_group_bys.push(ObjectGroupBy::new(
            "status",
            "status_id",
            request,
        ).with_operation(
            QueryOperation::new(QueryOperationKind::Aggregate, method)
                .with_relation("status")));
        self.__synced()
    }

    pub fn group_by_status_with_details(self) -> Self {
        self.__group_by_status_with_operation(crate::Q::order_statuses().unlimited(), "group_by_status_with_details")
    }

    pub fn group_by_status_with_details_from(self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_status_with_operation(request, "group_by_status_with_details_from")
    }


    pub fn roll_up_to_status(&mut self) -> crate::OrderStatusRequest {
        self.__bind_roll_up_to_status(crate::Q::order_statuses().unlimited(),
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_status")
                .with_relation("status"))
    }

    pub fn roll_up_to_status_with<T>(&mut self, request: crate::OrderStatusRequest<T>) -> crate::OrderStatusRequest<T> {
        self.__bind_roll_up_to_status(request,
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_status_with")
                .with_relation("status"))
    }

    fn __bind_roll_up_to_status<T>(&mut self, mut request: crate::OrderStatusRequest<T>, operation: QueryOperation) -> crate::OrderStatusRequest<T> {
        let target = request.__roll_up_target();
        if let Some(source) = &self.roll_up_publication {
            assert!(!crate::rollup_support::reaches(&target, source).expect("invalid roll-up configuration"),
                "cyclic roll-up configuration");
        }
        teaql_core::request::clear_rollup_navigation(&mut self.query,
            "status", "id").expect("invalid roll-up configuration");
        self.roll_up_links.retain(|link| link.property != "status");
        self.roll_up_links.push(RollupLink {
            property: "status".into(),
            storage: "status_id".into(),
            descriptor: <crate::OrderStatus as teaql_core::TeaqlEntity>::entity_descriptor(),
            operation,
            target,
        });
        if !self.query.group_by.iter().any(|field| field == "status_id") {
            self.query.group_by.push("status_id".into());
        }
        self.__publish_roll_up();
        request
    }

    pub fn count_status(self) -> Self {
        self.count_status_as("status_count")
    }

    pub fn count_status_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("status_id", alias)
    }
    pub fn count_distinct_status(self) -> Self {
        self.count_distinct_status_as("status_distinct_count")
    }

    pub fn count_distinct_status_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("status_id", alias, AggregateFunction::CountDistinct)
    }

    pub fn unselect_status(mut self) -> Self {
        self.query.projection.retain(|field| field != "status_id");
        self.query.relations.retain(|relation| relation.name != "status");
        self.__synced()
    }


    pub fn filter_by_customer(mut self, value: impl EntityReference) -> Self {
        self.query = self.query.and_filter(Expr::eq("customer_id", value.entity_id_value()));
        self.__synced()
    }

    pub fn with_customer_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "customer_id",
            <crate::Customer as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("customer", selection));
        self.__synced()
    }


    pub fn without_customer_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "customer_id",
            <crate::Customer as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("customer", selection));
        self.__synced()
    }


    pub fn have_customer(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("customer_id"));
        self.__synced()
    }

    pub fn have_no_customer(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("customer_id"));
        self.__synced()
    }

    pub fn order_by_customer_asc(mut self) -> Self {
        self.query = self.query.order_asc("customer_id");
        self.__synced()
    }

    pub fn order_by_customer_desc(mut self) -> Self {
        self.query = self.query.order_desc("customer_id");
        self.__synced()
    }


    pub fn group_by_customer(self) -> Self {
        self.group_by("customer_id")
    }

    pub fn group_by_customer_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("customer_id"));
        request.__synced()
    }

    pub fn group_by_customer_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("customer_id")]));
        request.__synced()
    }

    pub fn group_by_customer_with(mut self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_customer_with_operation(request, "group_by_customer_with")
    }

    fn __group_by_customer_with_operation(mut self, request: impl Into<QuerySelection>, method: &str) -> Self {
        self.query = self.query.group_by("customer_id");
        self.query_options.object_group_bys.push(ObjectGroupBy::new(
            "customer",
            "customer_id",
            request,
        ).with_operation(
            QueryOperation::new(QueryOperationKind::Aggregate, method)
                .with_relation("customer")));
        self.__synced()
    }

    pub fn group_by_customer_with_details(self) -> Self {
        self.__group_by_customer_with_operation(crate::Q::customers().unlimited(), "group_by_customer_with_details")
    }

    pub fn group_by_customer_with_details_from(self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_customer_with_operation(request, "group_by_customer_with_details_from")
    }


    pub fn roll_up_to_customer(&mut self) -> crate::CustomerRequest {
        self.__bind_roll_up_to_customer(crate::Q::customers().unlimited(),
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_customer")
                .with_relation("customer"))
    }

    pub fn roll_up_to_customer_with<T>(&mut self, request: crate::CustomerRequest<T>) -> crate::CustomerRequest<T> {
        self.__bind_roll_up_to_customer(request,
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_customer_with")
                .with_relation("customer"))
    }

    fn __bind_roll_up_to_customer<T>(&mut self, mut request: crate::CustomerRequest<T>, operation: QueryOperation) -> crate::CustomerRequest<T> {
        let target = request.__roll_up_target();
        if let Some(source) = &self.roll_up_publication {
            assert!(!crate::rollup_support::reaches(&target, source).expect("invalid roll-up configuration"),
                "cyclic roll-up configuration");
        }
        teaql_core::request::clear_rollup_navigation(&mut self.query,
            "customer", "id").expect("invalid roll-up configuration");
        self.roll_up_links.retain(|link| link.property != "customer");
        self.roll_up_links.push(RollupLink {
            property: "customer".into(),
            storage: "customer_id".into(),
            descriptor: <crate::Customer as teaql_core::TeaqlEntity>::entity_descriptor(),
            operation,
            target,
        });
        if !self.query.group_by.iter().any(|field| field == "customer_id") {
            self.query.group_by.push("customer_id".into());
        }
        self.__publish_roll_up();
        request
    }

    pub fn count_customer(self) -> Self {
        self.count_customer_as("customer_count")
    }

    pub fn count_customer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("customer_id", alias)
    }
    pub fn count_distinct_customer(self) -> Self {
        self.count_distinct_customer_as("customer_distinct_count")
    }

    pub fn count_distinct_customer_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("customer_id", alias, AggregateFunction::CountDistinct)
    }

    pub fn unselect_customer(mut self) -> Self {
        self.query.projection.retain(|field| field != "customer_id");
        self.query.relations.retain(|relation| relation.name != "customer");
        self.__synced()
    }


    pub fn filter_by_commerce_platform(mut self, value: impl EntityReference) -> Self {
        self.query = self.query.and_filter(Expr::eq("commerce_platform_id", value.entity_id_value()));
        self.__synced()
    }

    pub fn with_commerce_platform_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "commerce_platform_id",
            <crate::CommercePlatform as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("commerce_platform", selection));
        self.__synced()
    }


    pub fn without_commerce_platform_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "commerce_platform_id",
            <crate::CommercePlatform as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("commerce_platform", selection));
        self.__synced()
    }


    pub fn have_commerce_platform(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("commerce_platform_id"));
        self.__synced()
    }

    pub fn have_no_commerce_platform(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("commerce_platform_id"));
        self.__synced()
    }

    pub fn order_by_commerce_platform_asc(mut self) -> Self {
        self.query = self.query.order_asc("commerce_platform_id");
        self.__synced()
    }

    pub fn order_by_commerce_platform_desc(mut self) -> Self {
        self.query = self.query.order_desc("commerce_platform_id");
        self.__synced()
    }


    pub fn group_by_commerce_platform(self) -> Self {
        self.group_by("commerce_platform_id")
    }

    pub fn group_by_commerce_platform_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("commerce_platform_id"));
        request.__synced()
    }

    pub fn group_by_commerce_platform_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("commerce_platform_id")]));
        request.__synced()
    }

    pub fn group_by_commerce_platform_with(mut self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_commerce_platform_with_operation(request, "group_by_commerce_platform_with")
    }

    fn __group_by_commerce_platform_with_operation(mut self, request: impl Into<QuerySelection>, method: &str) -> Self {
        self.query = self.query.group_by("commerce_platform_id");
        self.query_options.object_group_bys.push(ObjectGroupBy::new(
            "commerce_platform",
            "commerce_platform_id",
            request,
        ).with_operation(
            QueryOperation::new(QueryOperationKind::Aggregate, method)
                .with_relation("commerce_platform")));
        self.__synced()
    }

    pub fn group_by_commerce_platform_with_details(self) -> Self {
        self.__group_by_commerce_platform_with_operation(crate::Q::commerce_platforms().unlimited(), "group_by_commerce_platform_with_details")
    }

    pub fn group_by_commerce_platform_with_details_from(self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_commerce_platform_with_operation(request, "group_by_commerce_platform_with_details_from")
    }


    pub fn roll_up_to_commerce_platform(&mut self) -> crate::CommercePlatformRequest {
        self.__bind_roll_up_to_commerce_platform(crate::Q::commerce_platforms().unlimited(),
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_commerce_platform")
                .with_relation("commerce_platform"))
    }

    pub fn roll_up_to_commerce_platform_with<T>(&mut self, request: crate::CommercePlatformRequest<T>) -> crate::CommercePlatformRequest<T> {
        self.__bind_roll_up_to_commerce_platform(request,
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_commerce_platform_with")
                .with_relation("commerce_platform"))
    }

    fn __bind_roll_up_to_commerce_platform<T>(&mut self, mut request: crate::CommercePlatformRequest<T>, operation: QueryOperation) -> crate::CommercePlatformRequest<T> {
        let target = request.__roll_up_target();
        if let Some(source) = &self.roll_up_publication {
            assert!(!crate::rollup_support::reaches(&target, source).expect("invalid roll-up configuration"),
                "cyclic roll-up configuration");
        }
        teaql_core::request::clear_rollup_navigation(&mut self.query,
            "commerce_platform", "id").expect("invalid roll-up configuration");
        self.roll_up_links.retain(|link| link.property != "commerce_platform");
        self.roll_up_links.push(RollupLink {
            property: "commerce_platform".into(),
            storage: "commerce_platform_id".into(),
            descriptor: <crate::CommercePlatform as teaql_core::TeaqlEntity>::entity_descriptor(),
            operation,
            target,
        });
        if !self.query.group_by.iter().any(|field| field == "commerce_platform_id") {
            self.query.group_by.push("commerce_platform_id".into());
        }
        self.__publish_roll_up();
        request
    }

    pub fn count_commerce_platform(self) -> Self {
        self.count_commerce_platform_as("commerce_platform_count")
    }

    pub fn count_commerce_platform_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("commerce_platform_id", alias)
    }
    pub fn count_distinct_commerce_platform(self) -> Self {
        self.count_distinct_commerce_platform_as("commerce_platform_distinct_count")
    }

    pub fn count_distinct_commerce_platform_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("commerce_platform_id", alias, AggregateFunction::CountDistinct)
    }

    pub fn unselect_commerce_platform(mut self) -> Self {
        self.query.projection.retain(|field| field != "commerce_platform_id");
        self.query.relations.retain(|relation| relation.name != "commerce_platform");
        self.__synced()
    }
    pub fn with_status_is_pending(self) -> Self {
        self.filter_by_status(1001_u64)
    }



    pub fn with_status_is_not_pending(mut self) -> Self {
        self.query = self.query.and_filter(Expr::ne("status_id", 1001_u64));
        self.__synced()
    }


    pub fn with_status_is_confirmed(self) -> Self {
        self.filter_by_status(1002_u64)
    }



    pub fn with_status_is_not_confirmed(mut self) -> Self {
        self.query = self.query.and_filter(Expr::ne("status_id", 1002_u64));
        self.__synced()
    }






    pub fn select_status(mut self) -> Self {
        self.query.relations.push(teaql_core::RelationLoad::new("status")
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_status").with_relation("status")));
        self.__synced()
    }

    pub fn select_status_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query.relations.push(teaql_core::RelationLoad::with_query("status", selection.into_query())
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_status_with").with_relation("status")));
        self.__synced()
}

    pub fn facet_by_status_as(self, facet_name: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        self.facet_by_status_internal(facet_name, request, true, "facet_by_status_as")
    }

    pub fn facet_by_status_as_with_options(
        self,
        facet_name: impl Into<String>,
        request: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        self.facet_by_status_internal(facet_name, request, include_all_facets, "facet_by_status_as_with_options")
    }

    fn facet_by_status_internal(mut self, facet_name: impl Into<String>, request: impl Into<QuerySelection>, include_all_facets: bool, method: &'static str) -> Self {
        let facet_name = facet_name.into();
        let operation = QueryOperation::new(QueryOperationKind::Facet, method)
            .with_alias(&facet_name).with_relation("status");
        self.query_options.facets.push(FacetRequest::new(
            facet_name,
            "status",
            request,
            include_all_facets,
        ).with_operation(operation));
        self.__synced()
    }

    pub fn select_customer(mut self) -> Self {
        self.query.relations.push(teaql_core::RelationLoad::new("customer")
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_customer").with_relation("customer")));
        self.__synced()
    }

    pub fn select_customer_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query.relations.push(teaql_core::RelationLoad::with_query("customer", selection.into_query())
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_customer_with").with_relation("customer")));
        self.__synced()
}

    pub fn facet_by_customer_as(self, facet_name: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        self.facet_by_customer_internal(facet_name, request, true, "facet_by_customer_as")
    }

    pub fn facet_by_customer_as_with_options(
        self,
        facet_name: impl Into<String>,
        request: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        self.facet_by_customer_internal(facet_name, request, include_all_facets, "facet_by_customer_as_with_options")
    }

    fn facet_by_customer_internal(mut self, facet_name: impl Into<String>, request: impl Into<QuerySelection>, include_all_facets: bool, method: &'static str) -> Self {
        let facet_name = facet_name.into();
        let operation = QueryOperation::new(QueryOperationKind::Facet, method)
            .with_alias(&facet_name).with_relation("customer");
        self.query_options.facets.push(FacetRequest::new(
            facet_name,
            "customer",
            request,
            include_all_facets,
        ).with_operation(operation));
        self.__synced()
    }

    pub fn select_commerce_platform(mut self) -> Self {
        self.query.relations.push(teaql_core::RelationLoad::new("commerce_platform")
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_commerce_platform").with_relation("commerce_platform")));
        self.__synced()
    }

    pub fn select_commerce_platform_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query.relations.push(teaql_core::RelationLoad::with_query("commerce_platform", selection.into_query())
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_commerce_platform_with").with_relation("commerce_platform")));
        self.__synced()
}

    pub fn facet_by_commerce_platform_as(self, facet_name: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        self.facet_by_commerce_platform_internal(facet_name, request, true, "facet_by_commerce_platform_as")
    }

    pub fn facet_by_commerce_platform_as_with_options(
        self,
        facet_name: impl Into<String>,
        request: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        self.facet_by_commerce_platform_internal(facet_name, request, include_all_facets, "facet_by_commerce_platform_as_with_options")
    }

    fn facet_by_commerce_platform_internal(mut self, facet_name: impl Into<String>, request: impl Into<QuerySelection>, include_all_facets: bool, method: &'static str) -> Self {
        let facet_name = facet_name.into();
        let operation = QueryOperation::new(QueryOperationKind::Facet, method)
            .with_alias(&facet_name).with_relation("commerce_platform");
        self.query_options.facets.push(FacetRequest::new(
            facet_name,
            "commerce_platform",
            request,
            include_all_facets,
        ).with_operation(operation));
        self.__synced()
    }
    pub fn have_order_lines(self) -> Self {
        self.with_order_line_list_matching(crate::Q::order_lines_minimal())
    }

    pub fn have_no_order_lines(self) -> Self {
        self.without_order_line_list_matching(crate::Q::order_lines_minimal())
    }

    pub fn with_order_line_list_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "id",
            <crate::OrderLine as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "customer_order_id",
        ));
        self.relation_filters.push(RelationFilter::new("order_line_list", selection));
        self.__synced()
    }

    pub fn without_order_line_list_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "id",
            <crate::OrderLine as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "customer_order_id",
        ));
        self.relation_filters.push(RelationFilter::new("order_line_list", selection));
        self.__synced()
    }

    pub fn select_order_line_list(mut self) -> Self {
        self.query.relations.push(teaql_core::RelationLoad::new("order_line_list")
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_order_line_list").with_relation("order_line_list")));
        self.__synced()
    }

    pub fn select_order_line_list_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query.relations.push(teaql_core::RelationLoad::with_query("order_line_list", selection.into_query())
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_order_line_list_with").with_relation("order_line_list")));
        self.__synced()
}
    pub fn count_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::RelationCount, "count_order_lines");
        self.count_order_lines_internal("count_order_lines".to_owned(), crate::Q::order_lines().unlimited(), operation)
    }

    pub fn count_order_lines_as(self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::RelationCount, "count_order_lines_as").with_alias(&alias);
        self.count_order_lines_internal(alias, crate::Q::order_lines().unlimited(), operation)
    }

    pub fn count_order_lines_with(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::RelationCount, "count_order_lines_with").with_alias(&alias);
        self.count_order_lines_internal(alias, request, operation)
    }

    fn count_order_lines_internal(mut self, alias: String, request: impl Into<QuerySelection>, operation: QueryOperation) -> Self {
        let selection = request.into();
        self.query_options.relation_aggregates.push(RelationAggregate::new(
            "order_line_list",
            alias,
            selection,
            true,
        ).with_operation(operation.with_relation("order_line_list")));
        self.__synced()
    }

    pub fn stats_from_order_lines(self, request: impl Into<QuerySelection>) -> Self {
        self.stats_from_order_lines_as("refinements", request)
    }

    pub fn stats_from_order_lines_as(mut self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "stats_from_order_lines_as")
            .with_alias(&alias).with_relation("order_line_list");
        self.query_options.relation_aggregates.push(RelationAggregate::new(
            "order_line_list",
            alias,
            selection,
            false,
        ).with_operation(operation));
        self.__synced()
    }

    fn scalar_from_order_lines_as(mut self, alias: impl Into<String>, request: impl Into<QuerySelection>, operation: QueryOperation) -> Self {
        let selection = request.into();
        self.query_options.relation_aggregates.push(RelationAggregate::new(
            "order_line_list",
            alias,
            selection,
            true,
        ).with_operation(operation.with_relation("order_line_list")));
        self.__synced()
    }

    pub fn group_by_order_lines_with_details(self, request: impl Into<QuerySelection>) -> Self {
        self.stats_from_order_lines(request)
    }


    pub fn sum_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "sum_quantity_of_order_lines");
        self.scalar_from_order_lines_as("sum_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().sum("quantity", "sum_quantity_of_order_lines"), operation)
    }

    pub fn sum_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "sum_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().sum("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn min_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "min_quantity_of_order_lines");
        self.scalar_from_order_lines_as("min_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().min("quantity", "min_quantity_of_order_lines"), operation)
    }

    pub fn min_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "min_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().min("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn max_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "max_quantity_of_order_lines");
        self.scalar_from_order_lines_as("max_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().max("quantity", "max_quantity_of_order_lines"), operation)
    }

    pub fn max_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "max_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().max("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn avg_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "avg_quantity_of_order_lines");
        self.scalar_from_order_lines_as("avg_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().avg("quantity", "avg_quantity_of_order_lines"), operation)
    }

    pub fn avg_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "avg_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().avg("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn standard_deviation_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "standard_deviation_quantity_of_order_lines");
        self.scalar_from_order_lines_as("standard_deviation_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().stddev("quantity", "standard_deviation_quantity_of_order_lines"), operation)
    }

    pub fn standard_deviation_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "standard_deviation_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().stddev("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn square_root_of_population_standard_deviation_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "square_root_of_population_standard_deviation_quantity_of_order_lines");
        self.scalar_from_order_lines_as("square_root_of_population_standard_deviation_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().stddev_pop("quantity", "square_root_of_population_standard_deviation_quantity_of_order_lines"), operation)
    }

    pub fn square_root_of_population_standard_deviation_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "square_root_of_population_standard_deviation_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().stddev_pop("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn sample_variance_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "sample_variance_quantity_of_order_lines");
        self.scalar_from_order_lines_as("sample_variance_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().var_samp("quantity", "sample_variance_quantity_of_order_lines"), operation)
    }

    pub fn sample_variance_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "sample_variance_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().var_samp("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn sample_population_variance_quantity_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "sample_population_variance_quantity_of_order_lines");
        self.scalar_from_order_lines_as("sample_population_variance_quantity_of_order_lines", crate::Q::order_lines().unlimited().into_query().var_pop("quantity", "sample_population_variance_quantity_of_order_lines"), operation)
    }

    pub fn sample_population_variance_quantity_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "sample_population_variance_quantity_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().var_pop("quantity", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn min_create_time_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "min_create_time_of_order_lines");
        self.scalar_from_order_lines_as("min_create_time_of_order_lines", crate::Q::order_lines().unlimited().into_query().min("create_time", "min_create_time_of_order_lines"), operation)
    }

    pub fn min_create_time_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "min_create_time_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().min("create_time", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
    pub fn max_create_time_of_order_lines(self) -> Self {
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "max_create_time_of_order_lines");
        self.scalar_from_order_lines_as("max_create_time_of_order_lines", crate::Q::order_lines().unlimited().into_query().max("create_time", "max_create_time_of_order_lines"), operation)
    }

    pub fn max_create_time_of_order_lines_as(self, alias: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        let alias = alias.into();
        let operation = QueryOperation::new(QueryOperationKind::Aggregate, "max_create_time_of_order_lines_as").with_alias(&alias);
        let selection = request.into().into_query().max("create_time", alias.clone());
        self.scalar_from_order_lines_as(alias, selection, operation)
    }
}

impl<R> Default for CustomerOrderRequest<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> From< CustomerOrderRequest<R> > for SelectQuery {
    fn from(request: CustomerOrderRequest<R>) -> Self {
        QuerySelection::from(request).into_query()
    }
}

impl<R> From< CustomerOrderRequest<R> > for QuerySelection {
    fn from(mut request: CustomerOrderRequest<R>) -> Self {
        request.__resolve_roll_ups().expect("invalid roll-up configuration");
        Self {
            query: request.query,
            relation_selections: request.relation_selections,
            relation_filters: request.relation_filters,
            child_enhancements: request.child_enhancements,
            query_options: request.query_options,
        }
    }
}


impl<'a, C> crate::request_support::AuditedSave<'a, C> for teaql_core::Audited<crate::CustomerOrder> 
where C: crate::TeaqlRuntime + ?Sized + 'a
{
    type Error = teaql_runtime::RuntimeError;
    type Entity = crate::CustomerOrder;
    fn save(self, context: &'a C) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Entity, Self::Error>> + Send + '_>> {
        Box::pin(async move {
            context.save_audited_entity(self).await
        })
    }
}

impl<R: teaql_core::Entity> crate::PurposedQuery<CustomerOrderRequest<R>> {
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.inner.query_options.comment = Some(comment.into());
        self.inner.__publish_roll_up();
        self
    }

    pub fn new_entity<C>(&self, context: &C) -> Result<crate::CustomerOrder, RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.require_comment();
        // Reserve identity before allocating mutation state. A failed reservation
        // must not escape as an entity or fall back to a different allocator.
        let id = context.user_context().next_id(crate::CustomerOrder::ENTITY_NAME)?;
        let mut entity = crate::CustomerOrder::runtime_new(context.user_context().entity_runtime_state());
        entity.update_id(id);
        teaql_core::Entity::mark_as_new(&mut entity);
        Ok(entity)
    }

    fn into_inner_with_trace(mut self) -> CustomerOrderRequest<R> {
        self.require_comment();
        self.inner.query.trace_chain.push(teaql_core::TraceNode::typed(
            teaql_core::TraceKind::Purpose,
            self.inner.query.entity.clone(),
            None,
            self.purpose,
        ));
        self.inner.__publish_roll_up();
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
