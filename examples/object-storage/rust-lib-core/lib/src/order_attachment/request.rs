use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use teaql_core::{Aggregate, AggregateFunction, EntityDescriptor, Expr, SelectQuery, SmartList};
use teaql_runtime::RuntimeError;

use crate::request_support::*;

impl EntityReference for crate::OrderAttachment {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(&self)
    }
}

impl EntityReference for &crate::OrderAttachment {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(self)
    }
}

// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_attachment
#[derive(Debug)]
pub struct OrderAttachmentRequest<R = crate::OrderAttachment> {
    query: SelectQuery,
    relation_selections: Vec<RelationSelection>,
    relation_filters: Vec<RelationFilter>,
    child_enhancements: Vec<QuerySelection>,
    query_options: QueryOptions,
    roll_up_links: Vec<RollupLink>,
    roll_up_publication: Option<RollupTarget>,
    marker: PhantomData<R>,
}

impl<R> Clone for OrderAttachmentRequest<R> {
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

impl<R> OrderAttachmentRequest<R> {
    pub(crate) fn new() -> Self {
        Self {
            query: SelectQuery::new("OrderAttachment")
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

    pub fn return_type<T>(self) -> OrderAttachmentRequest<T> {
        OrderAttachmentRequest {
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
        context.user_context().observe_generated_list("OrderAttachment", async {
        let mut total = None;
        if !use_id_set {
            if let Some((alias, count_query)) = &requested_count {
                let count_rows = context.fetch_compact_rows("OrderAttachment", count_query).await?;
                total = Some(exact_list_count(&count_rows, alias)?);
            }
        }
        if !query.as_query().group_by.is_empty() || !query.as_query().object_group_bys.is_empty() {
            return Err(RuntimeError::Graph("grouped projections require execute_for_rows".into()));
        }
        let mut rows = if !query.as_query().aggregates.is_empty() {
            let records = context.fetch_compact_rows("OrderAttachment", &query).await?;
            let mut rows = SmartList::new(Vec::new());
            if let Some(record) = records.into_iter().next() { rows.aggregations = record.into_map(); }
            rows
        } else {
            context.fetch_entity_smart_list::<R>(
                "OrderAttachment", query.clone(), relation_aggregates,
            ).await?
        };
        let facets = if query_options.facets.is_empty() { std::collections::BTreeMap::new() }
            else { execute_facets(context, query.as_query(), &query_options).await? };
        if let Some((alias, count_query)) = requested_count {
            if total.is_none() { total = rows.total_count; }
            if total.is_none() {
                let count_rows = context.fetch_compact_rows("OrderAttachment", &count_query).await?;
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
        context.fetch_compact_smart_list("OrderAttachment", &query).await
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
            "OrderAttachment", context.fetch_entity_stream("OrderAttachment", query),
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
        let rows = context.fetch_compact_rows("OrderAttachment", &query).await?;
        rows.first()
            .and_then(|row| row.get(&alias))
            .and_then(teaql_core::Value::try_u64)
            .ok_or_else(|| RuntimeError::Graph(format!("count result for OrderAttachment is missing or not numeric")))
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
        let rows = context.fetch_compact_rows("OrderAttachment", &query).await?;
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

    pub fn matching_any_of<O>(mut self, request: OrderAttachmentRequest<O>) -> Self {
        let filter = QuerySelection::from(request).into_query().filter;
        if let Some(criteria) = crate::request_support::criteria_alternatives_excluding_version(filter) {
            self.query = self.query.and_filter(criteria);
        }
        self.__synced()
    }

    pub fn match_any_of<O>(self, request: OrderAttachmentRequest<O>) -> Self {
        self.matching_any_of(request)
    }

    pub fn matching_ids_of<O>(mut self, request: OrderAttachmentRequest<O>) -> Self {
        let entity = <crate::OrderAttachment as teaql_core::TeaqlEntity>::entity_descriptor();
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
            "file_name" => Some("file_name"),
            "content_type" => Some("content_type"),
            "content_length" => Some("content_length"),
            "content_sha256" => Some("content_sha256"),
            "object_key" => Some("object_key"),
            "version" => Some("version"),
            "customer_order" | "customer_order_id" => Some("customer_order_id"),
            _ => None,
        }
    }

    fn apply_dynamic_json_chain_filter(self, head: &str, tail: &str, value: &JsonValue) -> Self {
        let _ = (tail, value);
        match head {
            "customer_order" => {
                self.with_customer_order_matching(
                    crate::Q::customer_orders_minimal()
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
        self.query = self.query.project("file_name");
        self.query = self.query.project("content_type");
        self.query = self.query.project("content_length");
        self.query = self.query.project("content_sha256");
        self.query = self.query.project("object_key");
        self.query = self.query.project("version");
        self.query = self.query.project("customer_order_id");
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
        request = request.select_customer_order();
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


    pub fn select_file_name(mut self) -> Self {
        self.query = self.query.project("file_name");
        self.__synced()
    }

    pub fn project_file_name(self) -> Self {
        self.select_file_name()
    }

    pub fn select_file_name_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_file_name_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_file_name_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("file_name", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_file_name(self) -> Self {
        self.group_by("file_name")
    }

    pub fn group_by_file_name_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("file_name"));
        request.__synced()
    }

    pub fn group_by_file_name_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("file_name")]));
        request.__synced()
    }

    pub fn count_file_name(self) -> Self {
        self.count_file_name_as("file_name_count")
    }

    pub fn count_file_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("file_name", alias)
    }
    pub fn count_distinct_file_name(self) -> Self {
        self.count_distinct_file_name_as("file_name_distinct_count")
    }

    pub fn count_distinct_file_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("file_name", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_file_name(self) -> Self {
        self.sum_file_name_as("sum_file_name")
    }

    pub fn sum_file_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("file_name", alias)
    }

    pub fn avg_file_name(self) -> Self {
        self.avg_file_name_as("avg_file_name")
    }

    pub fn avg_file_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("file_name", alias)
    }

    pub fn min_file_name(self) -> Self {
        self.min_file_name_as("min_file_name")
    }

    pub fn min_file_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("file_name", alias)
    }

    pub fn max_file_name(self) -> Self {
        self.max_file_name_as("max_file_name")
    }

    pub fn max_file_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("file_name", alias)
    }

    pub fn unselect_file_name(mut self) -> Self {
        self.query.projection.retain(|field| field != "file_name");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "file_name");
        self.__synced()
    }


    pub fn with_file_name(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "file_name",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_file_name_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "file_name",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_file_name_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("file_name", value));
        self.__synced()
    }



    pub fn with_file_name_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("file_name", lower, upper));
        self.__synced()
    }

    pub fn with_file_name_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "file_name",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_file_name_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "file_name",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_file_name_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "file_name",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_file_name_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("file_name", value));
        self.__synced()
    }
    pub fn with_file_name_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("file_name", value));
        self.__synced()
    }

    pub fn with_file_name_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("file_name"));
        self.__synced()
    }



    pub fn with_file_name_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("file_name"));
        self.__synced()
    }


    pub fn order_by_file_name_asc(mut self) -> Self {
        self.query = self.query.order_asc("file_name");
        self.__synced()
    }

    pub fn order_by_file_name_desc(mut self) -> Self {
        self.query = self.query.order_desc("file_name");
        self.__synced()
    }

    pub fn order_by_file_name_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("file_name");
        self.__synced()
    }

    pub fn order_by_file_name_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("file_name");
        self.__synced()
    }


    pub fn select_content_type(mut self) -> Self {
        self.query = self.query.project("content_type");
        self.__synced()
    }

    pub fn project_content_type(self) -> Self {
        self.select_content_type()
    }

    pub fn select_content_type_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_content_type_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_content_type_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("content_type", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_content_type(self) -> Self {
        self.group_by("content_type")
    }

    pub fn group_by_content_type_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("content_type"));
        request.__synced()
    }

    pub fn group_by_content_type_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("content_type")]));
        request.__synced()
    }

    pub fn count_content_type(self) -> Self {
        self.count_content_type_as("content_type_count")
    }

    pub fn count_content_type_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("content_type", alias)
    }
    pub fn count_distinct_content_type(self) -> Self {
        self.count_distinct_content_type_as("content_type_distinct_count")
    }

    pub fn count_distinct_content_type_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("content_type", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_content_type(self) -> Self {
        self.sum_content_type_as("sum_content_type")
    }

    pub fn sum_content_type_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("content_type", alias)
    }

    pub fn avg_content_type(self) -> Self {
        self.avg_content_type_as("avg_content_type")
    }

    pub fn avg_content_type_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("content_type", alias)
    }

    pub fn min_content_type(self) -> Self {
        self.min_content_type_as("min_content_type")
    }

    pub fn min_content_type_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("content_type", alias)
    }

    pub fn max_content_type(self) -> Self {
        self.max_content_type_as("max_content_type")
    }

    pub fn max_content_type_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("content_type", alias)
    }

    pub fn unselect_content_type(mut self) -> Self {
        self.query.projection.retain(|field| field != "content_type");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "content_type");
        self.__synced()
    }


    pub fn with_content_type(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "content_type",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_content_type_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "content_type",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_content_type_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("content_type", value));
        self.__synced()
    }



    pub fn with_content_type_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("content_type", lower, upper));
        self.__synced()
    }

    pub fn with_content_type_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "content_type",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_content_type_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "content_type",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_content_type_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "content_type",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_content_type_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("content_type", value));
        self.__synced()
    }
    pub fn with_content_type_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("content_type", value));
        self.__synced()
    }

    pub fn with_content_type_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("content_type"));
        self.__synced()
    }



    pub fn with_content_type_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("content_type"));
        self.__synced()
    }


    pub fn order_by_content_type_asc(mut self) -> Self {
        self.query = self.query.order_asc("content_type");
        self.__synced()
    }

    pub fn order_by_content_type_desc(mut self) -> Self {
        self.query = self.query.order_desc("content_type");
        self.__synced()
    }

    pub fn order_by_content_type_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("content_type");
        self.__synced()
    }

    pub fn order_by_content_type_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("content_type");
        self.__synced()
    }


    pub fn select_content_length(mut self) -> Self {
        self.query = self.query.project("content_length");
        self.__synced()
    }

    pub fn project_content_length(self) -> Self {
        self.select_content_length()
    }

    pub fn select_content_length_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_content_length_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_content_length_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("content_length", raw_sql_segment));
        self.__synced()
    }

    pub fn select_content_length_with_function(self, function: AggregateFunction) -> Self {
        self.select_content_length_as_with_function("content_length", function)
    }

    pub fn select_content_length_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("content_length", alias, function)
    }

    pub fn group_by_content_length(self) -> Self {
        self.group_by("content_length")
    }

    pub fn group_by_content_length_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("content_length"));
        request.__synced()
    }

    pub fn group_by_content_length_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("content_length")]));
        request.__synced()
    }

    pub fn count_content_length(self) -> Self {
        self.count_content_length_as("content_length_count")
    }

    pub fn count_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("content_length", alias)
    }
    pub fn count_distinct_content_length(self) -> Self {
        self.count_distinct_content_length_as("content_length_distinct_count")
    }

    pub fn count_distinct_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("content_length", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_content_length(self) -> Self {
        self.sum_content_length_as("sum_content_length")
    }

    pub fn sum_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("content_length", alias)
    }

    pub fn avg_content_length(self) -> Self {
        self.avg_content_length_as("avg_content_length")
    }

    pub fn avg_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("content_length", alias)
    }

    pub fn min_content_length(self) -> Self {
        self.min_content_length_as("min_content_length")
    }

    pub fn min_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("content_length", alias)
    }

    pub fn max_content_length(self) -> Self {
        self.max_content_length_as("max_content_length")
    }

    pub fn max_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("content_length", alias)
    }

    pub fn standard_deviation_content_length(self) -> Self {
        self.standard_deviation_content_length_as("stdDev_content_length")
    }

    pub fn standard_deviation_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("content_length", alias)
    }

    pub fn square_root_of_population_standard_deviation_content_length(self) -> Self {
        self.square_root_of_population_standard_deviation_content_length_as("stdDevPop_content_length")
    }

    pub fn square_root_of_population_standard_deviation_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("content_length", alias)
    }

    pub fn sample_variance_content_length(self) -> Self {
        self.sample_variance_content_length_as("varSamp_content_length")
    }

    pub fn sample_variance_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("content_length", alias)
    }

    pub fn sample_population_variance_content_length(self) -> Self {
        self.sample_population_variance_content_length_as("varPop_content_length")
    }

    pub fn sample_population_variance_content_length_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("content_length", alias)
    }

    pub fn unselect_content_length(mut self) -> Self {
        self.query.projection.retain(|field| field != "content_length");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "content_length");
        self.__synced()
    }


    pub fn with_content_length(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "content_length",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_content_length_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "content_length",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_content_length_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("content_length", value));
        self.__synced()
    }



    pub fn with_content_length_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("content_length", value));
        self.__synced()
    }

    pub fn with_content_length_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("content_length", value));
        self.__synced()
    }

    pub fn with_content_length_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("content_length", value));
        self.__synced()
    }

    pub fn with_content_length_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("content_length", value));
        self.__synced()
    }

    pub fn with_content_length_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("content_length", value));
        self.__synced()
    }

    pub fn with_content_length_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("content_length", lower, upper));
        self.__synced()
    }

    pub fn with_content_length_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "content_length",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_content_length_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "content_length",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_content_length_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "content_length",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_content_length_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("content_length", value));
        self.__synced()
    }

    pub fn with_content_length_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("content_length", value));
        self.__synced()
    }

    pub fn with_content_length_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("content_length"));
        self.__synced()
    }



    pub fn with_content_length_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("content_length"));
        self.__synced()
    }


    pub fn order_by_content_length_asc(mut self) -> Self {
        self.query = self.query.order_asc("content_length");
        self.__synced()
    }

    pub fn order_by_content_length_desc(mut self) -> Self {
        self.query = self.query.order_desc("content_length");
        self.__synced()
    }

    pub fn order_by_content_length_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("content_length");
        self.__synced()
    }

    pub fn order_by_content_length_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("content_length");
        self.__synced()
    }


    pub fn select_content_sha256(mut self) -> Self {
        self.query = self.query.project("content_sha256");
        self.__synced()
    }

    pub fn project_content_sha256(self) -> Self {
        self.select_content_sha256()
    }

    pub fn select_content_sha256_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_content_sha256_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_content_sha256_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("content_sha256", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_content_sha256(self) -> Self {
        self.group_by("content_sha256")
    }

    pub fn group_by_content_sha256_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("content_sha256"));
        request.__synced()
    }

    pub fn group_by_content_sha256_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("content_sha256")]));
        request.__synced()
    }

    pub fn count_content_sha256(self) -> Self {
        self.count_content_sha256_as("content_sha256_count")
    }

    pub fn count_content_sha256_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("content_sha256", alias)
    }
    pub fn count_distinct_content_sha256(self) -> Self {
        self.count_distinct_content_sha256_as("content_sha256_distinct_count")
    }

    pub fn count_distinct_content_sha256_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("content_sha256", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_content_sha256(self) -> Self {
        self.sum_content_sha256_as("sum_content_sha256")
    }

    pub fn sum_content_sha256_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("content_sha256", alias)
    }

    pub fn avg_content_sha256(self) -> Self {
        self.avg_content_sha256_as("avg_content_sha256")
    }

    pub fn avg_content_sha256_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("content_sha256", alias)
    }

    pub fn min_content_sha256(self) -> Self {
        self.min_content_sha256_as("min_content_sha256")
    }

    pub fn min_content_sha256_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("content_sha256", alias)
    }

    pub fn max_content_sha256(self) -> Self {
        self.max_content_sha256_as("max_content_sha256")
    }

    pub fn max_content_sha256_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("content_sha256", alias)
    }

    pub fn unselect_content_sha256(mut self) -> Self {
        self.query.projection.retain(|field| field != "content_sha256");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "content_sha256");
        self.__synced()
    }


    pub fn with_content_sha256(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "content_sha256",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_content_sha256_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "content_sha256",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_content_sha256_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("content_sha256", value));
        self.__synced()
    }



    pub fn with_content_sha256_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("content_sha256", lower, upper));
        self.__synced()
    }

    pub fn with_content_sha256_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "content_sha256",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_content_sha256_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "content_sha256",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_content_sha256_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "content_sha256",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_content_sha256_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("content_sha256", value));
        self.__synced()
    }
    pub fn with_content_sha256_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("content_sha256", value));
        self.__synced()
    }

    pub fn with_content_sha256_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("content_sha256"));
        self.__synced()
    }



    pub fn with_content_sha256_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("content_sha256"));
        self.__synced()
    }


    pub fn order_by_content_sha256_asc(mut self) -> Self {
        self.query = self.query.order_asc("content_sha256");
        self.__synced()
    }

    pub fn order_by_content_sha256_desc(mut self) -> Self {
        self.query = self.query.order_desc("content_sha256");
        self.__synced()
    }

    pub fn order_by_content_sha256_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("content_sha256");
        self.__synced()
    }

    pub fn order_by_content_sha256_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("content_sha256");
        self.__synced()
    }


    pub fn select_object_key(mut self) -> Self {
        self.query = self.query.project("object_key");
        self.__synced()
    }

    pub fn project_object_key(self) -> Self {
        self.select_object_key()
    }

    pub fn select_object_key_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_object_key_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_object_key_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("object_key", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_object_key(self) -> Self {
        self.group_by("object_key")
    }

    pub fn group_by_object_key_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("object_key"));
        request.__synced()
    }

    pub fn group_by_object_key_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("object_key")]));
        request.__synced()
    }

    pub fn count_object_key(self) -> Self {
        self.count_object_key_as("object_key_count")
    }

    pub fn count_object_key_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("object_key", alias)
    }
    pub fn count_distinct_object_key(self) -> Self {
        self.count_distinct_object_key_as("object_key_distinct_count")
    }

    pub fn count_distinct_object_key_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("object_key", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_object_key(self) -> Self {
        self.sum_object_key_as("sum_object_key")
    }

    pub fn sum_object_key_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("object_key", alias)
    }

    pub fn avg_object_key(self) -> Self {
        self.avg_object_key_as("avg_object_key")
    }

    pub fn avg_object_key_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("object_key", alias)
    }

    pub fn min_object_key(self) -> Self {
        self.min_object_key_as("min_object_key")
    }

    pub fn min_object_key_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("object_key", alias)
    }

    pub fn max_object_key(self) -> Self {
        self.max_object_key_as("max_object_key")
    }

    pub fn max_object_key_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("object_key", alias)
    }

    pub fn unselect_object_key(mut self) -> Self {
        self.query.projection.retain(|field| field != "object_key");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "object_key");
        self.__synced()
    }


    pub fn with_object_key(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "object_key",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_object_key_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "object_key",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_object_key_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("object_key", value));
        self.__synced()
    }



    pub fn with_object_key_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("object_key", lower, upper));
        self.__synced()
    }

    pub fn with_object_key_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "object_key",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_object_key_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "object_key",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_object_key_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "object_key",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_object_key_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("object_key", value));
        self.__synced()
    }
    pub fn with_object_key_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("object_key", value));
        self.__synced()
    }

    pub fn with_object_key_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("object_key"));
        self.__synced()
    }



    pub fn with_object_key_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("object_key"));
        self.__synced()
    }


    pub fn order_by_object_key_asc(mut self) -> Self {
        self.query = self.query.order_asc("object_key");
        self.__synced()
    }

    pub fn order_by_object_key_desc(mut self) -> Self {
        self.query = self.query.order_desc("object_key");
        self.__synced()
    }

    pub fn order_by_object_key_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("object_key");
        self.__synced()
    }

    pub fn order_by_object_key_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("object_key");
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
    pub fn filter_by_customer_order(mut self, value: impl EntityReference) -> Self {
        self.query = self.query.and_filter(Expr::eq("customer_order_id", value.entity_id_value()));
        self.__synced()
    }

    pub fn with_customer_order_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "customer_order_id",
            <crate::CustomerOrder as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("customer_order", selection));
        self.__synced()
    }


    pub fn without_customer_order_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "customer_order_id",
            <crate::CustomerOrder as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("customer_order", selection));
        self.__synced()
    }


    pub fn have_customer_order(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("customer_order_id"));
        self.__synced()
    }

    pub fn have_no_customer_order(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("customer_order_id"));
        self.__synced()
    }

    pub fn order_by_customer_order_asc(mut self) -> Self {
        self.query = self.query.order_asc("customer_order_id");
        self.__synced()
    }

    pub fn order_by_customer_order_desc(mut self) -> Self {
        self.query = self.query.order_desc("customer_order_id");
        self.__synced()
    }


    pub fn group_by_customer_order(self) -> Self {
        self.group_by("customer_order_id")
    }

    pub fn group_by_customer_order_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("customer_order_id"));
        request.__synced()
    }

    pub fn group_by_customer_order_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("customer_order_id")]));
        request.__synced()
    }

    pub fn group_by_customer_order_with(mut self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_customer_order_with_operation(request, "group_by_customer_order_with")
    }

    fn __group_by_customer_order_with_operation(mut self, request: impl Into<QuerySelection>, method: &str) -> Self {
        self.query = self.query.group_by("customer_order_id");
        self.query_options.object_group_bys.push(ObjectGroupBy::new(
            "customer_order",
            "customer_order_id",
            request,
        ).with_operation(
            QueryOperation::new(QueryOperationKind::Aggregate, method)
                .with_relation("customer_order")));
        self.__synced()
    }

    pub fn group_by_customer_order_with_details(self) -> Self {
        self.__group_by_customer_order_with_operation(crate::Q::customer_orders().unlimited(), "group_by_customer_order_with_details")
    }

    pub fn group_by_customer_order_with_details_from(self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_customer_order_with_operation(request, "group_by_customer_order_with_details_from")
    }


    pub fn roll_up_to_customer_order(&mut self) -> crate::CustomerOrderRequest {
        self.__bind_roll_up_to_customer_order(crate::Q::customer_orders().unlimited(),
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_customer_order")
                .with_relation("customer_order"))
    }

    pub fn roll_up_to_customer_order_with<T>(&mut self, request: crate::CustomerOrderRequest<T>) -> crate::CustomerOrderRequest<T> {
        self.__bind_roll_up_to_customer_order(request,
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_customer_order_with")
                .with_relation("customer_order"))
    }

    fn __bind_roll_up_to_customer_order<T>(&mut self, mut request: crate::CustomerOrderRequest<T>, operation: QueryOperation) -> crate::CustomerOrderRequest<T> {
        let target = request.__roll_up_target();
        if let Some(source) = &self.roll_up_publication {
            assert!(!crate::rollup_support::reaches(&target, source).expect("invalid roll-up configuration"),
                "cyclic roll-up configuration");
        }
        teaql_core::request::clear_rollup_navigation(&mut self.query,
            "customer_order", "id").expect("invalid roll-up configuration");
        self.roll_up_links.retain(|link| link.property != "customer_order");
        self.roll_up_links.push(RollupLink {
            property: "customer_order".into(),
            storage: "customer_order_id".into(),
            descriptor: <crate::CustomerOrder as teaql_core::TeaqlEntity>::entity_descriptor(),
            operation,
            target,
        });
        if !self.query.group_by.iter().any(|field| field == "customer_order_id") {
            self.query.group_by.push("customer_order_id".into());
        }
        self.__publish_roll_up();
        request
    }

    pub fn count_customer_order(self) -> Self {
        self.count_customer_order_as("customer_order_count")
    }

    pub fn count_customer_order_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("customer_order_id", alias)
    }
    pub fn count_distinct_customer_order(self) -> Self {
        self.count_distinct_customer_order_as("customer_order_distinct_count")
    }

    pub fn count_distinct_customer_order_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("customer_order_id", alias, AggregateFunction::CountDistinct)
    }

    pub fn unselect_customer_order(mut self) -> Self {
        self.query.projection.retain(|field| field != "customer_order_id");
        self.query.relations.retain(|relation| relation.name != "customer_order");
        self.__synced()
    }
    pub fn select_customer_order(mut self) -> Self {
        self.query.relations.push(teaql_core::RelationLoad::new("customer_order")
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_customer_order").with_relation("customer_order")));
        self.__synced()
    }

    pub fn select_customer_order_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query.relations.push(teaql_core::RelationLoad::with_query("customer_order", selection.into_query())
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_customer_order_with").with_relation("customer_order")));
        self.__synced()
}

    pub fn facet_by_customer_order_as(self, facet_name: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        self.facet_by_customer_order_internal(facet_name, request, true, "facet_by_customer_order_as")
    }

    pub fn facet_by_customer_order_as_with_options(
        self,
        facet_name: impl Into<String>,
        request: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        self.facet_by_customer_order_internal(facet_name, request, include_all_facets, "facet_by_customer_order_as_with_options")
    }

    fn facet_by_customer_order_internal(mut self, facet_name: impl Into<String>, request: impl Into<QuerySelection>, include_all_facets: bool, method: &'static str) -> Self {
        let facet_name = facet_name.into();
        let operation = QueryOperation::new(QueryOperationKind::Facet, method)
            .with_alias(&facet_name).with_relation("customer_order");
        self.query_options.facets.push(FacetRequest::new(
            facet_name,
            "customer_order",
            request,
            include_all_facets,
        ).with_operation(operation));
        self.__synced()
    }
}

impl<R> Default for OrderAttachmentRequest<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> From< OrderAttachmentRequest<R> > for SelectQuery {
    fn from(request: OrderAttachmentRequest<R>) -> Self {
        QuerySelection::from(request).into_query()
    }
}

impl<R> From< OrderAttachmentRequest<R> > for QuerySelection {
    fn from(mut request: OrderAttachmentRequest<R>) -> Self {
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


impl<'a, C> crate::request_support::AuditedSave<'a, C> for teaql_core::Audited<crate::OrderAttachment> 
where C: crate::TeaqlRuntime + ?Sized + 'a
{
    type Error = teaql_runtime::RuntimeError;
    type Entity = crate::OrderAttachment;
    fn save(self, context: &'a C) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Entity, Self::Error>> + Send + '_>> {
        Box::pin(async move {
            context.save_audited_entity(self).await
        })
    }
}

impl<R: teaql_core::Entity> crate::PurposedQuery<OrderAttachmentRequest<R>> {
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.inner.query_options.comment = Some(comment.into());
        self.inner.__publish_roll_up();
        self
    }

    pub fn new_entity<C>(&self, context: &C) -> Result<crate::OrderAttachment, RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.require_comment();
        // Reserve identity before allocating mutation state. A failed reservation
        // must not escape as an entity or fall back to a different allocator.
        let id = context.user_context().next_id(crate::OrderAttachment::ENTITY_NAME)?;
        let mut entity = crate::OrderAttachment::runtime_new(context.user_context().entity_runtime_state());
        entity.update_id(id);
        teaql_core::Entity::mark_as_new(&mut entity);
        Ok(entity)
    }

    fn into_inner_with_trace(mut self) -> OrderAttachmentRequest<R> {
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
            "{}", teaql_core::RequestIntentError {
                request_kind: teaql_core::RequestKind::Query,
                field: "comment",
            }
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
