use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use teaql_core::{Aggregate, AggregateFunction, EntityDescriptor, Expr, SelectQuery, SmartList};
use teaql_runtime::RuntimeError;

use crate::request_support::*;

impl EntityReference for crate::OrderLine {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(&self)
    }
}

impl EntityReference for &crate::OrderLine {
    fn entity_id_value(self) -> teaql_core::Value {
        teaql_core::IdentifiableEntity::id_value(self)
    }
}

// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_line
#[derive(Debug)]
pub struct OrderLineRequest<R = crate::OrderLine> {
    query: SelectQuery,
    relation_selections: Vec<RelationSelection>,
    relation_filters: Vec<RelationFilter>,
    child_enhancements: Vec<QuerySelection>,
    query_options: QueryOptions,
    roll_up_links: Vec<RollupLink>,
    roll_up_publication: Option<RollupTarget>,
    marker: PhantomData<R>,
}

impl<R> Clone for OrderLineRequest<R> {
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

impl<R> OrderLineRequest<R> {
    pub(crate) fn new() -> Self {
        Self {
            query: SelectQuery::new("OrderLine")
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

    pub fn return_type<T>(self) -> OrderLineRequest<T> {
        OrderLineRequest {
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
        context.user_context().observe_generated_list("OrderLine", async {
        let mut total = None;
        if !use_id_set {
            if let Some((alias, count_query)) = &requested_count {
                let count_rows = context.fetch_compact_rows("OrderLine", count_query).await?;
                total = Some(exact_list_count(&count_rows, alias)?);
            }
        }
        if !query.as_query().group_by.is_empty() || !query.as_query().object_group_bys.is_empty() {
            return Err(RuntimeError::Graph("grouped projections require execute_for_rows".into()));
        }
        let mut rows = if !query.as_query().aggregates.is_empty() {
            let records = context.fetch_compact_rows("OrderLine", &query).await?;
            let mut rows = SmartList::new(Vec::new());
            if let Some(record) = records.into_iter().next() { rows.aggregations = record.into_map(); }
            rows
        } else {
            context.fetch_entity_smart_list::<R>(
                "OrderLine", query.clone(), relation_aggregates,
            ).await?
        };
        let facets = if query_options.facets.is_empty() { std::collections::BTreeMap::new() }
            else { execute_facets(context, query.as_query(), &query_options).await? };
        if let Some((alias, count_query)) = requested_count {
            if total.is_none() { total = rows.total_count; }
            if total.is_none() {
                let count_rows = context.fetch_compact_rows("OrderLine", &count_query).await?;
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
        context.fetch_compact_smart_list("OrderLine", &query).await
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
            "OrderLine", context.fetch_entity_stream("OrderLine", query),
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
        let rows = context.fetch_compact_rows("OrderLine", &query).await?;
        rows.first()
            .and_then(|row| row.get(&alias))
            .and_then(teaql_core::Value::try_u64)
            .ok_or_else(|| RuntimeError::Graph(format!("count result for OrderLine is missing or not numeric")))
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
        let rows = context.fetch_compact_rows("OrderLine", &query).await?;
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

    pub fn matching_any_of<O>(mut self, request: OrderLineRequest<O>) -> Self {
        let filter = QuerySelection::from(request).into_query().filter;
        if let Some(criteria) = crate::request_support::criteria_alternatives_excluding_version(filter) {
            self.query = self.query.and_filter(criteria);
        }
        self.__synced()
    }

    pub fn match_any_of<O>(self, request: OrderLineRequest<O>) -> Self {
        self.matching_any_of(request)
    }

    pub fn matching_ids_of<O>(mut self, request: OrderLineRequest<O>) -> Self {
        let entity = <crate::OrderLine as teaql_core::TeaqlEntity>::entity_descriptor();
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
            "product_name" => Some("product_name"),
            "sku" => Some("sku"),
            "quantity" => Some("quantity"),
            "create_time" => Some("create_time"),
            "version" => Some("version"),
            "customer_order" | "customer_order_id" => Some("customer_order_id"),
            "product" | "product_id" => Some("product_id"),
            "commerce_platform" | "commerce_platform_id" => Some("commerce_platform_id"),
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
            "product" => {
                self.with_product_matching(
                    crate::Q::products_minimal()
                        .apply_dynamic_json_filter(tail, value),
                )
            }
            "commerce_platform" => {
                self.with_commerce_platform_matching(
                    crate::Q::commerce_platforms_minimal()
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
        self.query = self.query.project("product_name");
        self.query = self.query.project("sku");
        self.query = self.query.project("quantity");
        self.query = self.query.project("create_time");
        self.query = self.query.project("version");
        self.query = self.query.project("customer_order_id");
        self.query = self.query.project("product_id");
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
        request = request.select_customer_order();
        request = request.select_product();
        request = request.select_commerce_platform();
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


    pub fn select_product_name(mut self) -> Self {
        self.query = self.query.project("product_name");
        self.__synced()
    }

    pub fn project_product_name(self) -> Self {
        self.select_product_name()
    }

    pub fn select_product_name_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_product_name_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_product_name_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("product_name", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_product_name(self) -> Self {
        self.group_by("product_name")
    }

    pub fn group_by_product_name_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("product_name"));
        request.__synced()
    }

    pub fn group_by_product_name_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("product_name")]));
        request.__synced()
    }

    pub fn count_product_name(self) -> Self {
        self.count_product_name_as("product_name_count")
    }

    pub fn count_product_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("product_name", alias)
    }
    pub fn count_distinct_product_name(self) -> Self {
        self.count_distinct_product_name_as("product_name_distinct_count")
    }

    pub fn count_distinct_product_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("product_name", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_product_name(self) -> Self {
        self.sum_product_name_as("sum_product_name")
    }

    pub fn sum_product_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("product_name", alias)
    }

    pub fn avg_product_name(self) -> Self {
        self.avg_product_name_as("avg_product_name")
    }

    pub fn avg_product_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("product_name", alias)
    }

    pub fn min_product_name(self) -> Self {
        self.min_product_name_as("min_product_name")
    }

    pub fn min_product_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("product_name", alias)
    }

    pub fn max_product_name(self) -> Self {
        self.max_product_name_as("max_product_name")
    }

    pub fn max_product_name_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("product_name", alias)
    }

    pub fn unselect_product_name(mut self) -> Self {
        self.query.projection.retain(|field| field != "product_name");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "product_name");
        self.__synced()
    }


    pub fn with_product_name(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "product_name",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_product_name_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "product_name",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_product_name_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("product_name", value));
        self.__synced()
    }



    pub fn with_product_name_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("product_name", lower, upper));
        self.__synced()
    }

    pub fn with_product_name_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "product_name",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_product_name_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "product_name",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_product_name_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "product_name",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_product_name_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("product_name", value));
        self.__synced()
    }
    pub fn with_product_name_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("product_name", value));
        self.__synced()
    }

    pub fn with_product_name_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("product_name"));
        self.__synced()
    }



    pub fn with_product_name_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("product_name"));
        self.__synced()
    }


    pub fn order_by_product_name_asc(mut self) -> Self {
        self.query = self.query.order_asc("product_name");
        self.__synced()
    }

    pub fn order_by_product_name_desc(mut self) -> Self {
        self.query = self.query.order_desc("product_name");
        self.__synced()
    }

    pub fn order_by_product_name_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("product_name");
        self.__synced()
    }

    pub fn order_by_product_name_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("product_name");
        self.__synced()
    }


    pub fn select_sku(mut self) -> Self {
        self.query = self.query.project("sku");
        self.__synced()
    }

    pub fn project_sku(self) -> Self {
        self.select_sku()
    }

    pub fn select_sku_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_sku_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_sku_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("sku", raw_sql_segment));
        self.__synced()
    }

    pub fn group_by_sku(self) -> Self {
        self.group_by("sku")
    }

    pub fn group_by_sku_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("sku"));
        request.__synced()
    }

    pub fn group_by_sku_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("sku")]));
        request.__synced()
    }

    pub fn count_sku(self) -> Self {
        self.count_sku_as("sku_count")
    }

    pub fn count_sku_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("sku", alias)
    }
    pub fn count_distinct_sku(self) -> Self {
        self.count_distinct_sku_as("sku_distinct_count")
    }

    pub fn count_distinct_sku_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("sku", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_sku(self) -> Self {
        self.sum_sku_as("sum_sku")
    }

    pub fn sum_sku_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("sku", alias)
    }

    pub fn avg_sku(self) -> Self {
        self.avg_sku_as("avg_sku")
    }

    pub fn avg_sku_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("sku", alias)
    }

    pub fn min_sku(self) -> Self {
        self.min_sku_as("min_sku")
    }

    pub fn min_sku_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("sku", alias)
    }

    pub fn max_sku(self) -> Self {
        self.max_sku_as("max_sku")
    }

    pub fn max_sku_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("sku", alias)
    }

    pub fn unselect_sku(mut self) -> Self {
        self.query.projection.retain(|field| field != "sku");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "sku");
        self.__synced()
    }


    pub fn with_sku(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "sku",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_sku_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "sku",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_sku_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("sku", value));
        self.__synced()
    }



    pub fn with_sku_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("sku", value));
        self.__synced()
    }

    pub fn with_sku_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("sku", value));
        self.__synced()
    }

    pub fn with_sku_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("sku", value));
        self.__synced()
    }

    pub fn with_sku_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("sku", value));
        self.__synced()
    }

    pub fn with_sku_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("sku", value));
        self.__synced()
    }

    pub fn with_sku_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("sku", lower, upper));
        self.__synced()
    }

    pub fn with_sku_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "sku",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_sku_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "sku",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_sku_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "sku",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_sku_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::contain("sku", value));
        self.__synced()
    }

    pub fn with_sku_not_containing(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_contain("sku", value));
        self.__synced()
    }

    pub fn with_sku_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::begin_with("sku", value));
        self.__synced()
    }

    pub fn with_sku_not_starting_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_begin_with("sku", value));
        self.__synced()
    }

    pub fn with_sku_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::end_with("sku", value));
        self.__synced()
    }

    pub fn with_sku_not_ending_with(mut self, value: impl Into<String>) -> Self {
        self.query = self.query.and_filter(Expr::not_end_with("sku", value));
        self.__synced()
    }

    pub fn with_sku_sounding_like(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::sound_like("sku", value));
        self.__synced()
    }
    pub fn with_sku_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("sku", value));
        self.__synced()
    }

    pub fn with_sku_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("sku", value));
        self.__synced()
    }

    pub fn with_sku_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("sku"));
        self.__synced()
    }



    pub fn with_sku_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("sku"));
        self.__synced()
    }


    pub fn order_by_sku_asc(mut self) -> Self {
        self.query = self.query.order_asc("sku");
        self.__synced()
    }

    pub fn order_by_sku_desc(mut self) -> Self {
        self.query = self.query.order_desc("sku");
        self.__synced()
    }

    pub fn order_by_sku_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("sku");
        self.__synced()
    }

    pub fn order_by_sku_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("sku");
        self.__synced()
    }


    pub fn select_quantity(mut self) -> Self {
        self.query = self.query.project("quantity");
        self.__synced()
    }

    pub fn project_quantity(self) -> Self {
        self.select_quantity()
    }

    pub fn select_quantity_raw(self, raw_sql_segment: impl Into<String>) -> Self {
        self.select_quantity_unsafe_raw(UnsafeRawSqlSegment::trusted(raw_sql_segment))
    }

    pub fn select_quantity_unsafe_raw(mut self, raw_sql_segment: UnsafeRawSqlSegment) -> Self {
        self.query_options
            .raw_projections
            .push(RawProjection::new("quantity", raw_sql_segment));
        self.__synced()
    }

    pub fn select_quantity_with_function(self, function: AggregateFunction) -> Self {
        self.select_quantity_as_with_function("quantity", function)
    }

    pub fn select_quantity_as_with_function(
        self,
        alias: impl Into<String>,
        function: AggregateFunction,
    ) -> Self {
        self.aggregate_with_function("quantity", alias, function)
    }

    pub fn group_by_quantity(self) -> Self {
        self.group_by("quantity")
    }

    pub fn group_by_quantity_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("quantity"));
        request.__synced()
    }

    pub fn group_by_quantity_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("quantity")]));
        request.__synced()
    }

    pub fn count_quantity(self) -> Self {
        self.count_quantity_as("quantity_count")
    }

    pub fn count_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("quantity", alias)
    }
    pub fn count_distinct_quantity(self) -> Self {
        self.count_distinct_quantity_as("quantity_distinct_count")
    }

    pub fn count_distinct_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("quantity", alias, AggregateFunction::CountDistinct)
    }

    pub fn sum_quantity(self) -> Self {
        self.sum_quantity_as("sum_quantity")
    }

    pub fn sum_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_sum("quantity", alias)
    }

    pub fn avg_quantity(self) -> Self {
        self.avg_quantity_as("avg_quantity")
    }

    pub fn avg_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_avg("quantity", alias)
    }

    pub fn min_quantity(self) -> Self {
        self.min_quantity_as("min_quantity")
    }

    pub fn min_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_min("quantity", alias)
    }

    pub fn max_quantity(self) -> Self {
        self.max_quantity_as("max_quantity")
    }

    pub fn max_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_max("quantity", alias)
    }

    pub fn standard_deviation_quantity(self) -> Self {
        self.standard_deviation_quantity_as("stdDev_quantity")
    }

    pub fn standard_deviation_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev("quantity", alias)
    }

    pub fn square_root_of_population_standard_deviation_quantity(self) -> Self {
        self.square_root_of_population_standard_deviation_quantity_as("stdDevPop_quantity")
    }

    pub fn square_root_of_population_standard_deviation_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_stddev_pop("quantity", alias)
    }

    pub fn sample_variance_quantity(self) -> Self {
        self.sample_variance_quantity_as("varSamp_quantity")
    }

    pub fn sample_variance_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_samp("quantity", alias)
    }

    pub fn sample_population_variance_quantity(self) -> Self {
        self.sample_population_variance_quantity_as("varPop_quantity")
    }

    pub fn sample_population_variance_quantity_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_var_pop("quantity", alias)
    }

    pub fn unselect_quantity(mut self) -> Self {
        self.query.projection.retain(|field| field != "quantity");
        self.query_options.raw_projections.retain(|projection| projection.property_name != "quantity");
        self.__synced()
    }


    pub fn with_quantity(
        mut self,
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(field_operator_expr(
            "quantity",
            operator,
            values.into_iter().map(Into::into).collect(),
        ));
        self.__synced()
    }

    pub fn create_quantity_criteria(
        operator: FieldOperator,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Expr {
        field_operator_expr(
            "quantity",
            operator,
            values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn with_quantity_is(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::eq("quantity", value));
        self.__synced()
    }



    pub fn with_quantity_is_not(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::ne("quantity", value));
        self.__synced()
    }

    pub fn with_quantity_greater_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("quantity", value));
        self.__synced()
    }

    pub fn with_quantity_greater_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gte("quantity", value));
        self.__synced()
    }

    pub fn with_quantity_less_than(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("quantity", value));
        self.__synced()
    }

    pub fn with_quantity_less_than_or_equal_to(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lte("quantity", value));
        self.__synced()
    }

    pub fn with_quantity_between(
        mut self,
        lower: impl Into<teaql_core::Value>,
        upper: impl Into<teaql_core::Value>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::between("quantity", lower, upper));
        self.__synced()
    }

    pub fn with_quantity_between_range<T>(mut self, range: DateRange<T>) -> Self
    where
        T: Into<teaql_core::Value>,
    {
        self.query = self.query.and_filter(Expr::between(
            "quantity",
            range.start,
            range.end,
        ));
        self.__synced()
    }

    pub fn with_quantity_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::in_list(
            "quantity",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_quantity_not_in(
        mut self,
        values: impl IntoIterator<Item = impl Into<teaql_core::Value>>,
    ) -> Self {
        self.query = self.query.and_filter(Expr::not_in_list(
            "quantity",
            values.into_iter().map(Into::into),
        ));
        self.__synced()
    }

    pub fn with_quantity_before(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::lt("quantity", value));
        self.__synced()
    }

    pub fn with_quantity_after(mut self, value: impl Into<teaql_core::Value>) -> Self {
        self.query = self.query.and_filter(Expr::gt("quantity", value));
        self.__synced()
    }

    pub fn with_quantity_is_unknown(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("quantity"));
        self.__synced()
    }



    pub fn with_quantity_is_known(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("quantity"));
        self.__synced()
    }


    pub fn order_by_quantity_asc(mut self) -> Self {
        self.query = self.query.order_asc("quantity");
        self.__synced()
    }

    pub fn order_by_quantity_desc(mut self) -> Self {
        self.query = self.query.order_desc("quantity");
        self.__synced()
    }

    pub fn order_by_quantity_asc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_asc("quantity");
        self.__synced()
    }

    pub fn order_by_quantity_desc_using_gbk(mut self) -> Self {
        self.query = self.query.order_gbk_desc("quantity");
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


    pub fn filter_by_product(mut self, value: impl EntityReference) -> Self {
        self.query = self.query.and_filter(Expr::eq("product_id", value.entity_id_value()));
        self.__synced()
    }

    pub fn with_product_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::in_subquery(
            "product_id",
            <crate::Product as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("product", selection));
        self.__synced()
    }


    pub fn without_product_matching(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query = self.query.and_filter(Expr::not_in_subquery(
            "product_id",
            <crate::Product as teaql_core::TeaqlEntity>::entity_descriptor(),
            selection.clone().into_query(),
            "id",
        ));
        self.relation_filters.push(RelationFilter::new("product", selection));
        self.__synced()
    }


    pub fn have_product(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_not_null("product_id"));
        self.__synced()
    }

    pub fn have_no_product(mut self) -> Self {
        self.query = self.query.and_filter(Expr::is_null("product_id"));
        self.__synced()
    }

    pub fn order_by_product_asc(mut self) -> Self {
        self.query = self.query.order_asc("product_id");
        self.__synced()
    }

    pub fn order_by_product_desc(mut self) -> Self {
        self.query = self.query.order_desc("product_id");
        self.__synced()
    }


    pub fn group_by_product(self) -> Self {
        self.group_by("product_id")
    }

    pub fn group_by_product_as(self, alias: impl Into<String>) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias, Expr::column("product_id"));
        request.__synced()
    }

    pub fn group_by_product_with_function(
        self,
        alias: impl Into<String>,
        function: teaql_core::ExprFunction,
    ) -> Self {
        let mut request = self;
        request.query = request.query.group_by_expr(alias,
            Expr::function(function, vec![Expr::column("product_id")]));
        request.__synced()
    }

    pub fn group_by_product_with(mut self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_product_with_operation(request, "group_by_product_with")
    }

    fn __group_by_product_with_operation(mut self, request: impl Into<QuerySelection>, method: &str) -> Self {
        self.query = self.query.group_by("product_id");
        self.query_options.object_group_bys.push(ObjectGroupBy::new(
            "product",
            "product_id",
            request,
        ).with_operation(
            QueryOperation::new(QueryOperationKind::Aggregate, method)
                .with_relation("product")));
        self.__synced()
    }

    pub fn group_by_product_with_details(self) -> Self {
        self.__group_by_product_with_operation(crate::Q::products().unlimited(), "group_by_product_with_details")
    }

    pub fn group_by_product_with_details_from(self, request: impl Into<QuerySelection>) -> Self {
        self.__group_by_product_with_operation(request, "group_by_product_with_details_from")
    }


    pub fn roll_up_to_product(&mut self) -> crate::ProductRequest {
        self.__bind_roll_up_to_product(crate::Q::products().unlimited(),
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_product")
                .with_relation("product"))
    }

    pub fn roll_up_to_product_with<T>(&mut self, request: crate::ProductRequest<T>) -> crate::ProductRequest<T> {
        self.__bind_roll_up_to_product(request,
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_product_with")
                .with_relation("product"))
    }

    fn __bind_roll_up_to_product<T>(&mut self, mut request: crate::ProductRequest<T>, operation: QueryOperation) -> crate::ProductRequest<T> {
        let target = request.__roll_up_target();
        if let Some(source) = &self.roll_up_publication {
            assert!(!crate::rollup_support::reaches(&target, source).expect("invalid roll-up configuration"),
                "cyclic roll-up configuration");
        }
        teaql_core::request::clear_rollup_navigation(&mut self.query,
            "product", "id").expect("invalid roll-up configuration");
        self.roll_up_links.retain(|link| link.property != "product");
        self.roll_up_links.push(RollupLink {
            property: "product".into(),
            storage: "product_id".into(),
            descriptor: <crate::Product as teaql_core::TeaqlEntity>::entity_descriptor(),
            operation,
            target,
        });
        if !self.query.group_by.iter().any(|field| field == "product_id") {
            self.query.group_by.push("product_id".into());
        }
        self.__publish_roll_up();
        request
    }

    pub fn count_product(self) -> Self {
        self.count_product_as("product_count")
    }

    pub fn count_product_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_count_field("product_id", alias)
    }
    pub fn count_distinct_product(self) -> Self {
        self.count_distinct_product_as("product_distinct_count")
    }

    pub fn count_distinct_product_as(self, alias: impl Into<String>) -> Self {
        self.aggregate_with_function("product_id", alias, AggregateFunction::CountDistinct)
    }

    pub fn unselect_product(mut self) -> Self {
        self.query.projection.retain(|field| field != "product_id");
        self.query.relations.retain(|relation| relation.name != "product");
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

    pub fn select_product(mut self) -> Self {
        self.query.relations.push(teaql_core::RelationLoad::new("product")
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_product").with_relation("product")));
        self.__synced()
    }

    pub fn select_product_with(mut self, request: impl Into<QuerySelection>) -> Self {
        let selection = request.into();
        self.query.relations.push(teaql_core::RelationLoad::with_query("product", selection.into_query())
            .with_operation(QueryOperation::new(QueryOperationKind::RelationSelect, "select_product_with").with_relation("product")));
        self.__synced()
}

    pub fn facet_by_product_as(self, facet_name: impl Into<String>, request: impl Into<QuerySelection>) -> Self {
        self.facet_by_product_internal(facet_name, request, true, "facet_by_product_as")
    }

    pub fn facet_by_product_as_with_options(
        self,
        facet_name: impl Into<String>,
        request: impl Into<QuerySelection>,
        include_all_facets: bool,
    ) -> Self {
        self.facet_by_product_internal(facet_name, request, include_all_facets, "facet_by_product_as_with_options")
    }

    fn facet_by_product_internal(mut self, facet_name: impl Into<String>, request: impl Into<QuerySelection>, include_all_facets: bool, method: &'static str) -> Self {
        let facet_name = facet_name.into();
        let operation = QueryOperation::new(QueryOperationKind::Facet, method)
            .with_alias(&facet_name).with_relation("product");
        self.query_options.facets.push(FacetRequest::new(
            facet_name,
            "product",
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
}

impl<R> Default for OrderLineRequest<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> From< OrderLineRequest<R> > for SelectQuery {
    fn from(request: OrderLineRequest<R>) -> Self {
        QuerySelection::from(request).into_query()
    }
}

impl<R> From< OrderLineRequest<R> > for QuerySelection {
    fn from(mut request: OrderLineRequest<R>) -> Self {
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


impl<'a, C> crate::request_support::AuditedSave<'a, C> for teaql_core::Audited<crate::OrderLine> 
where C: crate::TeaqlRuntime + ?Sized + 'a
{
    type Error = teaql_runtime::RuntimeError;
    type Entity = crate::OrderLine;
    fn save(self, context: &'a C) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Entity, Self::Error>> + Send + '_>> {
        Box::pin(async move {
            context.save_audited_entity(self).await
        })
    }
}

impl<R: teaql_core::Entity> crate::PurposedQuery<OrderLineRequest<R>> {
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.inner.query_options.comment = Some(comment.into());
        self.inner.__publish_roll_up();
        self
    }

    pub fn new_entity<C>(&self, context: &C) -> Result<crate::OrderLine, RuntimeError>
    where
        C: crate::TeaqlRuntime + ?Sized,
    {
        self.require_comment();
        // Reserve identity before allocating mutation state. A failed reservation
        // must not escape as an entity or fall back to a different allocator.
        let id = context.user_context().next_id(crate::OrderLine::ENTITY_NAME)?;
        let mut entity = crate::OrderLine::runtime_new(context.user_context().entity_runtime_state());
        entity.update_id(id);
        teaql_core::Entity::mark_as_new(&mut entity);
        Ok(entity)
    }

    fn into_inner_with_trace(mut self) -> OrderLineRequest<R> {
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
