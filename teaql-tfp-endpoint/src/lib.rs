use serde_json::Value as JsonValue;
use std::sync::Arc;
use teaql_data_service::{
    GuardedMutationExecutor, GuardedMutationRequest, QueryExecutor, QueryRequest,
};
use teaql_runtime::{
    NoopRuntimeTelemetry, RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry,
    extract_runtime_context, start_runtime_operation,
};
use thiserror::Error;

pub mod models;
use models::{TfpFacetRequest, TfpMutationQuery, TfpSelectQuery};

#[derive(Clone, Debug)]
pub struct TrustedQueryContext {
    pub tenant_field: String,
    pub tenant_id: teaql_core::Value,
    /// Exhaustive per-entity query visibility. Every allowed query entity must
    /// be explicitly classified so omitted metadata cannot expose tombstones.
    pub entity_visibility: std::collections::BTreeMap<String, TrustedEntityVisibility>,
    pub authenticated_user: String,
    pub approved_purpose: String,
    pub allowed_entities: std::collections::BTreeSet<String>,
    /// Per entity mapping from public TFP field names to trusted core field names.
    pub field_mappings:
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    pub writable_field_mappings:
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    pub allowed_actions: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Generated serializer-independent field metadata, keyed by entity name.
    pub wire_metadata: std::collections::BTreeMap<String, WireEntityMetadata>,
    pub max_page_size: usize,
    /// Maximum client-requested row offset, independent from page size.
    pub max_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrustedEntityVisibility {
    Versioned { field: String },
    Unversioned,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireEntityMetadata {
    pub canonical_to_wire: std::collections::BTreeMap<String, String>,
    accepted_to_canonical: std::collections::BTreeMap<String, String>,
}

impl WireEntityMetadata {
    pub fn new(
        canonical_to_wire: std::collections::BTreeMap<String, String>,
        aliases: std::collections::BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let mut accepted = std::collections::BTreeMap::new();
        for (canonical, wire) in &canonical_to_wire {
            register_wire_name(&mut accepted, canonical, canonical)?;
            register_wire_name(&mut accepted, wire, canonical)?;
        }
        for (alias, canonical) in aliases {
            if !canonical_to_wire.contains_key(&canonical) {
                return Err(format!("Unknown canonical field for alias: {canonical}"));
            }
            register_wire_name(&mut accepted, &alias, &canonical)?;
        }
        Ok(Self {
            canonical_to_wire,
            accepted_to_canonical: accepted,
        })
    }

    pub fn canonical_field(&self, submitted: &str) -> Option<&str> {
        self.accepted_to_canonical
            .get(submitted)
            .map(String::as_str)
    }

    fn accepted_policy_map(
        &self,
        canonical_policy: &std::collections::BTreeMap<String, String>,
    ) -> std::collections::BTreeMap<String, String> {
        self.accepted_to_canonical
            .iter()
            .filter_map(|(accepted, canonical)| {
                canonical_policy
                    .get(canonical)
                    .map(|internal| (accepted.clone(), internal.clone()))
            })
            .collect()
    }

    fn response_policy_map(
        &self,
        canonical_policy: &std::collections::BTreeMap<String, String>,
    ) -> Result<std::collections::BTreeMap<String, String>, String> {
        let mut response = std::collections::BTreeMap::new();
        for (canonical, internal) in canonical_policy {
            let wire = self.canonical_to_wire.get(canonical).ok_or_else(|| {
                format!("Missing generated wire name for canonical field: {canonical}")
            })?;
            register_response_name(&mut response, internal, wire)?;
        }
        Ok(response)
    }
}

/// Converts dependency-free metadata emitted by a generated runtime module.
pub fn wire_metadata_from_generated(
    mappings: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    mut aliases: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
) -> Result<std::collections::BTreeMap<String, WireEntityMetadata>, String> {
    mappings
        .into_iter()
        .map(|(entity, fields)| {
            let metadata =
                WireEntityMetadata::new(fields, aliases.remove(&entity).unwrap_or_default())?;
            Ok((entity, metadata))
        })
        .collect()
}

fn register_wire_name(
    accepted: &mut std::collections::BTreeMap<String, String>,
    name: &str,
    canonical: &str,
) -> Result<(), String> {
    if let Some(previous) = accepted.insert(name.to_owned(), canonical.to_owned())
        && previous != canonical
    {
        return Err(format!("Wire field alias is ambiguous: {name}"));
    }
    Ok(())
}

fn register_response_name(
    response: &mut std::collections::BTreeMap<String, String>,
    internal: &str,
    public: &str,
) -> Result<(), String> {
    if let Some(previous) = response.insert(internal.to_owned(), public.to_owned())
        && previous != public
    {
        return Err(format!(
            "Runtime field has ambiguous response names: {internal}"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct NormalizedWireObject {
    pub values: serde_json::Map<String, JsonValue>,
    pub source_instance_paths: std::collections::BTreeMap<String, String>,
}

pub fn normalize_wire_object(
    submitted: &serde_json::Map<String, JsonValue>,
    metadata: &WireEntityMetadata,
) -> Result<NormalizedWireObject, TfpEndpointError> {
    let mut values = serde_json::Map::new();
    let mut paths = std::collections::BTreeMap::new();
    for (name, value) in submitted {
        let canonical = metadata.canonical_field(name).ok_or_else(|| {
            TfpEndpointError::WireInput(format!("Unknown field at /{}", escape_pointer(name)))
        })?;
        if values.contains_key(canonical) {
            return Err(TfpEndpointError::WireCollision(format!(
                "Multiple submitted fields resolve to {canonical}"
            )));
        }
        values.insert(canonical.to_owned(), value.clone());
        paths.insert(canonical.to_owned(), format!("/{}", escape_pointer(name)));
    }
    Ok(NormalizedWireObject {
        values,
        source_instance_paths: paths,
    })
}

/// Adds the submitted alias path without changing the canonical KSML checker location.
pub fn retain_submitted_paths(
    results: &mut [teaql_runtime::CheckResult],
    normalized: &NormalizedWireObject,
) {
    for result in results {
        let wire = result.to_wire(teaql_runtime::JsonFieldNamingProfile::SnakeCase);
        let Some(teaql_runtime::WireLocationSegment::Property { name: canonical }) =
            wire.location.first()
        else {
            continue;
        };
        if let Some(path) = normalized.source_instance_paths.get(canonical) {
            result.source_instance_path = Some(path.clone());
        }
    }
}

fn escape_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn approved_query_trace(
    trusted: &TrustedQueryContext,
    entity: &str,
    requested_purpose: &str,
) -> teaql_core::TraceNode {
    teaql_core::TraceNode {
        kind: teaql_core::TraceKind::Purpose,
        entity_type: entity.to_owned(),
        entity_id: None,
        comment: format!(
            "approved-purpose={}; authenticated-user={}; requested-purpose={}",
            trusted.approved_purpose, trusted.authenticated_user, requested_purpose,
        ),
    }
}

fn apply_allowlisted_default_projection(
    query: &mut teaql_core::SelectQuery,
    mappings: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    if !query.projection.is_empty()
        || !query.expr_projection.is_empty()
        || !query.group_by.is_empty()
        || !query.aggregates.is_empty()
    {
        return Ok(());
    }
    query.projection = mappings
        .values()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if query.projection.is_empty() {
        return Err(format!(
            "No readable fields are allowed for entity: {}",
            query.entity
        ));
    }
    Ok(())
}

fn compact_row_to_wire_json(
    row: &teaql_core::CompactRow,
    response_fields: &std::collections::BTreeMap<String, String>,
    aggregate_aliases: &std::collections::BTreeSet<String>,
) -> Result<JsonValue, String> {
    let JsonValue::Object(raw) = teaql_core::compact_row_to_json_value(row) else {
        return Err("TFP query result row must be an object".into());
    };
    let mut output = serde_json::Map::new();
    for (internal, value) in raw {
        let public = if let Some(public) = response_fields.get(&internal) {
            public.clone()
        } else if aggregate_aliases.contains(&internal) {
            internal.clone()
        } else {
            return Err(format!("Unexpected query result field: {internal}"));
        };
        if output.insert(public.clone(), value).is_some() {
            return Err(format!("Duplicate TFP response field: {public}"));
        }
    }
    Ok(JsonValue::Object(output))
}

fn generated_values_to_wire_json(
    generated: &teaql_core::Record,
    response_fields: &std::collections::BTreeMap<String, String>,
) -> JsonValue {
    let JsonValue::Object(raw) = teaql_core::record_to_json_value(generated) else {
        return JsonValue::Object(serde_json::Map::new());
    };
    let mut output = serde_json::Map::new();
    for (internal, value) in raw {
        if let Some(public) = response_fields.get(&internal) {
            output.insert(public.clone(), value);
        }
    }
    JsonValue::Object(output)
}

fn governed_query_result_limit(query: &teaql_core::SelectQuery) -> u64 {
    query
        .slice
        .and_then(|slice| slice.limit)
        .unwrap_or(query.hard_limit)
        .min(query.hard_limit)
}

fn validate_query_result_bound(
    governed_limit: u64,
    actual_rows: usize,
    phase: &str,
) -> Result<(), String> {
    if u64::try_from(actual_rows).unwrap_or(u64::MAX) > governed_limit {
        return Err(format!(
            "TFP {phase} executor returned {actual_rows} rows, exceeding governed limit {governed_limit}"
        ));
    }
    Ok(())
}

#[derive(Error, Debug)]
pub enum TfpEndpointError {
    #[error("Failed to parse JSON payload: {0}")]
    ParseError(#[from] serde_json::Error),
    #[error("Failed to translate to core query: {0}")]
    TranslationError(String),
    #[error("Data service execution failed")]
    ExecutionError(String),
    #[error("Mutation target is unavailable")]
    MutationTargetUnavailable,
    #[error("Invalid wire input: {0}")]
    WireInput(String),
    #[error("Conflicting wire input: {0}")]
    WireCollision(String),
}

impl TfpEndpointError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ParseError(_) => "TFP_INVALID_REQUEST",
            Self::TranslationError(message) if message.contains("audit reason") => {
                "TFP_AUDIT_REASON_REQUIRED"
            }
            Self::TranslationError(message) if message.starts_with("Entity is not allowed") => {
                "TFP_FORBIDDEN_ENTITY"
            }
            Self::TranslationError(message)
                if message.contains("Field is not allowed")
                    || message.contains("not writable")
                    || message.contains("Unknown field")
                    || message.contains("Unknown or forbidden field") =>
            {
                "TFP_FORBIDDEN_FIELD"
            }
            Self::TranslationError(message)
                if message.starts_with('$')
                    || message.starts_with("Invalid mutation request:")
                    || message.starts_with("Unsupported predicate operator")
                    || message.starts_with("Filter must not be empty")
                    || message.starts_with("Filter field aliases collide")
                    || message.starts_with("Filter nesting depth")
                    || message.starts_with("Filter predicate count")
                    || message.starts_with("Logical filter")
                    || message.starts_with("Predicate for ")
                    || message.starts_with("Query field is duplicated after mapping")
                    || message.starts_with("Facet ")
                    || message.starts_with("A TFP query ")
                    || message.starts_with("A TFP facet")
                    || message.starts_with("Nested facets")
                    || message.starts_with("Duplicate facet")
                    || message.starts_with("Duplicate aggregate alias")
                    || message.starts_with("Aggregate alias")
                    || message.starts_with("Order expressions")
                    || message.starts_with("Unsupported order direction")
                    || message.starts_with("Unsupported aggregate function")
                    || message.starts_with("Mutation payload must be an object")
                    || message.starts_with("Mutation field aliases collide")
                    || message.starts_with("Object values are forbidden")
                    || message.contains("does not accept null") =>
            {
                "TFP_INVALID_REQUEST"
            }
            Self::TranslationError(_) => "TFP_POLICY_VIOLATION",
            Self::ExecutionError(_) => "TFP_EXECUTION_FAILED",
            Self::MutationTargetUnavailable => "TFP_MUTATION_TARGET_UNAVAILABLE",
            Self::WireInput(_) => "WIRE_UNKNOWN_FIELD",
            Self::WireCollision(_) => "WIRE_FIELD_COLLISION",
        }
    }

    /// Returns a stable message suitable for untrusted transport clients.
    ///
    /// This deliberately excludes provider diagnostics, SQL, schema names, and
    /// data values. Transport adapters should use this method instead of
    /// serializing [`std::fmt::Display`] or [`std::fmt::Debug`] output.
    pub fn public_message(&self) -> &'static str {
        match self.code() {
            "TFP_INVALID_REQUEST" => "Invalid TFP request",
            "TFP_AUDIT_REASON_REQUIRED" => "Mutation audit reason is required",
            "TFP_FORBIDDEN_ENTITY" => "Entity is not allowed",
            "TFP_FORBIDDEN_FIELD" => "Field is not allowed",
            "TFP_POLICY_VIOLATION" => "Request violates federation policy",
            "TFP_EXECUTION_FAILED" => "Data service execution failed",
            "TFP_MUTATION_TARGET_UNAVAILABLE" => "Mutation target is unavailable",
            "WIRE_UNKNOWN_FIELD" => "Unknown wire field",
            "WIRE_FIELD_COLLISION" => "Conflicting wire input",
            _ => "TFP request failed",
        }
    }

    /// Returns sensitive provider detail for controlled internal diagnostics.
    ///
    /// Callers must never serialize this value into an untrusted response.
    pub fn internal_diagnostic(&self) -> Option<&str> {
        match self {
            Self::ExecutionError(detail) => Some(detail),
            _ => None,
        }
    }
}

/// The core TeaQL Federal Protocol Endpoint processor.
/// This struct is framework-agnostic. Web containers (like Axum or Actix)
/// can handle routing, auth, and IP whitelisting, and simply pass the JSON
/// payload here to get the result.
pub struct TfpEndpoint<Q, M>
where
    Q: QueryExecutor + Send + Sync,
    M: GuardedMutationExecutor + Send + Sync,
{
    query_executor: Arc<Q>,
    mutation_executor: Arc<M>,
    telemetry: Arc<dyn RuntimeTelemetry>,
}

impl<Q, M> TfpEndpoint<Q, M>
where
    Q: QueryExecutor + Send + Sync,
    M: GuardedMutationExecutor + Send + Sync,
{
    pub fn new(query_executor: Arc<Q>, mutation_executor: Arc<M>) -> Self {
        Self {
            query_executor,
            mutation_executor,
            telemetry: Arc::new(NoopRuntimeTelemetry),
        }
    }

    pub fn with_runtime_telemetry(mut self, telemetry: Arc<dyn RuntimeTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Handles a TFP Query request (usually mapped to /query).
    pub async fn handle_query(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        self.handle_query_with_carrier(trusted, json_payload, &Default::default())
            .await
    }

    pub async fn handle_query_with_carrier(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
        carrier: &std::collections::BTreeMap<String, String>,
    ) -> Result<JsonValue, TfpEndpointError> {
        let propagation = extract_runtime_context(&self.telemetry, carrier);
        propagation
            .run(self.handle_query_observed(trusted, json_payload))
            .await
    }

    async fn handle_query_observed(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        let scope = start_runtime_operation(
            &self.telemetry,
            RuntimeOperation::new("tfp", "server.query").attribute("teaql.tfp.role", "server"),
        );
        let result = scope
            .run(self.handle_query_inner(trusted, json_payload))
            .await;
        match &result {
            Ok(response) => scope.success(std::collections::BTreeMap::from([(
                "teaql.result.cardinality".to_owned(),
                RuntimeAttributeValue::Integer(
                    response
                        .get("data")
                        .and_then(JsonValue::as_array)
                        .map_or(0, Vec::len) as i64,
                ),
            )])),
            Err(error) => scope.failure(tfp_error_type(error)),
        }
        result
    }

    async fn handle_query_inner(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        reject_privileged_input(&json_payload).map_err(TfpEndpointError::TranslationError)?;
        let mut tfp_query: TfpSelectQuery =
            serde_json::from_value(json_payload).map_err(TfpEndpointError::ParseError)?;

        validate_policy(trusted, &tfp_query).map_err(TfpEndpointError::TranslationError)?;
        let configured_mappings =
            trusted
                .field_mappings
                .get(&tfp_query.entity)
                .ok_or_else(|| {
                    TfpEndpointError::TranslationError(format!(
                        "No field policy for entity: {}",
                        tfp_query.entity
                    ))
                })?;
        let generated_mappings = trusted
            .wire_metadata
            .get(&tfp_query.entity)
            .map(|metadata| metadata.accepted_policy_map(configured_mappings));
        let mappings = generated_mappings.as_ref().unwrap_or(configured_mappings);
        let response_fields = effective_response_field_mappings(trusted, &tfp_query.entity)
            .map_err(TfpEndpointError::TranslationError)?;
        prepare_facets(trusted, &mut tfp_query).map_err(TfpEndpointError::TranslationError)?;
        tfp_query
            .map_fields(mappings)
            .map_err(TfpEndpointError::TranslationError)?;
        let facets = std::mem::take(&mut tfp_query.facets);
        let aggregate_aliases = tfp_query
            .aggregate_items
            .iter()
            .map(|aggregate| aggregate.alias.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let client_comment = tfp_query
            .resolved_comment()
            .map_err(TfpEndpointError::TranslationError)?
            .to_owned();
        let requested_purpose = tfp_query
            .resolved_purpose()
            .map_err(TfpEndpointError::TranslationError)?
            .to_owned();
        let mut core_query = tfp_query
            .to_core()
            .map_err(TfpEndpointError::TranslationError)?;
        apply_allowlisted_default_projection(&mut core_query, mappings)
            .map_err(TfpEndpointError::TranslationError)?;
        core_query.hard_limit = trusted.max_page_size as u64;
        let trusted_scope = trusted_query_scope(trusted, &tfp_query.entity)
            .map_err(TfpEndpointError::TranslationError)?;
        core_query.filter = Some(match core_query.filter.take() {
            Some(filter) => teaql_core::Expr::And(vec![trusted_scope, filter]),
            None => trusted_scope,
        });
        let trace = approved_query_trace(trusted, &tfp_query.entity, &requested_purpose);
        core_query.trace_chain.push(trace.clone());

        let outer_query = core_query.clone();
        let response_bound = governed_query_result_limit(&outer_query);
        let request = QueryRequest {
            query: core_query,
            trace_chain: vec![trace.clone()],
            comment: Some(client_comment.clone()),
            capture_debug_query: false,
            capture_execution_metadata: true,
        };

        let result = self
            .query_executor
            .query(request)
            .await
            .map_err(|e| TfpEndpointError::ExecutionError(e.to_string()))?;
        validate_query_result_bound(response_bound, result.rows.len(), "query")
            .map_err(TfpEndpointError::ExecutionError)?;

        // Format into a standard response JSON.
        // We'll wrap the rows in a generic data format expected by TeaQL frontend.
        let mut response_obj = serde_json::Map::new();
        let rows_json: Vec<JsonValue> = result
            .rows
            .iter()
            .map(|row| compact_row_to_wire_json(row, &response_fields, &aggregate_aliases))
            .collect::<Result<_, _>>()
            .map_err(TfpEndpointError::ExecutionError)?;

        let result_count = rows_json.len();
        response_obj.insert("data".to_string(), JsonValue::Array(rows_json));
        let facet_values = self
            .execute_facets(trusted, &outer_query, &client_comment, facets)
            .await?;
        response_obj.insert("facets".to_string(), JsonValue::Object(facet_values));
        response_obj.insert("resultCode".to_string(), JsonValue::Number(0.into()));
        response_obj.insert("status".to_string(), JsonValue::String("YES".to_string()));
        let trace_json = std::slice::from_ref(&trace)
            .iter()
            .map(|node| {
                serde_json::json!({
                    "entity": node.entity_type,
                    "comment": node.comment,
                })
            })
            .collect::<Vec<_>>();
        response_obj.insert(
            "execution".to_string(),
            serde_json::json!({
                "backend": result.metadata.backend,
                "resultCount": result_count,
                "trace": trace_json,
                "sqlShape": result.metadata.parameterized_query,
            }),
        );

        Ok(JsonValue::Object(response_obj))
    }

    async fn execute_facets(
        &self,
        trusted: &TrustedQueryContext,
        outer_query: &teaql_core::SelectQuery,
        outer_comment: &str,
        facets: Vec<TfpFacetRequest>,
    ) -> Result<serde_json::Map<String, JsonValue>, TfpEndpointError> {
        let mut output = serde_json::Map::new();
        for facet in facets {
            let mut membership = outer_query.clone();
            membership.projection.clear();
            membership.expr_projection.clear();
            membership.order_by.clear();
            membership.slice = None;
            membership = membership.limit(trusted.max_page_size as u64);
            membership.group_by = vec![facet.relation_name.clone()];
            membership.aggregates = vec![teaql_core::Aggregate::new(
                teaql_core::AggregateFunction::Count,
                "id",
                "__tfpFacetCount",
            )];
            let membership_bound = governed_query_result_limit(&membership);
            let membership_trace = outer_query.trace_chain.clone();
            let membership_result = self
                .query_executor
                .query(QueryRequest {
                    query: membership,
                    trace_chain: membership_trace,
                    comment: Some(format!(
                        "{outer_comment}; derived-facet-membership={}",
                        facet.facet_name
                    )),
                    capture_debug_query: false,
                    capture_execution_metadata: true,
                })
                .await
                .map_err(|error| TfpEndpointError::ExecutionError(error.to_string()))?;
            validate_query_result_bound(
                membership_bound,
                membership_result.rows.len(),
                "facet membership",
            )
            .map_err(TfpEndpointError::ExecutionError)?;
            let mut counts = std::collections::BTreeMap::new();
            for row in &membership_result.rows {
                let relation = row.get(&facet.relation_name).ok_or_else(|| {
                    TfpEndpointError::ExecutionError(format!(
                        "Facet membership row is missing relation field: {}",
                        facet.relation_name
                    ))
                })?;
                let count = row.get("__tfpFacetCount").ok_or_else(|| {
                    TfpEndpointError::ExecutionError(
                        "Facet membership row is missing count field: __tfpFacetCount".into(),
                    )
                })?;
                let count = facet_count_value(count).map_err(TfpEndpointError::ExecutionError)?;
                if matches!(
                    relation,
                    teaql_core::Value::Null | teaql_core::Value::TypedNull(_)
                ) {
                    continue;
                }
                let key = json_key(&value_as_json(relation));
                if counts.insert(key.clone(), count).is_some() {
                    return Err(TfpEndpointError::ExecutionError(format!(
                        "Facet membership result contains duplicate identity: {key}"
                    )));
                }
            }

            let aliases = facet
                .query
                .aggregate_items
                .iter()
                .map(|aggregate| aggregate.alias.clone())
                .collect::<Vec<_>>();
            let nested_response_fields =
                effective_response_field_mappings(trusted, &facet.query.entity)
                    .map_err(TfpEndpointError::TranslationError)?;
            let mut nested = *facet.query;
            nested.aggregate_items.clear();
            nested.group_by_items.clear();
            let nested_comment = nested
                .resolved_comment()
                .map_err(TfpEndpointError::TranslationError)?
                .to_owned();
            let nested_purpose = nested
                .resolved_purpose()
                .map_err(TfpEndpointError::TranslationError)?
                .to_owned();
            let mut nested_query = nested
                .to_core()
                .map_err(TfpEndpointError::TranslationError)?;
            let nested_mappings = effective_field_mappings(trusted, &nested.entity, false)
                .map_err(TfpEndpointError::TranslationError)?;
            apply_allowlisted_default_projection(&mut nested_query, &nested_mappings)
                .map_err(TfpEndpointError::TranslationError)?;
            nested_query.hard_limit = trusted.max_page_size as u64;
            let trusted_scope = trusted_query_scope(trusted, &nested.entity)
                .map_err(TfpEndpointError::TranslationError)?;
            nested_query.filter = Some(match nested_query.filter.take() {
                Some(filter) => teaql_core::Expr::And(vec![trusted_scope, filter]),
                None => trusted_scope,
            });
            let nested_trace = approved_query_trace(trusted, &nested.entity, &nested_purpose);
            nested_query.trace_chain.push(nested_trace.clone());
            let nested_bound = governed_query_result_limit(&nested_query);
            let nested_result = self
                .query_executor
                .query(QueryRequest {
                    query: nested_query,
                    trace_chain: vec![nested_trace],
                    comment: Some(nested_comment),
                    capture_debug_query: false,
                    capture_execution_metadata: true,
                })
                .await
                .map_err(|error| TfpEndpointError::ExecutionError(error.to_string()))?;
            validate_query_result_bound(nested_bound, nested_result.rows.len(), "facet value")
                .map_err(TfpEndpointError::ExecutionError)?;
            let mut values = Vec::new();
            let no_aggregate_aliases = std::collections::BTreeSet::new();
            for row in &nested_result.rows {
                let mut value =
                    compact_row_to_wire_json(row, &nested_response_fields, &no_aggregate_aliases)
                        .map_err(TfpEndpointError::ExecutionError)?;
                let id = value
                    .get("id")
                    .filter(|id| !id.is_null())
                    .cloned()
                    .ok_or_else(|| {
                        TfpEndpointError::ExecutionError(
                            "Facet value row is missing non-null identity field: id".into(),
                        )
                    })?;
                let key = json_key(&id);
                if !facet.include_all_facets && !counts.contains_key(&key) {
                    continue;
                }
                let count = counts.get(&key).copied().unwrap_or(0);
                if let Some(object) = value.as_object_mut() {
                    for alias in &aliases {
                        object.insert(alias.clone(), JsonValue::Number(count.into()));
                    }
                }
                values.push(value);
            }
            output.insert(facet.facet_name, JsonValue::Array(values));
        }
        Ok(output)
    }

    /// Handles a TFP Mutation request (usually mapped to /mutate).
    pub async fn handle_mutation(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        self.handle_mutation_with_carrier(trusted, json_payload, &Default::default())
            .await
    }

    pub async fn handle_mutation_with_carrier(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
        carrier: &std::collections::BTreeMap<String, String>,
    ) -> Result<JsonValue, TfpEndpointError> {
        let propagation = extract_runtime_context(&self.telemetry, carrier);
        propagation
            .run(self.handle_mutation_observed(trusted, json_payload))
            .await
    }

    async fn handle_mutation_observed(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        let scope = start_runtime_operation(
            &self.telemetry,
            RuntimeOperation::new("tfp", "server.mutation").attribute("teaql.tfp.role", "server"),
        );
        let result = scope
            .run(self.handle_mutation_inner(trusted, json_payload))
            .await;
        match &result {
            Ok(_) => scope.success(std::collections::BTreeMap::new()),
            Err(error) => scope.failure(tfp_error_type(error)),
        }
        result
    }

    async fn handle_mutation_inner(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        reject_privileged_input(&json_payload).map_err(TfpEndpointError::TranslationError)?;
        let mut tfp_mutation: TfpMutationQuery =
            serde_json::from_value(json_payload).map_err(TfpEndpointError::ParseError)?;

        validate_mutation_policy(trusted, &tfp_mutation)
            .map_err(TfpEndpointError::TranslationError)?;
        let response_fields = match trusted.field_mappings.get(&tfp_mutation.entity) {
            Some(_) => effective_response_field_mappings(trusted, &tfp_mutation.entity)
                .map_err(TfpEndpointError::TranslationError)?,
            None => std::collections::BTreeMap::new(),
        };
        let mappings = trusted
            .writable_field_mappings
            .get(&tfp_mutation.entity)
            .ok_or_else(|| {
                TfpEndpointError::TranslationError(format!(
                    "No writable field policy for entity: {}",
                    tfp_mutation.entity
                ))
            })?;
        if let Some(metadata) = trusted.wire_metadata.get(&tfp_mutation.entity) {
            let object = tfp_mutation.payload.as_object().ok_or_else(|| {
                TfpEndpointError::TranslationError("Mutation payload must be an object".into())
            })?;
            tfp_mutation.payload =
                JsonValue::Object(normalize_wire_object(object, metadata)?.values);
        }
        tfp_mutation
            .map_writable_fields(mappings)
            .map_err(TfpEndpointError::TranslationError)?;
        if tfp_mutation.payload.get(&trusted.tenant_field).is_some() {
            return Err(TfpEndpointError::TranslationError(
                "Tenant field is server-owned and not allowed".into(),
            ));
        }
        if tfp_mutation.action == "Create" {
            let tenant_json = value_as_json(&trusted.tenant_id);
            tfp_mutation
                .payload
                .as_object_mut()
                .expect("validated object")
                .insert(trusted.tenant_field.clone(), tenant_json);
        }

        let core_mutation = tfp_mutation
            .to_core()
            .map_err(TfpEndpointError::TranslationError)?;

        let is_create = tfp_mutation.action == "Create";
        let result = if is_create {
            self.mutation_executor.mutate(core_mutation).await
        } else {
            let tenant_guard =
                teaql_core::Expr::eq(&trusted.tenant_field, trusted.tenant_id.clone());
            self.mutation_executor
                .mutate_guarded(GuardedMutationRequest::new(core_mutation, tenant_guard))
                .await
        }
        .map_err(|error| TfpEndpointError::ExecutionError(error.to_string()))?;

        if result.affected_rows != 1 {
            if is_create {
                return Err(TfpEndpointError::ExecutionError(format!(
                    "TFP Create executor affected {} rows; expected exactly one",
                    result.affected_rows
                )));
            }
            return Err(TfpEndpointError::MutationTargetUnavailable);
        }

        let mut response_obj = serde_json::Map::new();
        response_obj.insert(
            "affectedRows".to_string(),
            JsonValue::Number(result.affected_rows.into()),
        );
        response_obj.insert("resultCode".to_string(), JsonValue::Number(0.into()));
        response_obj.insert("status".to_string(), JsonValue::String("YES".to_string()));

        let mut data_arr = Vec::new();
        if !result.generated_values.is_empty() {
            let generated: teaql_core::Record = result.generated_values.clone().into();
            let public_generated = generated_values_to_wire_json(&generated, &response_fields);
            if public_generated
                .as_object()
                .is_some_and(|values| !values.is_empty())
            {
                data_arr.push(public_generated);
            }
        }
        response_obj.insert("data".to_string(), JsonValue::Array(data_arr));

        Ok(JsonValue::Object(response_obj))
    }
}

fn trusted_query_scope(
    trusted: &TrustedQueryContext,
    entity: &str,
) -> Result<teaql_core::Expr, String> {
    let tenant = teaql_core::Expr::eq(&trusted.tenant_field, trusted.tenant_id.clone());
    match trusted.entity_visibility.get(entity) {
        Some(TrustedEntityVisibility::Versioned { field }) => Ok(teaql_core::Expr::And(vec![
            tenant,
            teaql_core::Expr::gt(field, 0_i64),
        ])),
        Some(TrustedEntityVisibility::Unversioned) => Ok(tenant),
        None => Err(format!(
            "No trusted query visibility metadata for entity: {entity}"
        )),
    }
}

fn tfp_error_type(error: &TfpEndpointError) -> &'static str {
    error.code()
}

fn value_as_json(value: &teaql_core::Value) -> JsonValue {
    let record = teaql_core::Record::from([("value".to_owned(), value.clone())]);
    teaql_core::record_to_json_value(&record)["value"].clone()
}

fn facet_count_value(value: &teaql_core::Value) -> Result<u64, String> {
    match value {
        teaql_core::Value::I64(value) if *value >= 0 => Ok(*value as u64),
        teaql_core::Value::U64(value) => Ok(*value),
        _ => Err("Facet membership count must be a non-negative integer".into()),
    }
}

fn json_key(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn reject_privileged_input(payload: &JsonValue) -> Result<(), String> {
    const FORBIDDEN: &[&str] = &[
        "tenant",
        "tenantId",
        "merchant",
        "merchantId",
        "user",
        "userId",
        "permissions",
        "requestPolicy",
        "purposePolicy",
        "trustedContext",
        "hardLimit",
        "hard_limit",
        "hardLimitValue",
        "hard_limit_value",
        "idSetPagination",
        "id_set_pagination",
    ];

    fn reject_at(value: &JsonValue, path: &str, forbidden: &[&str]) -> Result<(), String> {
        match value {
            JsonValue::Object(object) => {
                for (field, child) in object {
                    if forbidden.contains(&field.as_str()) {
                        return Err(format!(
                            "Client cannot provide trusted or server-local field: {path}.{field}"
                        ));
                    }
                    reject_at(child, &format!("{path}.{field}"), forbidden)?;
                }
            }
            JsonValue::Array(values) => {
                for (index, child) in values.iter().enumerate() {
                    reject_at(child, &format!("{path}[{index}]"), forbidden)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    if !payload.is_object() {
        return Err("TFP payload must be an object".into());
    }
    reject_at(payload, "$", FORBIDDEN)
}

fn validate_policy(trusted: &TrustedQueryContext, query: &TfpSelectQuery) -> Result<(), String> {
    query.validate_filter_shape()?;
    if !trusted.allowed_entities.contains(&query.entity) {
        return Err(format!(
            "Entity is not allowed by federation policy: {}",
            query.entity
        ));
    }
    query.validate_limit_shape()?;
    if query.limit_value.unwrap_or(0) > trusted.max_page_size {
        return Err("Page size exceeds federation policy".into());
    }
    if query.offset_value.unwrap_or(0) > trusted.max_offset {
        return Err("Offset exceeds federation policy".into());
    }
    query.resolved_comment()?;
    query.resolved_purpose()?;
    let allowed = effective_field_mappings(trusted, &query.entity, false)?;
    for field in query
        .order_items
        .iter()
        .map(|value| &value.field)
        .chain(query.select_items.iter())
        .chain(query.group_by_items.iter())
        .chain(query.aggregate_items.iter().map(|value| &value.field))
    {
        if field != "*" && !allowed.contains_key(field) {
            return Err(format!(
                "Field is not allowed by federation policy: {field}"
            ));
        }
    }
    let response_fields = effective_response_field_mappings(trusted, &query.entity)?;
    let public_fields = response_fields
        .values()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let mut aliases = std::collections::BTreeSet::new();
    for aggregate in &query.aggregate_items {
        let alias = aggregate.alias.as_str();
        if alias.is_empty()
            || alias.len() > 64
            || !alias
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || value == '_')
        {
            return Err(
                "Aggregate alias must be 1-64 ASCII letters, digits, or underscores".into(),
            );
        }
        if !aliases.insert(alias) {
            return Err(format!("Duplicate aggregate alias: {alias}"));
        }
        if public_fields.contains(alias) {
            return Err(format!(
                "Aggregate alias collides with a response field: {alias}"
            ));
        }
    }
    Ok(())
}

fn prepare_facets(trusted: &TrustedQueryContext, query: &mut TfpSelectQuery) -> Result<(), String> {
    if query.facets.len() > 10 {
        return Err("A TFP query may contain at most 10 facets".into());
    }
    let outer_fields = effective_field_mappings(trusted, &query.entity, false)?;
    let mut names = std::collections::BTreeSet::new();
    for facet in &mut query.facets {
        if facet.facet_name.is_empty()
            || facet.facet_name.len() > 64
            || !facet
                .facet_name
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || value == '_')
        {
            return Err("Facet name must be 1-64 ASCII letters, digits, or underscores".into());
        }
        if !names.insert(facet.facet_name.clone()) {
            return Err(format!("Duplicate facet name: {}", facet.facet_name));
        }
        facet.relation_name = outer_fields
            .get(&facet.relation_name)
            .ok_or_else(|| {
                format!(
                    "Field is not allowed by federation policy: {}",
                    facet.relation_name
                )
            })?
            .clone();
        if !facet.query.facets.is_empty() {
            return Err("Nested facets are not supported by TFP".into());
        }
        validate_policy(trusted, &facet.query)?;
        let nested_fields = effective_field_mappings(trusted, &facet.query.entity, false)?;
        let nested_id = nested_fields
            .get("id")
            .ok_or("Facet query requires a governed id field")?;
        if !facet.query.select_items.is_empty()
            && !facet
                .query
                .select_items
                .iter()
                .any(|field| nested_fields.get(field) == Some(nested_id))
        {
            return Err("Facet query explicit projection must include id".into());
        }
        if !facet.query.group_by_items.is_empty() {
            return Err(
                "A TFP facet does not accept groupByItems; relation grouping is derived".into(),
            );
        }
        if facet.query.aggregate_items.len() != 1 {
            return Err("A TFP facet requires exactly one Count(id) aggregate".into());
        }
        let aggregate = &facet.query.aggregate_items[0];
        if !aggregate.function.eq_ignore_ascii_case("count") || aggregate.field != "id" {
            return Err("A TFP facet requires exactly one Count(id) aggregate".into());
        }
        facet.query.map_fields(&nested_fields)?;
    }
    Ok(())
}

fn effective_field_mappings(
    trusted: &TrustedQueryContext,
    entity: &str,
    writable: bool,
) -> Result<std::collections::BTreeMap<String, String>, String> {
    let configured = if writable {
        trusted.writable_field_mappings.get(entity)
    } else {
        trusted.field_mappings.get(entity)
    }
    .ok_or_else(|| format!("No field policy for entity: {entity}"))?;
    Ok(trusted
        .wire_metadata
        .get(entity)
        .map(|metadata| metadata.accepted_policy_map(configured))
        .unwrap_or_else(|| configured.clone()))
}

fn effective_response_field_mappings(
    trusted: &TrustedQueryContext,
    entity: &str,
) -> Result<std::collections::BTreeMap<String, String>, String> {
    let configured = trusted
        .field_mappings
        .get(entity)
        .ok_or_else(|| format!("No field policy for entity: {entity}"))?;
    if let Some(metadata) = trusted.wire_metadata.get(entity) {
        return metadata.response_policy_map(configured);
    }
    let mut response = std::collections::BTreeMap::new();
    for (public, internal) in configured {
        register_response_name(&mut response, internal, public)?;
    }
    Ok(response)
}

fn validate_mutation_policy(
    trusted: &TrustedQueryContext,
    mutation: &TfpMutationQuery,
) -> Result<(), String> {
    if !trusted.allowed_entities.contains(&mutation.entity) {
        return Err(format!(
            "Entity is not allowed by federation policy: {}",
            mutation.entity
        ));
    }
    let actions = trusted
        .allowed_actions
        .get(&mutation.entity)
        .ok_or_else(|| format!("No action policy for entity: {}", mutation.entity))?;
    if !actions.contains(&mutation.action) {
        return Err(format!(
            "Action is not allowed by federation policy: {}",
            mutation.action
        ));
    }
    mutation.validate_request_shape()?;
    if mutation.payload.get(&trusted.tenant_field).is_some() {
        return Err("Tenant field is server-owned and not allowed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;
    use teaql_core::Record;
    use teaql_data_service::{
        DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
        GuardedMutationExecutor, GuardedMutationRequest, MutationExecutor, MutationRequest,
        MutationResult, QueryResult,
    };
    use teaql_runtime::{RuntimeOperation, RuntimeTelemetryScope};

    #[derive(Clone, Default)]
    struct StubExecutor;

    #[derive(Clone, Default)]
    struct RecordingQueryExecutor(Arc<Mutex<Vec<QueryRequest>>>);

    #[derive(Clone, Default)]
    struct WireFacetExecutor;

    #[derive(Clone, Copy)]
    enum MalformedFacetResult {
        MissingRelation,
        MissingCount,
        NegativeCount,
        TextCount,
        DuplicateMembership,
        MissingNestedId,
    }

    #[derive(Clone, Copy)]
    struct MalformedFacetExecutor(MalformedFacetResult);

    #[derive(Clone, Copy)]
    enum OverReturningStage {
        Outer,
        Membership,
        Nested,
        ExactOuterBoundary,
    }

    #[derive(Clone, Copy)]
    struct OverReturningExecutor(OverReturningStage);

    #[derive(Clone, Default)]
    struct ParameterizedShapeExecutor;

    #[derive(Clone, Copy)]
    struct AffectedRowsMutationExecutor(u64);

    #[derive(Clone, Default)]
    struct GeneratedValuesMutationExecutor;

    #[derive(Debug)]
    struct StubError;
    impl std::fmt::Display for StubError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("stub error")
        }
    }
    impl std::error::Error for StubError {}

    impl DataServiceExecutor for StubExecutor {
        type Error = StubError;
        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for StubExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let rows = if !request.query.group_by.is_empty() {
                vec![teaql_core::CompactRow::from_map(Record::from([
                    ("status_id".into(), teaql_core::Value::I64(1001)),
                    ("__tfpFacetCount".into(), teaql_core::Value::I64(2)),
                ]))]
            } else if request.query.entity == "OrderStatus" {
                vec![
                    teaql_core::CompactRow::from_map(Record::from([
                        ("id".into(), teaql_core::Value::I64(1001)),
                        ("code".into(), teaql_core::Value::Text("NEW".into())),
                    ])),
                    teaql_core::CompactRow::from_map(Record::from([
                        ("id".into(), teaql_core::Value::I64(1002)),
                        ("code".into(), teaql_core::Value::Text("PAID".into())),
                    ])),
                ]
            } else {
                vec![teaql_core::CompactRow::from_map(Record::new())]
            };
            Ok(QueryResult {
                metadata: metadata(DataServiceOperation::Query, Some(rows.len()), None),
                rows,
            })
        }
    }

    impl DataServiceExecutor for RecordingQueryExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for RecordingQueryExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            self.0.lock().expect("recorded queries").push(request);
            Ok(QueryResult {
                metadata: metadata(DataServiceOperation::Query, Some(0), None),
                rows: Vec::new(),
            })
        }
    }

    impl DataServiceExecutor for WireFacetExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for WireFacetExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let rows = if !request.query.group_by.is_empty() {
                vec![teaql_core::CompactRow::from_map(Record::from([
                    ("status_id".into(), teaql_core::Value::I64(1001)),
                    ("__tfpFacetCount".into(), teaql_core::Value::I64(2)),
                ]))]
            } else if request.query.entity == "OrderStatus" {
                vec![teaql_core::CompactRow::from_map(Record::from([
                    ("id".into(), teaql_core::Value::I64(1001)),
                    (
                        "display_name".into(),
                        teaql_core::Value::Text("New order".into()),
                    ),
                ]))]
            } else {
                Vec::new()
            };
            Ok(QueryResult {
                metadata: metadata(DataServiceOperation::Query, Some(rows.len()), None),
                rows,
            })
        }
    }

    impl DataServiceExecutor for MalformedFacetExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for MalformedFacetExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let rows = if !request.query.group_by.is_empty() {
                match self.0 {
                    MalformedFacetResult::MissingRelation => {
                        vec![teaql_core::CompactRow::from_map(Record::from([(
                            "__tfpFacetCount".into(),
                            teaql_core::Value::I64(2),
                        )]))]
                    }
                    MalformedFacetResult::MissingCount => {
                        vec![teaql_core::CompactRow::from_map(Record::from([(
                            "status_id".into(),
                            teaql_core::Value::I64(1001),
                        )]))]
                    }
                    MalformedFacetResult::NegativeCount => {
                        vec![teaql_core::CompactRow::from_map(Record::from([
                            ("status_id".into(), teaql_core::Value::I64(1001)),
                            ("__tfpFacetCount".into(), teaql_core::Value::I64(-1)),
                        ]))]
                    }
                    MalformedFacetResult::TextCount => {
                        vec![teaql_core::CompactRow::from_map(Record::from([
                            ("status_id".into(), teaql_core::Value::I64(1001)),
                            (
                                "__tfpFacetCount".into(),
                                teaql_core::Value::Text("two".into()),
                            ),
                        ]))]
                    }
                    MalformedFacetResult::DuplicateMembership => vec![
                        teaql_core::CompactRow::from_map(Record::from([
                            ("status_id".into(), teaql_core::Value::I64(1001)),
                            ("__tfpFacetCount".into(), teaql_core::Value::I64(1)),
                        ])),
                        teaql_core::CompactRow::from_map(Record::from([
                            ("status_id".into(), teaql_core::Value::I64(1001)),
                            ("__tfpFacetCount".into(), teaql_core::Value::U64(2)),
                        ])),
                    ],
                    MalformedFacetResult::MissingNestedId => {
                        vec![teaql_core::CompactRow::from_map(Record::from([
                            ("status_id".into(), teaql_core::Value::I64(1001)),
                            ("__tfpFacetCount".into(), teaql_core::Value::U64(2)),
                        ]))]
                    }
                }
            } else if request.query.entity == "OrderStatus" {
                match self.0 {
                    MalformedFacetResult::MissingNestedId => {
                        vec![teaql_core::CompactRow::from_map(Record::from([(
                            "code".into(),
                            teaql_core::Value::Text("NEW".into()),
                        )]))]
                    }
                    _ => vec![teaql_core::CompactRow::from_map(Record::from([
                        ("id".into(), teaql_core::Value::I64(1001)),
                        ("code".into(), teaql_core::Value::Text("NEW".into())),
                    ]))],
                }
            } else {
                Vec::new()
            };
            Ok(QueryResult {
                metadata: metadata(DataServiceOperation::Query, Some(rows.len()), None),
                rows,
            })
        }
    }

    impl DataServiceExecutor for OverReturningExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for OverReturningExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let limit = governed_query_result_limit(&request.query) as usize;
            let rows: Vec<teaql_core::CompactRow> = if !request.query.group_by.is_empty() {
                let count = if matches!(self.0, OverReturningStage::Membership) {
                    limit + 1
                } else {
                    1
                };
                (0..count)
                    .map(|index| {
                        teaql_core::CompactRow::from_map(Record::from([
                            ("status_id".into(), teaql_core::Value::U64(index as u64 + 1)),
                            ("__tfpFacetCount".into(), teaql_core::Value::U64(1)),
                        ]))
                    })
                    .collect()
            } else if request.query.entity == "OrderStatus" {
                let count = if matches!(self.0, OverReturningStage::Nested) {
                    limit + 1
                } else {
                    1
                };
                (0..count)
                    .map(|index| {
                        teaql_core::CompactRow::from_map(Record::from([
                            ("id".into(), teaql_core::Value::U64(index as u64 + 1)),
                            (
                                "code".into(),
                                teaql_core::Value::Text(format!("STATUS_{index}")),
                            ),
                        ]))
                    })
                    .collect()
            } else {
                let count = match self.0 {
                    OverReturningStage::Outer => limit + 1,
                    OverReturningStage::ExactOuterBoundary => limit,
                    OverReturningStage::Membership | OverReturningStage::Nested => 0,
                };
                (0..count)
                    .map(|_| teaql_core::CompactRow::from_map(Record::new()))
                    .collect()
            };
            Ok(QueryResult {
                metadata: metadata(DataServiceOperation::Query, Some(rows.len()), None),
                rows,
            })
        }
    }

    impl DataServiceExecutor for ParameterizedShapeExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for ParameterizedShapeExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            assert!(request.capture_execution_metadata);
            assert!(!request.capture_debug_query);
            let mut execution = metadata(DataServiceOperation::Query, Some(999), None);
            execution.trace_chain = vec![teaql_core::TraceNode {
                kind: teaql_core::TraceKind::Purpose,
                entity_type: "ForgedEntity".into(),
                entity_id: None,
                comment: "forged-provider-trace".into(),
            }];
            execution.parameterized_query = Some(
                "SELECT * FROM orders WHERE id = ? AND active = ? AND happened_at = ? AND email = ?"
                    .into(),
            );
            execution.params = vec![
                teaql_core::Value::I64(987_654_321),
                teaql_core::Value::Bool(true),
                teaql_core::Value::Timestamp(teaql_core::time::Timestamp(1_787_110_200_123)),
                teaql_core::Value::Text("private-address@example.com".into()),
            ];
            execution.debug_query = Some(
                "SELECT * FROM orders WHERE id = 987654321 AND active = TRUE AND happened_at = 1787110200123 AND email = 'private-address@example.com'"
                    .into(),
            );
            Ok(QueryResult {
                metadata: execution,
                rows: vec![teaql_core::CompactRow::from_map(Record::new())],
            })
        }
    }

    impl DataServiceExecutor for AffectedRowsMutationExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl MutationExecutor for AffectedRowsMutationExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: self.0,
                generated_values: Record::from([("id".into(), teaql_core::Value::I64(42))]).into(),
                persisted_snapshot: None,
                metadata: metadata(DataServiceOperation::Insert, None, Some(self.0)),
            })
        }
    }

    impl GuardedMutationExecutor for AffectedRowsMutationExecutor {
        async fn mutate_guarded(
            &self,
            request: GuardedMutationRequest,
        ) -> Result<MutationResult, Self::Error> {
            self.mutate(request.mutation).await
        }
    }

    impl MutationExecutor for StubExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: 1,
                generated_values: Record::new().into(),
                persisted_snapshot: None,
                metadata: metadata(DataServiceOperation::Insert, None, Some(1)),
            })
        }
    }

    impl GuardedMutationExecutor for StubExecutor {
        async fn mutate_guarded(
            &self,
            request: GuardedMutationRequest,
        ) -> Result<MutationResult, Self::Error> {
            self.mutate(request.mutation).await
        }
    }

    impl DataServiceExecutor for GeneratedValuesMutationExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl MutationExecutor for GeneratedValuesMutationExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: 1,
                generated_values: Record::from([
                    ("id".into(), teaql_core::Value::I64(42)),
                    (
                        "order_number".into(),
                        teaql_core::Value::Text("ORD-42".into()),
                    ),
                    ("commerce_platform_id".into(), teaql_core::Value::I64(7)),
                    (
                        "provider_private".into(),
                        teaql_core::Value::Text("hidden".into()),
                    ),
                ])
                .into(),
                persisted_snapshot: None,
                metadata: metadata(DataServiceOperation::Insert, None, Some(1)),
            })
        }
    }

    impl GuardedMutationExecutor for GeneratedValuesMutationExecutor {
        async fn mutate_guarded(
            &self,
            request: GuardedMutationRequest,
        ) -> Result<MutationResult, Self::Error> {
            self.mutate(request.mutation).await
        }
    }

    #[derive(Clone)]
    struct RecordingMutationExecutor {
        ordinary: Arc<Mutex<Vec<MutationRequest>>>,
        guarded: Arc<Mutex<Vec<GuardedMutationRequest>>>,
        affected_rows: u64,
    }

    impl RecordingMutationExecutor {
        fn new(affected_rows: u64) -> Self {
            Self {
                ordinary: Arc::new(Mutex::new(Vec::new())),
                guarded: Arc::new(Mutex::new(Vec::new())),
                affected_rows,
            }
        }

        fn result(&self, operation: DataServiceOperation) -> MutationResult {
            MutationResult {
                affected_rows: self.affected_rows,
                generated_values: Record::new().into(),
                persisted_snapshot: None,
                metadata: metadata(operation, None, Some(self.affected_rows)),
            }
        }
    }

    impl DataServiceExecutor for RecordingMutationExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl MutationExecutor for RecordingMutationExecutor {
        async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
            self.ordinary
                .lock()
                .expect("ordinary mutations")
                .push(request);
            Ok(self.result(DataServiceOperation::Insert))
        }
    }

    impl GuardedMutationExecutor for RecordingMutationExecutor {
        async fn mutate_guarded(
            &self,
            request: GuardedMutationRequest,
        ) -> Result<MutationResult, Self::Error> {
            let operation = match &request.mutation {
                MutationRequest::Update(_) => DataServiceOperation::Update,
                MutationRequest::Delete(_) => DataServiceOperation::Delete,
                MutationRequest::Recover(_) => DataServiceOperation::Recover,
                MutationRequest::Insert(_) => DataServiceOperation::Insert,
                MutationRequest::Batch(_) => DataServiceOperation::Batch,
            };
            self.guarded
                .lock()
                .expect("guarded mutations")
                .push(request);
            Ok(self.result(operation))
        }
    }

    fn metadata(
        operation: DataServiceOperation,
        result_count: Option<usize>,
        affected_rows: Option<u64>,
    ) -> ExecutionMetadata {
        ExecutionMetadata {
            backend: "stub".into(),
            operation,
            started_at: std::time::SystemTime::now(),
            ended_at: std::time::SystemTime::now(),
            affected_rows,
            result_count,
            trace_chain: Vec::new(),
            comment: None,
            backend_request_id: None,
            parameterized_query: None,
            params: Vec::new(),
            debug_query: None,
        }
    }

    #[derive(Default)]
    struct RecordingTelemetry(Arc<Mutex<Vec<RecordedEvent>>>);

    #[derive(Debug)]
    struct RecordedEvent {
        operation: RuntimeOperation,
        completion: Option<BTreeMap<String, RuntimeAttributeValue>>,
        failure: Option<String>,
    }

    impl RuntimeTelemetry for RecordingTelemetry {
        fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
            let mut events = self.0.lock().expect("events");
            events.push(RecordedEvent {
                operation,
                completion: None,
                failure: None,
            });
            Box::new(RecordingScope {
                events: self.0.clone(),
                index: events.len() - 1,
            })
        }
    }

    struct RecordingScope {
        events: Arc<Mutex<Vec<RecordedEvent>>>,
        index: usize,
    }
    impl RuntimeTelemetryScope for RecordingScope {
        fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
            self.events.lock().expect("events")[self.index].completion = Some(attributes);
        }
        fn failure(&mut self, error_type: &str) {
            self.events.lock().expect("events")[self.index].failure = Some(error_type.into());
        }
    }

    fn trusted() -> TrustedQueryContext {
        TrustedQueryContext {
            tenant_field: "commerce_platform_id".into(),
            tenant_id: teaql_core::Value::I64(1),
            entity_visibility: BTreeMap::from([
                (
                    "CustomerOrder".into(),
                    TrustedEntityVisibility::Versioned {
                        field: "version".into(),
                    },
                ),
                ("OrderStatus".into(), TrustedEntityVisibility::Unversioned),
            ]),
            authenticated_user: "operator-42".into(),
            approved_purpose: "approved-order-search".into(),
            allowed_entities: BTreeSet::from(["CustomerOrder".into(), "OrderStatus".into()]),
            field_mappings: BTreeMap::from([
                (
                    "CustomerOrder".into(),
                    BTreeMap::from([
                        ("id".into(), "id".into()),
                        ("orderNumber".into(), "order_number".into()),
                        ("status".into(), "status_id".into()),
                    ]),
                ),
                (
                    "OrderStatus".into(),
                    BTreeMap::from([("id".into(), "id".into()), ("code".into(), "code".into())]),
                ),
            ]),
            writable_field_mappings: BTreeMap::from([(
                "CustomerOrder".into(),
                BTreeMap::from([("orderNumber".into(), "order_number".into())]),
            )]),
            allowed_actions: BTreeMap::from([(
                "CustomerOrder".into(),
                BTreeSet::from([
                    "Create".into(),
                    "Update".into(),
                    "Delete".into(),
                    "Recover".into(),
                ]),
            )]),
            wire_metadata: BTreeMap::new(),
            max_page_size: 100,
            max_offset: 10_000,
        }
    }

    fn trusted_with_generated_wire_metadata() -> TrustedQueryContext {
        let mut context = trusted();
        context.field_mappings.insert(
            "CustomerOrder".into(),
            BTreeMap::from([
                ("id".into(), "id".into()),
                ("order_number".into(), "order_number".into()),
                ("status".into(), "status_id".into()),
            ]),
        );
        context.writable_field_mappings.insert(
            "CustomerOrder".into(),
            BTreeMap::from([("order_number".into(), "order_number".into())]),
        );
        context.wire_metadata.insert(
            "CustomerOrder".into(),
            WireEntityMetadata::new(
                BTreeMap::from([
                    ("id".into(), "id".into()),
                    ("order_number".into(), "orderNumber".into()),
                    ("status".into(), "status".into()),
                ]),
                BTreeMap::new(),
            )
            .expect("order wire metadata"),
        );
        context
    }

    #[test]
    fn client_cannot_override_trusted_context() {
        for field in [
            "tenantId",
            "merchant",
            "user",
            "permissions",
            "requestPolicy",
            "purposePolicy",
            "trustedContext",
        ] {
            assert!(reject_privileged_input(&json!({(field): "attacker"})).is_err());
        }
        assert!(
            reject_privileged_input(&json!({"entity":"CustomerOrder","_purpose":"requested"}))
                .is_ok()
        );
    }

    #[test]
    fn trusted_query_scope_adds_active_visibility_only_for_versioned_entities() {
        let context = trusted();
        assert_eq!(
            trusted_query_scope(&context, "CustomerOrder").unwrap(),
            teaql_core::Expr::And(vec![
                teaql_core::Expr::eq("commerce_platform_id", 1_i64),
                teaql_core::Expr::gt("version", 0_i64),
            ])
        );
        assert_eq!(
            trusted_query_scope(&context, "OrderStatus").unwrap(),
            teaql_core::Expr::eq("commerce_platform_id", 1_i64)
        );
        assert!(trusted_query_scope(&context, "Unclassified").is_err());
    }

    #[tokio::test]
    async fn missing_trusted_visibility_metadata_fails_closed() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        let mut context = trusted();
        context.entity_visibility.remove("CustomerOrder");
        let error = endpoint
            .handle_query(
                &context,
                json!({
                    "entity":"CustomerOrder", "_limit":10,
                    "_comment":"exercise missing trusted visibility",
                    "_purpose":"prove omitted metadata cannot expose tombstones"
                }),
            )
            .await
            .expect_err("missing visibility metadata must fail closed");
        assert_eq!(error.code(), "TFP_POLICY_VIOLATION");
    }

    #[test]
    fn client_cannot_override_hard_limit_at_any_depth() {
        for field in [
            "hardLimit",
            "hard_limit",
            "hardLimitValue",
            "hard_limit_value",
        ] {
            let error = reject_privileged_input(&json!({
                "entity": "CustomerOrder",
                (field): 20_000
            }))
            .unwrap_err();
            assert!(error.contains(field));

            let nested_error = reject_privileged_input(&json!({
                "entity": "CustomerOrder",
                "relations": [{"query": {(field): 20_000}}]
            }))
            .unwrap_err();
            assert!(nested_error.contains(field));
        }
    }

    #[test]
    fn client_cannot_enable_id_set_pagination_at_any_depth() {
        for field in ["idSetPagination", "id_set_pagination"] {
            assert!(reject_privileged_input(&json!({(field): {"maxIds": 5_000_000}})).is_err());
            assert!(
                reject_privileged_input(&json!({
                    "relations": [{"query": {(field): {"namespace": "attacker"}}}]
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn policy_rejects_entity_field_and_page_size() {
        let context = trusted();
        let query = |value| serde_json::from_value::<TfpSelectQuery>(value).unwrap();
        assert!(validate_policy(&context, &query(json!({"entity":"Other"}))).is_err());
        assert!(
            validate_policy(
                &context,
                &query(json!({
                    "entity":"CustomerOrder", "_limit":101
                }))
            )
            .is_err()
        );
        assert!(validate_policy(&context, &query(json!({"entity":"CustomerOrder"}))).is_err());
        assert!(
            validate_policy(
                &context,
                &query(json!({"entity":"CustomerOrder", "_limit":0}))
            )
            .is_err()
        );
        assert!(
            validate_policy(
                &context,
                &query(json!({
                    "entity":"CustomerOrder", "_orderBy":[{"f":"secret","d":"asc"}]
                }))
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn bounded_query_and_facets_retain_trusted_hard_ceiling() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let query_executor = RecordingQueryExecutor(queries.clone());
        let endpoint = TfpEndpoint::new(Arc::new(query_executor), Arc::new(StubExecutor));

        endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "facets":[{
                        "facetName":"statusFacet",
                        "relationName":"status",
                        "query":{
                            "entity":"OrderStatus",
                            "selectItems":["id","code"],
                            "aggregateItems":[{"function":"Count","field":"id","alias":"orderCount"}],
                            "limitValue":20,
                            "commentText":"load bounded status facet",
                            "purposeText":"render bounded order filters"
                        }
                    }],
                    "limitValue":10,
                    "commentText":"load bounded orders",
                    "purposeText":"render bounded order list"
                }),
            )
            .await
            .expect("bounded query");

        let queries = queries.lock().expect("recorded queries");
        assert_eq!(queries.len(), 3);
        assert!(
            queries
                .iter()
                .all(|request| request.query.hard_limit == 100)
        );
        assert_eq!(queries[0].query.slice.as_ref().unwrap().limit, Some(10));
        assert_eq!(queries[1].query.slice.as_ref().unwrap().limit, Some(100));
        assert_eq!(queries[2].query.slice.as_ref().unwrap().limit, Some(20));
        assert_eq!(queries[0].comment.as_deref(), Some("load bounded orders"));
        assert_eq!(queries[0].trace_chain.len(), 1);
        assert!(
            queries[0].trace_chain[0]
                .comment
                .contains("requested-purpose=render bounded order list")
        );
        assert_eq!(queries[1].trace_chain, queries[0].trace_chain);
        assert_eq!(
            queries[1].comment.as_deref(),
            Some("load bounded orders; derived-facet-membership=statusFacet")
        );
        assert_eq!(
            queries[2].comment.as_deref(),
            Some("load bounded status facet")
        );
        assert_eq!(queries[2].trace_chain.len(), 1);
        assert!(
            queries[2].trace_chain[0]
                .comment
                .contains("approved-purpose=approved-order-search")
        );
        assert!(
            queries[2].trace_chain[0]
                .comment
                .contains("authenticated-user=operator-42")
        );
        assert!(
            queries[2].trace_chain[0]
                .comment
                .contains("requested-purpose=render bounded order filters")
        );
        assert_eq!(
            queries[2].query.trace_chain, queries[2].trace_chain,
            "core query and executor request must retain the same purpose evidence"
        );
    }

    #[tokio::test]
    async fn trusted_offset_ceiling_applies_to_outer_and_facet_queries() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let endpoint = TfpEndpoint::new(
            Arc::new(RecordingQueryExecutor(queries.clone())),
            Arc::new(StubExecutor),
        );

        for offset in [0, 10_000] {
            endpoint
                .handle_query(
                    &trusted(),
                    json!({
                        "entity":"CustomerOrder", "limitValue":10, "offsetValue":offset,
                        "commentText":"load bounded orders",
                        "purposeText":"verify trusted offset ceiling"
                    }),
                )
                .await
                .expect("offset at or below the trusted ceiling");
        }
        {
            let accepted = queries.lock().expect("recorded queries");
            assert_eq!(accepted.len(), 2);
            assert_eq!(accepted[0].query.slice.map(|slice| slice.offset), Some(0));
            assert_eq!(
                accepted[1].query.slice.map(|slice| slice.offset),
                Some(10_000)
            );
        }

        let rejected_queries = Arc::new(Mutex::new(Vec::new()));
        let rejected_endpoint = TfpEndpoint::new(
            Arc::new(RecordingQueryExecutor(rejected_queries.clone())),
            Arc::new(StubExecutor),
        );
        for payload in [
            json!({
                "entity":"CustomerOrder", "limitValue":10, "offsetValue":10_001,
                "commentText":"load deep orders",
                "purposeText":"reject excessive outer offset"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "facets":[{
                    "facetName":"statusFacet", "relationName":"status",
                    "query":{
                        "entity":"OrderStatus", "limitValue":10, "offsetValue":10_001,
                        "selectItems":["id"],
                        "aggregateItems":[{
                            "function":"Count", "field":"id", "alias":"orderCount"
                        }],
                        "commentText":"load deep status facet",
                        "purposeText":"reject excessive facet offset"
                    }
                }],
                "commentText":"load orders", "purposeText":"render orders"
            }),
        ] {
            let error = rejected_endpoint
                .handle_query(&trusted(), payload)
                .await
                .expect_err("offset above trusted ceiling must fail closed");
            assert_eq!(error.code(), "TFP_POLICY_VIOLATION");
        }
        assert!(
            rejected_queries
                .lock()
                .expect("rejected queries")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn empty_outer_and_facet_projections_are_narrowed_to_allowlisted_fields() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let query_executor = RecordingQueryExecutor(queries.clone());
        let endpoint = TfpEndpoint::new(Arc::new(query_executor), Arc::new(StubExecutor));

        endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "facets":[{
                        "facetName":"statusFacet",
                        "relationName":"status",
                        "query":{
                            "entity":"OrderStatus",
                            "aggregateItems":[{"function":"Count","field":"id","alias":"orderCount"}],
                            "limitValue":20,
                            "commentText":"load trusted status fields",
                            "purposeText":"render the status filter"
                        }
                    }],
                    "limitValue":10,
                    "commentText":"load trusted order fields",
                    "purposeText":"render the order list"
                }),
            )
            .await
            .expect("allowlisted default projections");

        let queries = queries.lock().expect("recorded queries");
        assert_eq!(queries.len(), 3);
        assert_eq!(
            queries[0].query.projection,
            vec!["id", "order_number", "status_id"]
        );
        assert!(queries[1].query.projection.is_empty());
        assert_eq!(queries[1].query.group_by, vec!["status_id"]);
        assert_eq!(queries[2].query.projection, vec!["code", "id"]);
    }

    #[tokio::test]
    async fn missing_or_zero_outer_and_facet_limits_fail_before_execution() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        for payload in [
            json!({
                "entity":"CustomerOrder", "commentText":"missing limit",
                "purposeText":"prove outer query is bounded"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":0, "commentText":"zero limit",
                "purposeText":"prove zero cannot bypass the bound"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "facets":[{
                    "facetName":"statusFacet", "relationName":"status",
                    "query":{
                        "entity":"OrderStatus", "selectItems":["id"],
                        "aggregateItems":[{"function":"Count","field":"id","alias":"orderCount"}],
                        "commentText":"missing facet limit", "purposeText":"prove facet is bounded"
                    }
                }],
                "commentText":"load orders", "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "facets":[{
                    "facetName":"statusFacet", "relationName":"status",
                    "query":{
                        "entity":"OrderStatus", "selectItems":["id"], "limitValue":0,
                        "aggregateItems":[{"function":"Count","field":"id","alias":"orderCount"}],
                        "commentText":"zero facet limit", "purposeText":"prove facet is bounded"
                    }
                }],
                "commentText":"load orders", "purposeText":"render orders"
            }),
        ] {
            let error = endpoint
                .handle_query(&trusted(), payload)
                .await
                .expect_err("unbounded query must fail closed");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }
    }

    #[tokio::test]
    async fn missing_blank_or_conflicting_query_evidence_fails_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let endpoint = TfpEndpoint::new(
            Arc::new(RecordingQueryExecutor(queries.clone())),
            Arc::new(StubExecutor),
        );

        for payload in [
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"load orders", "purposeText":" "
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
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"x".repeat(models::MAX_GOVERNANCE_EVIDENCE_BYTES + 1),
                "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"load orders", "purposeText":"render\nforged-log-line"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "facets":[{
                    "facetName":"statusFacet", "relationName":"status",
                    "query":{
                        "entity":"OrderStatus", "limitValue":10,
                        "selectItems":["id"],
                        "aggregateItems":[{
                            "function":"Count", "field":"id", "alias":"orderCount"
                        }],
                        "purposeText":"render order filters"
                    }
                }],
                "commentText":"load orders", "purposeText":"render orders"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "facets":[{
                    "facetName":"statusFacet", "relationName":"status",
                    "query":{
                        "entity":"OrderStatus", "limitValue":10,
                        "selectItems":["id"],
                        "aggregateItems":[{
                            "function":"Count", "field":"id", "alias":"orderCount"
                        }],
                        "commentText":"load status values",
                        "purposeText":"x".repeat(models::MAX_GOVERNANCE_EVIDENCE_BYTES + 1)
                    }
                }],
                "commentText":"load orders", "purposeText":"render orders"
            }),
        ] {
            let error = endpoint
                .handle_query(&trusted(), payload)
                .await
                .expect_err("invalid query evidence must fail closed");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }

        assert!(queries.lock().expect("recorded queries").is_empty());

        let exact = "🧱".repeat(models::MAX_GOVERNANCE_EVIDENCE_BYTES / 4);
        endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "limitValue":10,
                    "commentText":exact, "purposeText":"验证支付审批"
                }),
            )
            .await
            .expect("exact evidence byte boundary remains executable");
        assert_eq!(queries.lock().expect("recorded queries").len(), 1);
    }

    #[tokio::test]
    async fn ignored_facet_aggregate_shapes_fail_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let query_executor = RecordingQueryExecutor(queries.clone());
        let endpoint = TfpEndpoint::new(Arc::new(query_executor), Arc::new(StubExecutor));

        for facet_query in [
            json!({
                "entity":"OrderStatus", "limitValue":10,
                "aggregateItems":[
                    {"function":"Count", "field":"id", "alias":"orderCount"},
                    {"function":"Count", "field":"id", "alias":"duplicateCount"}
                ],
                "commentText":"load status facet", "purposeText":"reject duplicate counts"
            }),
            json!({
                "entity":"OrderStatus", "limitValue":10,
                "aggregateItems":[{"function":"Count", "field":"code", "alias":"orderCount"}],
                "commentText":"load status facet", "purposeText":"reject ignored count field"
            }),
            json!({
                "entity":"OrderStatus", "limitValue":10,
                "groupByItems":["code"],
                "aggregateItems":[{"function":"Count", "field":"id", "alias":"orderCount"}],
                "commentText":"load status facet", "purposeText":"reject ignored grouping"
            }),
        ] {
            let error = endpoint
                .handle_query(
                    &trusted(),
                    json!({
                        "entity":"CustomerOrder", "limitValue":10,
                        "facets":[{
                            "facetName":"statusFacet", "relationName":"status",
                            "query":facet_query
                        }],
                        "commentText":"load orders", "purposeText":"render orders"
                    }),
                )
                .await
                .expect_err("unsupported facet semantics must fail closed");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }

        assert!(queries.lock().expect("recorded queries").is_empty());
    }

    #[tokio::test]
    async fn unsupported_predicate_is_an_invalid_request() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        let error = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "filterCondition":{"id":{"$wat":7}},
                    "limitValue":10,
                    "commentText":"exercise invalid operator",
                    "purposeText":"verify stable TFP error classification"
                }),
            )
            .await
            .expect_err("unsupported operator must fail closed");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
    }

    #[tokio::test]
    async fn malformed_canonical_shapes_are_invalid_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let query_executor = RecordingQueryExecutor(queries.clone());
        let mutations = Arc::new(RecordingMutationExecutor::new(1));
        let endpoint = TfpEndpoint::new(Arc::new(query_executor), mutations.clone());

        for payload in [
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "orderItems":[{"field":"id", "direction":"sideways"}],
                "commentText":"exercise invalid direction", "purposeText":"verify request code"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "aggregateItems":[{"function":"Median", "field":"id", "alias":"medianId"}],
                "commentText":"exercise invalid aggregate", "purposeText":"verify request code"
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "filterCondition":{"id":{"$eq":{"id":42, "extra":true}}},
                "commentText":"exercise invalid reference", "purposeText":"verify request code"
            }),
        ] {
            let error = endpoint
                .handle_query(&trusted(), payload)
                .await
                .expect_err("malformed query shape must fail closed");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }

        let error = endpoint
            .handle_mutation(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "action":"Create",
                    "payload":[], "comment":"exercise invalid payload"
                }),
            )
            .await
            .expect_err("non-object mutation payload must fail closed");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");

        assert!(queries.lock().expect("recorded queries").is_empty());
        assert!(
            mutations
                .ordinary
                .lock()
                .expect("ordinary mutations")
                .is_empty()
        );
        assert!(
            mutations
                .guarded
                .lock()
                .expect("guarded mutations")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn invalid_scalar_operand_shapes_fail_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let query_executor = RecordingQueryExecutor(queries.clone());
        let endpoint = TfpEndpoint::new(Arc::new(query_executor), Arc::new(StubExecutor));

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
            let error = endpoint
                .handle_query(
                    &trusted(),
                    json!({
                        "entity":"CustomerOrder", "filterCondition":filter,
                        "limitValue":10, "commentText":"exercise null range",
                        "purposeText":"verify explicit known-value semantics"
                    }),
                )
                .await
                .expect_err("null range operands must fail closed");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }

        assert!(queries.lock().expect("recorded queries").is_empty());
    }

    #[tokio::test]
    async fn ambiguous_and_excessive_filters_fail_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let query_executor = RecordingQueryExecutor(queries.clone());
        let endpoint = TfpEndpoint::new(Arc::new(query_executor), Arc::new(StubExecutor));
        let excessive_filters = (0..=256)
            .map(|id| json!({"id":{"$eq":id}}))
            .collect::<Vec<_>>();
        for payload in [
            json!({
                "entity":"CustomerOrder",
                "filterCondition":{
                    "$and":[{"id":{"$eq":1}}],
                    "id":{"$eq":2}
                },
                "limitValue":10,
                "commentText":"attempt ambiguous filter",
                "purposeText":"prove every submitted predicate is enforced"
            }),
            json!({
                "entity":"CustomerOrder",
                "_filters":excessive_filters,
                "limitValue":10,
                "commentText":"attempt excessive filters",
                "purposeText":"prove filter work stays bounded"
            }),
        ] {
            let error = endpoint
                .handle_query(&trusted(), payload)
                .await
                .expect_err("invalid filter tree must fail closed");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }

        let error = endpoint
            .handle_query(
                &trusted_with_generated_wire_metadata(),
                json!({
                    "entity":"CustomerOrder",
                    "filterCondition":{
                        "order_number":{"$startsWith":"SAFE"},
                        "orderNumber":{"$contains":"OTHER"}
                    },
                    "limitValue":10,
                    "commentText":"attempt alias collision",
                    "purposeText":"prove every submitted predicate is enforced"
                }),
            )
            .await
            .expect_err("filter aliases must not overwrite a predicate");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        assert!(queries.lock().expect("recorded queries").is_empty());
    }

    #[tokio::test]
    async fn duplicate_mapped_query_fields_fail_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let endpoint = TfpEndpoint::new(
            Arc::new(RecordingQueryExecutor(queries.clone())),
            Arc::new(StubExecutor),
        );

        for shape in [
            json!({"selectItems":["order_number", "orderNumber"]}),
            json!({"groupByItems":["order_number", "orderNumber"]}),
            json!({"orderItems":[
                {"field":"order_number", "direction":"asc"},
                {"field":"orderNumber", "direction":"desc"}
            ]}),
        ] {
            let mut payload = json!({
                "entity":"CustomerOrder", "limitValue":10,
                "commentText":"attempt duplicate mapped fields",
                "purposeText":"prove query semantics remain unambiguous"
            });
            payload
                .as_object_mut()
                .expect("query object")
                .extend(shape.as_object().expect("shape object").clone());
            let error = endpoint
                .handle_query(&trusted_with_generated_wire_metadata(), payload)
                .await
                .expect_err("duplicate mapped fields must fail closed");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }

        assert!(queries.lock().expect("recorded queries").is_empty());
    }

    #[tokio::test]
    async fn unsupported_order_expression_fails_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let query_executor = RecordingQueryExecutor(queries.clone());
        let endpoint = TfpEndpoint::new(Arc::new(query_executor), Arc::new(StubExecutor));
        let error = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "orderItems":[{
                        "field":"id",
                        "expr":{"function":"lower", "arguments":["orderNumber"]},
                        "direction":"asc"
                    }],
                    "limitValue":10,
                    "commentText":"attempt expression ordering",
                    "purposeText":"prove unsupported semantics fail closed"
                }),
            )
            .await
            .expect_err("unsupported order expression must fail closed");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        assert!(queries.lock().expect("recorded queries").is_empty());
    }

    #[tokio::test]
    async fn executes_relation_facet_and_returns_count_alias() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        let response = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "filterCondition":{"id":{"$gt":0}},
                    "facets":[{
                        "facetName":"statusFacet",
                        "relationName":"status",
                        "includeAllFacets":true,
                        "query":{
                            "entity":"OrderStatus",
                            "selectItems":["id","code"],
                            "aggregateItems":[{"function":"Count","field":"id","alias":"orderCount"}],
                            "limitValue":100,
                            "commentText":"load status facet",
                            "purposeText":"render order filters"
                        }
                    }],
                    "limitValue":10,
                    "commentText":"load orders",
                    "purposeText":"render order list"
                }),
            )
            .await
            .expect("facet query");
        let facet = response["facets"]["statusFacet"]
            .as_array()
            .expect("facet array");
        assert_eq!(facet.len(), 2);
        assert_eq!(facet[0]["orderCount"], 2);
        assert_eq!(facet[1]["orderCount"], 0);
    }

    #[tokio::test]
    async fn facet_projection_requires_identity_before_execution() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let endpoint = TfpEndpoint::new(
            Arc::new(RecordingQueryExecutor(queries.clone())),
            Arc::new(StubExecutor),
        );
        let error = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "facets":[{
                        "facetName":"statusFacet", "relationName":"status",
                        "query":{
                            "entity":"OrderStatus", "selectItems":["code"],
                            "aggregateItems":[{
                                "function":"Count", "field":"id", "alias":"orderCount"
                            }],
                            "limitValue":20,
                            "commentText":"load status facet",
                            "purposeText":"render order filters"
                        }
                    }],
                    "limitValue":10,
                    "commentText":"load orders",
                    "purposeText":"render order list"
                }),
            )
            .await
            .expect_err("facet projection without id must fail closed");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        assert!(queries.lock().expect("recorded queries").is_empty());
    }

    #[tokio::test]
    async fn malformed_facet_executor_rows_fail_closed() {
        for malformed in [
            MalformedFacetResult::MissingRelation,
            MalformedFacetResult::MissingCount,
            MalformedFacetResult::NegativeCount,
            MalformedFacetResult::TextCount,
            MalformedFacetResult::DuplicateMembership,
            MalformedFacetResult::MissingNestedId,
        ] {
            let endpoint = TfpEndpoint::new(
                Arc::new(MalformedFacetExecutor(malformed)),
                Arc::new(StubExecutor),
            );
            let error = endpoint
                .handle_query(
                    &trusted(),
                    json!({
                        "entity":"CustomerOrder",
                        "facets":[{
                            "facetName":"statusFacet", "relationName":"status",
                            "query":{
                                "entity":"OrderStatus", "selectItems":["id", "code"],
                                "aggregateItems":[{
                                    "function":"Count", "field":"id", "alias":"orderCount"
                                }],
                                "limitValue":20,
                                "commentText":"load status facet",
                                "purposeText":"render order filters"
                            }
                        }],
                        "limitValue":10,
                        "commentText":"load orders",
                        "purposeText":"render order list"
                    }),
                )
                .await
                .expect_err("malformed facet result must fail closed");
            assert_eq!(error.code(), "TFP_EXECUTION_FAILED");
        }
    }

    #[tokio::test]
    async fn executor_results_must_respect_governed_query_limits() {
        let outer_payload = json!({
            "entity":"CustomerOrder",
            "selectItems":[],
            "limitValue":2,
            "commentText":"load bounded orders",
            "purposeText":"verify executor result limits"
        });
        let exact_endpoint = TfpEndpoint::new(
            Arc::new(OverReturningExecutor(
                OverReturningStage::ExactOuterBoundary,
            )),
            Arc::new(StubExecutor),
        );
        let exact = exact_endpoint
            .handle_query(&trusted(), outer_payload.clone())
            .await
            .expect("rows at the exact governed boundary remain valid");
        assert_eq!(exact["data"].as_array().expect("response data").len(), 2);

        let outer_endpoint = TfpEndpoint::new(
            Arc::new(OverReturningExecutor(OverReturningStage::Outer)),
            Arc::new(StubExecutor),
        );
        let error = outer_endpoint
            .handle_query(&trusted(), outer_payload)
            .await
            .expect_err("an over-returning outer executor must fail closed");
        assert_eq!(error.code(), "TFP_EXECUTION_FAILED");
        assert!(
            error
                .internal_diagnostic()
                .expect("execution detail remains available internally")
                .contains("TFP query executor returned 3 rows")
        );

        for (stage, phase) in [
            (OverReturningStage::Membership, "facet membership"),
            (OverReturningStage::Nested, "facet value"),
        ] {
            let endpoint = TfpEndpoint::new(
                Arc::new(OverReturningExecutor(stage)),
                Arc::new(StubExecutor),
            );
            let error = endpoint
                .handle_query(
                    &trusted(),
                    json!({
                        "entity":"CustomerOrder",
                        "facets":[{
                            "facetName":"statusFacet", "relationName":"status",
                            "query":{
                                "entity":"OrderStatus", "selectItems":["id", "code"],
                                "aggregateItems":[{
                                    "function":"Count", "field":"id", "alias":"orderCount"
                                }],
                                "limitValue":1,
                                "commentText":"load bounded status facet",
                                "purposeText":"verify executor result limits"
                            }
                        }],
                        "limitValue":1,
                        "commentText":"load bounded orders",
                        "purposeText":"verify executor result limits"
                    }),
                )
                .await
                .expect_err("an over-returning facet executor must fail closed");
            assert_eq!(error.code(), "TFP_EXECUTION_FAILED");
            assert!(
                error
                    .internal_diagnostic()
                    .expect("execution detail remains available internally")
                    .contains(phase)
            );
        }
    }

    #[test]
    fn provider_execution_details_are_redacted_from_public_errors() {
        let sensitive = "UNIQUE constraint failed: school_data.tenant_id=42; \
                         SQL=UPDATE school_data SET tenant_id=42";
        let error = TfpEndpointError::ExecutionError(sensitive.into());

        assert_eq!(error.code(), "TFP_EXECUTION_FAILED");
        assert_eq!(error.to_string(), "Data service execution failed");
        assert_eq!(error.public_message(), "Data service execution failed");
        assert_eq!(error.internal_diagnostic(), Some(sensitive));
        for secret in ["UNIQUE", "school_data", "tenant_id", "42", "UPDATE"] {
            assert!(!error.to_string().contains(secret));
            assert!(!error.public_message().contains(secret));
        }
    }

    #[tokio::test]
    async fn facet_values_use_generated_wire_names_and_preserve_count_alias() {
        let mut context = trusted();
        context
            .field_mappings
            .get_mut("OrderStatus")
            .expect("status field policy")
            .insert("display_name".into(), "display_name".into());
        context.wire_metadata.insert(
            "OrderStatus".into(),
            WireEntityMetadata::new(
                BTreeMap::from([
                    ("id".into(), "id".into()),
                    ("code".into(), "code".into()),
                    ("display_name".into(), "displayName".into()),
                ]),
                BTreeMap::new(),
            )
            .expect("status wire metadata"),
        );
        let endpoint = TfpEndpoint::new(Arc::new(WireFacetExecutor), Arc::new(StubExecutor));
        let response = endpoint
            .handle_query(
                &context,
                json!({
                    "entity":"CustomerOrder",
                    "facets":[{
                        "facetName":"statusFacet",
                        "relationName":"status",
                        "query":{
                            "entity":"OrderStatus",
                            "selectItems":["id","displayName"],
                            "aggregateItems":[{"function":"Count","field":"id","alias":"orderCount"}],
                            "limitValue":20,
                            "commentText":"load status facet labels",
                            "purposeText":"render the order status filter"
                        }
                    }],
                    "limitValue":10,
                    "commentText":"load orders",
                    "purposeText":"render the order list"
                }),
            )
            .await
            .expect("wire-mapped facet query");
        let value = &response["facets"]["statusFacet"][0];
        assert_eq!(value["id"], 1001);
        assert_eq!(value["displayName"], "New order");
        assert_eq!(value["orderCount"], 2);
        assert!(value.get("display_name").is_none());
    }

    #[test]
    fn response_mapping_and_aggregate_aliases_fail_closed_on_collisions() {
        let metadata = WireEntityMetadata::new(
            BTreeMap::from([
                ("first_name".into(), "firstName".into()),
                ("legal_name".into(), "legalName".into()),
            ]),
            BTreeMap::new(),
        )
        .expect("wire metadata");
        assert!(
            metadata
                .response_policy_map(&BTreeMap::from([
                    ("first_name".into(), "name".into()),
                    ("legal_name".into(), "name".into()),
                ]))
                .is_err()
        );

        let context = trusted();
        for payload in [
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "aggregateItems":[
                    {"function":"Count","field":"id","alias":"total"},
                    {"function":"Count","field":"id","alias":"total"}
                ]
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "aggregateItems":[{"function":"Count","field":"id","alias":"id"}]
            }),
            json!({
                "entity":"CustomerOrder", "limitValue":10,
                "aggregateItems":[{"function":"Count","field":"id","alias":"bad-name"}]
            }),
        ] {
            let query: TfpSelectQuery = serde_json::from_value(payload).expect("TFP query");
            assert!(validate_policy(&context, &query).is_err());
        }

        let unexpected = teaql_core::CompactRow::from_map(Record::from([(
            "private_value".into(),
            teaql_core::Value::Text("secret".into()),
        )]));
        assert!(
            compact_row_to_wire_json(
                &unexpected,
                &BTreeMap::from([("id".into(), "id".into())]),
                &BTreeSet::new(),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn sql_shape_uses_parameterized_metadata_without_requesting_debug_values() {
        let endpoint =
            TfpEndpoint::new(Arc::new(ParameterizedShapeExecutor), Arc::new(StubExecutor));
        let response = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "limitValue":10,
                    "commentText":"load bounded orders",
                    "purposeText":"verify safe SQL shape"
                }),
            )
            .await
            .expect("parameterized SQL shape response");
        let shape = response["execution"]["sqlShape"]
            .as_str()
            .expect("parameterized query shape");
        assert_eq!(
            shape,
            "SELECT * FROM orders WHERE id = ? AND active = ? AND happened_at = ? AND email = ?"
        );
        for secret in [
            "987654321",
            "TRUE",
            "1787110200123",
            "private-address@example.com",
        ] {
            assert!(!shape.contains(secret));
        }
        assert_eq!(response["execution"]["resultCount"], 1);
        let trace = response["execution"]["trace"]
            .as_array()
            .expect("server-approved trace");
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[0]["entity"], "CustomerOrder");
        assert!(
            trace[0]["comment"]
                .as_str()
                .expect("approved trace comment")
                .contains("approved-purpose=approved-order-search")
        );
        assert!(!response.to_string().contains("forged-provider-trace"));
        assert!(!response.to_string().contains("ForgedEntity"));
    }

    #[tokio::test]
    async fn records_tfp_server_query_mutation_and_failure_lifecycles() {
        let telemetry = Arc::new(RecordingTelemetry::default());
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor))
            .with_runtime_telemetry(telemetry.clone());

        let response = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "_comment":"generated query",
                    "_purpose":"requested purpose", "_limit":10
                }),
            )
            .await
            .expect("query response");
        assert_eq!(response["data"].as_array().map(Vec::len), Some(1));
        endpoint
            .handle_mutation(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "action":"Create",
                    "payload":{"orderNumber":"O-1"},
                    "comment":"create order"
                }),
            )
            .await
            .expect("mutation response");
        let error = endpoint
            .handle_query(&trusted(), json!({"entity":"Other"}))
            .await
            .expect_err("policy failure");
        assert!(matches!(error, TfpEndpointError::TranslationError(_)));

        let events = telemetry.0.lock().expect("events");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].operation.family, "tfp");
        assert_eq!(events[0].operation.name, "server.query");
        assert_eq!(
            events[0].operation.attributes["teaql.tfp.role"],
            "server".into()
        );
        assert_eq!(
            events[0].completion.as_ref().unwrap()["teaql.result.cardinality"],
            1usize.into()
        );
        assert_eq!(events[1].operation.name, "server.mutation");
        assert!(events[1].completion.is_some());
        assert_eq!(events[2].failure.as_deref(), Some("TFP_FORBIDDEN_ENTITY"));
    }

    #[tokio::test]
    async fn mutation_requires_trusted_entity_action_fields_and_audit_reason() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        for payload in [
            json!({"entity":"Other","action":"Create","payload":{},"comment":"x"}),
            json!({"entity":"CustomerOrder","action":"Publish","payload":{},"comment":"x"}),
            json!({"entity":"CustomerOrder","action":"Create","payload":{"secret":"x"},"comment":"x"}),
            json!({"entity":"CustomerOrder","action":"Create","payload":{"orderNumber":"x"},"comment":" "}),
            json!({"entity":"CustomerOrder","action":"Create","payload":{"commerce_platform_id":99},"comment":"x"}),
        ] {
            assert!(endpoint.handle_mutation(&trusted(), payload).await.is_err());
        }
    }

    #[tokio::test]
    async fn invalid_mutation_evidence_fails_before_execution() {
        let mutations = Arc::new(RecordingMutationExecutor::new(1));
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), mutations.clone());
        for (comment, expected_code) in [
            (" ".to_owned(), "TFP_AUDIT_REASON_REQUIRED"),
            (
                "x".repeat(models::MAX_GOVERNANCE_EVIDENCE_BYTES + 1),
                "TFP_INVALID_REQUEST",
            ),
            ("audit\rforged-log-line".to_owned(), "TFP_INVALID_REQUEST"),
        ] {
            let error = endpoint
                .handle_mutation(
                    &trusted(),
                    json!({
                        "entity":"CustomerOrder", "action":"Create",
                        "payload":{"orderNumber":"O-1"}, "comment":comment
                    }),
                )
                .await
                .expect_err("invalid mutation evidence must fail closed");
            assert_eq!(error.code(), expected_code);
        }
        assert!(mutations.ordinary.lock().expect("ordinary").is_empty());
        assert!(mutations.guarded.lock().expect("guarded").is_empty());
    }

    #[tokio::test]
    async fn mutation_lifecycle_requires_id_and_correct_optimistic_version_sign() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        for payload in [
            json!({
                "entity":"CustomerOrder", "action":"Create",
                "expectedVersion":1, "payload":{"orderNumber":"O-1"}, "comment":"create"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Update",
                "payload":{"orderNumber":"O-1"}, "comment":"update"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Update", "id":1,
                "payload":{"orderNumber":"O-1"}, "comment":"update"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Update", "id":1,
                "expectedVersion":-2, "payload":{"orderNumber":"O-1"}, "comment":"update"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Delete", "id":1,
                "expectedVersion":0, "payload":{}, "comment":"delete"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Recover", "id":1,
                "expectedVersion":2, "payload":{}, "comment":"recover"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Recover",
                "expectedVersion":-2, "payload":{}, "comment":"recover"
            }),
        ] {
            let error = endpoint
                .handle_mutation(&trusted(), payload)
                .await
                .expect_err("invalid lifecycle mutation must fail before execution");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }
    }

    #[tokio::test]
    async fn ignored_mutation_inputs_are_rejected_before_execution() {
        let mutations = Arc::new(RecordingMutationExecutor::new(1));
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), mutations.clone());

        let error = endpoint
            .handle_mutation(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "action":"Create", "id":42,
                    "payload":{"orderNumber":"O-42"}, "comment":"create order"
                }),
            )
            .await
            .expect_err("a top-level Create id must not be silently ignored");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        assert!(
            error
                .to_string()
                .contains("Invalid mutation request: Create must not carry id")
        );

        for payload in [
            json!({
                "entity":"CustomerOrder", "action":"Update", "id":{"id":null},
                "expectedVersion":3, "payload":{"orderNumber":"O-42"},
                "comment":"update order"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Delete", "id":[42],
                "expectedVersion":3, "payload":{}, "comment":"delete order"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Recover", "id":true,
                "expectedVersion":-3, "payload":{}, "comment":"recover order"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Update", "id":1.5,
                "expectedVersion":3, "payload":{"orderNumber":"O-42"},
                "comment":"update order"
            }),
        ] {
            let error = endpoint
                .handle_mutation(&trusted(), payload)
                .await
                .expect_err("invalid mutation id must fail before execution");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        }

        for payload in [
            json!({
                "entity":"CustomerOrder", "action":"Delete", "id":42,
                "expectedVersion":3, "payload":{"orderNumber":"ignored before #170"},
                "comment":"delete order"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Recover", "id":42,
                "expectedVersion":-4, "payload":{"orderNumber":"ignored before #170"},
                "comment":"recover order"
            }),
        ] {
            let action = payload["action"].as_str().expect("action").to_owned();
            let error = endpoint
                .handle_mutation(&trusted(), payload)
                .await
                .expect_err("lifecycle values must not be silently ignored");
            assert_eq!(error.code(), "TFP_INVALID_REQUEST");
            assert!(error.to_string().contains(&format!(
                "Invalid mutation request: {action} requires an empty payload"
            )));
        }

        assert!(
            mutations
                .ordinary
                .lock()
                .expect("ordinary mutations")
                .is_empty()
        );
        assert!(
            mutations
                .guarded
                .lock()
                .expect("guarded mutations")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn tenant_field_is_rejected_after_public_field_mapping() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        let mut policy = trusted();
        policy
            .writable_field_mappings
            .get_mut("CustomerOrder")
            .expect("writable policy")
            .insert("tenant".into(), "commerce_platform_id".into());

        let error = endpoint
            .handle_mutation(
                &policy,
                json!({
                    "entity":"CustomerOrder", "action":"Update", "id":42,
                    "payload":{"tenant":99}, "comment":"try tenant transfer"
                }),
            )
            .await
            .expect_err("mapped tenant field must remain server-owned");

        assert!(matches!(error, TfpEndpointError::TranslationError(_)));
        assert_eq!(error.code(), "TFP_POLICY_VIOLATION");
    }

    #[tokio::test]
    async fn non_create_mutations_use_one_atomic_trusted_tenant_guard() {
        let mutations = Arc::new(RecordingMutationExecutor::new(1));
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), mutations.clone());

        for payload in [
            json!({
                "entity":"CustomerOrder", "action":"Update", "id":42,
                "expectedVersion":3, "payload":{"orderNumber":"O-42"},
                "comment":"update order"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Delete", "id":42,
                "expectedVersion":4, "payload":{}, "comment":"delete order"
            }),
            json!({
                "entity":"CustomerOrder", "action":"Recover", "id":42,
                "expectedVersion":-5, "payload":{}, "comment":"recover order"
            }),
        ] {
            endpoint
                .handle_mutation(&trusted(), payload)
                .await
                .expect("guarded mutation");
        }

        assert!(
            mutations
                .ordinary
                .lock()
                .expect("ordinary mutations")
                .is_empty()
        );
        let guarded = mutations.guarded.lock().expect("guarded mutations");
        assert_eq!(guarded.len(), 3);
        let expected = teaql_core::Expr::eq("commerce_platform_id", 1_i64);
        assert!(guarded.iter().all(|request| request.guard == expected));
        let MutationRequest::Update(update) = &guarded[0].mutation else {
            panic!("first request should be update");
        };
        assert_eq!(update.values.get("order_number"), Some(&"O-42".into()));
        assert!(update.values.get("commerce_platform_id").is_none());
    }

    #[tokio::test]
    async fn create_injects_trusted_tenant_and_uses_ordinary_insert() {
        let mutations = Arc::new(RecordingMutationExecutor::new(1));
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), mutations.clone());

        endpoint
            .handle_mutation(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "action":"Create",
                    "payload":{"orderNumber":"O-1"}, "comment":"create order"
                }),
            )
            .await
            .expect("create mutation");

        assert!(
            mutations
                .guarded
                .lock()
                .expect("guarded mutations")
                .is_empty()
        );
        let ordinary = mutations.ordinary.lock().expect("ordinary mutations");
        let MutationRequest::Insert(insert) = &ordinary[0] else {
            panic!("create should use insert");
        };
        assert_eq!(insert.values.get("order_number"), Some(&"O-1".into()));
        assert_eq!(
            insert.values.get("commerce_platform_id"),
            Some(&teaql_core::Value::I64(1))
        );
    }

    #[tokio::test]
    async fn create_requires_exactly_one_affected_row() {
        for affected_rows in [0, 2] {
            let endpoint = TfpEndpoint::new(
                Arc::new(StubExecutor),
                Arc::new(AffectedRowsMutationExecutor(affected_rows)),
            );
            let error = endpoint
                .handle_mutation(
                    &trusted(),
                    json!({
                        "entity":"CustomerOrder", "action":"Create",
                        "payload":{"orderNumber":"O-1"}, "comment":"create order"
                    }),
                )
                .await
                .expect_err("invalid Create affected-row count must fail closed");
            assert_eq!(error.code(), "TFP_EXECUTION_FAILED");
            assert!(
                error
                    .internal_diagnostic()
                    .expect("execution detail remains available internally")
                    .contains("expected exactly one")
            );
        }

        let endpoint = TfpEndpoint::new(
            Arc::new(StubExecutor),
            Arc::new(AffectedRowsMutationExecutor(1)),
        );
        let response = endpoint
            .handle_mutation(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "action":"Create",
                    "payload":{"orderNumber":"O-1"}, "comment":"create order"
                }),
            )
            .await
            .expect("one affected row is a valid Create result");
        assert_eq!(response["affectedRows"], 1);
        assert_eq!(response["data"][0]["id"], 42);
    }

    #[tokio::test]
    async fn mutation_generated_values_are_filtered_and_mapped_to_wire_names() {
        let endpoint = TfpEndpoint::new(
            Arc::new(StubExecutor),
            Arc::new(GeneratedValuesMutationExecutor),
        );
        let response = endpoint
            .handle_mutation(
                &trusted_with_generated_wire_metadata(),
                json!({
                    "entity":"CustomerOrder", "action":"Create",
                    "payload":{"orderNumber":"ORD-42"}, "comment":"create order"
                }),
            )
            .await
            .expect("create mutation");

        assert_eq!(response["affectedRows"], 1);
        assert_eq!(response["data"][0]["id"], 42);
        assert_eq!(response["data"][0]["orderNumber"], "ORD-42");
        assert!(response["data"][0].get("order_number").is_none());
        assert!(response["data"][0].get("commerce_platform_id").is_none());
        assert!(response["data"][0].get("provider_private").is_none());
    }

    #[tokio::test]
    async fn ambiguous_mutation_response_mapping_fails_before_execution() {
        let mutations = Arc::new(RecordingMutationExecutor::new(1));
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), mutations.clone());
        let mut context = trusted_with_generated_wire_metadata();
        context
            .field_mappings
            .get_mut("CustomerOrder")
            .expect("order field policy")
            .insert("status".into(), "order_number".into());

        let error = endpoint
            .handle_mutation(
                &context,
                json!({
                    "entity":"CustomerOrder", "action":"Create",
                    "payload":{"orderNumber":"ORD-42"}, "comment":"create order"
                }),
            )
            .await
            .expect_err("ambiguous response mapping must fail closed");
        assert_eq!(error.code(), "TFP_POLICY_VIOLATION");
        assert!(
            mutations
                .ordinary
                .lock()
                .expect("ordinary mutations")
                .is_empty()
        );
        assert!(
            mutations
                .guarded
                .lock()
                .expect("guarded mutations")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn mutation_alias_collision_fails_before_execution() {
        let mutations = Arc::new(RecordingMutationExecutor::new(1));
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), mutations.clone());
        let mut context = trusted();
        context
            .writable_field_mappings
            .get_mut("CustomerOrder")
            .expect("order writable policy")
            .insert("order_number".into(), "order_number".into());

        let error = endpoint
            .handle_mutation(
                &context,
                json!({
                    "entity":"CustomerOrder", "action":"Update", "id":42,
                    "expectedVersion":3,
                    "payload":{
                        "order_number":"SAFE-42",
                        "orderNumber":"OTHER-42"
                    },
                    "comment":"update order"
                }),
            )
            .await
            .expect_err("mutation aliases must not overwrite a value");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
        assert!(
            mutations
                .ordinary
                .lock()
                .expect("ordinary mutations")
                .is_empty()
        );
        assert!(
            mutations
                .guarded
                .lock()
                .expect("guarded mutations")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn zero_row_guarded_mutation_does_not_disclose_target_ownership() {
        let mutations = Arc::new(RecordingMutationExecutor::new(0));
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), mutations);

        let error = endpoint
            .handle_mutation(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "action":"Update", "id":42,
                    "expectedVersion":3, "payload":{"orderNumber":"O-42"},
                    "comment":"update order"
                }),
            )
            .await
            .expect_err("unavailable target must fail closed");

        assert!(matches!(error, TfpEndpointError::MutationTargetUnavailable));
        assert_eq!(error.code(), "TFP_MUTATION_TARGET_UNAVAILABLE");
        assert!(!error.to_string().contains("tenant"));
        assert!(!error.to_string().contains("42"));
    }

    #[test]
    fn wire_metadata_matches_typescript_fixture_and_rejects_collisions() {
        let metadata = WireEntityMetadata::new(
            BTreeMap::from([
                ("user_url".into(), "userUrl".into()),
                ("school_type".into(), "schoolType".into()),
            ]),
            BTreeMap::from([("legacyUrl".into(), "user_url".into())]),
        )
        .unwrap();
        let submitted = json!({"legacyUrl":"https://teaql.io","schoolType":1001});
        let normalized = normalize_wire_object(submitted.as_object().unwrap(), &metadata).unwrap();
        assert_eq!(normalized.values["user_url"], json!("https://teaql.io"));
        assert_eq!(normalized.values["school_type"], json!(1001));
        assert_eq!(normalized.source_instance_paths["user_url"], "/legacyUrl");
        assert_eq!(
            normalized.source_instance_paths["school_type"],
            "/schoolType"
        );
        let mut violations = vec![teaql_runtime::CheckResult::required(
            teaql_runtime::ObjectLocation::hash_root("user_url"),
        )];
        retain_submitted_paths(&mut violations, &normalized);
        assert_eq!(
            violations[0].source_instance_path.as_deref(),
            Some("/legacyUrl")
        );

        let collision = json!({"userUrl":"a","legacyUrl":"a"});
        assert_eq!(
            normalize_wire_object(collision.as_object().unwrap(), &metadata)
                .unwrap_err()
                .code(),
            "WIRE_FIELD_COLLISION"
        );
        let unknown = json!({"unknown":1});
        assert_eq!(
            normalize_wire_object(unknown.as_object().unwrap(), &metadata)
                .unwrap_err()
                .code(),
            "WIRE_UNKNOWN_FIELD"
        );
    }
}
