use axum::{
    Json,
    extract::FromRequestParts,
    http::{StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use teaql_core::SmartList;
use teaql_runtime::{
    UserContext, ViewCachePolicy, ViewEntity, ViewErrorCode, ViewInput, ViewOptions,
    ViewPreparationError,
};

/// A unified WebResponse struct that perfectly aligns with the Java legacy API.
/// This ensures frontend clients and AI agents receive the exact same JSON structure.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebResponse<T> {
    pub data: Vec<T>,
    pub result_code: i32,
    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    pub record_count: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<HashMap<String, serde_json::Value>>,

    pub version: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl<T> WebResponse<T> {
    /// Base success state
    pub fn success() -> Self {
        Self {
            data: Vec::new(),
            result_code: 0,
            status: "YES".to_string(),
            message: None,
            record_count: 0,
            facets: None,
            version: "1.001".to_string(),
            trace_id: None,
        }
    }

    /// Base fail state
    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            data: Vec::new(),
            result_code: 1,
            status: "NO".to_string(),
            message: Some(message.into()),
            record_count: 0,
            facets: None,
            version: "1.001".to_string(),
            trace_id: None,
        }
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    pub fn with_facets(mut self, facets: HashMap<String, serde_json::Value>) -> Self {
        self.facets = Some(facets);
        self
    }

    pub fn with_record_count(mut self, count: usize) -> Self {
        self.record_count = count;
        self
    }

    pub fn of_single(entity: T) -> Self {
        Self {
            data: vec![entity],
            record_count: 1,
            ..Self::success()
        }
    }

    pub fn of_list(list: Vec<T>) -> Self {
        let count = list.len();
        Self {
            data: list,
            record_count: count,
            ..Self::success()
        }
    }

    pub fn from_smart_list(mut smart_list: SmartList<T>) -> Self {
        let count = smart_list.total_count_or_len() as usize;
        let mut facets = HashMap::new();
        for (key, facet_list) in smart_list.take_facets() {
            let data: Vec<_> = facet_list
                .data
                .iter()
                .map(teaql_core::compact_row_to_json_value)
                .collect();
            facets.insert(key, serde_json::Value::Array(data));
        }

        let mut response = Self::of_list(smart_list.data).with_record_count(count);
        if !facets.is_empty() {
            response = response.with_facets(facets);
        }
        response
    }
}

impl<T: Serialize> IntoResponse for WebResponse<T> {
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}

/// Explicit async edge for externally rendered business entities. The borrowed
/// entity adapter carries canonical field identity; serializing it first would
/// erase the information needed for authorization and Blob projection.
impl WebResponse<&dyn ViewEntity> {
    pub async fn prepare_and_render(
        self,
        context: &UserContext,
        options: &ViewOptions,
    ) -> Result<Response, ViewPreparationError> {
        if self.result_code != 0 || self.status != "YES" || self.facets.is_some() {
            return Err(ViewPreparationError {
                code: ViewErrorCode::ResultUnsupported,
                business_completion: options.business_completion(),
            });
        }
        if self
            .data
            .iter()
            .any(|entity| context.entity(&entity.target().entity).is_none())
        {
            return Err(ViewPreparationError {
                code: ViewErrorCode::CapabilityMissing,
                business_completion: options.business_completion(),
            });
        }
        let input = ViewInput::Array(self.data.into_iter().map(ViewInput::Entity).collect());
        let prepared = context.prepare_view(input, options).await?;
        // Final current-context check precedes the only response serialization.
        context.reuse_prepared_view(&prepared, options).await?;
        let data = prepared
            .value()
            .as_array()
            .ok_or(ViewPreparationError {
                code: ViewErrorCode::PreparationFailed,
                business_completion: prepared.business_completion(),
            })?
            .clone();
        let credentials = prepared.document_credentials().cloned();
        let cache = prepared.cache_policy_at(chrono::Utc::now());
        let response = WebResponse {
            data,
            result_code: self.result_code,
            status: self.status,
            message: self.message,
            record_count: self.record_count,
            facets: None,
            version: self.version,
            trace_id: self.trace_id,
        };
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct PreparedEnvelope {
            #[serde(flatten)]
            response: WebResponse<serde_json::Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            document_credentials: Option<serde_json::Value>,
        }
        let mut rendered = Json(PreparedEnvelope {
            response,
            document_credentials: credentials,
        })
        .into_response();
        let cache_header = match cache {
            ViewCachePolicy::NoStore => header::HeaderValue::from_static("private, no-store"),
            ViewCachePolicy::PrivateMaxAge(lifetime) => {
                header::HeaderValue::from_str(&format!("private, max-age={}", lifetime.as_secs()))
                    .expect("numeric cache lifetime is a valid header")
            }
        };
        rendered
            .headers_mut()
            .insert(header::CACHE_CONTROL, cache_header);
        Ok(rendered)
    }
}

/// A wrapper around standard errors to provide automatic HTTP status code mapping.
#[derive(Debug)]
pub struct AxumTeaError(pub String);

impl<E: std::error::Error> From<E> for AxumTeaError {
    fn from(err: E) -> Self {
        AxumTeaError(err.to_string())
    }
}

impl IntoResponse for AxumTeaError {
    fn into_response(self) -> Response {
        // Wrap the error inside the standard WebResponse structure
        let web_response: WebResponse<()> = WebResponse::fail(self.0);

        // Return as JSON with the mapped status code
        (StatusCode::INTERNAL_SERVER_ERROR, Json(web_response)).into_response()
    }
}

/// A trait that user's State must implement to build a fully configured UserContext
pub trait ContextProvider {
    fn build_context(&self) -> UserContext;
}

/// Web Request Information injected by Axum Extractor
#[derive(Debug, Clone)]
pub struct WebRequestInfo {
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
    pub request_uri: String,
    pub method: String,
}

/// Extension trait for UserContext to access HTTP/Web details
pub trait WebContextExt {
    fn web_info(&self) -> Option<&WebRequestInfo>;
    fn client_ip(&self) -> Option<&str>;
    fn request_uri(&self) -> Option<&str>;
    fn method(&self) -> Option<&str>;
}

impl WebContextExt for UserContext {
    fn web_info(&self) -> Option<&WebRequestInfo> {
        self.get_resource::<WebRequestInfo>()
    }

    fn client_ip(&self) -> Option<&str> {
        self.web_info().and_then(|info| info.client_ip.as_deref())
    }

    fn request_uri(&self) -> Option<&str> {
        self.web_info().map(|info| info.request_uri.as_str())
    }

    fn method(&self) -> Option<&str> {
        self.web_info().map(|info| info.method.as_str())
    }
}

/// To bypass the Rust Orphan Rule, we wrap UserContext in our own TeaContext
pub struct TeaContext(pub UserContext);

/// Extractor implementation for TeaContext
impl<S> FromRequestParts<S> for TeaContext
where
    S: ContextProvider + Send + Sync,
{
    type Rejection = AxumTeaError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let mut context = state.build_context();

        if let Some(user_id) = parts.headers.get("X-User-Id")
            && let Ok(id_str) = user_id.to_str()
        {
            context.set_user_identifier(id_str);
        }

        if let Some(trace_id) = parts.headers.get("X-Trace-Id")
            && let Ok(trace_str) = trace_id.to_str()
        {
            context.set_trace_id(trace_str);
        }

        // Build WebRequestInfo
        let client_ip = parts
            .headers
            .get("X-Forwarded-For")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.split(',').next().unwrap_or("").trim().to_string());

        let user_agent = parts
            .headers
            .get("User-Agent")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        let web_info = WebRequestInfo {
            client_ip,
            user_agent,
            request_uri: parts.uri.to_string(),
            method: parts.method.as_str().to_string(),
        };

        context.insert_resource(web_info);

        Ok(TeaContext(context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use teaql_core::{
        BlobFieldSpec, BlobRef, DataType, EntityDescriptor, PropertyDescriptor, Value,
    };
    use teaql_runtime::{
        AuthorizedResponseGraph, BlobViewProjection, BlobViewRead, CurrentBlobViewValidation,
        CurrentDocumentViewValidation, DocumentContext, DocumentViewProjection,
        InMemoryMetadataStore, ReferenceDocumentScope, TrustedReferencePrincipal,
        ViewAuthorization, ViewBlobContributor, ViewDocumentContributor, ViewEntityTarget,
        ViewField, ViewFieldDecision, ViewIdentity, ViewPath, ViewPreparationService,
    };

    struct MockContextProvider;
    impl ContextProvider for MockContextProvider {
        fn build_context(&self) -> UserContext {
            UserContext::new()
        }
    }

    #[tokio::test]
    async fn test_teacontext_request_header_extraction() {
        let request = Request::builder()
            .method("POST")
            .uri("/api/test")
            .header("X-User-Id", "user123")
            .header("X-Trace-Id", "trace456")
            .header("X-Forwarded-For", "192.168.1.1, 10.0.0.1")
            .header("User-Agent", "TestAgent/1.0")
            .body(())
            .unwrap();

        let (mut parts, _) = request.into_parts();
        let state = MockContextProvider;

        let tea_context_result = TeaContext::from_request_parts(&mut parts, &state).await;
        assert!(tea_context_result.is_ok());

        let tea_context = tea_context_result.unwrap();
        let context = tea_context.0;

        assert_eq!(context.user_identifier(), Some("user123"));
        assert_eq!(context.trace_id(), "trace456");

        let web_info = context.web_info().unwrap();
        assert_eq!(web_info.client_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(web_info.user_agent.as_deref(), Some("TestAgent/1.0"));
        assert_eq!(web_info.request_uri, "/api/test");
        assert_eq!(web_info.method, "POST");

        assert_eq!(context.client_ip(), Some("192.168.1.1"));
        assert_eq!(context.request_uri(), Some("/api/test"));
        assert_eq!(context.method(), Some("POST"));
    }

    struct Policy;
    impl ViewAuthorization for Policy {
        fn identity(&self, _: &UserContext) -> Result<ViewIdentity, ViewErrorCode> {
            ViewIdentity::new("web", "actor", "tenant")
        }
        fn can_read_entity(
            &self,
            _: &UserContext,
            _: &ViewEntityTarget,
            _: &ViewPath,
        ) -> Result<bool, ViewErrorCode> {
            Ok(true)
        }
        fn field(
            &self,
            _: &UserContext,
            _: &ViewEntityTarget,
            field: &str,
            _: &ViewPath,
        ) -> Result<ViewFieldDecision, ViewErrorCode> {
            Ok(if field == "private_note" {
                ViewFieldDecision::Hidden
            } else {
                ViewFieldDecision::value()
            })
        }
    }
    struct Entity {
        key: Value,
        id: Value,
    }
    impl ViewEntity for Entity {
        fn target(&self) -> ViewEntityTarget {
            ViewEntityTarget {
                entity: "Asset".into(),
                id: Some(42),
                revision: Some(1),
            }
        }
        fn loaded_fields(&self) -> Vec<ViewField> {
            ["id", "document", "private_note"]
                .into_iter()
                .map(|field| ViewField::new(field, field))
                .collect()
        }
        fn read_field(&self, field: &str) -> Result<ViewInput<'_>, ViewErrorCode> {
            match field {
                "id" => Ok(ViewInput::Value(&self.id)),
                "document" => Ok(ViewInput::Value(&self.key)),
                "private_note" => panic!("hidden getter must not run"),
                _ => Err(ViewErrorCode::ResultUnsupported),
            }
        }
    }
    struct BlobSpy {
        signed: Arc<AtomicUsize>,
        fail: Arc<AtomicBool>,
    }
    struct BlobValidation;
    struct DocumentSpy;
    struct DocumentValidation;
    #[async_trait::async_trait]
    impl CurrentDocumentViewValidation for DocumentValidation {
        async fn validate(
            &self,
            _: &UserContext,
            _: &DocumentContext,
        ) -> Result<(), ViewErrorCode> {
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl ViewDocumentContributor for DocumentSpy {
        async fn prepare(
            &self,
            _: &UserContext,
            graph: &AuthorizedResponseGraph,
            _: &ViewOptions,
        ) -> Result<DocumentViewProjection, ViewErrorCode> {
            Ok(DocumentViewProjection {
                graph: graph.clone(),
                context: DocumentContext {
                    principal: TrustedReferencePrincipal::new("oidc", "actor", "Tenant", 7)
                        .unwrap(),
                    scope: ReferenceDocumentScope::new("doc", "edit", "Asset", 42, 1).unwrap(),
                    expires_at: chrono::Utc::now() + chrono::TimeDelta::seconds(40),
                },
                credentials: serde_json::json!({"documentToken":"opaque-test-token"}),
                validation: Arc::new(DocumentValidation),
            })
        }
    }
    #[async_trait::async_trait]
    impl CurrentBlobViewValidation for BlobValidation {
        async fn validate(
            &self,
            _: &UserContext,
            _: &ViewOptions,
            _: Option<&DocumentContext>,
        ) -> Result<(), ViewErrorCode> {
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl ViewBlobContributor for BlobSpy {
        async fn project(
            &self,
            _: &UserContext,
            graph: &AuthorizedResponseGraph,
            _: &ViewOptions,
            _: Option<&DocumentContext>,
        ) -> Result<BlobViewProjection, ViewErrorCode> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(ViewErrorCode::PreparationFailed);
            }
            self.signed.fetch_add(1, Ordering::SeqCst);
            Ok(BlobViewProjection {
                reads: graph
                    .blobs()
                    .iter()
                    .map(|blob| BlobViewRead {
                        reference: blob.reference.clone(),
                        url: "https://storage.example/read?sig=fixture".into(),
                        expires_at: chrono::Utc::now() + chrono::TimeDelta::seconds(30),
                        file_name: "sample.pdf".into(),
                        content_type: "application/pdf".into(),
                        size_bytes: 7,
                        descriptor: false,
                        max_cache_age: Duration::from_secs(15),
                        context_checked: false,
                    })
                    .collect(),
                validation: Arc::new(BlobValidation),
            })
        }
    }
    fn view_context(signed: Arc<AtomicUsize>, fail: Arc<AtomicBool>) -> UserContext {
        let metadata = InMemoryMetadataStore::new().with_entity(
            EntityDescriptor::new("Asset")
                .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                .property(
                    PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(
                        BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap(),
                    ),
                )
                .property(PropertyDescriptor::new("private_note", DataType::Text)),
        );
        let mut context = UserContext::new().with_metadata(metadata);
        ViewPreparationService::new(Arc::new(Policy))
            .with_blob_contributor(Arc::new(BlobSpy { signed, fail }))
            .install_into(&mut context)
            .unwrap();
        context
    }

    #[tokio::test]
    async fn async_entity_web_response_projects_once_and_emits_private_cache() {
        let signed = Arc::new(AtomicUsize::new(0));
        let context = view_context(signed.clone(), Arc::new(AtomicBool::new(false)));
        let entity = Entity {
            key: Value::BlobRef(BlobRef::new("blob_secret_key").unwrap()),
            id: Value::U64(42),
        };
        let options = ViewOptions::read_only().with_cache_lifetime(Duration::from_secs(25));
        let response = WebResponse::of_single(&entity as &dyn ViewEntity)
            .with_record_count(91)
            .with_trace_id("trace-1")
            .prepare_and_render(&context, &options)
            .await
            .unwrap();
        let cache = response
            .headers()
            .get(header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cache.starts_with("private, max-age="));
        let age: u64 = cache
            .trim_start_matches("private, max-age=")
            .parse()
            .unwrap();
        assert!(age > 0 && age <= 15);
        let body = axum::body::to_bytes(response.into_body(), 64_000)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["data"][0]["document"],
            "https://storage.example/read?sig=fixture"
        );
        assert!(json["data"][0].get("private_note").is_none());
        assert_eq!(json["recordCount"], 91);
        assert_eq!(json["traceId"], "trace-1");
        assert!(!String::from_utf8_lossy(&body).contains("blob_secret_key"));
        assert_eq!(
            entity.key,
            Value::BlobRef(BlobRef::new("blob_secret_key").unwrap())
        );
        assert_eq!(signed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unsupported_facets_and_post_commit_projection_failure_never_render_raw_key() {
        let signed = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let context = view_context(signed.clone(), fail.clone());
        let entity = Entity {
            key: Value::BlobRef(BlobRef::new("blob_secret_key").unwrap()),
            id: Value::U64(42),
        };
        let response = WebResponse::of_single(&entity as &dyn ViewEntity).with_facets(
            HashMap::from([("unsafe".into(), serde_json::json!("blob_secret_key"))]),
        );
        assert_eq!(
            response
                .prepare_and_render(&context, &ViewOptions::read_only())
                .await
                .unwrap_err()
                .code,
            ViewErrorCode::ResultUnsupported
        );
        assert_eq!(signed.load(Ordering::SeqCst), 0);
        let malformed = Entity {
            key: Value::Text("blob_secret_key".into()),
            id: Value::U64(42),
        };
        assert_eq!(
            WebResponse::of_single(&malformed as &dyn ViewEntity)
                .prepare_and_render(&context, &ViewOptions::read_only())
                .await
                .unwrap_err()
                .code,
            ViewErrorCode::ResultUnsupported,
        );
        assert_eq!(signed.load(Ordering::SeqCst), 0);
        fail.store(true, Ordering::SeqCst);
        let error = WebResponse::of_single(&entity as &dyn ViewEntity)
            .prepare_and_render(&context, &ViewOptions::read_only().after_commit())
            .await
            .unwrap_err();
        assert_eq!(error.code, ViewErrorCode::PreparationFailed);
        assert_eq!(
            error.business_completion,
            teaql_runtime::BusinessCompletion::Committed
        );
        assert_eq!(signed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn governed_web_response_carries_credentials_after_authorized_projection() {
        let signed = Arc::new(AtomicUsize::new(0));
        // The host assembles the document and Blob contributors in one service.
        let metadata = InMemoryMetadataStore::new().with_entity(
            EntityDescriptor::new("Asset")
                .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                .property(
                    PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(
                        BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap(),
                    ),
                )
                .property(PropertyDescriptor::new("private_note", DataType::Text)),
        );
        let mut context = UserContext::new()
            .with_metadata(metadata)
            .with_trusted_reference_principal(
                TrustedReferencePrincipal::new("oidc", "actor", "Tenant", 7).unwrap(),
            );
        ViewPreparationService::new(Arc::new(Policy))
            .with_document_contributor(Arc::new(DocumentSpy))
            .with_blob_contributor(Arc::new(BlobSpy {
                signed: signed.clone(),
                fail: Arc::new(AtomicBool::new(false)),
            }))
            .install_into(&mut context)
            .unwrap();
        let entity = Entity {
            key: Value::BlobRef(BlobRef::new("blob_secret_key").unwrap()),
            id: Value::U64(42),
        };
        let response = WebResponse::of_single(&entity as &dyn ViewEntity)
            .prepare_and_render(&context, &ViewOptions::governed())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 64_000)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["documentCredentials"]["documentToken"],
            "opaque-test-token"
        );
        assert_eq!(
            json["data"][0]["document"],
            "https://storage.example/read?sig=fixture"
        );
        assert!(!String::from_utf8_lossy(&body).contains("blob_secret_key"));
        assert_eq!(signed.load(Ordering::SeqCst), 1);
    }
}
