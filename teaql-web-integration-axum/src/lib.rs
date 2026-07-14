use axum::{
    Json,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use teaql_core::SmartList;
use teaql_runtime::UserContext;

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
                .map(|record| teaql_core::record_to_json_value(record))
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

/// A wrapper around standard errors to provide automatic HTTP status code mapping.
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
#[axum::async_trait]
impl<S> FromRequestParts<S> for TeaContext
where
    S: ContextProvider + Send + Sync,
{
    type Rejection = AxumTeaError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let mut ctx = state.build_context();

        if let Some(user_id) = parts.headers.get("X-User-Id") {
            if let Ok(id_str) = user_id.to_str() {
                ctx.set_user_identifier(id_str);
            }
        }

        if let Some(trace_id) = parts.headers.get("X-Trace-Id") {
            if let Ok(trace_str) = trace_id.to_str() {
                ctx.set_trace_id(trace_str);
            }
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

        ctx.insert_resource(web_info);

        Ok(TeaContext(ctx))
    }
}
