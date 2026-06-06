use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
                ctx.put_local("trace_id", trace_str);
            }
        }

        Ok(TeaContext(ctx))
    }
}
