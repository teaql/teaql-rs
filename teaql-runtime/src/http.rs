//! Context-owned, provider-neutral calls to trusted outbound HTTP profiles.
//! A transport implementation owns the profile registry, client pool, limits,
//! and retry policy; callers cannot provide an arbitrary destination URL.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::UserContext;

/// Cooperative cancellation shared by a caller and one or more HTTP calls.
/// Cancelling a call drops its transport future, including an active response
/// body read, a rate-limit wait, or retry backoff.
#[derive(Clone, Debug, Default)]
pub struct HttpCancellation {
    token: tokio_util::sync::CancellationToken,
}

impl HttpCancellation {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.token.cancel();
    }
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpIntentKind {
    Purpose,
    Audit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryMode {
    None,
    ConfirmedIdempotent,
}

/// Public failures deliberately contain no URL, header, body, or provider cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpFailure {
    InvalidInput,
    ProfileNotConfigured,
    Timeout,
    Cancelled,
    RateLimited,
    CircuitOpen,
    ResponseTooLarge,
    MalformedResponse,
    AmbiguousOutcome,
    UpstreamFailure,
}

impl std::fmt::Display for HttpFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "HTTP {}",
            match self {
                Self::InvalidInput => "invalid input",
                Self::ProfileNotConfigured => "profile not configured",
                Self::Timeout => "timeout",
                Self::Cancelled => "cancelled",
                Self::RateLimited => "rate limited",
                Self::CircuitOpen => "circuit open",
                Self::ResponseTooLarge => "response too large",
                Self::MalformedResponse => "malformed response",
                Self::AmbiguousOutcome => "ambiguous outcome",
                Self::UpstreamFailure => "upstream failure",
            }
        )
    }
}

impl std::error::Error for HttpFailure {}

#[derive(Clone, Eq, PartialEq)]
pub struct HttpCall {
    profile: String,
    method: HttpMethod,
    relative_path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    content_type: Option<String>,
    comment: String,
    intent_kind: HttpIntentKind,
    intent: String,
    retry_mode: HttpRetryMode,
}

impl std::fmt::Debug for HttpCall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpCall")
            .field("profile", &self.profile)
            .field("method", &self.method)
            .field("details", &"masked")
            .finish()
    }
}

impl HttpCall {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        profile: impl Into<String>,
        method: HttpMethod,
        relative_path: impl Into<String>,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
        content_type: Option<String>,
        comment: impl Into<String>,
        intent_kind: HttpIntentKind,
        intent: impl Into<String>,
        retry_mode: HttpRetryMode,
    ) -> Result<Self, HttpFailure> {
        let call = Self {
            profile: profile.into(),
            method,
            relative_path: relative_path.into(),
            headers,
            body,
            content_type,
            comment: comment.into(),
            intent_kind,
            intent: intent.into(),
            retry_mode,
        };
        call.validate()?;
        Ok(call)
    }

    pub fn get(
        profile: impl Into<String>,
        path: impl Into<String>,
        comment: impl Into<String>,
        purpose: impl Into<String>,
    ) -> Result<Self, HttpFailure> {
        Self::new(
            profile,
            HttpMethod::Get,
            path,
            BTreeMap::new(),
            Vec::new(),
            None,
            comment,
            HttpIntentKind::Purpose,
            purpose,
            HttpRetryMode::None,
        )
    }

    pub fn post_text(
        profile: impl Into<String>,
        path: impl Into<String>,
        body: impl Into<String>,
        comment: impl Into<String>,
        purpose: impl Into<String>,
    ) -> Result<Self, HttpFailure> {
        Self::new(
            profile,
            HttpMethod::Post,
            path,
            BTreeMap::new(),
            body.into().into_bytes(),
            Some("text/plain; charset=utf-8".to_owned()),
            comment,
            HttpIntentKind::Purpose,
            purpose,
            HttpRetryMode::None,
        )
    }

    pub fn with_retry_mode(mut self, retry_mode: HttpRetryMode) -> Self {
        self.retry_mode = retry_mode;
        self
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }
    pub fn method(&self) -> HttpMethod {
        self.method
    }
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn intent_kind(&self) -> HttpIntentKind {
        self.intent_kind
    }
    pub fn intent(&self) -> &str {
        &self.intent
    }
    pub fn retry_mode(&self) -> HttpRetryMode {
        self.retry_mode
    }

    pub fn validate(&self) -> Result<(), HttpFailure> {
        let mut chars = self.profile.chars();
        if self.profile.len() > 64
            || !chars.next().is_some_and(|c| c.is_ascii_alphabetic())
            || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            || self.relative_path.trim().is_empty()
            || self.comment.trim().is_empty()
            || self.intent.trim().is_empty()
            || (matches!(self.method, HttpMethod::Get | HttpMethod::Delete)
                && !self.body.is_empty())
            || self
                .content_type
                .as_ref()
                .is_some_and(|value| contains_control(value))
            || self
                .headers
                .iter()
                .any(|(name, value)| !safe_header(name, value))
        {
            return Err(HttpFailure::InvalidInput);
        }
        Ok(())
    }
}

pub fn contains_control(value: &str) -> bool {
    value.chars().any(|c| c.is_control())
}

fn safe_header(name: &str, value: &str) -> bool {
    let forbidden = ["host", "connection", "content-length", "transfer-encoding"];
    !name.is_empty()
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c))
        && !forbidden
            .iter()
            .any(|entry| name.eq_ignore_ascii_case(entry))
        && !contains_control(value)
}

#[derive(Clone, Eq, PartialEq)]
pub struct HttpResponse {
    status: u16,
    body: Vec<u8>,
    attempts: u8,
}

impl std::fmt::Debug for HttpResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("attempts", &self.attempts)
            .field("body", &"masked")
            .finish()
    }
}

impl HttpResponse {
    pub fn new(status: u16, body: Vec<u8>, attempts: u8) -> Result<Self, HttpFailure> {
        if !(100..=599).contains(&status) || attempts == 0 {
            return Err(HttpFailure::InvalidInput);
        }
        Ok(Self {
            status,
            body,
            attempts,
        })
    }
    pub fn status(&self) -> u16 {
        self.status
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    pub fn attempts(&self) -> u8 {
        self.attempts
    }
    pub fn text(&self) -> Result<&str, HttpFailure> {
        std::str::from_utf8(&self.body).map_err(|_| HttpFailure::MalformedResponse)
    }
    pub fn json_value(&self) -> Result<serde_json::Value, HttpFailure> {
        serde_json::from_slice(&self.body).map_err(|_| HttpFailure::MalformedResponse)
    }
}

#[async_trait]
pub trait HttpRequestService: Send + Sync {
    async fn execute(
        &self,
        context: &UserContext,
        call: &HttpCall,
    ) -> Result<HttpResponse, HttpFailure>;

    async fn execute_cancellable(
        &self,
        context: &UserContext,
        call: &HttpCall,
        cancellation: &HttpCancellation,
    ) -> Result<HttpResponse, HttpFailure> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(HttpFailure::Cancelled),
            result = self.execute(context, call) => result,
        }
    }
}

pub(crate) struct HttpRequestServiceResource(pub Arc<dyn HttpRequestService>);

#[cfg(test)]
mod tests {
    use super::*;

    struct CountingService(std::sync::atomic::AtomicUsize);

    #[async_trait]
    impl HttpRequestService for CountingService {
        async fn execute(
            &self,
            _context: &UserContext,
            _call: &HttpCall,
        ) -> Result<HttpResponse, HttpFailure> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            HttpResponse::new(200, Vec::new(), 1)
        }
    }

    #[tokio::test]
    async fn missing_service_and_pre_cancelled_call_fail_before_transport() {
        let context = UserContext::new();
        assert!(matches!(
            context.http_requests(),
            Err(HttpFailure::ProfileNotConfigured)
        ));
        let service = CountingService(std::sync::atomic::AtomicUsize::new(0));
        let cancellation = HttpCancellation::new();
        cancellation.cancel();
        let call = HttpCall::get("orders", "orders/1", "load order", "render order").unwrap();
        assert_eq!(
            service
                .execute_cancellable(&context, &call, &cancellation)
                .await,
            Err(HttpFailure::Cancelled)
        );
        assert_eq!(service.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn request_and_response_debug_do_not_expose_secrets() {
        let call = HttpCall::post_text(
            "orders",
            "orders/1?token=PRIVATE",
            "PRIVATE-BODY",
            "submit order",
            "deliver order",
        )
        .unwrap();
        assert!(!format!("{call:?}").contains("PRIVATE"));
        let response = HttpResponse::new(200, b"PRIVATE-RESPONSE".to_vec(), 1).unwrap();
        assert!(!format!("{response:?}").contains("PRIVATE"));
        assert_eq!(response.text().unwrap(), "PRIVATE-RESPONSE");
        assert_eq!(HttpFailure::InvalidInput.to_string(), "HTTP invalid input");
    }
}
