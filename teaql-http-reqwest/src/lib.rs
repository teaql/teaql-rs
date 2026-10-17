//! Reqwest transport for Context-owned, named TeaQL HTTP service profiles.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use teaql_runtime::{
    HttpCall, HttpFailure, HttpMethod, HttpRequestService, HttpResponse, HttpRetryMode,
    RuntimeAttributeValue, RuntimeOperation, UserContext,
};
use url::Url;

const MAX_DURATION: Duration = Duration::from_secs(3600);

fn valid_duration(value: Duration) -> bool {
    !value.is_zero() && value <= MAX_DURATION
}

fn valid_alias(value: &str) -> bool {
    let mut chars = value.chars();
    value.len() <= 64
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Trusted startup configuration, never selected by an untrusted request.
#[derive(Clone)]
pub struct HttpServiceProfile {
    alias: String,
    base: Url,
    trusted_roots: Vec<reqwest::Certificate>,
    connect_timeout: Duration,
    request_timeout: Duration,
    total_timeout: Duration,
    max_response_bytes: usize,
    max_attempts: u8,
    retry_delay: Duration,
    retryable_methods: BTreeSet<HttpMethod>,
    circuit_failures: u32,
    circuit_open: Duration,
    half_open_probes: u32,
    rate_per_second: u32,
}

impl std::fmt::Debug for HttpServiceProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpServiceProfile")
            .field("alias", &self.alias)
            .field("endpoint", &"masked")
            .finish()
    }
}

impl HttpServiceProfile {
    pub fn new(alias: impl Into<String>, mut base: Url) -> Result<Self, HttpFailure> {
        let alias = alias.into();
        if !valid_alias(&alias)
            || !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.path().contains('%')
        {
            return Err(HttpFailure::InvalidInput);
        }
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        Ok(Self {
            alias,
            base,
            trusted_roots: Vec::new(),
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(10),
            max_response_bytes: 1024 * 1024,
            max_attempts: 1,
            retry_delay: Duration::from_millis(100),
            retryable_methods: BTreeSet::new(),
            circuit_failures: 0,
            circuit_open: Duration::from_secs(10),
            half_open_probes: 1,
            rate_per_second: 0,
        })
    }

    pub fn budgets(
        mut self,
        connect: Duration,
        request: Duration,
        total: Duration,
    ) -> Result<Self, HttpFailure> {
        if ![connect, request, total].into_iter().all(valid_duration) {
            return Err(HttpFailure::InvalidInput);
        }
        self.connect_timeout = connect;
        self.request_timeout = request;
        self.total_timeout = total;
        Ok(self)
    }

    pub fn max_response_bytes(mut self, value: usize) -> Result<Self, HttpFailure> {
        if value == 0 || value > 16 * 1024 * 1024 {
            return Err(HttpFailure::InvalidInput);
        }
        self.max_response_bytes = value;
        Ok(self)
    }

    /// Replace this profile's custom trust set with exactly one PEM root.
    pub fn trusted_root_pem(self, pem: &[u8]) -> Result<Self, HttpFailure> {
        self.trusted_roots_pem(&[pem])
    }

    /// Explicit short overlap for CA rotation; custom roots replace system roots
    /// for this profile without disabling peer or hostname verification.
    pub fn trusted_roots_pem(mut self, pems: &[&[u8]]) -> Result<Self, HttpFailure> {
        if self.base.scheme() != "https" || pems.is_empty() || pems.len() > 4 {
            return Err(HttpFailure::InvalidInput);
        }
        let mut parsed = Vec::with_capacity(pems.len());
        let mut seen = BTreeSet::new();
        let mut store = rustls::RootCertStore::empty();
        for pem in pems {
            if pem.is_empty() || pem.len() > 64 * 1024 {
                return Err(HttpFailure::InvalidInput);
            }
            let mut certificates = CertificateDer::pem_slice_iter(pem);
            let der = certificates
                .next()
                .ok_or(HttpFailure::InvalidInput)?
                .map_err(|_| HttpFailure::InvalidInput)?;
            if certificates.next().is_some() || !seen.insert(der.as_ref().to_vec()) {
                return Err(HttpFailure::InvalidInput);
            }
            store
                .add(der.clone())
                .map_err(|_| HttpFailure::InvalidInput)?;
            parsed.push(
                reqwest::Certificate::from_der(der.as_ref())
                    .map_err(|_| HttpFailure::InvalidInput)?,
            );
        }
        self.trusted_roots = parsed;
        Ok(self)
    }

    pub fn retry(
        mut self,
        attempts: u8,
        delay: Duration,
        methods: BTreeSet<HttpMethod>,
    ) -> Result<Self, HttpFailure> {
        if !(1..=5).contains(&attempts) || !valid_duration(delay) {
            return Err(HttpFailure::InvalidInput);
        }
        self.max_attempts = attempts;
        self.retry_delay = delay;
        self.retryable_methods = methods;
        Ok(self)
    }

    pub fn circuit(
        mut self,
        failures: u32,
        open: Duration,
        half_open_probes: u32,
    ) -> Result<Self, HttpFailure> {
        if !valid_duration(open) || half_open_probes == 0 {
            return Err(HttpFailure::InvalidInput);
        }
        self.circuit_failures = failures;
        self.circuit_open = open;
        self.half_open_probes = half_open_probes;
        Ok(self)
    }

    pub fn rate_per_second(mut self, value: u32) -> Self {
        self.rate_per_second = value;
        self
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

#[async_trait]
pub trait HttpOperationalClock: Send + Sync {
    fn now(&self) -> Instant;
    async fn sleep(&self, duration: Duration);
}

pub struct TokioHttpOperationalClock;

#[async_trait]
impl HttpOperationalClock for TokioHttpOperationalClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

struct Gate {
    open_until: Option<Instant>,
    consecutive_failures: u32,
    half_open_inflight: u32,
    tokens: f64,
    refilled_at: Instant,
}

struct State {
    profile: HttpServiceProfile,
    client: reqwest::Client,
    gate: Mutex<Gate>,
}

/// A cancelled future must release its half-open probe even when transport
/// execution never reaches its normal result handling.
struct CircuitAttempt<'a> {
    state: &'a State,
    probe: bool,
    settled: bool,
}

impl CircuitAttempt<'_> {
    fn succeeded(&mut self) {
        self.state.succeeded(self.probe);
        self.settled = true;
    }
    fn neutral(&mut self) {
        self.state.neutral(self.probe);
        self.settled = true;
    }
    fn failed(&mut self, clock: &dyn HttpOperationalClock) {
        self.state.failed(clock, self.probe);
        self.settled = true;
    }
}

impl Drop for CircuitAttempt<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.state.neutral(self.probe);
        }
    }
}

pub struct ReqwestHttpRequestService {
    profiles: HashMap<String, Arc<State>>,
    clock: Arc<dyn HttpOperationalClock>,
}

impl ReqwestHttpRequestService {
    pub fn new(profiles: Vec<HttpServiceProfile>) -> Result<Self, HttpFailure> {
        Self::with_clock(profiles, Arc::new(TokioHttpOperationalClock))
    }

    pub fn with_clock(
        profiles: Vec<HttpServiceProfile>,
        clock: Arc<dyn HttpOperationalClock>,
    ) -> Result<Self, HttpFailure> {
        let mut states = HashMap::new();
        for profile in profiles {
            let mut builder = reqwest::Client::builder()
                .connect_timeout(profile.connect_timeout)
                .redirect(reqwest::redirect::Policy::none());
            if !profile.trusted_roots.is_empty() {
                builder = builder.tls_certs_only(profile.trusted_roots.clone());
            }
            let client = builder.build().map_err(|_| HttpFailure::InvalidInput)?;
            let now = clock.now();
            let gate = Gate {
                open_until: None,
                consecutive_failures: 0,
                half_open_inflight: 0,
                tokens: profile.rate_per_second as f64,
                refilled_at: now,
            };
            if states
                .insert(
                    profile.alias.clone(),
                    Arc::new(State {
                        profile,
                        client,
                        gate: Mutex::new(gate),
                    }),
                )
                .is_some()
            {
                return Err(HttpFailure::InvalidInput);
            }
        }
        Ok(Self {
            profiles: states,
            clock,
        })
    }

    fn target(base: &Url, relative: &str) -> Result<Url, HttpFailure> {
        if relative.starts_with('/') || relative.contains('\\') || relative.contains('#') {
            return Err(HttpFailure::InvalidInput);
        }
        let path = relative.split('?').next().unwrap_or_default();
        if path.is_empty() || path.contains('%') || path.split('/').any(|s| s == "." || s == "..") {
            return Err(HttpFailure::InvalidInput);
        }
        let target = base.join(relative).map_err(|_| HttpFailure::InvalidInput)?;
        if target.origin() != base.origin() || !target.path().starts_with(base.path()) {
            return Err(HttpFailure::InvalidInput);
        }
        Ok(target)
    }

    fn headers(context: &UserContext, call: &HttpCall) -> Result<HeaderMap, HttpFailure> {
        let mut values = call.headers().clone();
        let carrier = context.runtime_propagation_carrier();
        for name in ["traceparent", "tracestate", "baggage"] {
            if let Some(value) = carrier
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value)
                && !values.keys().any(|key| key.eq_ignore_ascii_case(name))
            {
                values.insert(name.to_owned(), value.to_owned());
            }
        }
        if let Some(content_type) = call.content_type()
            && !values
                .keys()
                .any(|key| key.eq_ignore_ascii_case("content-type"))
        {
            values.insert("content-type".to_owned(), content_type.to_owned());
        }
        let mut headers = HeaderMap::new();
        for (name, value) in values {
            let name =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| HttpFailure::InvalidInput)?;
            let value = HeaderValue::from_str(&value).map_err(|_| HttpFailure::InvalidInput)?;
            headers.insert(name, value);
        }
        Ok(headers)
    }

    fn remaining(&self, started: Instant, total: Duration) -> Result<Duration, HttpFailure> {
        total
            .checked_sub(self.clock.now().saturating_duration_since(started))
            .filter(|left| !left.is_zero())
            .ok_or(HttpFailure::Timeout)
    }

    async fn backoff(
        &self,
        delay: Duration,
        attempt: u8,
        started: Instant,
        total: Duration,
    ) -> Result<(), HttpFailure> {
        let multiplier = 1_u32 << (attempt.saturating_sub(1).min(4));
        let wait = delay.saturating_mul(multiplier);
        let remaining = self.remaining(started, total)?;
        if wait >= remaining {
            return Err(HttpFailure::Timeout);
        }
        tokio::time::timeout(remaining, self.clock.sleep(wait))
            .await
            .map_err(|_| HttpFailure::Timeout)?;
        self.remaining(started, total)?;
        Ok(())
    }

    async fn execute_inner(
        &self,
        state: &State,
        context: &UserContext,
        call: &HttpCall,
        target: Url,
        attempts: &mut u8,
    ) -> Result<HttpResponse, HttpFailure> {
        let headers = Self::headers(context, call)?;
        let started = self.clock.now();
        let total = state.profile.total_timeout;
        let max_attempts = if call.retry_mode() == HttpRetryMode::ConfirmedIdempotent
            && state.profile.retryable_methods.contains(&call.method())
        {
            state.profile.max_attempts
        } else {
            1
        };
        loop {
            state
                .acquire_rate(self.clock.as_ref(), self.remaining(started, total)?)
                .await?;
            let probe = state.acquire_circuit(self.clock.as_ref())?;
            let mut circuit = CircuitAttempt {
                state,
                probe,
                settled: false,
            };
            let budget = self.remaining(started, total)?;
            // Reaching the profile gate is not a physical attempt. Only mark
            // one after the final pre-send budget check has succeeded.
            *attempts += 1;
            let result = self
                .one_attempt(state, call, &target, &headers, budget)
                .await;
            match result {
                Ok((status, body)) => {
                    if status == 429 || status >= 500 {
                        circuit.failed(self.clock.as_ref());
                        if *attempts < max_attempts {
                            self.backoff(state.profile.retry_delay, *attempts, started, total)
                                .await?;
                            continue;
                        }
                    } else {
                        circuit.succeeded();
                    }
                    return HttpResponse::new(status, body, *attempts);
                }
                Err(error) => {
                    if matches!(
                        error,
                        HttpFailure::ResponseTooLarge | HttpFailure::InvalidInput
                    ) {
                        circuit.neutral();
                    } else {
                        circuit.failed(self.clock.as_ref());
                    }
                    if *attempts < max_attempts
                        && !matches!(
                            error,
                            HttpFailure::ResponseTooLarge | HttpFailure::InvalidInput
                        )
                    {
                        self.backoff(state.profile.retry_delay, *attempts, started, total)
                            .await?;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn one_attempt(
        &self,
        state: &State,
        call: &HttpCall,
        target: &Url,
        headers: &HeaderMap,
        budget: Duration,
    ) -> Result<(u16, Vec<u8>), HttpFailure> {
        let attempt_started = self.clock.now();
        let per_attempt_budget = budget.min(state.profile.request_timeout);
        let method = match call.method() {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Delete => reqwest::Method::DELETE,
        };
        let request = state
            .client
            .request(method, target.clone())
            .headers(headers.clone())
            .timeout(per_attempt_budget)
            .body(call.body().to_vec());
        let response = tokio::time::timeout(per_attempt_budget, request.send())
            .await
            .map_err(|_| HttpFailure::Timeout)?
            .map_err(|error| {
                if error.is_timeout() {
                    HttpFailure::Timeout
                } else if call.method() == HttpMethod::Post {
                    HttpFailure::AmbiguousOutcome
                } else {
                    HttpFailure::UpstreamFailure
                }
            })?;
        let status = response.status().as_u16();
        let mut response = response;
        let mut body = Vec::new();
        loop {
            let remaining = per_attempt_budget
                .checked_sub(self.clock.now().saturating_duration_since(attempt_started))
                .filter(|left| !left.is_zero())
                .ok_or(HttpFailure::Timeout)?;
            let chunk = tokio::time::timeout(remaining, response.chunk())
                .await
                .map_err(|_| HttpFailure::Timeout)?
                .map_err(|error| {
                    if error.is_timeout() {
                        HttpFailure::Timeout
                    } else {
                        HttpFailure::UpstreamFailure
                    }
                })?;
            let Some(chunk) = chunk else {
                break;
            };
            if chunk.len() > state.profile.max_response_bytes - body.len() {
                return Err(HttpFailure::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, body))
    }
}

#[async_trait]
impl HttpRequestService for ReqwestHttpRequestService {
    async fn execute(
        &self,
        context: &UserContext,
        call: &HttpCall,
    ) -> Result<HttpResponse, HttpFailure> {
        call.validate()?;
        let state = self
            .profiles
            .get(call.profile())
            .ok_or(HttpFailure::ProfileNotConfigured)?;
        let target = Self::target(&state.profile.base, call.relative_path())?;
        let scope = context.start_runtime_operation(
            RuntimeOperation::new("http", call.profile())
                .attribute("teaql.http.method", format!("{:?}", call.method()))
                .attribute("teaql.http.comment", safe_text(call.comment()))
                .attribute("teaql.http.intent", safe_text(call.intent()))
                .attribute("teaql.http.trace_chain", call.business_trace_projection()),
        );
        let started = self.clock.now();
        let mut attempts = 0;
        let mut result = scope
            .run(async {
                tokio::time::timeout(
                    state.profile.total_timeout,
                    self.execute_inner(state, context, call, target, &mut attempts),
                )
                .await
                .map_err(|_| HttpFailure::Timeout)?
            })
            .await;
        // Once a POST attempt begins, a transport timeout or truncated
        // response cannot prove that the upstream effect did not commit.
        // Preserve Timeout before the first attempt (for example, rate wait).
        if attempts > 0
            && call.method() == HttpMethod::Post
            && matches!(
                &result,
                Err(HttpFailure::Timeout | HttpFailure::UpstreamFailure)
            )
        {
            result = Err(HttpFailure::AmbiguousOutcome);
        }
        let attributes = BTreeMap::from([
            (
                "teaql.http.outcome".to_owned(),
                RuntimeAttributeValue::String(
                    match &result {
                        Ok(_) => "success",
                        Err(_) => "failure",
                    }
                    .to_owned(),
                ),
            ),
            (
                "teaql.http.attempts".to_owned(),
                RuntimeAttributeValue::Integer(attempts.into()),
            ),
        ]);
        match &result {
            Ok(_) => scope.success(attributes),
            Err(error) => scope.failure_with_attributes(&format!("{error:?}"), attributes),
        }
        eprintln!(
            "TeaQL HTTP alias={} method={:?} intent={:?} comment={} purpose={} attempts={} outcome={} elapsed_ms={}",
            call.profile(),
            call.method(),
            call.intent_kind(),
            safe_text(call.comment()),
            safe_text(call.intent()),
            attempts,
            match &result {
                Ok(_) => "success",
                Err(error) => error_label(*error),
            },
            self.clock
                .now()
                .saturating_duration_since(started)
                .as_millis()
        );
        result
    }
}

fn safe_text(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn error_label(error: HttpFailure) -> &'static str {
    match error {
        HttpFailure::InvalidInput => "invalid_input",
        HttpFailure::ProfileNotConfigured => "profile_not_configured",
        HttpFailure::Timeout => "timeout",
        HttpFailure::Cancelled => "cancelled",
        HttpFailure::RateLimited => "rate_limited",
        HttpFailure::CircuitOpen => "circuit_open",
        HttpFailure::ResponseTooLarge => "response_too_large",
        HttpFailure::MalformedResponse => "malformed_response",
        HttpFailure::AmbiguousOutcome => "ambiguous_outcome",
        HttpFailure::UpstreamFailure => "upstream_failure",
    }
}

impl State {
    fn acquire_circuit(&self, clock: &dyn HttpOperationalClock) -> Result<bool, HttpFailure> {
        if self.profile.circuit_failures == 0 {
            return Ok(false);
        }
        let mut gate = self
            .gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(until) = gate.open_until else {
            return Ok(false);
        };
        if clock.now() < until || gate.half_open_inflight >= self.profile.half_open_probes {
            return Err(HttpFailure::CircuitOpen);
        }
        gate.half_open_inflight += 1;
        Ok(true)
    }

    fn succeeded(&self, probe: bool) {
        let mut gate = self
            .gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if probe {
            gate.half_open_inflight = gate.half_open_inflight.saturating_sub(1);
        }
        gate.consecutive_failures = 0;
        gate.open_until = None;
    }

    fn neutral(&self, probe: bool) {
        if probe {
            let mut gate = self
                .gate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            gate.half_open_inflight = gate.half_open_inflight.saturating_sub(1);
        }
    }

    fn failed(&self, clock: &dyn HttpOperationalClock, probe: bool) {
        if self.profile.circuit_failures == 0 {
            return;
        }
        let mut gate = self
            .gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if probe {
            gate.half_open_inflight = gate.half_open_inflight.saturating_sub(1);
        }
        gate.consecutive_failures = gate.consecutive_failures.saturating_add(1);
        if probe || gate.consecutive_failures >= self.profile.circuit_failures {
            gate.open_until = clock.now().checked_add(self.profile.circuit_open);
            gate.consecutive_failures = 0;
        }
    }

    async fn acquire_rate(
        &self,
        clock: &dyn HttpOperationalClock,
        budget: Duration,
    ) -> Result<(), HttpFailure> {
        if self.profile.rate_per_second == 0 {
            return Ok(());
        }
        let started = clock.now();
        loop {
            let wait = {
                let mut gate = self
                    .gate
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                let now = clock.now();
                let elapsed = now
                    .saturating_duration_since(gate.refilled_at)
                    .as_secs_f64();
                gate.tokens = (gate.tokens + elapsed * self.profile.rate_per_second as f64)
                    .min(self.profile.rate_per_second as f64);
                gate.refilled_at = now;
                if gate.tokens >= 1.0 {
                    gate.tokens -= 1.0;
                    return Ok(());
                }
                Duration::from_secs_f64((1.0 - gate.tokens) / self.profile.rate_per_second as f64)
            };
            if wait >= budget.saturating_sub(clock.now().saturating_duration_since(started)) {
                return Err(HttpFailure::RateLimited);
            }
            tokio::time::timeout(budget, clock.sleep(wait))
                .await
                .map_err(|_| HttpFailure::Timeout)?;
        }
    }
}
