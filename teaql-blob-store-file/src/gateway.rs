//! Explicit provider-owned bearer-URL listener. It is independent of business web frameworks.
use crate::{FileBlobStore, is_hex};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use http_body_util::{BodyExt, Empty, StreamBody, combinators::UnsyncBoxBody};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use teaql_blob_reference::*;
use tokio::{net::TcpListener, task::JoinSet};

const DOMAIN: &[u8] = b"teaql.rust.file.gateway/v1\0";
type Body = UnsyncBoxBody<Bytes, BlobError>;
type Signature = Hmac<Sha256>;

pub trait BlobGatewayClock: Send + Sync {
    fn now_millis(&self) -> BlobResult<i64>;
}
pub struct SystemBlobGatewayClock;
impl BlobGatewayClock for SystemBlobGatewayClock {
    fn now_millis(&self) -> BlobResult<i64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|v| i64::try_from(v.as_millis()).ok())
            .ok_or_else(|| BlobErrorCode::SignFailed.into())
    }
}
/// Constructor is passive. Host owns binding/TLS/public endpoint configuration,
/// explicitly runs serve and awaits its completion to release listener/connections.
pub struct FileBlobHttpGateway {
    files: FileBlobStore,
    endpoint: url::Url,
    active_key: String,
    keys: BTreeMap<String, Vec<u8>>,
    clock: Arc<dyn BlobGatewayClock>,
    maximum_ttl: Duration,
    serving: AtomicBool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    schema: u8,
    origin: String,
    location: String,
    version: String,
    content_type: String,
    file_name: String,
    inline: bool,
    cache_seconds: Option<u64>,
    issued: i64,
    expires: i64,
}
impl FileBlobHttpGateway {
    pub fn new(
        files: FileBlobStore,
        endpoint: &str,
        active_key: impl Into<String>,
        keys: BTreeMap<String, Vec<u8>>,
        clock: Arc<dyn BlobGatewayClock>,
        maximum_ttl: Duration,
    ) -> BlobResult<Self> {
        if endpoint.chars().any(|c| c.is_control() || c == ' ')
            || endpoint.split_once("://").is_some_and(|(_, rest)| {
                rest.split(['/', '?', '#'])
                    .next()
                    .is_some_and(|authority| authority.contains('@'))
            })
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        let endpoint =
            url::Url::parse(endpoint).map_err(|_| BlobError::from(BlobErrorCode::PolicyInvalid))?;
        let active_key = active_key.into();
        if endpoint.as_str().len() > 1024
            || !["http", "https"].contains(&endpoint.scheme())
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.port() == Some(0)
            || endpoint.path().len() < 2
            || !endpoint
                .path()
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/_-".contains(&b))
            || maximum_ttl.is_zero()
            || maximum_ttl.subsec_nanos() != 0
            || maximum_ttl.as_secs() > 604800
            || keys.is_empty()
            || keys.len() > 64
            || !keys.contains_key(&active_key)
            || keys.iter().any(|(id, key)| {
                id.is_empty()
                    || id.len() > 64
                    || !id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
                    || !(32..=128).contains(&key.len())
            })
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        Ok(Self {
            files,
            endpoint,
            active_key,
            keys,
            clock,
            maximum_ttl,
            serving: AtomicBool::new(false),
        })
    }
    /// Bounds active connections and each complete connection lifetime. Stop drains
    /// only this listener's children. A dropped serve future also aborts its JoinSet.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        stop: impl Future<Output = ()> + Send,
    ) -> BlobResult<()> {
        self.files.validate_ready().await?;
        if self
            .serving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        struct Active(Arc<FileBlobHttpGateway>);
        impl Drop for Active {
            fn drop(&mut self) {
                self.0.serving.store(false, Ordering::Release);
            }
        }
        let _active = Active(self.clone());
        tokio::pin!(stop);
        let mut connections = JoinSet::new();
        let result = loop {
            tokio::select! {
                _ = &mut stop => break Ok(()),
                Some(_) = connections.join_next(), if !connections.is_empty() => (),
                accepted = listener.accept() => {
                    let Ok((socket, _)) = accepted else { break Err(BlobErrorCode::StoreFailed.into()); };
                    if connections.len() >= 64 { drop(socket); continue; }
                    let gateway = self.clone();
                    connections.spawn(async move {
                        let service = service_fn(move |request| { let gateway = gateway.clone(); async move { Ok::<_, Infallible>(gateway.handle(request).await) } });
                        let mut http = hyper::server::conn::http1::Builder::new();
                        http.keep_alive(false).max_headers(32).max_buf_size(16384)
                            .timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5));
                        let _ = tokio::time::timeout(Duration::from_secs(30), http.serve_connection(TokioIo::new(socket), service)).await;
                    });
                }
            }
        };
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        result
    }
    fn signature(&self, key: &[u8], message: &str) -> Signature {
        // Key lengths have already been checked; HMAC accepts every key length.
        let mut mac = Signature::new_from_slice(key).expect("HMAC accepts validated keys");
        mac.update(DOMAIN);
        mac.update(message.as_bytes());
        mac
    }
    fn decode(&self, token: &str, now: i64) -> BlobResult<Claims> {
        let denied = || BlobError::from(BlobErrorCode::ReadDenied);
        if token.len() > 8192 {
            return Err(denied());
        }
        let parts: Vec<_> = token.split('.').collect();
        if parts.len() != 4 || parts[0] != "rbf1" {
            return Err(denied());
        }
        let key = self.keys.get(parts[1]).ok_or_else(denied)?;
        let signature = B64.decode(parts[3]).map_err(|_| denied())?;
        if B64.encode(&signature) != parts[3] {
            return Err(denied());
        }
        let message = format!("{}.{}.{}", parts[0], parts[1], parts[2]);
        self.signature(key, &message)
            .verify_slice(&signature)
            .map_err(|_| denied())?;
        let bytes = B64.decode(parts[2]).map_err(|_| denied())?;
        if B64.encode(&bytes) != parts[2] {
            return Err(denied());
        }
        let claims: Claims = serde_json::from_slice(&bytes).map_err(|_| denied())?;
        if claims.schema != 1
            || claims.origin != self.endpoint.as_str()
            || claims.issued <= 0
            || claims.issued > now
            || claims.expires <= now
            || claims.expires <= claims.issued
            || claims
                .expires
                .checked_sub(claims.issued)
                .is_none_or(|v| v as u128 > self.maximum_ttl.as_millis())
            || !is_hex(&claims.location, 64)
            || !is_hex(&claims.version, 32)
        {
            return Err(denied());
        }
        BlobReadRequest::new(
            &claims.location,
            &claims.version,
            &claims.content_type,
            &claims.file_name,
            if claims.inline {
                Disposition::Inline
            } else {
                Disposition::Attachment
            },
            Duration::from_millis((claims.expires - claims.issued) as u64),
            claims
                .cache_seconds
                .map_or(CachePolicy::NoStore, CachePolicy::PrivateMaxAge),
        )
        .map_err(|_| denied())?;
        Ok(claims)
    }
    async fn handle(&self, request: Request<Incoming>) -> Response<Body> {
        let fail = |status| empty(status);
        if request.uri().path() != self.endpoint.path()
            || request.uri().authority().is_some()
            || request.uri().scheme().is_some()
        {
            return fail(StatusCode::NOT_FOUND);
        }
        if request.method() != Method::GET && request.method() != Method::HEAD {
            let mut response = fail(StatusCode::METHOD_NOT_ALLOWED);
            response
                .headers_mut()
                .insert("allow", "GET, HEAD".parse().unwrap());
            return response;
        }
        if request.headers().contains_key("transfer-encoding")
            || request
                .headers()
                .get_all("content-length")
                .iter()
                .any(|v| v.as_bytes() != b"0")
        {
            return fail(StatusCode::BAD_REQUEST);
        }
        let Some(query) = request.uri().query().filter(|q| q.len() <= 8198) else {
            return fail(StatusCode::FORBIDDEN);
        };
        // The service emits one canonical token parameter. Reject duplicates/extra input.
        let Some(token) = query
            .strip_prefix("token=")
            .filter(|s| !s.contains('&') && !s.contains('%') && !s.contains('+'))
        else {
            return fail(StatusCode::FORBIDDEN);
        };
        let Ok(now) = self.clock.now_millis() else {
            return fail(StatusCode::SERVICE_UNAVAILABLE);
        };
        let Ok(claims) = self.decode(token, now) else {
            return fail(StatusCode::FORBIDDEN);
        };
        let content = match self.files.stat(&claims.location).await {
            Ok(Some(content)) if content.content_version() == claims.version => content,
            Ok(_) => return fail(StatusCode::NOT_FOUND),
            Err(_) => return fail(StatusCode::SERVICE_UNAVAILABLE),
        };
        let values: Vec<_> = request.headers().get_all("range").iter().collect();
        let range = if values.is_empty() {
            Some((0, content.size_bytes(), false))
        } else if values.len() == 1 {
            values[0]
                .to_str()
                .ok()
                .and_then(|v| parse_range(v, content.size_bytes()))
        } else {
            None
        };
        let Some((offset, length, partial)) = range else {
            let mut response = fail(StatusCode::RANGE_NOT_SATISFIABLE);
            response.headers_mut().insert(
                "content-range",
                format!("bytes */{}", content.size_bytes()).parse().unwrap(),
            );
            return response;
        };
        let Ok(opened) = self
            .files
            .open_range(&claims.location, &claims.version, offset, length)
            .await
        else {
            return fail(StatusCode::NOT_FOUND);
        };
        let Ok(now) = self.clock.now_millis() else {
            return fail(StatusCode::SERVICE_UNAVAILABLE);
        };
        if now < claims.issued || now >= claims.expires {
            return fail(StatusCode::FORBIDDEN);
        }
        let body = if request.method() == Method::HEAD {
            empty_body()
        } else {
            StreamBody::new(opened.stream.map(|chunk| chunk.map(Frame::data))).boxed_unsync()
        };
        let mut response = Response::new(body);
        *response.status_mut() = if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        let headers = response.headers_mut();
        headers.insert("content-length", length.to_string().parse().unwrap());
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("x-content-type-options", "nosniff".parse().unwrap());
        headers.insert("content-type", claims.content_type.parse().unwrap());
        headers.insert(
            "content-disposition",
            disposition(&claims.file_name, claims.inline)
                .parse()
                .unwrap(),
        );
        let cache = claims.cache_seconds.map_or_else(
            || "private, no-store".into(),
            |seconds| {
                format!(
                    "private, max-age={}",
                    seconds.min(((claims.expires - now) / 1000) as u64)
                )
            },
        );
        headers.insert("cache-control", cache.parse().unwrap());
        if partial {
            headers.insert(
                "content-range",
                format!(
                    "bytes {}-{}/{}",
                    offset,
                    offset + length - 1,
                    content.size_bytes()
                )
                .parse()
                .unwrap(),
            );
        }
        response
    }
}
#[async_trait::async_trait]
impl BlobStore for FileBlobHttpGateway {
    async fn validate_ready(&self) -> BlobResult<()> {
        if !self.serving.load(Ordering::Acquire) {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        self.files.validate_ready().await
    }
    fn capabilities(&self) -> BlobStoreCapabilities {
        BlobStoreCapabilities {
            signed_reads: true,
            maximum_read_ttl: self.maximum_ttl,
            ..self.files.capabilities()
        }
    }
    fn maximum_write_bytes(&self) -> u64 {
        self.files.maximum_write_bytes()
    }
    async fn create_upload(&self, upload_id: &str) -> BlobResult<BlobUploadTarget> {
        self.files.create_upload(upload_id).await
    }
    async fn write(
        &self,
        target: &BlobUploadTarget,
        source: BlobContentStream,
        maximum_bytes: u64,
    ) -> BlobResult<StoredBlob> {
        self.files.write(target, source, maximum_bytes).await
    }
    async fn stat(&self, location: &str) -> BlobResult<Option<StoredBlob>> {
        self.files.stat(location).await
    }
    async fn seal_for_deletion(
        &self,
        location: &str,
        expected_version: Option<&str>,
    ) -> BlobResult<Option<StoredBlob>> {
        self.files
            .seal_for_deletion(location, expected_version)
            .await
    }
    async fn open_range(
        &self,
        location: &str,
        version: &str,
        offset: u64,
        length: u64,
    ) -> BlobResult<BlobRangeContent> {
        self.files
            .open_range(location, version, offset, length)
            .await
    }
    async fn delete_if_version(
        &self,
        location: &str,
        expected_version: &str,
    ) -> BlobResult<BlobDeleteResult> {
        self.files
            .delete_if_version(location, expected_version)
            .await
    }
    async fn sign_read_batch(
        &self,
        requests: &[BlobReadRequest],
    ) -> BlobResult<Vec<SignedBlobRead>> {
        self.validate_ready().await?;
        if requests.len() > 512 {
            return Err(BlobErrorCode::SignFailed.into());
        }
        let now = self.clock.now_millis()?;
        if now <= 0 {
            return Err(BlobErrorCode::SignFailed.into());
        }
        let key = &self.keys[&self.active_key];
        let mut result = Vec::with_capacity(requests.len());
        for request in requests {
            if !is_hex(request.location(), 64) || !is_hex(request.content_version(), 32) {
                return Err(BlobErrorCode::SignFailed.into());
            }
            let ttl = request.lifetime().min(self.maximum_ttl);
            let expires = now
                .checked_add(ttl.as_millis() as i64)
                .ok_or_else(|| BlobError::from(BlobErrorCode::SignFailed))?;
            let claims = Claims {
                schema: 1,
                origin: self.endpoint.to_string(),
                location: request.location().into(),
                version: request.content_version().into(),
                content_type: request.content_type().into(),
                file_name: request.file_name().into(),
                inline: request.disposition() == Disposition::Inline,
                cache_seconds: match request.cache() {
                    CachePolicy::NoStore => None,
                    CachePolicy::PrivateMaxAge(seconds) => Some((*seconds).min(ttl.as_secs())),
                },
                issued: now,
                expires,
            };
            let payload = serde_json::to_vec(&claims)
                .map_err(|_| BlobError::from(BlobErrorCode::SignFailed))?;
            let message = format!("rbf1.{}.{}", self.active_key, B64.encode(payload));
            let token = format!(
                "{}.{}",
                message,
                B64.encode(self.signature(key, &message).finalize().into_bytes())
            );
            let mut url = self.endpoint.clone();
            url.query_pairs_mut().append_pair("token", &token);
            result.push(SignedBlobRead::new(url.to_string(), expires)?);
        }
        Ok(result)
    }
}
fn empty_body() -> Body {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed_unsync()
}
fn empty(status: StatusCode) -> Response<Body> {
    let mut result = Response::new(empty_body());
    *result.status_mut() = status;
    result
        .headers_mut()
        .insert("cache-control", "private, no-store".parse().unwrap());
    result
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    result
}
fn parse_range(value: &str, size: u64) -> Option<(u64, u64, bool)> {
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    let number = |text: &str| {
        if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
            text.parse::<u64>().ok()
        } else {
            None
        }
    };
    if size == 0 {
        return None;
    }
    if start.is_empty() {
        let suffix = number(end)?;
        if suffix == 0 {
            return None;
        }
        let length = suffix.min(size);
        return Some((size - length, length, true));
    }
    let start = number(start)?;
    if start >= size {
        return None;
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        number(end)?.min(size - 1)
    };
    if end < start {
        None
    } else {
        Some((start, end - start + 1, true))
    }
}
fn disposition(name: &str, inline: bool) -> String {
    let fallback: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || " ._-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    let encoded: String = name
        .as_bytes()
        .iter()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(b) {
                (*b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    format!(
        "{}; filename=\"{}\"; filename*=UTF-8''{}",
        if inline { "inline" } else { "attachment" },
        fallback,
        encoded
    )
}
