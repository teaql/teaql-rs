use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, TimeDelta, Utc};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value, json};
use sha2::Sha256;
use subtle::ConstantTimeEq;

pub const UNSAFE_EXPOSE_RAW_ENTITY_IDS_ENVIRONMENT: &str = "TEAQL_UNSAFE_EXPOSE_RAW_ENTITY_IDS";
pub const UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT: &str =
    "I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY";

const TOKEN_PREFIX: &str = "tqr1";
const HKDF_SALT: &[u8] = b"teaql.round-trip-reference.v1";
const MAX_KEY_ID_LENGTH: usize = 64;
const MAX_BOUND_TEXT_LENGTH: usize = 4_096;
const MAX_TOKEN_LENGTH: usize = 24_000;

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
type NonceSource = Arc<dyn Fn() -> [u8; 12] + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentProfile {
    Development,
    Test,
    Production,
}

impl DeploymentProfile {
    const fn label(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Test => "test",
            Self::Production => "production",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferenceMode {
    Governed,
    Raw,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedReferencePrincipal {
    pub authentication_realm: String,
    pub subject: String,
    pub domain_root_type: String,
    pub domain_root_id: u64,
}

impl TrustedReferencePrincipal {
    pub fn new(
        authentication_realm: impl Into<String>,
        subject: impl Into<String>,
        domain_root_type: impl Into<String>,
        domain_root_id: u64,
    ) -> Result<Self, RoundTripReferenceError> {
        let value = Self {
            authentication_realm: authentication_realm.into(),
            subject: subject.into(),
            domain_root_type: domain_root_type.into(),
            domain_root_id,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<(), RoundTripReferenceError> {
        require_text(&self.authentication_realm)?;
        require_text(&self.subject)?;
        require_text(&self.domain_root_type)?;
        if self.domain_root_id == 0 {
            return Err(RoundTripReferenceError::invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceDocumentScope {
    pub document_id: String,
    pub purpose: String,
    pub aggregate_type: String,
    pub aggregate_id: u64,
    pub aggregate_revision: i64,
}

impl ReferenceDocumentScope {
    pub fn new(
        document_id: impl Into<String>,
        purpose: impl Into<String>,
        aggregate_type: impl Into<String>,
        aggregate_id: u64,
        aggregate_revision: i64,
    ) -> Result<Self, RoundTripReferenceError> {
        let value = Self {
            document_id: document_id.into(),
            purpose: purpose.into(),
            aggregate_type: aggregate_type.into(),
            aggregate_id,
            aggregate_revision,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), RoundTripReferenceError> {
        require_text(&self.document_id)?;
        require_text(&self.purpose)?;
        require_text(&self.aggregate_type)?;
        if self.aggregate_id == 0 || self.aggregate_revision < 0 {
            return Err(RoundTripReferenceError::invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceIdentity {
    pub entity_type: String,
    pub id: u64,
    pub version: i64,
}

impl ReferenceIdentity {
    pub fn new(
        entity_type: impl Into<String>,
        id: u64,
        version: i64,
    ) -> Result<Self, RoundTripReferenceError> {
        let value = Self {
            entity_type: entity_type.into(),
            id,
            version,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), RoundTripReferenceError> {
        require_text(&self.entity_type)?;
        if self.id == 0 || self.version < 0 {
            return Err(RoundTripReferenceError::invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalEntityReference {
    Governed(String),
    Raw { id: u64, version: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedEntityReference {
    pub identity: ReferenceIdentity,
    pub mode: ReferenceMode,
    pub key_id: Option<String>,
    pub issued_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceStartupNotice {
    pub level: &'static str,
    pub message: &'static str,
    pub deployment_profile: &'static str,
    pub telemetry_key: &'static str,
    pub telemetry_value: &'static str,
    pub response_headers: [(&'static str, &'static str); 2],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoundTripReferenceError {
    code: &'static str,
}

impl RoundTripReferenceError {
    pub const fn invalid() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_INVALID",
        }
    }

    pub const fn expired() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_EXPIRED",
        }
    }

    pub const fn scope_mismatch() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH",
        }
    }

    pub const fn authorization_required() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED",
        }
    }

    pub const fn configuration() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_CONFIGURATION_INVALID",
        }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl Display for RoundTripReferenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for RoundTripReferenceError {}

#[derive(Clone)]
pub struct ReferenceKey {
    pub key_id: String,
    master_key: [u8; 32],
}

impl ReferenceKey {
    pub fn new(
        key_id: impl Into<String>,
        master_key: impl AsRef<[u8]>,
    ) -> Result<Self, RoundTripReferenceError> {
        let key_id = key_id.into();
        validate_key_id(&key_id)?;
        let master_key: [u8; 32] = master_key
            .as_ref()
            .try_into()
            .map_err(|_| RoundTripReferenceError::configuration())?;
        Ok(Self { key_id, master_key })
    }

    pub(crate) fn secret(&self) -> &[u8; 32] {
        &self.master_key
    }
}

impl Debug for ReferenceKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceKey")
            .field("key_id", &self.key_id)
            .field("master_key", &"[REDACTED]")
            .finish()
    }
}

pub trait ReferenceKeyProvider: Send + Sync {
    fn current_key(&self) -> Result<ReferenceKey, RoundTripReferenceError>;
    fn key_by_id(&self, key_id: &str) -> Result<Option<ReferenceKey>, RoundTripReferenceError>;
}

#[derive(Clone)]
pub struct StaticReferenceKeyProvider {
    current_key_id: String,
    keys: BTreeMap<String, ReferenceKey>,
}

impl StaticReferenceKeyProvider {
    pub fn new(
        current_key_id: impl Into<String>,
        keys: impl IntoIterator<Item = ReferenceKey>,
    ) -> Result<Self, RoundTripReferenceError> {
        let current_key_id = current_key_id.into();
        validate_key_id(&current_key_id)?;
        let mut retained = BTreeMap::new();
        for key in keys {
            if retained.insert(key.key_id.clone(), key).is_some() {
                return Err(RoundTripReferenceError::configuration());
            }
        }
        if !retained.contains_key(&current_key_id) {
            return Err(RoundTripReferenceError::configuration());
        }
        Ok(Self {
            current_key_id,
            keys: retained,
        })
    }
}

impl ReferenceKeyProvider for StaticReferenceKeyProvider {
    fn current_key(&self) -> Result<ReferenceKey, RoundTripReferenceError> {
        self.keys
            .get(&self.current_key_id)
            .cloned()
            .ok_or_else(RoundTripReferenceError::configuration)
    }

    fn key_by_id(&self, key_id: &str) -> Result<Option<ReferenceKey>, RoundTripReferenceError> {
        Ok(self.keys.get(key_id).cloned())
    }
}

pub trait ReferenceAuthorizationPolicy: Send + Sync {
    fn authorize(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        identity: &ReferenceIdentity,
    ) -> Result<(), RoundTripReferenceError>;
}

impl<F> ReferenceAuthorizationPolicy for F
where
    F: Fn(
            &TrustedReferencePrincipal,
            &ReferenceDocumentScope,
            &ReferenceIdentity,
        ) -> Result<(), RoundTripReferenceError>
        + Send
        + Sync,
{
    fn authorize(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        identity: &ReferenceIdentity,
    ) -> Result<(), RoundTripReferenceError> {
        self(principal, scope, identity)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReferenceWireCodec;

impl ReferenceWireCodec {
    pub fn serialize(reference: &ExternalEntityReference) -> Value {
        match reference {
            ExternalEntityReference::Governed(token) => Value::String(token.clone()),
            ExternalEntityReference::Raw { id, version } => {
                json!({"id": id, "version": version})
            }
        }
    }

    pub fn deserialize(
        value: &Value,
        mode: ReferenceMode,
    ) -> Result<ExternalEntityReference, RoundTripReferenceError> {
        match (mode, value) {
            (ReferenceMode::Governed, Value::String(token))
                if token.starts_with(&format!("{TOKEN_PREFIX}.")) =>
            {
                Ok(ExternalEntityReference::Governed(token.clone()))
            }
            (ReferenceMode::Raw, Value::Object(object)) => decode_raw_wire(object),
            _ => Err(RoundTripReferenceError::invalid()),
        }
    }
}

pub struct ContextBoundReferenceRuntime {
    profile: DeploymentProfile,
    mode: ReferenceMode,
    service: String,
    environment: String,
    keys: Arc<dyn ReferenceKeyProvider>,
    authorization: Arc<dyn ReferenceAuthorizationPolicy>,
    clock: Clock,
    nonce_source: NonceSource,
}

#[derive(Clone)]
pub(crate) struct ContextBoundReferenceRuntimeResource(pub Arc<ContextBoundReferenceRuntime>);

#[derive(Clone)]
pub(crate) struct TrustedReferencePrincipalResource(pub TrustedReferencePrincipal);

impl ContextBoundReferenceRuntime {
    pub fn from_process_environment(
        profile: DeploymentProfile,
        service: impl Into<String>,
        environment: impl Into<String>,
        keys: Arc<dyn ReferenceKeyProvider>,
        authorization: Arc<dyn ReferenceAuthorizationPolicy>,
    ) -> Result<Self, RoundTripReferenceError> {
        let acknowledgement = std::env::var(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ENVIRONMENT).ok();
        let runtime = Self::from_acknowledgement(
            profile,
            service,
            environment,
            keys,
            authorization,
            acknowledgement.as_deref(),
        )?;
        if let Some(notice) = runtime.startup_notice() {
            eprintln!(
                "{}: {}; deployment_profile={}; {}={}",
                notice.level,
                notice.message,
                notice.deployment_profile,
                notice.telemetry_key,
                notice.telemetry_value
            );
        }
        Ok(runtime)
    }

    pub fn governed(
        profile: DeploymentProfile,
        service: impl Into<String>,
        environment: impl Into<String>,
        keys: Arc<dyn ReferenceKeyProvider>,
        authorization: Arc<dyn ReferenceAuthorizationPolicy>,
    ) -> Result<Self, RoundTripReferenceError> {
        Self::from_acknowledgement(profile, service, environment, keys, authorization, None)
    }

    fn from_acknowledgement(
        profile: DeploymentProfile,
        service: impl Into<String>,
        environment: impl Into<String>,
        keys: Arc<dyn ReferenceKeyProvider>,
        authorization: Arc<dyn ReferenceAuthorizationPolicy>,
        acknowledgement: Option<&str>,
    ) -> Result<Self, RoundTripReferenceError> {
        let service = service.into();
        let environment = environment.into();
        require_text(&service).map_err(|_| RoundTripReferenceError::configuration())?;
        require_text(&environment).map_err(|_| RoundTripReferenceError::configuration())?;
        let requested_raw = acknowledgement == Some(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT);
        if requested_raw && profile == DeploymentProfile::Production {
            return Err(RoundTripReferenceError::configuration());
        }
        let mode = if requested_raw {
            ReferenceMode::Raw
        } else {
            ReferenceMode::Governed
        };
        // Validate the active key even in diagnostic mode so moving the same
        // assembly to a governed environment cannot reveal a latent key error.
        keys.current_key()?;
        Ok(Self {
            profile,
            mode,
            service,
            environment,
            keys,
            authorization,
            clock: Arc::new(Utc::now),
            nonce_source: Arc::new(|| Aes256Gcm::generate_nonce(&mut OsRng).into()),
        })
    }

    pub fn mode(&self) -> ReferenceMode {
        self.mode
    }

    pub fn deployment_profile(&self) -> DeploymentProfile {
        self.profile
    }

    pub(crate) fn service_name(&self) -> &str {
        &self.service
    }

    pub(crate) fn environment_name(&self) -> &str {
        &self.environment
    }

    pub(crate) fn current_reference_key(&self) -> Result<ReferenceKey, RoundTripReferenceError> {
        self.keys.current_key()
    }

    pub(crate) fn reference_key_by_id(
        &self,
        key_id: &str,
    ) -> Result<Option<ReferenceKey>, RoundTripReferenceError> {
        self.keys.key_by_id(key_id)
    }

    pub(crate) fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    pub(crate) fn next_nonce(&self) -> [u8; 12] {
        (self.nonce_source)()
    }

    pub(crate) fn authorize_identity(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        identity: &ReferenceIdentity,
    ) -> Result<(), RoundTripReferenceError> {
        self.authorization.authorize(principal, scope, identity)
    }

    pub(crate) fn actor_fingerprint_for(
        &self,
        key: &ReferenceKey,
        principal: &TrustedReferencePrincipal,
    ) -> Result<[u8; 32], RoundTripReferenceError> {
        actor_fingerprint(key, &self.service, &self.environment, principal)
    }

    pub fn startup_notice(&self) -> Option<ReferenceStartupNotice> {
        (self.mode == ReferenceMode::Raw).then_some(ReferenceStartupNotice {
            level: "ERROR",
            message: "TeaQL raw internal entity reference mode is active for local diagnostics",
            deployment_profile: self.profile.label(),
            telemetry_key: "teaql.reference.mode",
            telemetry_value: "raw",
            response_headers: [
                ("TeaQL-Reference-Mode", "raw-internal-id"),
                ("Cache-Control", "no-store"),
            ],
        })
    }

    pub fn with_clock(mut self, clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    pub fn with_nonce_source(
        mut self,
        source: impl Fn() -> [u8; 12] + Send + Sync + 'static,
    ) -> Self {
        self.nonce_source = Arc::new(source);
        self
    }

    pub fn issue(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        identity: ReferenceIdentity,
        lifetime: Duration,
    ) -> Result<ExternalEntityReference, RoundTripReferenceError> {
        principal.validate()?;
        scope.validate()?;
        identity.validate()?;
        if lifetime.as_secs() == 0 {
            return Err(RoundTripReferenceError::invalid());
        }
        self.authorization.authorize(principal, scope, &identity)?;
        if self.mode == ReferenceMode::Raw {
            return Ok(ExternalEntityReference::Raw {
                id: identity.id,
                version: identity.version,
            });
        }
        self.issue_governed(principal, scope, identity, lifetime)
    }

    pub fn consume(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        reference: &ExternalEntityReference,
        expected_entity_type: &str,
    ) -> Result<ResolvedEntityReference, RoundTripReferenceError> {
        principal.validate()?;
        scope.validate()?;
        require_text(expected_entity_type)?;
        let resolved = match (self.mode, reference) {
            (ReferenceMode::Raw, ExternalEntityReference::Raw { id, version }) => {
                let identity = ReferenceIdentity::new(expected_entity_type, *id, *version)?;
                ResolvedEntityReference {
                    identity,
                    mode: ReferenceMode::Raw,
                    key_id: None,
                    issued_at: None,
                    expires_at: None,
                }
            }
            (ReferenceMode::Governed, ExternalEntityReference::Governed(token)) => {
                self.consume_governed(principal, scope, token, expected_entity_type)?
            }
            _ => return Err(RoundTripReferenceError::invalid()),
        };
        self.authorization
            .authorize(principal, scope, &resolved.identity)?;
        Ok(resolved)
    }

    fn issue_governed(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        identity: ReferenceIdentity,
        lifetime: Duration,
    ) -> Result<ExternalEntityReference, RoundTripReferenceError> {
        let key = self.keys.current_key()?;
        let now = (self.clock)();
        let lifetime =
            TimeDelta::from_std(lifetime).map_err(|_| RoundTripReferenceError::invalid())?;
        let expires_at = now
            .checked_add_signed(lifetime)
            .ok_or_else(RoundTripReferenceError::invalid)?;
        let actor_fingerprint =
            actor_fingerprint(&key, &self.service, &self.environment, principal)?;
        let plaintext = encode_payload(
            &identity,
            scope,
            actor_fingerprint,
            now.timestamp(),
            expires_at.timestamp(),
        )?;
        let encryption_key = derive_key(
            key.secret(),
            b"entity-reference-encryption",
            &self.service,
            &self.environment,
            &key.key_id,
        )?;
        let aad = encode_aad(
            &key.key_id,
            &self.service,
            &self.environment,
            &scope.purpose,
        )?;
        let nonce = (self.nonce_source)();
        let cipher = Aes256Gcm::new_from_slice(&encryption_key)
            .map_err(|_| RoundTripReferenceError::configuration())?;
        let encrypted = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| RoundTripReferenceError::invalid())?;
        let mut envelope = Vec::with_capacity(nonce.len() + encrypted.len());
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&encrypted);
        Ok(ExternalEntityReference::Governed(format!(
            "{TOKEN_PREFIX}.{}.{}",
            key.key_id,
            URL_SAFE_NO_PAD.encode(envelope)
        )))
    }

    fn consume_governed(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        token: &str,
        expected_entity_type: &str,
    ) -> Result<ResolvedEntityReference, RoundTripReferenceError> {
        let (key_id, encoded) = parse_token(token)?;
        let key = self
            .keys
            .key_by_id(key_id)?
            .ok_or_else(RoundTripReferenceError::invalid)?;
        let envelope = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| RoundTripReferenceError::invalid())?;
        if envelope.len() < 12 + 16 {
            return Err(RoundTripReferenceError::invalid());
        }
        let encryption_key = derive_key(
            key.secret(),
            b"entity-reference-encryption",
            &self.service,
            &self.environment,
            &key.key_id,
        )?;
        let aad = encode_aad(
            &key.key_id,
            &self.service,
            &self.environment,
            &scope.purpose,
        )?;
        let cipher = Aes256Gcm::new_from_slice(&encryption_key)
            .map_err(|_| RoundTripReferenceError::configuration())?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&envelope[..12]),
                Payload {
                    msg: &envelope[12..],
                    aad: &aad,
                },
            )
            .map_err(|_| RoundTripReferenceError::invalid())?;
        let payload = decode_payload(&plaintext)?;
        let now = (self.clock)();
        if payload.expires_at <= now.timestamp() {
            return Err(RoundTripReferenceError::expired());
        }
        if payload.issued_at > now.timestamp() + 60 {
            return Err(RoundTripReferenceError::invalid());
        }
        let expected_actor = actor_fingerprint(&key, &self.service, &self.environment, principal)?;
        if !bool::from(payload.actor_fingerprint.ct_eq(&expected_actor))
            || payload.document_id != scope.document_id
            || payload.aggregate_type != scope.aggregate_type
            || payload.aggregate_id != scope.aggregate_id
            || payload.aggregate_revision != scope.aggregate_revision
            || payload.identity.entity_type != expected_entity_type
        {
            return Err(RoundTripReferenceError::scope_mismatch());
        }
        Ok(ResolvedEntityReference {
            identity: payload.identity,
            mode: ReferenceMode::Governed,
            key_id: Some(key.key_id),
            issued_at: DateTime::from_timestamp(payload.issued_at, 0),
            expires_at: DateTime::from_timestamp(payload.expires_at, 0),
        })
    }
}

struct DecodedPayload {
    actor_fingerprint: [u8; 32],
    identity: ReferenceIdentity,
    document_id: String,
    aggregate_type: String,
    aggregate_id: u64,
    aggregate_revision: i64,
    issued_at: i64,
    expires_at: i64,
}

pub(crate) fn derive_key(
    master: &[u8; 32],
    label: &[u8],
    service: &str,
    environment: &str,
    key_id: &str,
) -> Result<[u8; 32], RoundTripReferenceError> {
    let mut info = Vec::new();
    write_bytes(&mut info, label)?;
    write_text(&mut info, service)?;
    write_text(&mut info, environment)?;
    write_text(&mut info, key_id)?;
    let hkdf = Hkdf::<Sha256>::new(Some(HKDF_SALT), master);
    let mut output = [0u8; 32];
    hkdf.expand(&info, &mut output)
        .map_err(|_| RoundTripReferenceError::configuration())?;
    Ok(output)
}

fn actor_fingerprint(
    key: &ReferenceKey,
    service: &str,
    environment: &str,
    principal: &TrustedReferencePrincipal,
) -> Result<[u8; 32], RoundTripReferenceError> {
    let fingerprint_key = derive_key(
        key.secret(),
        b"actor-fingerprint",
        service,
        environment,
        &key.key_id,
    )?;
    let mut canonical = Vec::new();
    write_text(&mut canonical, &principal.authentication_realm)?;
    write_text(&mut canonical, &principal.subject)?;
    write_text(&mut canonical, &principal.domain_root_type)?;
    canonical.extend_from_slice(&principal.domain_root_id.to_be_bytes());
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&fingerprint_key)
        .map_err(|_| RoundTripReferenceError::configuration())?;
    mac.update(&canonical);
    Ok(mac.finalize().into_bytes().into())
}

fn encode_aad(
    key_id: &str,
    service: &str,
    environment: &str,
    purpose: &str,
) -> Result<Vec<u8>, RoundTripReferenceError> {
    let mut output = Vec::new();
    write_text(&mut output, TOKEN_PREFIX)?;
    write_text(&mut output, key_id)?;
    write_text(&mut output, service)?;
    write_text(&mut output, environment)?;
    write_text(&mut output, purpose)?;
    Ok(output)
}

fn encode_payload(
    identity: &ReferenceIdentity,
    scope: &ReferenceDocumentScope,
    actor_fingerprint: [u8; 32],
    issued_at: i64,
    expires_at: i64,
) -> Result<Vec<u8>, RoundTripReferenceError> {
    let mut output = Vec::new();
    output.extend_from_slice(&actor_fingerprint);
    write_text(&mut output, &identity.entity_type)?;
    output.extend_from_slice(&identity.id.to_be_bytes());
    output.extend_from_slice(&identity.version.to_be_bytes());
    write_text(&mut output, &scope.document_id)?;
    write_text(&mut output, &scope.aggregate_type)?;
    output.extend_from_slice(&scope.aggregate_id.to_be_bytes());
    output.extend_from_slice(&scope.aggregate_revision.to_be_bytes());
    output.extend_from_slice(&issued_at.to_be_bytes());
    output.extend_from_slice(&expires_at.to_be_bytes());
    Ok(output)
}

fn decode_payload(data: &[u8]) -> Result<DecodedPayload, RoundTripReferenceError> {
    let mut offset = 0;
    let actor_fingerprint: [u8; 32] = read_exact(data, &mut offset, 32)?
        .try_into()
        .map_err(|_| RoundTripReferenceError::invalid())?;
    let entity_type = read_text(data, &mut offset)?;
    let id = read_u64(data, &mut offset)?;
    let version = read_i64(data, &mut offset)?;
    let document_id = read_text(data, &mut offset)?;
    let aggregate_type = read_text(data, &mut offset)?;
    let aggregate_id = read_u64(data, &mut offset)?;
    let aggregate_revision = read_i64(data, &mut offset)?;
    let issued_at = read_i64(data, &mut offset)?;
    let expires_at = read_i64(data, &mut offset)?;
    if offset != data.len() {
        return Err(RoundTripReferenceError::invalid());
    }
    Ok(DecodedPayload {
        actor_fingerprint,
        identity: ReferenceIdentity::new(entity_type, id, version)?,
        document_id,
        aggregate_type,
        aggregate_id,
        aggregate_revision,
        issued_at,
        expires_at,
    })
}

fn parse_token(token: &str) -> Result<(&str, &str), RoundTripReferenceError> {
    if token.len() > MAX_TOKEN_LENGTH {
        return Err(RoundTripReferenceError::invalid());
    }
    let mut parts = token.split('.');
    if parts.next() != Some(TOKEN_PREFIX) {
        return Err(RoundTripReferenceError::invalid());
    }
    let key_id = parts.next().ok_or_else(RoundTripReferenceError::invalid)?;
    let encoded = parts.next().ok_or_else(RoundTripReferenceError::invalid)?;
    if parts.next().is_some() {
        return Err(RoundTripReferenceError::invalid());
    }
    validate_key_id(key_id).map_err(|_| RoundTripReferenceError::invalid())?;
    Ok((key_id, encoded))
}

fn decode_raw_wire(
    object: &Map<String, Value>,
) -> Result<ExternalEntityReference, RoundTripReferenceError> {
    if object.len() != 2 {
        return Err(RoundTripReferenceError::invalid());
    }
    let id = object
        .get("id")
        .and_then(Value::as_u64)
        .filter(|id| *id > 0)
        .ok_or_else(RoundTripReferenceError::invalid)?;
    let version = object
        .get("version")
        .and_then(Value::as_i64)
        .filter(|version| *version >= 0)
        .ok_or_else(RoundTripReferenceError::invalid)?;
    Ok(ExternalEntityReference::Raw { id, version })
}

fn validate_key_id(key_id: &str) -> Result<(), RoundTripReferenceError> {
    if key_id.is_empty()
        || key_id.len() > MAX_KEY_ID_LENGTH
        || !key_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(RoundTripReferenceError::configuration());
    }
    Ok(())
}

fn require_text(value: &str) -> Result<(), RoundTripReferenceError> {
    if value.is_empty() || value.len() > MAX_BOUND_TEXT_LENGTH || value.trim() != value {
        return Err(RoundTripReferenceError::invalid());
    }
    Ok(())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), RoundTripReferenceError> {
    let length: u16 = value
        .len()
        .try_into()
        .map_err(|_| RoundTripReferenceError::invalid())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Result<(), RoundTripReferenceError> {
    require_text(value)?;
    write_bytes(output, value.as_bytes())
}

fn read_exact<'a>(
    data: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], RoundTripReferenceError> {
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= data.len())
        .ok_or_else(RoundTripReferenceError::invalid)?;
    let result = &data[*offset..end];
    *offset = end;
    Ok(result)
}

fn read_text(data: &[u8], offset: &mut usize) -> Result<String, RoundTripReferenceError> {
    let length = u16::from_be_bytes(
        read_exact(data, offset, 2)?
            .try_into()
            .map_err(|_| RoundTripReferenceError::invalid())?,
    ) as usize;
    let value = std::str::from_utf8(read_exact(data, offset, length)?)
        .map_err(|_| RoundTripReferenceError::invalid())?
        .to_owned();
    require_text(&value)?;
    Ok(value)
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, RoundTripReferenceError> {
    Ok(u64::from_be_bytes(
        read_exact(data, offset, 8)?
            .try_into()
            .map_err(|_| RoundTripReferenceError::invalid())?,
    ))
}

fn read_i64(data: &[u8], offset: &mut usize) -> Result<i64, RoundTripReferenceError> {
    Ok(i64::from_be_bytes(
        read_exact(data, offset, 8)?
            .try_into()
            .map_err(|_| RoundTripReferenceError::invalid())?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserContext;
    use std::sync::atomic::{AtomicU8, Ordering};

    fn key_provider() -> Arc<dyn ReferenceKeyProvider> {
        Arc::new(
            StaticReferenceKeyProvider::new(
                "k2",
                [
                    ReferenceKey::new("k1", [0x11; 32]).unwrap(),
                    ReferenceKey::new("k2", [0x22; 32]).unwrap(),
                ],
            )
            .unwrap(),
        )
    }

    fn allow_known_item(
        principal: &TrustedReferencePrincipal,
        _: &ReferenceDocumentScope,
        identity: &ReferenceIdentity,
    ) -> Result<(), RoundTripReferenceError> {
        if principal.subject == "alice" && identity.entity_type == "OrderItem" && identity.id == 42
        {
            Ok(())
        } else {
            Err(RoundTripReferenceError::authorization_required())
        }
    }

    fn runtime(
        profile: DeploymentProfile,
        acknowledgement: Option<&str>,
    ) -> ContextBoundReferenceRuntime {
        let now = DateTime::parse_from_rfc3339("2026-09-27T00:00:00Z")
            .unwrap()
            .to_utc();
        ContextBoundReferenceRuntime::from_acknowledgement(
            profile,
            "order-service",
            "test-a",
            key_provider(),
            Arc::new(allow_known_item),
            acknowledgement,
        )
        .unwrap()
        .with_clock(move || now)
        .with_nonce_source(|| [0x33; 12])
    }

    fn alice() -> TrustedReferencePrincipal {
        TrustedReferencePrincipal::new("oidc", "alice", "Platform", 7).unwrap()
    }

    fn bob() -> TrustedReferencePrincipal {
        TrustedReferencePrincipal::new("oidc", "bob", "Platform", 7).unwrap()
    }

    fn scope() -> ReferenceDocumentScope {
        ReferenceDocumentScope::new("doc-100", "edit-order", "Order", 100, 9).unwrap()
    }

    fn identity() -> ReferenceIdentity {
        ReferenceIdentity::new("OrderItem", 42, 3).unwrap()
    }

    #[test]
    fn governed_reference_is_bound_opaque_rotatable_and_deterministic_under_vector_inputs() {
        let runtime = runtime(DeploymentProfile::Test, None);
        let reference = runtime
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();
        let ExternalEntityReference::Governed(token) = &reference else {
            panic!("governed profile must issue a protected token")
        };
        assert_eq!(
            token,
            "tqr1.k2.MzMzMzMzMzMzMzMzmhCET15vBdSc1BbFJcrvMMnRP1wAyjunShYlFPnDZL7Gk5V1LuptWyrbiQ8LQSDHFUmN3o6vb-MEgXZuh3N-YkeO55CYLeVgiyBBYslzuSu-fvWC2KMZcnZ5D8BRmWpGYA2Ok5sWe1Fg1aIyadTjVoKB-H2vPW_0okj-"
        );
        assert!(token.starts_with("tqr1.k2."));
        assert!(!token.contains("OrderItem"));
        assert!(!token.contains("alice"));
        let resolved = runtime
            .consume(&alice(), &scope(), &reference, "OrderItem")
            .unwrap();
        assert_eq!(resolved.identity, identity());
        assert_eq!(resolved.key_id.as_deref(), Some("k2"));
        assert_eq!(resolved.mode, ReferenceMode::Governed);

        assert_eq!(
            runtime
                .consume(&bob(), &scope(), &reference, "OrderItem")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
        );
        let other_scope =
            ReferenceDocumentScope::new("doc-101", "edit-order", "Order", 100, 9).unwrap();
        assert_eq!(
            runtime
                .consume(&alice(), &other_scope, &reference, "OrderItem")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
        );
        assert!(
            runtime
                .consume(&alice(), &scope(), &reference, "InvoiceItem")
                .is_err()
        );
    }

    #[test]
    fn old_decode_only_key_survives_rotation_and_unknown_key_fails_closed() {
        let old_keys: Arc<dyn ReferenceKeyProvider> = Arc::new(
            StaticReferenceKeyProvider::new("k1", [ReferenceKey::new("k1", [0x11; 32]).unwrap()])
                .unwrap(),
        );
        let now = DateTime::parse_from_rfc3339("2026-09-27T00:00:00Z")
            .unwrap()
            .to_utc();
        let old = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test-a",
            old_keys,
            Arc::new(allow_known_item),
        )
        .unwrap()
        .with_clock(move || now)
        .with_nonce_source(|| [0x44; 12]);
        let reference = old
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();
        assert!(
            runtime(DeploymentProfile::Test, None)
                .consume(&alice(), &scope(), &reference, "OrderItem")
                .is_ok()
        );

        let without_old: Arc<dyn ReferenceKeyProvider> = Arc::new(
            StaticReferenceKeyProvider::new("k2", [ReferenceKey::new("k2", [0x22; 32]).unwrap()])
                .unwrap(),
        );
        let current = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test-a",
            without_old,
            Arc::new(allow_known_item),
        )
        .unwrap()
        .with_clock(move || now);
        assert_eq!(
            current
                .consume(&alice(), &scope(), &reference, "OrderItem")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_INVALID"
        );
    }

    #[test]
    fn tokens_are_environment_and_service_bound_and_tamper_evident() {
        let runtime = runtime(DeploymentProfile::Test, None);
        let reference = runtime
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();
        let other_environment = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test-b",
            key_provider(),
            Arc::new(allow_known_item),
        )
        .unwrap();
        assert!(
            other_environment
                .consume(&alice(), &scope(), &reference, "OrderItem")
                .is_err()
        );
        let other_service = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "invoice-service",
            "test-a",
            key_provider(),
            Arc::new(allow_known_item),
        )
        .unwrap();
        assert!(
            other_service
                .consume(&alice(), &scope(), &reference, "OrderItem")
                .is_err()
        );
        let another_domain_root =
            TrustedReferencePrincipal::new("oidc", "alice", "Platform", 8).unwrap();
        assert_eq!(
            runtime
                .consume(&another_domain_root, &scope(), &reference, "OrderItem")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
        );
        for substituted_scope in [
            ReferenceDocumentScope::new("doc-100", "view-order", "Order", 100, 9).unwrap(),
            ReferenceDocumentScope::new("doc-100", "edit-order", "Order", 101, 9).unwrap(),
            ReferenceDocumentScope::new("doc-100", "edit-order", "Order", 100, 10).unwrap(),
        ] {
            assert!(
                runtime
                    .consume(&alice(), &substituted_scope, &reference, "OrderItem")
                    .is_err()
            );
        }

        let ExternalEntityReference::Governed(token) = reference else {
            unreachable!()
        };
        let mut tampered = token.into_bytes();
        let last = tampered.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = ExternalEntityReference::Governed(String::from_utf8(tampered).unwrap());
        assert!(
            runtime
                .consume(&alice(), &scope(), &tampered, "OrderItem")
                .is_err()
        );
    }

    #[test]
    fn fresh_nonces_make_equal_references_unlinkable() {
        let now = DateTime::parse_from_rfc3339("2026-09-27T00:00:00Z")
            .unwrap()
            .to_utc();
        let next = Arc::new(AtomicU8::new(1));
        let source = next.clone();
        let runtime = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test-a",
            key_provider(),
            Arc::new(allow_known_item),
        )
        .unwrap()
        .with_clock(move || now)
        .with_nonce_source(move || [source.fetch_add(1, Ordering::SeqCst); 12]);
        let first = runtime
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();
        let second = runtime
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();
        assert_ne!(first, second);
        assert!(
            runtime
                .consume(&alice(), &scope(), &first, "OrderItem")
                .is_ok()
        );
        assert!(
            runtime
                .consume(&alice(), &scope(), &second, "OrderItem")
                .is_ok()
        );
    }

    #[test]
    fn expired_reference_returns_coarse_expiry_code() {
        let issued = DateTime::parse_from_rfc3339("2026-09-27T00:00:00Z")
            .unwrap()
            .to_utc();
        let issuer = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test-a",
            key_provider(),
            Arc::new(allow_known_item),
        )
        .unwrap()
        .with_clock(move || issued)
        .with_nonce_source(|| [0x55; 12]);
        let reference = issuer
            .issue(&alice(), &scope(), identity(), Duration::from_secs(60))
            .unwrap();
        let later = issued + TimeDelta::seconds(61);
        let consumer = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test-a",
            key_provider(),
            Arc::new(allow_known_item),
        )
        .unwrap()
        .with_clock(move || later);
        assert_eq!(
            consumer
                .consume(&alice(), &scope(), &reference, "OrderItem")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_EXPIRED"
        );
    }

    #[test]
    fn user_context_is_the_only_high_level_issue_and_consume_boundary() {
        let runtime = Arc::new(runtime(DeploymentProfile::Test, None));
        let missing_principal = UserContext::new()
            .with_round_trip_reference_runtime(runtime.clone())
            .reference_for(identity(), &scope(), Duration::from_secs(600))
            .unwrap_err();
        assert_eq!(
            missing_principal.code(),
            "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED"
        );

        let context = UserContext::new()
            .with_round_trip_reference_runtime(runtime)
            .with_trusted_reference_principal(alice());
        let reference = context
            .reference_for(identity(), &scope(), Duration::from_secs(600))
            .unwrap();
        let wire = context.serialize_reference(&reference).unwrap();
        let decoded = context.deserialize_reference(&wire).unwrap();
        let resolved = context
            .resolve_reference(&decoded, "OrderItem", &scope())
            .unwrap();
        assert_eq!(resolved.identity, identity());

        assert_eq!(
            UserContext::new()
                .reference_for(identity(), &scope(), Duration::from_secs(600))
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_CONFIGURATION_INVALID"
        );
    }

    #[test]
    fn raw_01_unset_uses_governed_references() {
        assert_eq!(
            runtime(DeploymentProfile::Development, None).mode(),
            ReferenceMode::Governed
        );
    }

    #[test]
    fn raw_02_exact_acknowledgement_enables_development_raw_mode() {
        let runtime = runtime(
            DeploymentProfile::Development,
            Some(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT),
        );
        assert_eq!(runtime.mode(), ReferenceMode::Raw);
        assert_eq!(
            runtime
                .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
                .unwrap(),
            ExternalEntityReference::Raw { id: 42, version: 3 }
        );
    }

    #[test]
    fn raw_03_exact_acknowledgement_enables_test_raw_mode() {
        assert_eq!(
            runtime(
                DeploymentProfile::Test,
                Some(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT),
            )
            .mode(),
            ReferenceMode::Raw
        );
    }

    #[test]
    fn raw_04_near_match_does_not_enable_raw_mode() {
        for acknowledgement in [
            "true",
            "1",
            "raw",
            " I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY",
            "I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY ",
        ] {
            assert_eq!(
                runtime(DeploymentProfile::Development, Some(acknowledgement)).mode(),
                ReferenceMode::Governed
            );
        }
    }

    #[test]
    fn raw_05_production_fails_fast_when_raw_mode_is_requested() {
        let result = ContextBoundReferenceRuntime::from_acknowledgement(
            DeploymentProfile::Production,
            "order-service",
            "production",
            key_provider(),
            Arc::new(allow_known_item),
            Some(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT),
        );
        let error = match result {
            Ok(_) => panic!("production raw mode must fail"),
            Err(error) => error,
        };
        assert_eq!(error.code(), "ROUND_TRIP_REFERENCE_CONFIGURATION_INVALID");
    }

    #[test]
    fn raw_06_reference_from_another_actor_is_still_rejected_by_current_authorization() {
        let runtime = runtime(
            DeploymentProfile::Test,
            Some(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT),
        );
        let reference = runtime
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();
        assert_eq!(
            runtime
                .consume(&bob(), &scope(), &reference, "OrderItem")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED"
        );
    }

    #[test]
    fn raw_07_mode_change_rejects_old_wire_shape() {
        let raw = ExternalEntityReference::Raw { id: 42, version: 3 };
        let raw_wire = ReferenceWireCodec::serialize(&raw);
        assert!(ReferenceWireCodec::deserialize(&raw_wire, ReferenceMode::Governed).is_err());

        let governed = runtime(DeploymentProfile::Test, None)
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();
        let governed_wire = ReferenceWireCodec::serialize(&governed);
        assert!(ReferenceWireCodec::deserialize(&governed_wire, ReferenceMode::Raw).is_err());
    }

    #[test]
    fn raw_08_retains_version_type_and_authorization_guards() {
        let runtime = runtime(
            DeploymentProfile::Test,
            Some(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT),
        );
        let reference = ExternalEntityReference::Raw { id: 42, version: 3 };
        let resolved = runtime
            .consume(&alice(), &scope(), &reference, "OrderItem")
            .unwrap();
        assert_eq!(resolved.identity.version, 3);
        assert_eq!(resolved.identity.entity_type, "OrderItem");
        assert!(
            runtime
                .consume(&alice(), &scope(), &reference, "InvoiceItem")
                .is_err()
        );
    }

    #[test]
    fn raw_09_exposes_safe_downgrade_metadata_without_secret_material() {
        let runtime = runtime(
            DeploymentProfile::Test,
            Some(UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT),
        );
        let notice = runtime.startup_notice().unwrap();
        assert_eq!(notice.level, "ERROR");
        assert_eq!(notice.deployment_profile, "test");
        assert_eq!(notice.telemetry_key, "teaql.reference.mode");
        assert_eq!(notice.telemetry_value, "raw");
        assert_eq!(
            notice.response_headers,
            [
                ("TeaQL-Reference-Mode", "raw-internal-id"),
                ("Cache-Control", "no-store"),
            ]
        );
        assert!(!notice.message.contains("k2"));
        assert!(!notice.message.contains("22"));
    }

    #[test]
    fn current_authorization_is_rechecked_when_a_reference_returns() {
        let allowed = Arc::new(AtomicU8::new(1));
        let policy_state = allowed.clone();
        let authorization = move |principal: &TrustedReferencePrincipal,
                                  _: &ReferenceDocumentScope,
                                  identity: &ReferenceIdentity| {
            if policy_state.load(Ordering::SeqCst) == 1
                && principal.subject == "alice"
                && identity.entity_type == "OrderItem"
                && identity.id == 42
            {
                Ok(())
            } else {
                Err(RoundTripReferenceError::authorization_required())
            }
        };
        let runtime = ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test-a",
            key_provider(),
            Arc::new(authorization),
        )
        .unwrap();
        let reference = runtime
            .issue(&alice(), &scope(), identity(), Duration::from_secs(600))
            .unwrap();

        allowed.store(0, Ordering::SeqCst);
        assert_eq!(
            runtime
                .consume(&alice(), &scope(), &reference, "OrderItem")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED"
        );
    }

    #[test]
    fn wire_codec_rejects_extra_raw_fields_and_invalid_numbers() {
        assert!(
            ReferenceWireCodec::deserialize(
                &json!({"id": 42, "version": 3, "type": "OrderItem"}),
                ReferenceMode::Raw,
            )
            .is_err()
        );
        assert!(
            ReferenceWireCodec::deserialize(&json!({"id": 0, "version": 3}), ReferenceMode::Raw,)
                .is_err()
        );
        assert!(
            ReferenceWireCodec::deserialize(&json!({"id": 42, "version": -1}), ReferenceMode::Raw,)
                .is_err()
        );
        assert!(
            runtime(DeploymentProfile::Test, None)
                .issue(&alice(), &scope(), identity(), Duration::from_millis(999))
                .is_err()
        );
        let oversized =
            ExternalEntityReference::Governed(format!("tqr1.k2.{}", "A".repeat(24_001)));
        assert!(
            runtime(DeploymentProfile::Test, None)
                .consume(&alice(), &scope(), &oversized, "OrderItem")
                .is_err()
        );
    }
}
