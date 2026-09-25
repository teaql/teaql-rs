use std::collections::BTreeMap;
use std::sync::Arc;

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine as _;
use hkdf::Hkdf;
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use teaql_core::{IdentifiableEntity, TeaqlEntity, VersionedEntity};

use crate::UserContext;

pub const RAW_ID_ENV: &str = "TEAQL_UNSAFE_EXPOSE_RAW_ENTITY_IDS";
pub const RAW_ID_ACKNOWLEDGEMENT: &str =
    "I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalEntityIdentity {
    pub entity_type: String,
    pub id: u64,
    pub version: i64,
}

impl InternalEntityIdentity {
    pub fn new(
        entity_type: impl Into<String>,
        id: u64,
        version: i64,
    ) -> Result<Self, RoundTripReferenceError> {
        let entity_type = entity_type.into();
        if entity_type.trim().is_empty() || id == 0 || version <= 0 {
            return Err(RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::InvalidEntityIdentity,
                "entity type, id and version must identify a persisted live entity",
            ));
        }
        Ok(Self {
            entity_type,
            id,
            version,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundTripReference(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRoundTripReference {
    pub entity_type: String,
    pub id: u64,
    pub version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundTripReferenceErrorCode {
    ProviderNotConfigured,
    ContextBindingRequired,
    InvalidReference,
    ContextMismatch,
    TypeMismatch,
    InvalidEntityIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundTripReferenceError {
    pub code: RoundTripReferenceErrorCode,
    pub message: String,
}

impl RoundTripReferenceError {
    pub fn new(code: RoundTripReferenceErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RoundTripReferenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for RoundTripReferenceError {}

pub trait RoundTripReferenceProvider: Send + Sync {
    fn issue(
        &self,
        context: &UserContext,
        identity: &InternalEntityIdentity,
    ) -> Result<RoundTripReference, RoundTripReferenceError>;

    fn resolve(
        &self,
        context: &UserContext,
        reference: &RoundTripReference,
        expected_entity_type: &str,
    ) -> Result<ResolvedRoundTripReference, RoundTripReferenceError>;
}

#[derive(Clone)]
pub struct RoundTripReferenceService {
    provider: Arc<dyn RoundTripReferenceProvider>,
}

impl RoundTripReferenceService {
    pub fn new(provider: impl RoundTripReferenceProvider + 'static) -> Self {
        Self {
            provider: Arc::new(provider),
        }
    }

    pub fn serialize<E>(
        &self,
        context: &UserContext,
        entity: &E,
    ) -> Result<RoundTripReference, RoundTripReferenceError>
    where
        E: TeaqlEntity + IdentifiableEntity + VersionedEntity,
    {
        let id = entity.id_value().try_u64().ok_or_else(|| {
            RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::InvalidEntityIdentity,
                "entity id must be a positive u64",
            )
        })?;
        let identity = InternalEntityIdentity::new(E::ENTITY_NAME, id, entity.version())?;
        self.provider.issue(context, &identity)
    }

    pub fn deserialize(
        &self,
        context: &UserContext,
        reference: &str,
        expected_entity_type: &str,
    ) -> Result<ResolvedRoundTripReference, RoundTripReferenceError> {
        if reference.trim().is_empty() {
            return Err(RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::InvalidReference,
                "reference must not be blank",
            ));
        }
        self.provider.resolve(
            context,
            &RoundTripReference(reference.to_owned()),
            expected_entity_type,
        )
    }
}

#[derive(Clone)]
pub struct RoundTripReferenceKey {
    pub key_id: String,
    key: [u8; 32],
}

impl RoundTripReferenceKey {
    pub fn new(key_id: impl Into<String>, key: [u8; 32]) -> Result<Self, RoundTripReferenceError> {
        let key_id = key_id.into();
        if key_id.is_empty()
            || key_id.len() > 32
            || !key_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::ContextBindingRequired,
                "key id must be 1-32 URL-safe characters",
            ));
        }
        Ok(Self { key_id, key })
    }

    fn bytes(&self) -> &[u8; 32] {
        &self.key
    }
}

/// Runtime customization point selected from the complete context.
pub trait RoundTripReferenceKeyProvider: Send + Sync {
    fn current_key(
        &self,
        context: &UserContext,
    ) -> Result<RoundTripReferenceKey, RoundTripReferenceError>;

    fn key_by_id(
        &self,
        context: &UserContext,
        key_id: &str,
    ) -> Result<Option<RoundTripReferenceKey>, RoundTripReferenceError>;
}

pub trait ContextReferenceBindingProvider: Send + Sync {
    fn binding_for(&self, context: &UserContext) -> Option<Vec<u8>>;
}

impl<F> ContextReferenceBindingProvider for F
where
    F: Fn(&UserContext) -> Option<Vec<u8>> + Send + Sync,
{
    fn binding_for(&self, context: &UserContext) -> Option<Vec<u8>> {
        self(context)
    }
}

pub trait RoundTripReferenceMasterKeyRing: Send + Sync {
    fn current_key(&self) -> RoundTripReferenceKey;
    fn key_by_id(&self, key_id: &str) -> Option<RoundTripReferenceKey>;
}

#[derive(Clone)]
pub struct StaticRoundTripReferenceMasterKeyRing {
    current: RoundTripReferenceKey,
    keys: BTreeMap<String, RoundTripReferenceKey>,
}

impl StaticRoundTripReferenceMasterKeyRing {
    pub fn new(
        current: RoundTripReferenceKey,
        decode_only: impl IntoIterator<Item = RoundTripReferenceKey>,
    ) -> Self {
        let mut keys = BTreeMap::new();
        keys.insert(current.key_id.clone(), current.clone());
        for key in decode_only {
            keys.insert(key.key_id.clone(), key);
        }
        Self { current, keys }
    }
}

impl RoundTripReferenceMasterKeyRing for StaticRoundTripReferenceMasterKeyRing {
    fn current_key(&self) -> RoundTripReferenceKey {
        self.current.clone()
    }

    fn key_by_id(&self, key_id: &str) -> Option<RoundTripReferenceKey> {
        self.keys.get(key_id).cloned()
    }
}

pub struct DerivedRoundTripReferenceKeyProvider {
    master_keys: Arc<dyn RoundTripReferenceMasterKeyRing>,
    bindings: Arc<dyn ContextReferenceBindingProvider>,
    namespace: Vec<u8>,
}

impl DerivedRoundTripReferenceKeyProvider {
    pub fn new(
        master_keys: impl RoundTripReferenceMasterKeyRing + 'static,
        bindings: impl ContextReferenceBindingProvider + 'static,
        namespace: impl Into<String>,
    ) -> Result<Self, RoundTripReferenceError> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::ContextBindingRequired,
                "reference namespace must not be blank",
            ));
        }
        Ok(Self {
            master_keys: Arc::new(master_keys),
            bindings: Arc::new(bindings),
            namespace: namespace.into_bytes(),
        })
    }

    fn derive(
        &self,
        context: &UserContext,
        master: RoundTripReferenceKey,
    ) -> Result<RoundTripReferenceKey, RoundTripReferenceError> {
        let binding = self
            .bindings
            .binding_for(context)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RoundTripReferenceError::new(
                    RoundTripReferenceErrorCode::ContextBindingRequired,
                    "runtime customization did not provide a round-trip reference binding",
                )
            })?;
        let hkdf = Hkdf::<Sha256>::new(Some(b"teaql-round-trip-reference-v1"), master.bytes());
        let mut info = b"teaql:tqr1".to_vec();
        append_length_prefixed(&mut info, &self.namespace);
        append_length_prefixed(&mut info, &binding);
        let mut key = [0_u8; 32];
        hkdf.expand(&info, &mut key).map_err(|_| {
            RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::ContextBindingRequired,
                "unable to derive round-trip reference key",
            )
        })?;
        RoundTripReferenceKey::new(master.key_id, key)
    }
}

impl RoundTripReferenceKeyProvider for DerivedRoundTripReferenceKeyProvider {
    fn current_key(
        &self,
        context: &UserContext,
    ) -> Result<RoundTripReferenceKey, RoundTripReferenceError> {
        self.derive(context, self.master_keys.current_key())
    }

    fn key_by_id(
        &self,
        context: &UserContext,
        key_id: &str,
    ) -> Result<Option<RoundTripReferenceKey>, RoundTripReferenceError> {
        self.master_keys
            .key_by_id(key_id)
            .map(|master| self.derive(context, master))
            .transpose()
    }
}

pub struct AeadRoundTripReferenceProvider {
    keys: Arc<dyn RoundTripReferenceKeyProvider>,
}

impl AeadRoundTripReferenceProvider {
    pub fn new(keys: impl RoundTripReferenceKeyProvider + 'static) -> Self {
        Self {
            keys: Arc::new(keys),
        }
    }
}

impl RoundTripReferenceProvider for AeadRoundTripReferenceProvider {
    fn issue(
        &self,
        context: &UserContext,
        identity: &InternalEntityIdentity,
    ) -> Result<RoundTripReference, RoundTripReferenceError> {
        let key = self.keys.current_key(context)?;
        let cipher = Aes256Gcm::new_from_slice(key.bytes()).map_err(|_| invalid_reference())?;
        let mut nonce = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let aad = aad(&key.key_id);
        let clear = encode_identity(identity)?;
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &clear,
                    aad: &aad,
                },
            )
            .map_err(|_| invalid_reference())?;
        let mut envelope = nonce.to_vec();
        envelope.extend_from_slice(&ciphertext);
        Ok(RoundTripReference(format!(
            "tqr1.{}.{}",
            key.key_id,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(envelope)
        )))
    }

    fn resolve(
        &self,
        context: &UserContext,
        reference: &RoundTripReference,
        expected_entity_type: &str,
    ) -> Result<ResolvedRoundTripReference, RoundTripReferenceError> {
        let parts = reference.0.split('.').collect::<Vec<_>>();
        if parts.len() != 3 || parts[0] != "tqr1" {
            return Err(invalid_reference());
        }
        let key = self
            .keys
            .key_by_id(context, parts[1])?
            .ok_or_else(invalid_reference)?;
        let envelope = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[2])
            .map_err(|_| invalid_reference())?;
        if envelope.len() <= 28 {
            return Err(invalid_reference());
        }
        let (nonce, ciphertext) = envelope.split_at(12);
        let cipher = Aes256Gcm::new_from_slice(key.bytes()).map_err(|_| invalid_reference())?;
        let aad = aad(&key.key_id);
        let clear = cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| {
                RoundTripReferenceError::new(
                    RoundTripReferenceErrorCode::ContextMismatch,
                    "round-trip reference is invalid for the current runtime context",
                )
            })?;
        let identity = decode_identity(&clear)?;
        if identity.entity_type != expected_entity_type {
            return Err(RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::TypeMismatch,
                "round-trip reference does not match the expected entity type",
            ));
        }
        Ok(ResolvedRoundTripReference {
            entity_type: identity.entity_type,
            id: identity.id,
            version: identity.version,
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RawRoundTripReferenceProvider;

impl RoundTripReferenceProvider for RawRoundTripReferenceProvider {
    fn issue(
        &self,
        _context: &UserContext,
        identity: &InternalEntityIdentity,
    ) -> Result<RoundTripReference, RoundTripReferenceError> {
        if identity.entity_type.contains('.') {
            return Err(RoundTripReferenceError::new(
                RoundTripReferenceErrorCode::InvalidEntityIdentity,
                "raw diagnostic entity type must not contain '.'",
            ));
        }
        Ok(RoundTripReference(format!(
            "raw1.{}.{}.{}",
            identity.entity_type, identity.id, identity.version
        )))
    }

    fn resolve(
        &self,
        _context: &UserContext,
        reference: &RoundTripReference,
        expected_entity_type: &str,
    ) -> Result<ResolvedRoundTripReference, RoundTripReferenceError> {
        let parts = reference.0.split('.').collect::<Vec<_>>();
        if parts.len() != 4 || parts[0] != "raw1" || parts[1] != expected_entity_type {
            return Err(invalid_reference());
        }
        let identity = InternalEntityIdentity::new(
            parts[1],
            parts[2].parse().map_err(|_| invalid_reference())?,
            parts[3].parse().map_err(|_| invalid_reference())?,
        )?;
        Ok(ResolvedRoundTripReference {
            entity_type: identity.entity_type,
            id: identity.id,
            version: identity.version,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundTripReferenceMode {
    Governed,
    RawDiagnostic,
}

pub fn round_trip_reference_mode(
    deployment_profile: &str,
    acknowledgement: Option<&str>,
) -> Result<RoundTripReferenceMode, RoundTripReferenceError> {
    if acknowledgement != Some(RAW_ID_ACKNOWLEDGEMENT) {
        return Ok(RoundTripReferenceMode::Governed);
    }
    if deployment_profile != "development" && deployment_profile != "test" {
        return Err(RoundTripReferenceError::new(
            RoundTripReferenceErrorCode::InvalidReference,
            format!("{RAW_ID_ENV} cannot enable raw entity IDs outside development/test"),
        ));
    }
    Ok(RoundTripReferenceMode::RawDiagnostic)
}

/// Selects once at runtime startup; Web adapters only consume the resulting service.
pub fn configured_round_trip_reference_service(
    deployment_profile: &str,
    governed_provider: impl RoundTripReferenceProvider + 'static,
) -> Result<RoundTripReferenceService, RoundTripReferenceError> {
    let acknowledgement = std::env::var(RAW_ID_ENV).ok();
    match round_trip_reference_mode(deployment_profile, acknowledgement.as_deref())? {
        RoundTripReferenceMode::Governed => Ok(RoundTripReferenceService::new(governed_provider)),
        RoundTripReferenceMode::RawDiagnostic => Ok(RoundTripReferenceService::new(
            RawRoundTripReferenceProvider,
        )),
    }
}

fn aad(key_id: &str) -> Vec<u8> {
    format!("tqr1.{key_id}").into_bytes()
}

fn encode_identity(identity: &InternalEntityIdentity) -> Result<Vec<u8>, RoundTripReferenceError> {
    let entity_type = identity.entity_type.as_bytes();
    let length: u16 = entity_type.len().try_into().map_err(|_| {
        RoundTripReferenceError::new(
            RoundTripReferenceErrorCode::InvalidEntityIdentity,
            "entity type is too long",
        )
    })?;
    let mut bytes = Vec::with_capacity(19 + entity_type.len());
    bytes.push(1);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(entity_type);
    bytes.extend_from_slice(&identity.id.to_be_bytes());
    bytes.extend_from_slice(&identity.version.to_be_bytes());
    Ok(bytes)
}

fn decode_identity(bytes: &[u8]) -> Result<InternalEntityIdentity, RoundTripReferenceError> {
    if bytes.len() < 19 || bytes[0] != 1 {
        return Err(invalid_reference());
    }
    let length = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
    if bytes.len() != 19 + length {
        return Err(invalid_reference());
    }
    let entity_type =
        std::str::from_utf8(&bytes[3..3 + length]).map_err(|_| invalid_reference())?;
    let id = u64::from_be_bytes(
        bytes[3 + length..11 + length]
            .try_into()
            .map_err(|_| invalid_reference())?,
    );
    let version = i64::from_be_bytes(
        bytes[11 + length..19 + length]
            .try_into()
            .map_err(|_| invalid_reference())?,
    );
    InternalEntityIdentity::new(entity_type, id, version)
}

fn append_length_prefixed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u32).to_be_bytes());
    target.extend_from_slice(value);
}

fn invalid_reference() -> RoundTripReferenceError {
    RoundTripReferenceError::new(
        RoundTripReferenceErrorCode::InvalidReference,
        "invalid round-trip reference",
    )
}
