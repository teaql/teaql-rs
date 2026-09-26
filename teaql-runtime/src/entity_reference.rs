use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, TimeDelta, Utc};

pub const ENTITY_REFERENCE_AAD: &str = "teaql.entity-reference.v1";
pub const UNSAFE_RAW_ENTITY_REFERENCES_ENVIRONMENT: &str = "TEAQL_UNSAFE_RAW_ENTITY_REFERENCES";
pub const UNSAFE_RAW_ENTITY_REFERENCES_ACKNOWLEDGEMENT: &str =
    "I_UNDERSTAND_RAW_ENTITY_IDS_ARE_VISIBLE_FOR_LOCAL_DEVELOPMENT_ONLY";
const ENCRYPTED_PREFIX: &str = "tqr1.";
const RAW_PREFIX: &str = "tqr0.";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityReferenceClaims {
    pub entity_type: String,
    pub id: u64,
    pub version: i64,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub purpose: String,
    pub key_version: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityReferenceTokenError {
    code: &'static str,
}

impl EntityReferenceTokenError {
    pub const fn invalid() -> Self {
        Self {
            code: "ENTITY_REFERENCE_INVALID",
        }
    }

    pub const fn codec_required() -> Self {
        Self {
            code: "ENTITY_REFERENCE_CODEC_REQUIRED",
        }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl Display for EntityReferenceTokenError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for EntityReferenceTokenError {}

pub trait EntityReferenceCodec: Send + Sync {
    fn encode(
        &self,
        entity_type: &str,
        id: u64,
        version: i64,
        purpose: &str,
        lifetime: Duration,
    ) -> Result<String, EntityReferenceTokenError>;

    fn decode(
        &self,
        token: &str,
        expected_entity_type: &str,
        purpose: &str,
    ) -> Result<EntityReferenceClaims, EntityReferenceTokenError>;
}

#[derive(Clone)]
pub(crate) struct EntityReferenceCodecResource(pub Arc<dyn EntityReferenceCodec>);

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
type NonceSource = Arc<dyn Fn() -> [u8; 12] + Send + Sync>;

pub struct AeadEntityReferenceCodec {
    active_key_version: u32,
    keys: BTreeMap<u32, [u8; 32]>,
    clock: Clock,
    nonce_source: NonceSource,
}

impl AeadEntityReferenceCodec {
    pub fn new(
        active_key_version: u32,
        keys: impl IntoIterator<Item = (u32, Vec<u8>)>,
    ) -> Result<Self, EntityReferenceTokenError> {
        let mut validated = BTreeMap::new();
        for (version, key) in keys {
            let key: [u8; 32] = key
                .try_into()
                .map_err(|_| EntityReferenceTokenError::invalid())?;
            validated.insert(version, key);
        }
        if !validated.contains_key(&active_key_version) {
            return Err(EntityReferenceTokenError::invalid());
        }
        Ok(Self {
            active_key_version,
            keys: validated,
            clock: Arc::new(Utc::now),
            nonce_source: Arc::new(|| Aes256Gcm::generate_nonce(&mut OsRng).into()),
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
}

impl EntityReferenceCodec for AeadEntityReferenceCodec {
    fn encode(
        &self,
        entity_type: &str,
        id: u64,
        version: i64,
        purpose: &str,
        lifetime: Duration,
    ) -> Result<String, EntityReferenceTokenError> {
        if entity_type.trim().is_empty() || id == 0 || lifetime.is_zero() {
            return Err(EntityReferenceTokenError::invalid());
        }
        let now = (self.clock)();
        let delta =
            TimeDelta::from_std(lifetime).map_err(|_| EntityReferenceTokenError::invalid())?;
        let claims = EntityReferenceClaims {
            entity_type: entity_type.to_owned(),
            id,
            version,
            issued_at: now,
            expires_at: now + delta,
            purpose: purpose.to_owned(),
            key_version: self.active_key_version,
        };
        let plaintext = encode_claims(&claims)?;
        let nonce = (self.nonce_source)();
        let cipher = Aes256Gcm::new_from_slice(&self.keys[&self.active_key_version])
            .map_err(|_| EntityReferenceTokenError::invalid())?;
        let encrypted = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &plaintext,
                    aad: ENTITY_REFERENCE_AAD.as_bytes(),
                },
            )
            .map_err(|_| EntityReferenceTokenError::invalid())?;
        let mut envelope = Vec::with_capacity(4 + nonce.len() + encrypted.len());
        envelope.extend_from_slice(&self.active_key_version.to_be_bytes());
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&encrypted);
        Ok(format!(
            "{ENCRYPTED_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(envelope)
        ))
    }

    fn decode(
        &self,
        token: &str,
        expected_entity_type: &str,
        purpose: &str,
    ) -> Result<EntityReferenceClaims, EntityReferenceTokenError> {
        let encoded = token
            .strip_prefix(ENCRYPTED_PREFIX)
            .ok_or_else(EntityReferenceTokenError::invalid)?;
        let envelope = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| EntityReferenceTokenError::invalid())?;
        if envelope.len() < 4 + 12 + 16 {
            return Err(EntityReferenceTokenError::invalid());
        }
        let key_version = u32::from_be_bytes(envelope[0..4].try_into().unwrap());
        let key = self
            .keys
            .get(&key_version)
            .ok_or_else(EntityReferenceTokenError::invalid)?;
        let cipher =
            Aes256Gcm::new_from_slice(key).map_err(|_| EntityReferenceTokenError::invalid())?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&envelope[4..16]),
                Payload {
                    msg: &envelope[16..],
                    aad: ENTITY_REFERENCE_AAD.as_bytes(),
                },
            )
            .map_err(|_| EntityReferenceTokenError::invalid())?;
        let mut claims = decode_claims(&plaintext)?;
        claims.key_version = key_version;
        let now = (self.clock)();
        if claims.expires_at <= now
            || claims.issued_at > now + TimeDelta::minutes(1)
            || claims.entity_type != expected_entity_type
            || claims.purpose != purpose
        {
            return Err(EntityReferenceTokenError::invalid());
        }
        Ok(claims)
    }
}

pub(crate) fn raw_references_enabled() -> bool {
    std::env::var(UNSAFE_RAW_ENTITY_REFERENCES_ENVIRONMENT).as_deref()
        == Ok(UNSAFE_RAW_ENTITY_REFERENCES_ACKNOWLEDGEMENT)
}

pub(crate) fn encode_raw(
    entity_type: &str,
    id: u64,
    version: i64,
    purpose: &str,
    lifetime: Duration,
) -> Result<String, EntityReferenceTokenError> {
    if entity_type.trim().is_empty() || id == 0 || lifetime.is_zero() {
        return Err(EntityReferenceTokenError::invalid());
    }
    let now = Utc::now();
    let delta = TimeDelta::from_std(lifetime).map_err(|_| EntityReferenceTokenError::invalid())?;
    let claims = EntityReferenceClaims {
        entity_type: entity_type.to_owned(),
        id,
        version,
        issued_at: now,
        expires_at: now + delta,
        purpose: purpose.to_owned(),
        key_version: 0,
    };
    Ok(format!(
        "{RAW_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(encode_claims(&claims)?)
    ))
}

pub(crate) fn decode_raw(
    token: &str,
    expected_entity_type: &str,
    purpose: &str,
) -> Result<EntityReferenceClaims, EntityReferenceTokenError> {
    let encoded = token
        .strip_prefix(RAW_PREFIX)
        .ok_or_else(EntityReferenceTokenError::invalid)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| EntityReferenceTokenError::invalid())?;
    let claims = decode_claims(&bytes)?;
    let now = Utc::now();
    if claims.expires_at <= now
        || claims.issued_at > now + TimeDelta::minutes(1)
        || claims.entity_type != expected_entity_type
        || claims.purpose != purpose
    {
        return Err(EntityReferenceTokenError::invalid());
    }
    Ok(claims)
}

pub(crate) fn is_raw_reference_token(token: &str) -> bool {
    token.starts_with(RAW_PREFIX)
}

fn encode_claims(claims: &EntityReferenceClaims) -> Result<Vec<u8>, EntityReferenceTokenError> {
    let mut output = Vec::with_capacity(claims.entity_type.len() + claims.purpose.len() + 36);
    write_text(&mut output, &claims.entity_type)?;
    output.extend_from_slice(&claims.id.to_be_bytes());
    output.extend_from_slice(&claims.version.to_be_bytes());
    output.extend_from_slice(&claims.issued_at.timestamp().to_be_bytes());
    output.extend_from_slice(&claims.expires_at.timestamp().to_be_bytes());
    write_text(&mut output, &claims.purpose)?;
    Ok(output)
}

fn decode_claims(data: &[u8]) -> Result<EntityReferenceClaims, EntityReferenceTokenError> {
    let mut offset = 0;
    let entity_type = read_text(data, &mut offset)?;
    if data.len().saturating_sub(offset) < 32 {
        return Err(EntityReferenceTokenError::invalid());
    }
    let id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let version = i64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let issued = i64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let expires = i64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let purpose = read_text(data, &mut offset)?;
    if offset != data.len() || entity_type.is_empty() || id == 0 {
        return Err(EntityReferenceTokenError::invalid());
    }
    Ok(EntityReferenceClaims {
        entity_type,
        id,
        version,
        issued_at: DateTime::from_timestamp(issued, 0)
            .ok_or_else(EntityReferenceTokenError::invalid)?,
        expires_at: DateTime::from_timestamp(expires, 0)
            .ok_or_else(EntityReferenceTokenError::invalid)?,
        purpose,
        key_version: 0,
    })
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Result<(), EntityReferenceTokenError> {
    let length: u16 = value
        .len()
        .try_into()
        .map_err(|_| EntityReferenceTokenError::invalid())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_text(data: &[u8], offset: &mut usize) -> Result<String, EntityReferenceTokenError> {
    if data.len().saturating_sub(*offset) < 2 {
        return Err(EntityReferenceTokenError::invalid());
    }
    let length = u16::from_be_bytes(data[*offset..*offset + 2].try_into().unwrap()) as usize;
    *offset += 2;
    if data.len().saturating_sub(*offset) < length {
        return Err(EntityReferenceTokenError::invalid());
    }
    let value = std::str::from_utf8(&data[*offset..*offset + length])
        .map_err(|_| EntityReferenceTokenError::invalid())?
        .to_owned();
    *offset += length;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserContext;

    const GOLDEN: &str = "tqr1.AAAAAjMzMzMzMzMzMzMzM3bKiZgRSQQhfIj2cBXRDZIloUGHWLBp8QrXL_aejwIXPFtvV_E71O7wbOXy3cvYo_SwxvuS-89x572T9CO_pDAY4tbjWCNv";

    #[test]
    fn portable_reference_is_opaque_bound_rotatable_and_expiring() {
        let now = DateTime::parse_from_rfc3339("2026-09-26T12:00:00Z")
            .unwrap()
            .to_utc();
        let codec = AeadEntityReferenceCodec::new(2, [(2, vec![0x22; 32])])
            .unwrap()
            .with_clock(move || now)
            .with_nonce_source(|| [0x33; 12]);
        let token = codec
            .encode("OrderItem", 42, 7, "edit-order", Duration::from_secs(3600))
            .unwrap();
        assert_eq!(token, GOLDEN);
        assert!(!token.contains("OrderItem"));
        let claims = codec.decode(&token, "OrderItem", "edit-order").unwrap();
        assert_eq!((claims.id, claims.version, claims.key_version), (42, 7, 2));
        assert!(codec.decode(&token, "InvoiceItem", "edit-order").is_err());
        assert!(codec.decode(&token, "OrderItem", "other-purpose").is_err());
        let tampered = format!("{}A", &token[..token.len() - 1]);
        assert!(codec.decode(&tampered, "OrderItem", "edit-order").is_err());
    }

    #[test]
    fn user_context_owns_codec_and_missing_provider_fails_closed() {
        let missing = UserContext::new()
            .encode_entity_reference("Order", 1, 1, "edit", Duration::from_secs(60))
            .unwrap_err();
        assert_eq!(missing.code(), "ENTITY_REFERENCE_CODEC_REQUIRED");

        let now = DateTime::parse_from_rfc3339("2026-09-26T12:00:00Z")
            .unwrap()
            .to_utc();
        let codec = AeadEntityReferenceCodec::new(2, [(2, vec![0x22; 32])])
            .unwrap()
            .with_clock(move || now)
            .with_nonce_source(|| [0x33; 12]);
        let context = UserContext::new().with_entity_reference_codec(Arc::new(codec));
        let token = context
            .encode_entity_reference("OrderItem", 42, 7, "edit-order", Duration::from_secs(3600))
            .unwrap();
        assert_eq!(token, GOLDEN);
        assert_eq!(
            context
                .decode_entity_reference(&token, "OrderItem", "edit-order")
                .unwrap()
                .id,
            42
        );
    }
}
