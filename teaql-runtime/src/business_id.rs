use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{NaiveDate, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use teaql_core::business_id::{
    BUSINESS_ID_V1_DOMAIN_SIZE, BusinessIdAllocation, BusinessIdAllocator, BusinessIdDefinition,
    BusinessIdError, BusinessIdErrorCode, BusinessIdGenerationRequest, BusinessIdPlan,
    BusinessIdProfile, BusinessIdScope, BusinessIdSlot, BusinessIdValue,
    DEFAULT_BUSINESS_ID_PROFILE, LEGACY_DAILY_SEQUENCE_PROFILE,
};

use crate::UserContext;

/// Provider-owned Business ID schema contribution.
///
/// Installing an allocator is passive. The runtime invokes this hook only from
/// [`UserContext::ensure_schema`], so constructing a context never performs DDL.
pub trait BusinessIdSchemaContributor: Send + Sync {
    fn ensure_schema(&self, context: &UserContext) -> Result<(), BusinessIdError>;
}

#[derive(Clone)]
pub(crate) struct BusinessIdSchemaService {
    contributor: Arc<dyn BusinessIdSchemaContributor>,
}

impl BusinessIdSchemaService {
    pub(crate) fn from_shared(contributor: Arc<dyn BusinessIdSchemaContributor>) -> Self {
        Self { contributor }
    }

    pub(crate) fn ensure_schema(&self, context: &UserContext) -> Result<(), BusinessIdError> {
        self.contributor.ensure_schema(context)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BusinessDate(pub NaiveDate);

impl BusinessDate {
    pub fn today() -> Self {
        Self(Utc::now().date_naive())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DailySequenceBusinessIdProfile;

impl BusinessIdProfile for DailySequenceBusinessIdProfile {
    fn plan(
        &self,
        request: &BusinessIdGenerationRequest<'_>,
    ) -> Result<BusinessIdPlan, BusinessIdError> {
        let definition = request.definition;
        if definition.profile != LEGACY_DAILY_SEQUENCE_PROFILE {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                format!("unsupported business ID profile {}", definition.profile),
            ));
        }
        if definition.prefix.trim().is_empty()
            || definition.namespace.trim().is_empty()
            || definition.separator.is_empty()
        {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "prefix, namespace and separator must not be blank",
            ));
        }
        let date_text = definition
            .split_by_date
            .then(|| request.business_date.format("%Y%m%d").to_string());
        Ok(BusinessIdPlan {
            scope: BusinessIdScope {
                domain_root_key: request.domain_root_key.to_owned(),
                aggregate_type: request.aggregate_type.to_owned(),
                namespace: definition.namespace.clone(),
                period_key: date_text.clone().unwrap_or_default(),
            },
            prefix: definition.prefix.clone(),
            date_text,
            preserve_digits: definition.preserve_digits,
            separator: definition.separator.clone(),
            initial_sequence: 1,
            max_sequence: definition.max_sequence()?,
        })
    }

    fn format(
        &self,
        plan: &BusinessIdPlan,
        allocation: &BusinessIdAllocation,
    ) -> Result<BusinessIdValue, BusinessIdError> {
        if allocation.scope != plan.scope || allocation.sequence > plan.max_sequence {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::Exhausted,
                "business ID allocation does not belong to the plan or exceeds its range",
            ));
        }
        let sequence = format!(
            "{:0width$}",
            allocation.sequence,
            width = usize::from(plan.preserve_digits)
        );
        let value = match &plan.date_text {
            Some(date) => [plan.prefix.as_str(), date, sequence.as_str()].join(&plan.separator),
            None => [plan.prefix.as_str(), sequence.as_str()].join(&plan.separator),
        };
        Ok(BusinessIdValue(value))
    }

    fn validate(
        &self,
        definition: &BusinessIdDefinition,
        value: &str,
    ) -> Result<BusinessIdValue, BusinessIdError> {
        if definition.profile != LEGACY_DAILY_SEQUENCE_PROFILE
            || definition.prefix.trim().is_empty()
            || definition.namespace.trim().is_empty()
            || definition.separator.is_empty()
        {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "invalid daily-sequence Business ID definition",
            ));
        }
        let parts = value.split(&definition.separator).collect::<Vec<_>>();
        let valid = if definition.split_by_date {
            parts.len() == 3
                && parts[0] == definition.prefix
                && parts[1].len() == 8
                && parts[1].bytes().all(|value| value.is_ascii_digit())
                && chrono::NaiveDate::parse_from_str(parts[1], "%Y%m%d").is_ok()
                && valid_sequence(parts[2], definition.preserve_digits)
        } else {
            parts.len() == 2
                && parts[0] == definition.prefix
                && valid_sequence(parts[1], definition.preserve_digits)
        };
        if !valid {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidFormat,
                format!("invalid daily-sequence Business ID: {value}"),
            ));
        }
        Ok(BusinessIdValue(value.to_owned()))
    }
}

fn valid_sequence(value: &str, preserve_digits: u8) -> bool {
    value.len() == usize::from(preserve_digits) && value.bytes().all(|value| value.is_ascii_digit())
}

pub const BUSINESS_ID_V1_ALPHABET: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
pub const BUSINESS_ID_V1_WIDTH: usize = 6;
const BUSINESS_ID_V1_ROUNDS: u8 = 8;
const BUSINESS_ID_V1_MAGIC: &[u8] = b"teaql-business-id-fp-v1\0";

#[derive(Clone, PartialEq, Eq)]
pub struct BusinessIdEncodingKey {
    version: u32,
    bytes: [u8; 32],
}

impl BusinessIdEncodingKey {
    pub fn new(version: u32, bytes: [u8; 32]) -> Result<Self, BusinessIdError> {
        if version == 0 {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "business ID key version must be positive",
            ));
        }
        Ok(Self { version, bytes })
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

pub trait BusinessIdKeyProvider: Send + Sync {
    fn current_key(
        &self,
        context: &UserContext,
        definition: &BusinessIdDefinition,
    ) -> Result<BusinessIdEncodingKey, BusinessIdError>;
}

#[derive(Clone)]
pub struct StaticBusinessIdKeyProvider {
    key: BusinessIdEncodingKey,
}

impl StaticBusinessIdKeyProvider {
    pub fn new(key: BusinessIdEncodingKey) -> Self {
        Self { key }
    }
}

impl BusinessIdKeyProvider for StaticBusinessIdKeyProvider {
    fn current_key(
        &self,
        _context: &UserContext,
        _definition: &BusinessIdDefinition,
    ) -> Result<BusinessIdEncodingKey, BusinessIdError> {
        Ok(self.key.clone())
    }
}

pub struct BusinessIdPermutationV1;

impl BusinessIdPermutationV1 {
    pub fn encode(
        sequence: u64,
        scope: &BusinessIdScope,
        key: &BusinessIdEncodingKey,
    ) -> Result<String, BusinessIdError> {
        if sequence >= BUSINESS_ID_V1_DOMAIN_SIZE {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::Exhausted,
                format!(
                    "business ID V1 sequence must be in 0..{}",
                    BUSINESS_ID_V1_DOMAIN_SIZE - 1
                ),
            ));
        }
        let tweak = canonical_tweak(scope, key.version)?;
        let mut candidate = u32::try_from(sequence).map_err(|_| {
            BusinessIdError::new(
                BusinessIdErrorCode::Exhausted,
                "business ID V1 sequence exceeds u32",
            )
        })?;
        loop {
            candidate = permute32(candidate, &tweak, &key.bytes)?;
            if u64::from(candidate) < BUSINESS_ID_V1_DOMAIN_SIZE {
                return Ok(encode_base36(candidate));
            }
        }
    }
}

fn canonical_tweak(scope: &BusinessIdScope, key_version: u32) -> Result<Vec<u8>, BusinessIdError> {
    let fields = [
        scope.domain_root_key.as_str(),
        scope.aggregate_type.as_str(),
        scope.namespace.as_str(),
        scope.period_key.as_str(),
    ];
    if fields.iter().any(|field| field.trim().is_empty()) {
        return Err(BusinessIdError::new(
            BusinessIdErrorCode::InvalidDefinition,
            "business ID V1 scope fields must not be blank",
        ));
    }
    let mut result = Vec::with_capacity(128);
    result.extend_from_slice(BUSINESS_ID_V1_MAGIC);
    result.push(1);
    result.extend_from_slice(&key_version.to_be_bytes());
    for field in fields {
        let bytes = field.as_bytes();
        let length = u32::try_from(bytes.len()).map_err(|_| {
            BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "business ID V1 scope field is too long",
            )
        })?;
        result.extend_from_slice(&length.to_be_bytes());
        result.extend_from_slice(bytes);
    }
    Ok(result)
}

fn permute32(value: u32, tweak: &[u8], key: &[u8; 32]) -> Result<u32, BusinessIdError> {
    let mut left = (value >> 16) as u16;
    let mut right = value as u16;
    for round in 0..BUSINESS_ID_V1_ROUNDS {
        let mut hmac = <Hmac<Sha256> as Mac>::new_from_slice(key).map_err(|_| {
            BusinessIdError::new(
                BusinessIdErrorCode::Encoding,
                "unable to initialize HMAC-SHA256 for Business ID V1",
            )
        })?;
        hmac.update(tweak);
        hmac.update(&[round]);
        hmac.update(&right.to_be_bytes());
        let digest = hmac.finalize().into_bytes();
        let function = u16::from_be_bytes([digest[0], digest[1]]);
        (left, right) = (right, left ^ function);
    }
    Ok((u32::from(left) << 16) | u32::from(right))
}

fn encode_base36(mut value: u32) -> String {
    let alphabet = BUSINESS_ID_V1_ALPHABET.as_bytes();
    let mut encoded = [b'0'; BUSINESS_ID_V1_WIDTH];
    for index in (0..BUSINESS_ID_V1_WIDTH).rev() {
        encoded[index] = alphabet[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8(encoded.to_vec()).expect("Base36 alphabet is UTF-8")
}

#[derive(Clone)]
pub struct PermutedDailyBusinessIdProfile {
    key: BusinessIdEncodingKey,
}

impl PermutedDailyBusinessIdProfile {
    pub fn new(key: BusinessIdEncodingKey) -> Self {
        Self { key }
    }
}

impl BusinessIdProfile for PermutedDailyBusinessIdProfile {
    fn plan(
        &self,
        request: &BusinessIdGenerationRequest<'_>,
    ) -> Result<BusinessIdPlan, BusinessIdError> {
        let definition = request.definition;
        if definition.profile != DEFAULT_BUSINESS_ID_PROFILE
            || !definition.split_by_date
            || definition.preserve_digits as usize != BUSINESS_ID_V1_WIDTH
        {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "daily-permuted-v1 requires split_by_date=true and preserve_digits=6",
            ));
        }
        if definition.prefix.trim().is_empty()
            || definition.namespace.trim().is_empty()
            || definition.separator.is_empty()
        {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "prefix, namespace and separator must not be blank",
            ));
        }
        let date_text = request.business_date.format("%Y%m%d").to_string();
        Ok(BusinessIdPlan {
            scope: BusinessIdScope {
                domain_root_key: request.domain_root_key.to_owned(),
                aggregate_type: request.aggregate_type.to_owned(),
                namespace: definition.namespace.clone(),
                period_key: date_text.clone(),
            },
            prefix: definition.prefix.clone(),
            date_text: Some(date_text),
            preserve_digits: definition.preserve_digits,
            separator: definition.separator.clone(),
            initial_sequence: 0,
            max_sequence: BUSINESS_ID_V1_DOMAIN_SIZE - 1,
        })
    }

    fn format(
        &self,
        plan: &BusinessIdPlan,
        allocation: &BusinessIdAllocation,
    ) -> Result<BusinessIdValue, BusinessIdError> {
        if allocation.scope != plan.scope || allocation.sequence > plan.max_sequence {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::Exhausted,
                "business ID allocation does not belong to the V1 plan or exceeds its range",
            ));
        }
        let code = BusinessIdPermutationV1::encode(allocation.sequence, &plan.scope, &self.key)?;
        let date = plan.date_text.as_deref().ok_or_else(|| {
            BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "daily-permuted-v1 plan is missing its period key",
            )
        })?;
        Ok(BusinessIdValue(
            [plan.prefix.as_str(), date, code.as_str()].join(&plan.separator),
        ))
    }

    fn validate(
        &self,
        definition: &BusinessIdDefinition,
        value: &str,
    ) -> Result<BusinessIdValue, BusinessIdError> {
        let parts = value.split(&definition.separator).collect::<Vec<_>>();
        let valid = definition.profile == DEFAULT_BUSINESS_ID_PROFILE
            && parts.len() == 3
            && parts[0] == definition.prefix
            && chrono::NaiveDate::parse_from_str(parts[1], "%Y%m%d").is_ok()
            && parts[2].len() == BUSINESS_ID_V1_WIDTH
            && parts[2]
                .bytes()
                .all(|value| value.is_ascii_digit() || value.is_ascii_uppercase());
        if !valid {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidFormat,
                format!("invalid daily-permuted-v1 Business ID: {value}"),
            ));
        }
        Ok(BusinessIdValue(value.to_owned()))
    }
}

#[derive(Debug, Default)]
pub struct InMemoryBusinessIdAllocator {
    levels: Mutex<HashMap<String, u64>>,
}

impl BusinessIdAllocator for InMemoryBusinessIdAllocator {
    fn allocate(&self, plan: &BusinessIdPlan) -> Result<BusinessIdAllocation, BusinessIdError> {
        plan.validate_allocation_range()?;
        let key = plan.scope.canonical_key();
        let mut levels = self.levels.lock().map_err(|_| {
            BusinessIdError::new(
                BusinessIdErrorCode::Allocation,
                "in-memory business ID allocator lock is poisoned",
            )
        })?;
        let current = levels.get(&key).copied().unwrap_or(plan.initial_sequence);
        if current > plan.max_sequence {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::Exhausted,
                format!("business ID sequence exhausted for scope {key}"),
            ));
        }
        let next = current.checked_add(1).ok_or_else(|| {
            BusinessIdError::new(
                BusinessIdErrorCode::Exhausted,
                "business ID sequence overflow",
            )
        })?;
        levels.insert(key, next);
        Ok(BusinessIdAllocation {
            scope: plan.scope.clone(),
            sequence: current,
        })
    }
}

#[derive(Clone)]
pub struct BusinessIdService {
    allocator: Arc<dyn BusinessIdAllocator>,
    key_provider: Option<Arc<dyn BusinessIdKeyProvider>>,
}

impl BusinessIdService {
    pub fn new(allocator: impl BusinessIdAllocator + 'static) -> Self {
        Self {
            allocator: Arc::new(allocator),
            key_provider: None,
        }
    }

    pub fn from_shared(allocator: Arc<dyn BusinessIdAllocator>) -> Self {
        Self {
            allocator,
            key_provider: None,
        }
    }

    pub fn with_key_provider(mut self, key_provider: impl BusinessIdKeyProvider + 'static) -> Self {
        self.key_provider = Some(Arc::new(key_provider));
        self
    }

    pub fn with_shared_key_provider(
        mut self,
        key_provider: Arc<dyn BusinessIdKeyProvider>,
    ) -> Self {
        self.key_provider = Some(key_provider);
        self
    }

    pub fn ensure<S: BusinessIdSlot>(
        &self,
        context: &UserContext,
        definition: &BusinessIdDefinition,
        domain_root_key: &str,
        aggregate_type: &str,
        slot: &mut S,
    ) -> Result<BusinessIdValue, BusinessIdError> {
        let profile: Box<dyn BusinessIdProfile> =
            if definition.profile == DEFAULT_BUSINESS_ID_PROFILE {
                let provider = self.key_provider.as_ref().ok_or_else(|| {
                    BusinessIdError::new(
                        BusinessIdErrorCode::KeyNotFound,
                        "BusinessIdKeyProvider is not registered for daily-permuted-v1",
                    )
                })?;
                Box::new(PermutedDailyBusinessIdProfile::new(
                    provider.current_key(context, definition)?,
                ))
            } else if definition.profile == LEGACY_DAILY_SEQUENCE_PROFILE {
                Box::new(DailySequenceBusinessIdProfile)
            } else {
                return Err(BusinessIdError::new(
                    BusinessIdErrorCode::InvalidDefinition,
                    format!("unsupported business ID profile {}", definition.profile),
                ));
            };
        if let Some(current) = slot.current_business_id().filter(|value| !value.is_empty()) {
            return profile.validate(definition, current);
        }
        if !slot.is_new_aggregate() {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::Immutable,
                format!(
                    "{} is immutable after the aggregate is persisted",
                    definition.field_name
                ),
            ));
        }
        let business_date = context
            .get_resource::<BusinessDate>()
            .copied()
            .unwrap_or_else(BusinessDate::today)
            .0;
        let request = BusinessIdGenerationRequest {
            definition,
            domain_root_key,
            aggregate_type,
            business_date,
        };
        let plan = profile.plan(&request)?;
        let allocation = self.allocator.allocate(&plan)?;
        let value = profile.format(&plan, &allocation)?;
        slot.assign_business_id(value.clone());
        Ok(value)
    }
}
