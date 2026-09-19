use std::error::Error;
use std::fmt::{Display, Formatter};

use chrono::NaiveDate;

pub const DEFAULT_BUSINESS_ID_PROFILE: &str = "daily-sequence";
pub const DEFAULT_BUSINESS_ID_DIGITS: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusinessIdErrorCode {
    InvalidDefinition,
    InvalidFormat,
    Immutable,
    Exhausted,
    Allocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessIdError {
    pub code: BusinessIdErrorCode,
    pub message: String,
}

impl BusinessIdError {
    pub fn new(code: BusinessIdErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl Display for BusinessIdError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.message)
    }
}

impl Error for BusinessIdError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessIdDefinition {
    pub field_name: String,
    pub profile: String,
    pub prefix: String,
    pub namespace: String,
    pub split_by_date: bool,
    pub preserve_digits: u8,
    pub separator: String,
}

impl BusinessIdDefinition {
    pub fn daily_sequence(
        field_name: impl Into<String>,
        prefix: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            profile: DEFAULT_BUSINESS_ID_PROFILE.to_owned(),
            prefix: prefix.into(),
            namespace: namespace.into(),
            split_by_date: true,
            preserve_digits: DEFAULT_BUSINESS_ID_DIGITS,
            separator: "-".to_owned(),
        }
    }

    pub fn max_sequence(&self) -> Result<u64, BusinessIdError> {
        if self.preserve_digits == 0 || self.preserve_digits > 19 {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::InvalidDefinition,
                "preserve_digits must be between 1 and 19",
            ));
        }
        10_u64
            .checked_pow(u32::from(self.preserve_digits))
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| {
                BusinessIdError::new(
                    BusinessIdErrorCode::InvalidDefinition,
                    "preserve_digits exceeds the supported u64 range",
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessIdScope {
    pub tenant: String,
    pub namespace: String,
    pub date_partition: Option<String>,
}

impl BusinessIdScope {
    pub fn canonical_key(&self) -> String {
        [
            escape(&self.tenant),
            escape(&self.namespace),
            escape(self.date_partition.as_deref().unwrap_or("")),
        ]
        .join("|")
    }
}

fn escape(value: &str) -> String {
    value.replace('%', "%25").replace('|', "%7C")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessIdGenerationRequest<'a> {
    pub definition: &'a BusinessIdDefinition,
    pub tenant: &'a str,
    pub aggregate_type: &'a str,
    pub business_date: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessIdPlan {
    pub scope: BusinessIdScope,
    pub prefix: String,
    pub date_text: Option<String>,
    pub preserve_digits: u8,
    pub separator: String,
    pub max_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessIdAllocation {
    pub scope: BusinessIdScope,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessIdValue(pub String);

pub trait BusinessIdAllocator: Send + Sync {
    fn allocate(&self, plan: &BusinessIdPlan) -> Result<BusinessIdAllocation, BusinessIdError>;
}

pub trait BusinessIdProfile: Send + Sync {
    fn plan(
        &self,
        request: &BusinessIdGenerationRequest<'_>,
    ) -> Result<BusinessIdPlan, BusinessIdError>;

    fn format(
        &self,
        plan: &BusinessIdPlan,
        allocation: &BusinessIdAllocation,
    ) -> Result<BusinessIdValue, BusinessIdError>;

    fn validate(
        &self,
        definition: &BusinessIdDefinition,
        value: &str,
    ) -> Result<BusinessIdValue, BusinessIdError>;
}

pub trait BusinessIdSlot {
    fn current_business_id(&self) -> Option<&str>;
    fn is_new_aggregate(&self) -> bool;
    fn assign_business_id(&mut self, value: BusinessIdValue);
}
