use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{NaiveDate, Utc};
use teaql_core::business_id::{
    BusinessIdAllocation, BusinessIdAllocator, BusinessIdDefinition, BusinessIdError,
    BusinessIdErrorCode, BusinessIdGenerationRequest, BusinessIdPlan, BusinessIdProfile,
    BusinessIdScope, BusinessIdSlot, BusinessIdValue, DEFAULT_BUSINESS_ID_PROFILE,
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
        if definition.profile != DEFAULT_BUSINESS_ID_PROFILE {
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
                tenant: request.tenant.to_owned(),
                namespace: definition.namespace.clone(),
                date_partition: date_text.clone(),
            },
            prefix: definition.prefix.clone(),
            date_text,
            preserve_digits: definition.preserve_digits,
            separator: definition.separator.clone(),
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
}

#[derive(Debug, Default)]
pub struct InMemoryBusinessIdAllocator {
    levels: Mutex<HashMap<String, u64>>,
}

impl BusinessIdAllocator for InMemoryBusinessIdAllocator {
    fn allocate(&self, plan: &BusinessIdPlan) -> Result<BusinessIdAllocation, BusinessIdError> {
        let key = plan.scope.canonical_key();
        let mut levels = self.levels.lock().map_err(|_| {
            BusinessIdError::new(
                BusinessIdErrorCode::Allocation,
                "in-memory business ID allocator lock is poisoned",
            )
        })?;
        let next = levels
            .get(&key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                BusinessIdError::new(
                    BusinessIdErrorCode::Exhausted,
                    "business ID sequence overflow",
                )
            })?;
        if next > plan.max_sequence {
            return Err(BusinessIdError::new(
                BusinessIdErrorCode::Exhausted,
                format!("business ID sequence exhausted for scope {key}"),
            ));
        }
        levels.insert(key, next);
        Ok(BusinessIdAllocation {
            scope: plan.scope.clone(),
            sequence: next,
        })
    }
}

#[derive(Clone)]
pub struct BusinessIdService {
    allocator: Arc<dyn BusinessIdAllocator>,
    profile: DailySequenceBusinessIdProfile,
}

impl BusinessIdService {
    pub fn new(allocator: impl BusinessIdAllocator + 'static) -> Self {
        Self {
            allocator: Arc::new(allocator),
            profile: DailySequenceBusinessIdProfile,
        }
    }

    pub fn from_shared(allocator: Arc<dyn BusinessIdAllocator>) -> Self {
        Self {
            allocator,
            profile: DailySequenceBusinessIdProfile,
        }
    }

    pub fn ensure<S: BusinessIdSlot>(
        &self,
        context: &UserContext,
        definition: &BusinessIdDefinition,
        tenant: &str,
        aggregate_type: &str,
        slot: &mut S,
    ) -> Result<BusinessIdValue, BusinessIdError> {
        if let Some(current) = slot.current_business_id().filter(|value| !value.is_empty()) {
            return Ok(BusinessIdValue(current.to_owned()));
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
            tenant,
            aggregate_type,
            business_date,
        };
        let plan = self.profile.plan(&request)?;
        let allocation = self.allocator.allocate(&plan)?;
        let value = self.profile.format(&plan, &allocation)?;
        slot.assign_business_id(value.clone());
        Ok(value)
    }
}
