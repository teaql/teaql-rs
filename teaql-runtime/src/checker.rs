use std::collections::BTreeMap;
use std::sync::Arc;

use teaql_core::{Entity, Record, TeaqlEntity, Value};

use crate::UserContext;

pub const CHECK_OBJECT_STATUS_FIELD: &str = "__teaql_object_status";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckObjectStatus {
    Create,
    Update,
    Unknown,
}

impl CheckObjectStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_record(record: &Record) -> Self {
        match record.get(CHECK_OBJECT_STATUS_FIELD) {
            Some(Value::Text(value)) if value == Self::Create.as_str() => Self::Create,
            Some(Value::Text(value)) if value == Self::Update.as_str() => Self::Update,
            _ => match record.get("id") {
                None | Some(Value::Null) => Self::Create,
                Some(_) => Self::Update,
            },
        }
    }

    pub fn is_create(self) -> bool {
        matches!(self, Self::Create)
    }

    pub fn is_update(self) -> bool {
        matches!(self, Self::Update)
    }
}

impl From<CheckObjectStatus> for Value {
    fn from(value: CheckObjectStatus) -> Self {
        Value::Text(value.as_str().to_owned())
    }
}

pub fn mark_record_status(record: &mut Record, status: CheckObjectStatus) {
    record.insert(CHECK_OBJECT_STATUS_FIELD.to_owned(), status.into());
}

pub fn clear_record_status(record: &mut Record) {
    record.remove(CHECK_OBJECT_STATUS_FIELD);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckRule {
    Required,
    Min,
    Max,
    MinStringLength,
    MaxStringLength,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocationSegment {
    Member(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectLocation {
    segments: Vec<LocationSegment>,
}

impl ObjectLocation {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn hash_root(member: impl Into<String>) -> Self {
        Self::root().member(member)
    }

    pub fn array_root(index: usize) -> Self {
        Self::root().element(index)
    }

    pub fn member(mut self, member: impl Into<String>) -> Self {
        self.segments.push(LocationSegment::Member(member.into()));
        self
    }

    pub fn element(mut self, index: usize) -> Self {
        self.segments.push(LocationSegment::Index(index));
        self
    }

    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn level(&self) -> usize {
        self.segments.len()
    }
}

impl std::fmt::Display for ObjectLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.segments.is_empty() {
            return write!(f, "$");
        }
        let mut first = true;
        for segment in &self.segments {
            match segment {
                LocationSegment::Member(member) => {
                    if !first {
                        write!(f, ".")?;
                    }
                    write!(f, "{member}")?;
                }
                LocationSegment::Index(index) => {
                    write!(f, "[{index}]")?;
                }
            }
            first = false;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckResult {
    pub rule: CheckRule,
    pub location: ObjectLocation,
    pub input_value: Option<Value>,
    pub system_value: Option<Value>,
    pub message: Option<String>,
}

impl CheckResult {
    pub fn new(rule: CheckRule, location: ObjectLocation) -> Self {
        Self {
            rule,
            location,
            input_value: None,
            system_value: None,
            message: None,
        }
    }

    pub fn required(location: ObjectLocation) -> Self {
        Self::new(CheckRule::Required, location)
    }

    pub fn min(location: ObjectLocation, min: impl Into<Value>, current: impl Into<Value>) -> Self {
        Self::new(CheckRule::Min, location)
            .with_system_value(min)
            .with_input_value(current)
    }

    pub fn max(location: ObjectLocation, max: impl Into<Value>, current: impl Into<Value>) -> Self {
        Self::new(CheckRule::Max, location)
            .with_system_value(max)
            .with_input_value(current)
    }

    pub fn min_str(location: ObjectLocation, min_len: u64, current: impl Into<Value>) -> Self {
        Self::new(CheckRule::MinStringLength, location)
            .with_system_value(min_len)
            .with_input_value(current)
    }

    pub fn max_str(location: ObjectLocation, max_len: u64, current: impl Into<Value>) -> Self {
        Self::new(CheckRule::MaxStringLength, location)
            .with_system_value(max_len)
            .with_input_value(current)
    }

    pub fn with_input_value(mut self, value: impl Into<Value>) -> Self {
        self.input_value = Some(value.into());
        self
    }

    pub fn with_system_value(mut self, value: impl Into<Value>) -> Self {
        self.system_value = Some(value.into());
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

impl std::fmt::Display for CheckResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.message {
            Some(message) => write!(f, "{message}"),
            None => write!(f, "{}: {:?}", self.location, self.rule),
        }
    }
}

pub type CheckResults = Vec<CheckResult>;

pub trait Checker: Send + Sync {
    fn entity(&self) -> &str;

    fn check_and_fix(
        &self,
        ctx: &UserContext,
        record: &mut Record,
        location: &ObjectLocation,
        results: &mut CheckResults,
    );

    fn required(
        &self,
        record: &Record,
        field: &str,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if matches!(record.get(field), None | Some(Value::Null)) {
            results.push(CheckResult::required(location.clone().member(field)));
        }
    }

    fn min_string_length(
        &self,
        record: &Record,
        field: &str,
        min_len: usize,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if let Some(Value::Text(value)) = record.get(field) {
            if value.chars().count() < min_len {
                results.push(CheckResult::min_str(
                    location.clone().member(field),
                    min_len as u64,
                    value.clone(),
                ));
            }
        }
    }

    fn max_string_length(
        &self,
        record: &Record,
        field: &str,
        max_len: usize,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if let Some(Value::Text(value)) = record.get(field) {
            if value.chars().count() > max_len {
                results.push(CheckResult::max_str(
                    location.clone().member(field),
                    max_len as u64,
                    value.clone(),
                ));
            }
        }
    }
}

pub trait CheckerRegistry: Send + Sync {
    fn checker(&self, entity: &str) -> Option<Arc<dyn Checker>>;
}

#[derive(Default, Clone)]
pub struct InMemoryCheckerRegistry {
    checkers: BTreeMap<String, Arc<dyn Checker>>,
}

impl InMemoryCheckerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, checker: impl Checker + 'static) {
        self.checkers
            .insert(checker.entity().to_owned(), Arc::new(checker));
    }

    pub fn with_checker(mut self, checker: impl Checker + 'static) -> Self {
        self.register(checker);
        self
    }
}

impl CheckerRegistry for InMemoryCheckerRegistry {
    fn checker(&self, entity: &str) -> Option<Arc<dyn Checker>> {
        self.checkers.get(entity).cloned()
    }
}

// ---------------------------------------------------------------------------
// TypedChecker & TypedEntityChecker
// ---------------------------------------------------------------------------

/// Typed version of [`Checker`] that works with concrete entity types (`T`)
/// instead of generic [`Record`]s.
///
/// Implement this trait for per-entity checker logic structs, then wrap
/// them in [`TypedEntityChecker`] so they satisfy the [`Checker`] trait
/// expected by [`InMemoryCheckerRegistry`].
pub trait TypedChecker<T>: Send + Sync {
    fn check_and_fix_typed(
        &self,
        ctx: &UserContext,
        entity: &mut T,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    );
}

/// Adapter that turns a [`TypedChecker<T>`] into a [`Checker`].
///
/// On [`Checker::check_and_fix`], it:
/// 1. Extracts [`CheckObjectStatus`] from the `Record`.
/// 2. Deserializes the `Record` into `T` via [`Entity::from_record`].
/// 3. Delegates to [`TypedChecker::check_and_fix_typed`].
/// 4. Serializes the (possibly mutated) `T` back into the `Record`
///    via [`Entity::into_record`].
pub struct TypedEntityChecker<T, C> {
    checker: C,
    entity_name: String,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T, C> TypedEntityChecker<T, C>
where
    T: TeaqlEntity,
{
    /// Create a new `TypedEntityChecker` wrapping `checker`.
    pub fn new(checker: C) -> Self {
        let entity_name = T::entity_descriptor().name.clone();
        Self {
            checker,
            entity_name,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, C> Checker for TypedEntityChecker<T, C>
where
    T: Entity + TeaqlEntity + Send + Sync + Clone,
    C: TypedChecker<T>,
{
    fn entity(&self) -> &str {
        &self.entity_name
    }

    fn check_and_fix(
        &self,
        ctx: &UserContext,
        record: &mut Record,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        let status = CheckObjectStatus::from_record(record);
        // Take ownership of the record (replace with empty) so we can
        // call T::from_record which consumes the Record.
        let owned_record = std::mem::take(record);
        match T::from_record(owned_record) {
            Ok(mut entity) => {
                self.checker
                    .check_and_fix_typed(ctx, &mut entity, status, location, results);
                // Write mutated entity back into the original record slot.
                *record = entity.into_record();
            }
            Err(_e) => {
                // If deserialization fails, re-build an empty record so
                // the caller always sees a valid (though empty) Record.
                *record = Record::default();
                // Push a generic error result.
                results.push(CheckResult::new(CheckRule::Required, location.clone()));
            }
        }
    }
}
