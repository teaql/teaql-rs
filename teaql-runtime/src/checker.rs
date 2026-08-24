use std::collections::BTreeMap;
use std::sync::Arc;

use teaql_core::{Entity, TeaqlEntity, Value};

use crate::{EntityValues, UserContext};

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

    pub fn from_values(values: &EntityValues) -> Self {
        match values.get(CHECK_OBJECT_STATUS_FIELD) {
            Some(Value::Text(value)) if value == Self::Create.as_str() => Self::Create,
            Some(Value::Text(value)) if value == Self::Update.as_str() => Self::Update,
            _ => match values.get("id") {
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

pub fn mark_entity_status(values: &mut EntityValues, status: CheckObjectStatus) {
    values.insert(CHECK_OBJECT_STATUS_FIELD.to_owned(), status.into());
}

pub fn clear_entity_status(values: &mut EntityValues) {
    values.remove(CHECK_OBJECT_STATUS_FIELD);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckRule {
    Required,
    Min,
    Max,
    MinStringLength,
    MaxStringLength,
    ContextRootMissing,
    ContextRootMismatch,
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
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    );

    fn required(
        &self,
        values: &EntityValues,
        field: &str,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if matches!(values.get(field), None | Some(Value::Null)) {
            results.push(CheckResult::required(location.clone().member(field)));
        }
    }

    fn min_string_length(
        &self,
        values: &EntityValues,
        field: &str,
        min_len: usize,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if let Some(Value::Text(value)) = values.get(field) {
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
        values: &EntityValues,
        field: &str,
        max_len: usize,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if let Some(Value::Text(value)) = values.get(field) {
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
/// instead of generic value maps.
///
/// Implement this trait for per-entity checker logic structs, then wrap
/// them in [`TypedEntityChecker`] so they satisfy the [`Checker`] trait
/// expected by [`InMemoryCheckerRegistry`].
pub trait TypedChecker<T>: Send + Sync {
    fn check_and_fix_typed(
        &self,
        context: &UserContext,
        entity: &mut T,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    );
}

/// Adapter that turns a [`TypedChecker<T>`] into a [`Checker`].
///
/// On [`Checker::check_and_fix`], it:
/// 1. Extracts [`CheckObjectStatus`] from the entity values.
/// 2. Materializes `T` from a compact row.
/// 3. Delegates to [`TypedChecker::check_and_fix_typed`].
/// 4. Serializes the (possibly mutated) `T` back into entity values.
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
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        let status = CheckObjectStatus::from_values(values);
        // Materializing a partial update necessarily fills omitted Rust fields
        // with their type defaults. Those defaults are only a checker view;
        // they must never become mutation intent. Keep the original sparse
        // record and merge back only fields the typed checker actually changed.
        let original_values = std::mem::take(values);
        let owned_record = original_values.clone().into();
        match T::from_compact_row(teaql_core::CompactRow::from_map(owned_record)) {
            Ok(mut entity) => {
                let before_check = entity.clone().into_values();
                self.checker
                    .check_and_fix_typed(context, &mut entity, status, location, results);
                let after_check = entity.into_values();
                *values = original_values;
                for (field, after_value) in after_check {
                    if before_check.get(&field) != Some(&after_value) {
                        values.insert(field, after_value);
                    }
                }
            }
            Err(_e) => {
                // If deserialization fails, re-build an empty record so
                // the caller always sees a valid (though empty) entity value set.
                *values = EntityValues::default();
                // Push a generic error result.
                results.push(CheckResult::new(CheckRule::Required, location.clone()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_location_formatting_and_nesting_levels() {
        // Test root
        let root = ObjectLocation::root();
        assert_eq!(root.to_string(), "$");
        assert!(root.is_root());
        assert_eq!(root.level(), 0);

        // Test hash_root
        let hash = ObjectLocation::hash_root("user");
        assert_eq!(hash.to_string(), "user");
        assert!(!hash.is_root());
        assert_eq!(hash.level(), 1);

        // Test array_root
        let arr = ObjectLocation::array_root(5);
        assert_eq!(arr.to_string(), "[5]");
        assert!(!arr.is_root());
        assert_eq!(arr.level(), 1);

        // Test nesting
        let nested = ObjectLocation::root()
            .member("users")
            .element(2)
            .member("address")
            .member("city");

        assert_eq!(nested.to_string(), "users[2].address.city");
        assert_eq!(nested.level(), 4);
    }

    #[test]
    fn test_check_object_status_inference_and_explicit_markers() {
        let mut values = EntityValues::default();

        // No id -> Create
        assert_eq!(
            CheckObjectStatus::from_values(&values),
            CheckObjectStatus::Create
        );

        // Has id -> Update
        values.insert("id".to_string(), Value::I64(1));
        assert_eq!(
            CheckObjectStatus::from_values(&values),
            CheckObjectStatus::Update
        );

        // Explicit marker Create overrides id
        mark_entity_status(&mut values, CheckObjectStatus::Create);
        assert_eq!(
            CheckObjectStatus::from_values(&values),
            CheckObjectStatus::Create
        );

        // Explicit marker Update
        mark_entity_status(&mut values, CheckObjectStatus::Update);
        assert_eq!(
            CheckObjectStatus::from_values(&values),
            CheckObjectStatus::Update
        );

        // Clear marker
        clear_entity_status(&mut values);
        assert_eq!(
            CheckObjectStatus::from_values(&values),
            CheckObjectStatus::Update
        ); // falls back to id -> Update
    }
}
