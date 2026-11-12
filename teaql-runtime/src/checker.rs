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
    InvalidType,
    InvalidDeletionState,
    InvalidVersionState,
    VersionExhausted,
    Min,
    Max,
    MinStringLength,
    MaxStringLength,
    ContextRootMissing,
    ContextRootMismatch,
}

impl CheckRule {
    pub fn wire_id(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::InvalidType => "invalid_type",
            Self::InvalidDeletionState => "invalid_deletion_state",
            Self::InvalidVersionState => "invalid_version_state",
            Self::VersionExhausted => "version_exhausted",
            Self::Min => "min",
            Self::Max => "max",
            Self::MinStringLength => "min_string_length",
            Self::MaxStringLength => "max_string_length",
            Self::ContextRootMissing => "context_root_missing",
            Self::ContextRootMismatch => "context_root_mismatch",
        }
    }
}

pub use teaql_core::object_location::{
    JsonFieldNamingProfile, LocationSegment, ObjectLocation, WireLocationSegment,
};

#[derive(Debug, Clone)]
pub struct CheckResult {
    pub rule: CheckRule,
    pub location: ObjectLocation,
    pub input_value: Option<Value>,
    pub system_value: Option<Value>,
    pub message: Option<String>,
    pub entity_type: Option<String>,
    pub source_instance_path: Option<String>,
    pub(crate) presentation: Option<Arc<crate::checker_messages::RenderedState>>,
}

// Presentation caching is not part of a violation's public identity or equality.
impl PartialEq for CheckResult {
    fn eq(&self, other: &Self) -> bool {
        self.rule == other.rule
            && self.location == other.location
            && self.input_value == other.input_value
            && self.system_value == other.system_value
            && self.message == other.message
            && self.entity_type == other.entity_type
            && self.source_instance_path == other.source_instance_path
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireCheckResult {
    pub rule_id: String,
    pub entity_type: Option<String>,
    pub location: Vec<WireLocationSegment>,
    pub instance_path: String,
    pub source_instance_path: Option<String>,
    pub input_value: Option<serde_json::Value>,
    pub system_value: Option<serde_json::Value>,
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
            entity_type: None,
            source_instance_path: None,
            presentation: None,
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

    pub fn with_entity_type(mut self, entity_type: impl Into<String>) -> Self {
        self.entity_type = Some(entity_type.into());
        self
    }

    pub fn with_source_instance_path(mut self, path: impl Into<String>) -> Self {
        self.source_instance_path = Some(path.into());
        self
    }

    pub fn to_wire(&self, profile: JsonFieldNamingProfile) -> WireCheckResult {
        WireCheckResult {
            rule_id: self.rule.wire_id().to_owned(),
            entity_type: self.entity_type.clone(),
            location: self.location.wire_segments(),
            instance_path: self.location.instance_path_with(profile),
            source_instance_path: self.source_instance_path.clone(),
            input_value: self.input_value.as_ref().map(Value::to_json_value),
            system_value: self.system_value.as_ref().map(Value::to_json_value),
            message: self.message.clone(),
        }
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

/// Runtime-owned, per-entity intent. Never accepted as a JSON eligibility flag.
pub(crate) struct CheckMutationState<'a> {
    pub original: Option<&'a teaql_core::EntitySnapshot>,
    pub dirty: Option<&'a std::collections::BTreeSet<String>>,
    pub dynamic_changed: bool,
}

impl CheckMutationState<'_> {
    pub(crate) fn has_effective_change(
        &self,
        values: &EntityValues,
        descriptor: &teaql_core::EntityDescriptor,
        context_fields: &[&str],
        custom_context_fields: &[&str],
    ) -> bool {
        if self.dynamic_changed {
            return true;
        }
        let changed = |field: &str| {
            let canonical = crate::ledger_keys::canonical_field_from_storage(descriptor, field)
                .unwrap_or_else(|_| field.to_owned());
            !canonical.starts_with('_')
                && canonical != "id"
                && canonical != "version"
                && !context_fields.contains(&canonical.as_str())
                && !custom_context_fields.contains(&canonical.as_str())
                && self
                    .original
                    .is_none_or(|old| old.get(field) != values.get(field))
        };
        self.dirty.map_or_else(
            || values.keys().any(|field| changed(field)),
            |fields| fields.iter().any(|field| changed(field)),
        )
    }
}

pub trait Checker: Send + Sync {
    fn entity(&self) -> &str;

    fn managed_context_fields(&self) -> &'static [&'static str] {
        &[]
    }
    fn custom_context_fields(&self) -> &'static [&'static str] {
        &[]
    }

    fn check_and_fix_for_mutation(
        &self,
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
        _has_effective_change: bool,
    ) {
        self.check_and_fix(context, values, location, results);
    }

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
        if let Some(Value::Text(value)) = values.get(field)
            && value.chars().count() < min_len
        {
            results.push(CheckResult::min_str(
                location.clone().member(field),
                min_len as u64,
                value.clone(),
            ));
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
        if let Some(Value::Text(value)) = values.get(field)
            && value.chars().count() > max_len
        {
            results.push(CheckResult::max_str(
                location.clone().member(field),
                max_len as u64,
                value.clone(),
            ));
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
    fn separate_fix_and_check(&self) -> bool {
        false
    }
    fn managed_context_fields(&self) -> &'static [&'static str] {
        &[]
    }
    fn custom_context_fields(&self) -> &'static [&'static str] {
        &[]
    }
    fn fix_typed(
        &self,
        _context: &UserContext,
        _entity: &mut T,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
    }
    fn check_typed(
        &self,
        _context: &UserContext,
        _entity: &T,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
        _has_mutation: bool,
    ) {
    }
    fn check_and_fix_typed(
        &self,
        context: &UserContext,
        entity: &mut T,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    );

    /// Model constraints observe the canonical values after typed/custom Fix.
    /// Borrow the existing sparse payload: absent fields and explicit null must
    /// not be mistaken for defaults introduced while materializing `T`.
    fn check_fixed_values(
        &self,
        _context: &UserContext,
        _values: &EntityValues,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
    }
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

    fn managed_context_fields(&self) -> &'static [&'static str] {
        self.checker.managed_context_fields()
    }
    fn custom_context_fields(&self) -> &'static [&'static str] {
        self.checker.custom_context_fields()
    }

    fn check_and_fix(
        &self,
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        self.check_and_fix_scoped(context, values, location, results, None);
    }

    fn check_and_fix_for_mutation(
        &self,
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
        changed: bool,
    ) {
        self.check_and_fix_scoped(context, values, location, results, Some(changed));
    }
}

impl<T, C> TypedEntityChecker<T, C>
where
    T: Entity + TeaqlEntity + Send + Sync + Clone,
    C: TypedChecker<T>,
{
    fn check_and_fix_scoped(
        &self,
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
        has_effective_change: Option<bool>,
    ) {
        let descriptor = T::entity_descriptor();
        if let Err(error) = crate::ledger_keys::map_checker_values(&descriptor, values, false) {
            results.push(
                CheckResult::new(CheckRule::InvalidType, location.clone())
                    .with_message(error.to_string()),
            );
            return;
        }
        let status = CheckObjectStatus::from_values(values);
        // Materializing a partial update necessarily fills omitted Rust fields
        // with their type defaults. Those defaults are only a checker view;
        // they must never become mutation intent. Keep the original sparse
        // record and merge back only fields the typed checker actually changed.
        let mut original_values = std::mem::take(values);
        let loaded_fields = match original_values.remove("_loaded_fields") {
            Some(Value::List(fields)) => Some(
                fields
                    .into_iter()
                    .filter_map(|field| match field {
                        Value::Text(field) => Some(field),
                        _ => None,
                    })
                    .collect::<std::collections::BTreeSet<_>>(),
            ),
            _ => None,
        };
        let dirty_fields = match original_values.remove("_dirty_fields") {
            Some(Value::List(fields)) => Some(
                fields
                    .into_iter()
                    .filter_map(|field| match field {
                        Value::Text(field) => Some(field),
                        _ => None,
                    })
                    .collect::<std::collections::BTreeSet<_>>(),
            ),
            _ => None,
        };
        let owned_record = original_values.clone().into();
        let eligible = status.is_create()
            || has_effective_change.unwrap_or_else(|| {
                let business = |field: &str| {
                    let field =
                        crate::ledger_keys::canonical_field_from_storage(&descriptor, field)
                            .unwrap_or_else(|_| field.to_owned());
                    !field.starts_with('_')
                        && field != "id"
                        && field != "version"
                        && !self
                            .checker
                            .managed_context_fields()
                            .contains(&field.as_str())
                        && !self
                            .checker
                            .custom_context_fields()
                            .contains(&field.as_str())
                };
                dirty_fields.as_ref().map_or_else(
                    || original_values.keys().any(|field| business(field)),
                    |fields| fields.iter().any(|field| business(field)),
                )
            });
        match T::from_compact_row(teaql_core::CompactRow::from_map(owned_record)) {
            Ok(mut entity) => {
                if let Some(loaded_fields) = loaded_fields {
                    entity.set_checker_loaded_fields(loaded_fields);
                }
                if let Some(dirty_fields) = dirty_fields {
                    // Only mutation-aware reconstruction needs the original value map.
                    // Readonly checks must not pay for an unused deep copy.
                    let mut checker_values = original_values.clone();
                    if let Err(error) = crate::ledger_keys::map_checker_values(
                        &descriptor,
                        &mut checker_values,
                        true,
                    ) {
                        *values = original_values;
                        results.push(
                            CheckResult::new(CheckRule::InvalidType, location.clone())
                                .with_message(error.to_string()),
                        );
                        return;
                    }
                    let dirty_fields = dirty_fields
                        .into_iter()
                        .map(|field| {
                            crate::ledger_keys::canonical_field_from_storage(&descriptor, &field)
                                .unwrap_or(field)
                        })
                        .collect();
                    entity.set_checker_dirty_fields(dirty_fields, &checker_values.into());
                }
                let before_check = entity.clone().into_values();
                let key = descriptor.id_property().and_then(|property| {
                    before_check
                        .get(&property.name)
                        .map(|id| crate::EntityKey::new(self.entity_name.clone(), id.clone()))
                });
                let native_intent = |entity: &T| {
                    entity
                        .__teaql_runtime_state_any()
                        .and_then(|state| state.downcast_ref::<crate::EntityRuntimeState>())
                        .and_then(|state| {
                            key.as_ref()
                                .and_then(|key| state.native_mutation_values_for(key))
                        })
                };
                let before_intent = native_intent(&entity);
                if self.checker.separate_fix_and_check() {
                    if eligible {
                        self.checker
                            .fix_typed(context, &mut entity, status, location, results);
                    }
                    self.checker
                        .check_typed(context, &entity, status, location, results, eligible);
                } else {
                    self.checker.check_and_fix_typed(
                        context,
                        &mut entity,
                        status,
                        location,
                        results,
                    );
                }
                let after_intent = native_intent(&entity);
                let after_check = entity.into_values();
                let descriptor = T::entity_descriptor();
                *values = original_values;
                for (field, after_value) in after_check {
                    if before_check.get(&field) != Some(&after_value) {
                        values.insert(field, after_value);
                    }
                }
                // A required typed member cannot represent Null (or another
                // unconvertible raw value). Its generated setter preserves the
                // old typed member and records the exact new intent in the
                // carrier's ledger. Overlay only NEW customer-Fix intent, not
                // stale caller intent which would undo direct typed edits.
                if let Some(after) = after_intent {
                    let changed: teaql_core::MutationValues = after
                        .iter()
                        .filter(|(field, value)| {
                            before_intent.as_ref().and_then(|before| before.get(*field))
                                != Some(*value)
                        })
                        .map(|(field, value)| (field.clone(), value.clone()))
                        .collect::<BTreeMap<_, _>>()
                        .into();
                    match crate::ledger_keys::storage_values_from_ledger(&descriptor, &changed) {
                        Ok(changed) => {
                            for (field, value) in changed {
                                values.insert(field, value);
                            }
                        }
                        Err(error) => {
                            results.push(
                                CheckResult::new(CheckRule::InvalidType, location.clone())
                                    .with_message(error.to_string()),
                            );
                            return;
                        }
                    }
                }
                // Required observes the actual merged values AFTER typed and
                // ledger Fix. Missing values stay missing; typed defaults never
                // materialize omitted caller input into mutation intent.
                for property in descriptor.properties.iter().filter(|property| {
                    !property.nullable && !property.is_id && !property.is_version
                }) {
                    if values
                        .get(&property.name)
                        .is_none_or(|value| matches!(value, Value::Null))
                    {
                        results.push(CheckResult::required(
                            location.clone().member(
                                crate::ledger_keys::canonical_field_from_storage(
                                    &descriptor,
                                    &property.name,
                                )
                                .unwrap_or_else(|_| property.name.clone()),
                            ),
                        ));
                    }
                }
            }
            Err(error) => {
                // A malformed value is not an absent required value. Preserve
                // the caller's mutation boundary and report the materialization
                // error so the offending field and actual value remain visible.
                *values = original_values;
                results.push(
                    CheckResult::new(CheckRule::InvalidType, location.clone()).with_message(
                        format!(
                            "failed to materialize {} for checker: {error}",
                            self.entity_name
                        ),
                    ),
                );
            }
        }
        if let Err(error) = crate::ledger_keys::map_checker_values(&descriptor, values, true) {
            results.push(
                CheckResult::new(CheckRule::InvalidType, location.clone())
                    .with_message(error.to_string()),
            );
            return;
        }
        let first_model_result = results.len();
        self.checker
            .check_fixed_values(context, values, location, results);
        // A customer checker may already report the same modeled rule. Keep
        // its message/metadata instead of displaying a second mechanical copy.
        let mut index = first_model_result;
        while index < results.len() {
            if results[..index].iter().any(|prior| {
                prior.rule == results[index].rule && prior.location == results[index].location
            }) {
                results.remove(index);
            } else {
                index += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_intent_separates_restored_edits_from_other_business_repairs() {
        use teaql_core::{DataType, EntityDescriptor, EntitySnapshot, PropertyDescriptor};
        let descriptor = EntityDescriptor::new("Entry")
            .property(PropertyDescriptor::new("name", DataType::Text))
            .property(PropertyDescriptor::new("memo", DataType::Text))
            .property(PropertyDescriptor::new("touched_at", DataType::I64));
        let old = BTreeMap::from([
            ("name".into(), Value::from("original")),
            ("memo".into(), Value::from("old memo")),
            ("touched_at".into(), Value::I64(100)),
        ]);
        let snapshot = EntitySnapshot::from(old.clone());
        let mut values = EntityValues::from(old);
        values.insert("touched_at".into(), Value::I64(200));
        let state = CheckMutationState {
            original: Some(&snapshot),
            dirty: None,
            dynamic_changed: false,
        };
        assert!(!state.has_effective_change(&values, &descriptor, &["touched_at"], &[]));
        values.insert("memo".into(), Value::from("business repair"));
        assert!(state.has_effective_change(&values, &descriptor, &["touched_at"], &[]));
    }

    #[test]
    fn final_intent_cannot_cancel_dynamic_changes_or_unknown_original_values() {
        use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
        let descriptor = EntityDescriptor::new("Entry")
            .property(PropertyDescriptor::new("name", DataType::Text));
        let values = EntityValues::from(BTreeMap::from([(
            "name".into(),
            Value::from("unknown baseline"),
        )]));
        let unknown = CheckMutationState {
            original: None,
            dirty: None,
            dynamic_changed: false,
        };
        assert!(unknown.has_effective_change(&values, &descriptor, &[], &[]));
        let dynamic = CheckMutationState {
            original: None,
            dirty: None,
            dynamic_changed: true,
        };
        assert!(dynamic.has_effective_change(&EntityValues::new(), &descriptor, &[], &[]));
    }

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
    fn object_location_renders_model_native_and_external_paths() {
        let location = ObjectLocation::hash_root("order_items")
            .element(2)
            .member("user_url");

        assert_eq!(location.model_path(), "order_items[2].user_url");
        assert_eq!(location.native_path(), "order_items[2].user_url");
        assert_eq!(location.instance_path(), "/orderItems/2/userUrl");
        assert_eq!(
            location.instance_path_with(JsonFieldNamingProfile::SnakeCase),
            "/order_items/2/user_url"
        );
        assert_eq!(
            location.instance_path_with(JsonFieldNamingProfile::PascalCase),
            "/OrderItems/2/UserUrl"
        );
        assert_eq!(location.to_string(), "order_items[2].user_url");
    }

    #[test]
    fn checker_wire_projection_preserves_submitted_alias() {
        let result = CheckResult::required(ObjectLocation::hash_root("user_url"))
            .with_entity_type("customer_account")
            .with_source_instance_path("/user_url");
        let wire = result.to_wire(JsonFieldNamingProfile::CamelCase);
        assert_eq!(wire.rule_id, "required");
        assert_eq!(wire.entity_type.as_deref(), Some("customer_account"));
        assert_eq!(wire.instance_path, "/userUrl");
        assert_eq!(wire.source_instance_path.as_deref(), Some("/user_url"));

        let json = serde_json::to_value(&wire).expect("wire result must serialize");
        assert_eq!(json["ruleId"], "required");
        assert_eq!(json["entityType"], "customer_account");
        assert_eq!(json["location"][0]["kind"], "property");
        assert_eq!(json["location"][0]["name"], "user_url");
        assert_eq!(json["instancePath"], "/userUrl");
        assert_eq!(json["sourceInstancePath"], "/user_url");
    }

    #[test]
    fn object_location_escapes_json_pointer_members() {
        assert_eq!(ObjectLocation::hash_root("a~/b").instance_path(), "/a~0~1b");
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
