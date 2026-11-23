
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/scalar_type_probe
use std::collections::BTreeMap;

use teaql_macros::{teaql_entity, TeaqlEntity};

// Fail to compile against a runtime without modeled Integer-width support.
const _: fn(teaql_core::PropertyDescriptor) -> teaql_core::PropertyDescriptor = teaql_core::PropertyDescriptor::int32;


/// [TEAQL AI WARNING]
/// TeaQL was explicitly designed to PREVENT AI hallucinations and random guessing.
/// DO NOT GUESS METHOD NAMES!
/// The methods listed below are the ONLY valid ways to interact with this entity.
/// If you encounter compilation errors (e.g., method not found), DO NOT guess another method name.
/// Read the method signatures in this file before proceeding.
#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(indexed_layout)]
#[teaql(entity = "ScalarTypeProbe", table = "scalar_type_probe_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct ScalarTypeProbe {
#[teaql(id)]
    id: u64,

// @source model.xml:38
#[teaql(max_length = 100)]
    required_string: String,

// @source model.xml:38
#[teaql(max_length = 100)]
    optional_string: Option<String>,

// @source model.xml:38
#[teaql(large_text)]
    large_text: Option<String>,

// @source model.xml:38
    required_boolean: bool,

// @source model.xml:38
    optional_boolean: Option<bool>,

// @source model.xml:38
#[teaql(integer32)]
    required_integer: i64,

// @source model.xml:38
#[teaql(integer32)]
    optional_integer: Option<i64>,

// @source model.xml:38
    required_long: i64,

// @source model.xml:38
    optional_long: Option<i64>,

// @source model.xml:38
#[teaql(numeric_precision = 19)]
#[teaql(numeric_scale = 7)]
    required_decimal: rust_decimal::Decimal,

// @source model.xml:38
#[teaql(numeric_precision = 19)]
#[teaql(numeric_scale = 7)]
    optional_decimal: Option<rust_decimal::Decimal>,

// @source model.xml:38
    required_float: f32,

// @source model.xml:38
    optional_float: Option<f32>,

// @source model.xml:38
    required_double: f64,

// @source model.xml:38
    optional_double: Option<f64>,

// @source model.xml:38
    required_date: chrono::NaiveDate,

// @source model.xml:38
    optional_date: Option<chrono::NaiveDate>,

// @source model.xml:38
    required_time: String,

// @source model.xml:38
    optional_time: Option<String>,

// @source model.xml:38
    required_datetime: teaql_core::time::Timestamp,

// @source model.xml:38
    optional_datetime: Option<teaql_core::time::Timestamp>,

// @source model.xml:38
    create_time: teaql_core::time::Timestamp,

// @source model.xml:38
    update_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
// @source model.xml:38
#[teaql(column = "type_platform")]
    type_platform_id: u64,
// @source model.xml:38
#[teaql(relation(target = "TypePlatform", local_key = "type_platform_id", foreign_key = "id"))]
    type_platform: Option<Box<crate::TypePlatform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl ScalarTypeProbe {
    pub const ENTITY_NAME: &'static str = "Scalar Type Probe";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "8d1d53171c01628f604255bcfa7e7d52b6260332cee701781027b0233eae926b";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("type_platform", 1),
        ("required_string", 2),
        ("optional_string", 3),
        ("large_text", 4),
        ("required_boolean", 5),
        ("optional_boolean", 6),
        ("required_integer", 7),
        ("optional_integer", 8),
        ("required_long", 9),
        ("optional_long", 10),
        ("required_decimal", 11),
        ("optional_decimal", 12),
        ("required_float", 13),
        ("optional_float", 14),
        ("required_double", 15),
        ("optional_double", 16),
        ("required_date", 17),
        ("optional_date", 18),
        ("required_time", 19),
        ("optional_time", 20),
        ("required_datetime", 21),
        ("optional_datetime", 22),
        ("create_time", 23),
        ("update_time", 24),
        ("version", 25),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("type_platform", "type_platform_id", "type_platform"),
        ("required_string", "required_string", "required_string"),
        ("optional_string", "optional_string", "optional_string"),
        ("large_text", "large_text", "large_text"),
        ("required_boolean", "required_boolean", "required_boolean"),
        ("optional_boolean", "optional_boolean", "optional_boolean"),
        ("required_integer", "required_integer", "required_integer"),
        ("optional_integer", "optional_integer", "optional_integer"),
        ("required_long", "required_long", "required_long"),
        ("optional_long", "optional_long", "optional_long"),
        ("required_decimal", "required_decimal", "required_decimal"),
        ("optional_decimal", "optional_decimal", "optional_decimal"),
        ("required_float", "required_float", "required_float"),
        ("optional_float", "optional_float", "optional_float"),
        ("required_double", "required_double", "required_double"),
        ("optional_double", "optional_double", "optional_double"),
        ("required_date", "required_date", "required_date"),
        ("optional_date", "optional_date", "optional_date"),
        ("required_time", "required_time", "required_time"),
        ("optional_time", "optional_time", "optional_time"),
        ("required_datetime", "required_datetime", "required_datetime"),
        ("optional_datetime", "optional_datetime", "optional_datetime"),
        ("create_time", "create_time", "create_time"),
        ("update_time", "update_time", "update_time"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            required_string: String::new(),
            optional_string: None,
            large_text: None,
            required_boolean: false,
            optional_boolean: None,
            required_integer: 0_i64,
            optional_integer: None,
            required_long: 0_i64,
            optional_long: None,
            required_decimal: rust_decimal::Decimal::ZERO,
            optional_decimal: None,
            required_float: 0.0_f32,
            optional_float: None,
            required_double: 0.0_f64,
            optional_double: None,
            required_date: chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            optional_date: None,
            required_time: String::new(),
            optional_time: None,
            required_datetime: teaql_core::time::Timestamp::now(),
            optional_datetime: None,
            create_time: teaql_core::time::Timestamp::now(),
            update_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            type_platform_id: 0_u64,
            type_platform: None,
            dynamic: BTreeMap::new(),
            __teaql_runtime_state: root,
            __load_state: teaql_core::eval::LoadState::NotLoaded.into_indexed(
                <Self as teaql_core::TeaqlEntity>::field_layout().expect("generated field layout").expect("indexed layout")
            ).expect("new entity load state"),
        }
    }

    pub fn attach_runtime_state_recursive(&mut self, root: teaql_runtime::EntityRuntimeState) {
        root.adopt_mutations_from(self.__teaql_runtime_state());
        self.__teaql_replace_runtime_state(root.clone());
        if let Some(entity) = &mut self.type_platform {
            entity.attach_runtime_state_recursive(root.clone());
        }
    }

    pub fn is_loaded(&self, field_or_relation: &str) -> bool {
        self.__load_state.is_loaded(field_or_relation)
    }

    pub fn set_load_state(&mut self, state: teaql_core::eval::LoadState) {
        self.__load_state = state.into_indexed(
            <Self as teaql_core::TeaqlEntity>::field_layout().expect("generated field layout").expect("indexed layout")
        ).expect("compatible entity load state");
    }

    pub fn id(&self) -> u64 {
        self.changed_id().and_then(|value| value.try_u64()).unwrap_or(self.id)
    }

    pub fn update_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.id = value.try_u64().unwrap_or(self.id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "id", value);
        self.__load_state.mark_loaded("id").expect("generated field layout");
        self
    }

    pub fn changed_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "id")
    }

    pub fn eval_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "id".to_string(), attempted_path: "id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.id())
                }}


    pub fn required_string(&self) -> String {
        self.changed_required_string().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.required_string.clone())
    }

    pub fn update_required_string(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_string = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.required_string.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_string", value);
        self.__load_state.mark_loaded("required_string").expect("generated field layout");
        self
    }

    pub fn changed_required_string(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_string")
    }

    pub fn eval_required_string(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("required_string") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_string".to_string(), attempted_path: "required_string".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_string())
                }}


    pub fn optional_string(&self) -> Option<String> {
        self.optional_string.clone()
    }

    pub fn update_optional_string(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_string = if matches!(value, teaql_core::Value::Null) { None } else { value.try_text().map(|value| value.trim().to_owned()).map(Some).unwrap_or_else(|| self.optional_string.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_string", value);
        self.__load_state.mark_loaded("optional_string").expect("generated field layout");
        self
    }

    pub fn changed_optional_string(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_string")
    }

    pub fn eval_optional_string(&self) -> teaql_core::eval::EvalResult<Option<String>> {
        if !self.is_loaded("optional_string") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_string".to_string(), attempted_path: "optional_string".to_string() }
                } else {
                    match &self.optional_string {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn large_text(&self) -> Option<String> {
        self.large_text.clone()
    }

    pub fn update_large_text(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.large_text = if matches!(value, teaql_core::Value::Null) { None } else { value.try_text().map(|value| value.trim().to_owned()).map(Some).unwrap_or_else(|| self.large_text.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "large_text", value);
        self.__load_state.mark_loaded("large_text").expect("generated field layout");
        self
    }

    pub fn changed_large_text(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "large_text")
    }

    pub fn eval_large_text(&self) -> teaql_core::eval::EvalResult<Option<String>> {
        if !self.is_loaded("large_text") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "large_text".to_string(), attempted_path: "large_text".to_string() }
                } else {
                    match &self.large_text {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_boolean(&self) -> bool {
        self.changed_required_boolean().and_then(|value| value.try_bool()).unwrap_or(self.required_boolean)
    }

    pub fn update_required_boolean(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_boolean = value.try_bool().unwrap_or(self.required_boolean.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_boolean", value);
        self.__load_state.mark_loaded("required_boolean").expect("generated field layout");
        self
    }

    pub fn changed_required_boolean(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_boolean")
    }

    pub fn eval_required_boolean(&self) -> teaql_core::eval::EvalResult<bool> {
        if !self.is_loaded("required_boolean") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_boolean".to_string(), attempted_path: "required_boolean".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_boolean())
                }}


    pub fn optional_boolean(&self) -> Option<bool> {
        self.optional_boolean.clone()
    }

    pub fn update_optional_boolean(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_boolean = if matches!(value, teaql_core::Value::Null) { None } else { value.try_bool().map(Some).unwrap_or_else(|| self.optional_boolean.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_boolean", value);
        self.__load_state.mark_loaded("optional_boolean").expect("generated field layout");
        self
    }

    pub fn changed_optional_boolean(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_boolean")
    }

    pub fn eval_optional_boolean(&self) -> teaql_core::eval::EvalResult<Option<bool>> {
        if !self.is_loaded("optional_boolean") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_boolean".to_string(), attempted_path: "optional_boolean".to_string() }
                } else {
                    match &self.optional_boolean {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_integer(&self) -> i64 {
        self.changed_required_integer().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.required_integer)
    }

    pub fn update_required_integer(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_integer = value.try_i64().map(|value| value as i64).unwrap_or(self.required_integer.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_integer", value);
        self.__load_state.mark_loaded("required_integer").expect("generated field layout");
        self
    }

    pub fn changed_required_integer(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_integer")
    }

    pub fn eval_required_integer(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("required_integer") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_integer".to_string(), attempted_path: "required_integer".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_integer())
                }}


    pub fn optional_integer(&self) -> Option<i64> {
        self.optional_integer.clone()
    }

    pub fn update_optional_integer(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_integer = if matches!(value, teaql_core::Value::Null) { None } else { value.try_i64().map(|value| value as i64).map(Some).unwrap_or_else(|| self.optional_integer.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_integer", value);
        self.__load_state.mark_loaded("optional_integer").expect("generated field layout");
        self
    }

    pub fn changed_optional_integer(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_integer")
    }

    pub fn eval_optional_integer(&self) -> teaql_core::eval::EvalResult<Option<i64>> {
        if !self.is_loaded("optional_integer") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_integer".to_string(), attempted_path: "optional_integer".to_string() }
                } else {
                    match &self.optional_integer {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_long(&self) -> i64 {
        self.changed_required_long().and_then(|value| value.try_i64()).unwrap_or(self.required_long)
    }

    pub fn update_required_long(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_long = value.try_i64().unwrap_or(self.required_long.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_long", value);
        self.__load_state.mark_loaded("required_long").expect("generated field layout");
        self
    }

    pub fn changed_required_long(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_long")
    }

    pub fn eval_required_long(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("required_long") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_long".to_string(), attempted_path: "required_long".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_long())
                }}


    pub fn optional_long(&self) -> Option<i64> {
        self.optional_long.clone()
    }

    pub fn update_optional_long(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_long = if matches!(value, teaql_core::Value::Null) { None } else { value.try_i64().map(Some).unwrap_or_else(|| self.optional_long.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_long", value);
        self.__load_state.mark_loaded("optional_long").expect("generated field layout");
        self
    }

    pub fn changed_optional_long(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_long")
    }

    pub fn eval_optional_long(&self) -> teaql_core::eval::EvalResult<Option<i64>> {
        if !self.is_loaded("optional_long") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_long".to_string(), attempted_path: "optional_long".to_string() }
                } else {
                    match &self.optional_long {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_decimal(&self) -> rust_decimal::Decimal {
        self.changed_required_decimal().and_then(|value| value.try_decimal()).unwrap_or(self.required_decimal)
    }

    pub fn update_required_decimal(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_decimal = value.try_decimal().unwrap_or(self.required_decimal.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_decimal", value);
        self.__load_state.mark_loaded("required_decimal").expect("generated field layout");
        self
    }

    pub fn changed_required_decimal(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_decimal")
    }

    pub fn eval_required_decimal(&self) -> teaql_core::eval::EvalResult<rust_decimal::Decimal> {
        if !self.is_loaded("required_decimal") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_decimal".to_string(), attempted_path: "required_decimal".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_decimal())
                }}


    pub fn optional_decimal(&self) -> Option<rust_decimal::Decimal> {
        self.optional_decimal.clone()
    }

    pub fn update_optional_decimal(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_decimal = if matches!(value, teaql_core::Value::Null) { None } else { value.try_decimal().map(Some).unwrap_or_else(|| self.optional_decimal.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_decimal", value);
        self.__load_state.mark_loaded("optional_decimal").expect("generated field layout");
        self
    }

    pub fn changed_optional_decimal(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_decimal")
    }

    pub fn eval_optional_decimal(&self) -> teaql_core::eval::EvalResult<Option<rust_decimal::Decimal>> {
        if !self.is_loaded("optional_decimal") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_decimal".to_string(), attempted_path: "optional_decimal".to_string() }
                } else {
                    match &self.optional_decimal {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_float(&self) -> f32 {
        self.changed_required_float().and_then(|value| value.try_f64()).map(|value| value as f32).unwrap_or(self.required_float)
    }

    pub fn update_required_float(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_float = value.try_f64().map(|value| value as f32).unwrap_or(self.required_float.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_float", value);
        self.__load_state.mark_loaded("required_float").expect("generated field layout");
        self
    }

    pub fn changed_required_float(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_float")
    }

    pub fn eval_required_float(&self) -> teaql_core::eval::EvalResult<f32> {
        if !self.is_loaded("required_float") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_float".to_string(), attempted_path: "required_float".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_float())
                }}


    pub fn optional_float(&self) -> Option<f32> {
        self.optional_float.clone()
    }

    pub fn update_optional_float(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_float = if matches!(value, teaql_core::Value::Null) { None } else { value.try_f64().map(|value| value as f32).map(Some).unwrap_or_else(|| self.optional_float.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_float", value);
        self.__load_state.mark_loaded("optional_float").expect("generated field layout");
        self
    }

    pub fn changed_optional_float(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_float")
    }

    pub fn eval_optional_float(&self) -> teaql_core::eval::EvalResult<Option<f32>> {
        if !self.is_loaded("optional_float") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_float".to_string(), attempted_path: "optional_float".to_string() }
                } else {
                    match &self.optional_float {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_double(&self) -> f64 {
        self.changed_required_double().and_then(|value| value.try_f64()).unwrap_or(self.required_double)
    }

    pub fn update_required_double(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_double = value.try_f64().unwrap_or(self.required_double.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_double", value);
        self.__load_state.mark_loaded("required_double").expect("generated field layout");
        self
    }

    pub fn changed_required_double(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_double")
    }

    pub fn eval_required_double(&self) -> teaql_core::eval::EvalResult<f64> {
        if !self.is_loaded("required_double") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_double".to_string(), attempted_path: "required_double".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_double())
                }}


    pub fn optional_double(&self) -> Option<f64> {
        self.optional_double.clone()
    }

    pub fn update_optional_double(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_double = if matches!(value, teaql_core::Value::Null) { None } else { value.try_f64().map(Some).unwrap_or_else(|| self.optional_double.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_double", value);
        self.__load_state.mark_loaded("optional_double").expect("generated field layout");
        self
    }

    pub fn changed_optional_double(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_double")
    }

    pub fn eval_optional_double(&self) -> teaql_core::eval::EvalResult<Option<f64>> {
        if !self.is_loaded("optional_double") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_double".to_string(), attempted_path: "optional_double".to_string() }
                } else {
                    match &self.optional_double {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_date(&self) -> chrono::NaiveDate {
        self.changed_required_date().and_then(|value| value.try_date()).unwrap_or(self.required_date)
    }

    pub fn update_required_date(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_date = value.try_date().unwrap_or(self.required_date.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_date", value);
        self.__load_state.mark_loaded("required_date").expect("generated field layout");
        self
    }

    pub fn changed_required_date(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_date")
    }

    pub fn eval_required_date(&self) -> teaql_core::eval::EvalResult<chrono::NaiveDate> {
        if !self.is_loaded("required_date") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_date".to_string(), attempted_path: "required_date".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_date())
                }}


    pub fn optional_date(&self) -> Option<chrono::NaiveDate> {
        self.optional_date.clone()
    }

    pub fn update_optional_date(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_date = if matches!(value, teaql_core::Value::Null) { None } else { value.try_date().map(Some).unwrap_or_else(|| self.optional_date.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_date", value);
        self.__load_state.mark_loaded("optional_date").expect("generated field layout");
        self
    }

    pub fn changed_optional_date(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_date")
    }

    pub fn eval_optional_date(&self) -> teaql_core::eval::EvalResult<Option<chrono::NaiveDate>> {
        if !self.is_loaded("optional_date") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_date".to_string(), attempted_path: "optional_date".to_string() }
                } else {
                    match &self.optional_date {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_time(&self) -> String {
        self.changed_required_time().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.required_time.clone())
    }

    pub fn update_required_time(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.required_time = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.required_time.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "required_time", value);
        self.__load_state.mark_loaded("required_time").expect("generated field layout");
        self
    }

    pub fn changed_required_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_time")
    }

    pub fn eval_required_time(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("required_time") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_time".to_string(), attempted_path: "required_time".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_time())
                }}


    pub fn optional_time(&self) -> Option<String> {
        self.optional_time.clone()
    }

    pub fn update_optional_time(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.optional_time = if matches!(value, teaql_core::Value::Null) { None } else { value.try_text().map(|value| value.trim().to_owned()).map(Some).unwrap_or_else(|| self.optional_time.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "optional_time", value);
        self.__load_state.mark_loaded("optional_time").expect("generated field layout");
        self
    }

    pub fn changed_optional_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_time")
    }

    pub fn eval_optional_time(&self) -> teaql_core::eval::EvalResult<Option<String>> {
        if !self.is_loaded("optional_time") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_time".to_string(), attempted_path: "optional_time".to_string() }
                } else {
                    match &self.optional_time {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn required_datetime(&self) -> teaql_core::time::Timestamp {
        self.changed_required_datetime().and_then(|value| value.try_timestamp()).unwrap_or(self.required_datetime)
    }

    pub fn update_required_datetime(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.required_datetime = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), "required_datetime", value);
        self.__load_state.mark_loaded("required_datetime").expect("generated field layout");
        self
    }
    pub fn changed_required_datetime(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "required_datetime")
    }

    pub fn eval_required_datetime(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("required_datetime") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "required_datetime".to_string(), attempted_path: "required_datetime".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.required_datetime())
                }}


    pub fn optional_datetime(&self) -> Option<teaql_core::time::Timestamp> {
        self.optional_datetime.clone()
    }

    pub fn update_optional_datetime(&mut self, value: Option<teaql_core::time::Timestamp>) -> &mut Self {
        self.optional_datetime = value;
        let value = value.map(teaql_core::Value::from).unwrap_or(teaql_core::Value::Null);
        self.__teaql_runtime_state().set(self.entity_key(), "optional_datetime", value);
        self.__load_state.mark_loaded("optional_datetime").expect("generated field layout");
        self
    }
    pub fn changed_optional_datetime(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "optional_datetime")
    }

    pub fn eval_optional_datetime(&self) -> teaql_core::eval::EvalResult<Option<teaql_core::time::Timestamp>> {
        if !self.is_loaded("optional_datetime") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "optional_datetime".to_string(), attempted_path: "optional_datetime".to_string() }
                } else {
                    match &self.optional_datetime {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
                }}


    pub fn create_time(&self) -> teaql_core::time::Timestamp {
        self.changed_create_time().and_then(|value| value.try_timestamp()).unwrap_or(self.create_time)
    }

    pub fn update_create_time(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.create_time = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), "create_time", value);
        self.__load_state.mark_loaded("create_time").expect("generated field layout");
        self
    }
    pub fn changed_create_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "create_time")
    }

    pub fn eval_create_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("create_time") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "create_time".to_string(), attempted_path: "create_time".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.create_time())
                }}


    pub fn update_time(&self) -> teaql_core::time::Timestamp {
        self.changed_update_time().and_then(|value| value.try_timestamp()).unwrap_or(self.update_time)
    }

    pub fn update_update_time(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.update_time = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), "update_time", value);
        self.__load_state.mark_loaded("update_time").expect("generated field layout");
        self
    }
    pub fn changed_update_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "update_time")
    }

    pub fn eval_update_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("update_time") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "update_time".to_string(), attempted_path: "update_time".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.update_time())
                }}


    pub fn version(&self) -> i64 {
        self.changed_version().and_then(|value| value.try_i64()).unwrap_or(self.version)
    }

    pub fn update_version(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.version = value.try_i64().unwrap_or(self.version.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "version", value);
        self.__load_state.mark_loaded("version").expect("generated field layout");
        self
    }

    pub fn changed_version(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "version")
    }

    pub fn eval_version(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("version") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "version".to_string(), attempted_path: "version".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.version())
                }}

    pub fn type_platform_id(&self) -> u64 {
        self.changed_type_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.type_platform_id)
    }

    pub fn update_type_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.type_platform_id = value.try_u64().unwrap_or(self.type_platform_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "type_platform", value);
        self.__load_state.mark_loaded("type_platform_id").expect("generated field layout");
        self
    }

    pub fn changed_type_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "type_platform")
    }

    pub fn eval_type_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("type_platform_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "type_platform_id".to_string(), attempted_path: "type_platform_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.type_platform_id())
                }}
    pub fn type_platform(&self) -> Option<&crate::TypePlatform> {
        if self.changed_type_platform_id().is_some()
            || !self.is_loaded("type_platform") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::TypePlatform>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "type_platform",
        ) {
            return edge.as_ref();
        }
        self.type_platform.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.type_platform_id())})
    }

    pub fn eval_type_platform(&self) -> teaql_core::eval::EvalResult<&crate::TypePlatform> {
        match self.type_platform() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "type_platform".to_string(), attempted_path: "type_platform".to_string() },
        }
    }

}

