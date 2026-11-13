
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/training_academy
use std::collections::BTreeMap;

use teaql_macros::{teaql_entity, TeaqlEntity};

// Fail to compile against a runtime without modeled Integer-width support.
const _: fn(teaql_core::PropertyDescriptor) -> teaql_core::PropertyDescriptor = teaql_core::PropertyDescriptor::int32;

// An older derive must not silently flatten an inherited entity's storage.
const _: () = assert!(<TrainingAcademy as teaql_core::TeaqlEntity>::JOINED_INHERITANCE_METADATA_REVISION >= 1);


/// [TEAQL AI WARNING]
/// TeaQL was explicitly designed to PREVENT AI hallucinations and random guessing.
/// DO NOT GUESS METHOD NAMES!
/// The methods listed below are the ONLY valid ways to interact with this entity.
/// If you encounter compilation errors (e.g., method not found), DO NOT guess another method name.
/// Read the method signatures in this file before proceeding.
#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(indexed_layout)]
#[teaql(parent = crate::Academy)]
#[teaql(entity = "TrainingAcademy", table = "training_academy_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct TrainingAcademy {
#[teaql(id)]
    id: u64,

// @source model.xml:13
#[teaql(max_length = 100)]
    name: String,

// @source model.xml:13
#[teaql(integer32)]
    student_capacity: i64,
#[teaql(version)]
    version: i64,

// @source model.xml:15
#[teaql(max_length = 100)]
#[teaql(column = "Legacy Campus Code")]
    campus_code: String,

// @source model.xml:17
#[teaql(max_length = 100)]
    specialty: String,
// @source model.xml:13
#[teaql(column = "platform")]
    platform_id: u64,

// @source model.xml:13
#[teaql(column = "school_type")]
    school_type_id: u64,
// @source model.xml:13
#[teaql(relation(target = "Platform", local_key = "platform_id", foreign_key = "id"))]
    platform: Option<Box<crate::Platform>>,

// @source model.xml:13
#[teaql(relation(target = "SchoolType", local_key = "school_type_id", foreign_key = "id"))]
    school_type: Option<Box<crate::SchoolType>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl TrainingAcademy {
    pub const ENTITY_NAME: &'static str = "Training Academy";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "febc19f1547b3c9d17b9acd2ae34f398323bbe38ad635615cf4c19a534590fbc";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("platform", 1),
        ("school_type", 2),
        ("name", 3),
        ("student_capacity", 4),
        ("version", 5),
        ("campus_code", 6),
        ("specialty", 7),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("platform", "platform_id", "platform"),
        ("school_type", "school_type_id", "school_type"),
        ("name", "name", "name"),
        ("student_capacity", "student_capacity", "student_capacity"),
        ("version", "version", "version"),
        ("campus_code", "campus_code", "Legacy Campus Code"),
        ("specialty", "specialty", "specialty"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            student_capacity: 0_i64,
            version: 0_i64,
            campus_code: String::new(),
            specialty: String::new(),
            platform_id: 0_u64,
            school_type_id: 0_u64,
            platform: None,
            school_type: None,
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
        if let Some(entity) = &mut self.platform {
            entity.attach_runtime_state_recursive(root.clone());
        }
        if let Some(entity) = &mut self.school_type {
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


    pub fn name(&self) -> String {
        self.changed_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.name.clone())
    }

    pub fn update_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.name.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "name", value);
        self.__load_state.mark_loaded("name").expect("generated field layout");
        self
    }

    pub fn changed_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "name")
    }

    pub fn eval_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("name") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "name".to_string(), attempted_path: "name".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.name())
                }}


    pub fn student_capacity(&self) -> i64 {
        self.changed_student_capacity().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.student_capacity)
    }

    pub fn update_student_capacity(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.student_capacity = value.try_i64().map(|value| value as i64).unwrap_or(self.student_capacity.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "student_capacity", value);
        self.__load_state.mark_loaded("student_capacity").expect("generated field layout");
        self
    }

    pub fn changed_student_capacity(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "student_capacity")
    }

    pub fn eval_student_capacity(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("student_capacity") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "student_capacity".to_string(), attempted_path: "student_capacity".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.student_capacity())
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


    pub fn campus_code(&self) -> String {
        self.changed_campus_code().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.campus_code.clone())
    }

    pub fn update_campus_code(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.campus_code = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.campus_code.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "campus_code", value);
        self.__load_state.mark_loaded("campus_code").expect("generated field layout");
        self
    }

    pub fn changed_campus_code(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "campus_code")
    }

    pub fn eval_campus_code(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("campus_code") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "campus_code".to_string(), attempted_path: "campus_code".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.campus_code())
                }}


    pub fn specialty(&self) -> String {
        self.changed_specialty().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.specialty.clone())
    }

    pub fn update_specialty(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.specialty = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.specialty.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "specialty", value);
        self.__load_state.mark_loaded("specialty").expect("generated field layout");
        self
    }

    pub fn changed_specialty(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "specialty")
    }

    pub fn eval_specialty(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("specialty") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "specialty".to_string(), attempted_path: "specialty".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.specialty())
                }}

    pub fn platform_id(&self) -> u64 {
        self.changed_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.platform_id)
    }

    pub fn update_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.platform_id = value.try_u64().unwrap_or(self.platform_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "platform", value);
        self.__load_state.mark_loaded("platform_id").expect("generated field layout");
        self
    }

    pub fn changed_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "platform")
    }

    pub fn eval_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("platform_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "platform_id".to_string(), attempted_path: "platform_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.platform_id())
                }}

    pub fn school_type_id(&self) -> u64 {
        self.changed_school_type_id().and_then(|value| value.try_u64()).unwrap_or(self.school_type_id)
    }

    pub(crate) fn update_school_type_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.school_type_id = value.try_u64().unwrap_or(self.school_type_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "school_type", value);
        self.__load_state.mark_loaded("school_type_id").expect("generated field layout");
        self
    }

    pub fn changed_school_type_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "school_type")
    }

    pub fn eval_school_type_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("school_type_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "school_type_id".to_string(), attempted_path: "school_type_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.school_type_id())
                }}
    pub fn update_school_type_to_primary(&mut self) -> &mut Self {
        self.update_school_type_id(1001_u64)
    }

    pub fn school_type_is_primary(&self) -> bool {
        self.school_type_id() == 1001_u64
    }
    pub fn update_school_type_to_secondary(&mut self) -> &mut Self {
        self.update_school_type_id(1002_u64)
    }

    pub fn school_type_is_secondary(&self) -> bool {
        self.school_type_id() == 1002_u64
    }
    pub fn platform(&self) -> Option<&crate::Platform> {
        if self.changed_platform_id().is_some()
            || !self.is_loaded("platform") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Platform>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "platform",
        ) {
            return edge.as_ref();
        }
        self.platform.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.platform_id())})
    }

    pub fn eval_platform(&self) -> teaql_core::eval::EvalResult<&crate::Platform> {
        match self.platform() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "platform".to_string(), attempted_path: "platform".to_string() },
        }
    }

    pub fn school_type(&self) -> Option<&crate::SchoolType> {
        if self.changed_school_type_id().is_some()
            || !self.is_loaded("school_type") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::SchoolType>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "school_type",
        ) {
            return edge.as_ref();
        }
        self.school_type.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.school_type_id())})
    }

    pub fn eval_school_type(&self) -> teaql_core::eval::EvalResult<&crate::SchoolType> {
        match self.school_type() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "school_type".to_string(), attempted_path: "school_type".to_string() },
        }
    }

}

