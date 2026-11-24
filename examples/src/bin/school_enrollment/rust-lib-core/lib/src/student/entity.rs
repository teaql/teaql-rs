
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/student
use std::collections::BTreeMap;

use teaql_macros::{teaql_entity, TeaqlEntity};


/// [TEAQL AI WARNING]
/// TeaQL was explicitly designed to PREVENT AI hallucinations and random guessing.
/// DO NOT GUESS METHOD NAMES!
/// The methods listed below are the ONLY valid ways to interact with this entity.
/// If you encounter compilation errors (e.g., method not found), DO NOT guess another method name.
/// Read the method signatures in this file before proceeding.
#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(indexed_layout)]
#[teaql(entity = "Student", table = "student_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct Student {
#[teaql(id)]
    id: u64,

// @source model.xml:15
#[teaql(max_length = 100)]
    name: String,
#[teaql(version)]
    version: i64,
// @source model.xml:15
#[teaql(column = "school")]
    school_id: u64,
// @source model.xml:15
#[teaql(relation(target = "School", local_key = "school_id", foreign_key = "id"))]
    school: Option<Box<crate::School>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl Student {
    pub const ENTITY_NAME: &'static str = "Student";

    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_id: crate::GeneratedField = crate::GeneratedField {
        model_key: "id",
        query_key: "id",
        member_name: "id",
        column_name: "id",
        load_index: 0,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_school: crate::GeneratedField = crate::GeneratedField {
        model_key: "school",
        query_key: "school_id",
        member_name: "school_id",
        column_name: "school",
        load_index: 1,
        reference: true,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_name: crate::GeneratedField = crate::GeneratedField {
        model_key: "name",
        query_key: "name",
        member_name: "name",
        column_name: "name",
        load_index: 2,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_version: crate::GeneratedField = crate::GeneratedField {
        model_key: "version",
        query_key: "version",
        member_name: "version",
        column_name: "version",
        load_index: 3,
        reference: false,
    };

    fn set_generated_field(&mut self, field: crate::GeneratedField, value: teaql_core::Value) {
        self.__teaql_runtime_state().set(self.entity_key(), field.mutation_key(), value);
        self.__load_state.mark_loaded(field.query_key).expect("generated field layout");
    }

    fn generated_change(&self, field: crate::GeneratedField) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), field.mutation_key())
    }

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "c1de328428e6ef379ce29f836458bdb544dddaabf75edf13c9fe8c149897eede";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        (Self::__TEAQL_FIELD_id.model_key, Self::__TEAQL_FIELD_id.load_index),
        (Self::__TEAQL_FIELD_school.model_key, Self::__TEAQL_FIELD_school.load_index),
        (Self::__TEAQL_FIELD_name.model_key, Self::__TEAQL_FIELD_name.load_index),
        (Self::__TEAQL_FIELD_version.model_key, Self::__TEAQL_FIELD_version.load_index),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        (
            Self::__TEAQL_FIELD_id.model_key,
            Self::__TEAQL_FIELD_id.member_name,
            Self::__TEAQL_FIELD_id.column_name,
        ),
        (
            Self::__TEAQL_FIELD_school.model_key,
            Self::__TEAQL_FIELD_school.member_name,
            Self::__TEAQL_FIELD_school.column_name,
        ),
        (
            Self::__TEAQL_FIELD_name.model_key,
            Self::__TEAQL_FIELD_name.member_name,
            Self::__TEAQL_FIELD_name.column_name,
        ),
        (
            Self::__TEAQL_FIELD_version.model_key,
            Self::__TEAQL_FIELD_version.member_name,
            Self::__TEAQL_FIELD_version.column_name,
        ),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            version: 0_i64,
            school_id: 0_u64,
            school: None,
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
        if let Some(entity) = &mut self.school {
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
        self.set_generated_field(Self::__TEAQL_FIELD_id, value);
        self
    }

    pub fn changed_id(&self) -> Option<teaql_core::Value> {
        self.generated_change(Self::__TEAQL_FIELD_id)
    }

    pub fn eval_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_id.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: Self::__TEAQL_FIELD_id.query_key.to_string(), attempted_path: Self::__TEAQL_FIELD_id.query_key.to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.id())
                }}


    pub fn name(&self) -> String {
        self.changed_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.name.clone())
    }

    pub fn update_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.name.clone());
        self.set_generated_field(Self::__TEAQL_FIELD_name, value);
        self
    }

    pub fn changed_name(&self) -> Option<teaql_core::Value> {
        self.generated_change(Self::__TEAQL_FIELD_name)
    }

    pub fn eval_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_name.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: Self::__TEAQL_FIELD_name.query_key.to_string(), attempted_path: Self::__TEAQL_FIELD_name.query_key.to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.name())
                }}


    pub fn version(&self) -> i64 {
        self.changed_version().and_then(|value| value.try_i64()).unwrap_or(self.version)
    }

    pub fn update_version(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.version = value.try_i64().unwrap_or(self.version.clone());
        self.set_generated_field(Self::__TEAQL_FIELD_version, value);
        self
    }

    pub fn changed_version(&self) -> Option<teaql_core::Value> {
        self.generated_change(Self::__TEAQL_FIELD_version)
    }

    pub fn eval_version(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_version.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: Self::__TEAQL_FIELD_version.query_key.to_string(), attempted_path: Self::__TEAQL_FIELD_version.query_key.to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.version())
                }}

    pub fn school_id(&self) -> u64 {
        self.changed_school_id().and_then(|value| value.try_u64()).unwrap_or(self.school_id)
    }

    pub fn update_school_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.school_id = value.try_u64().unwrap_or(self.school_id.clone());
        self.set_generated_field(Self::__TEAQL_FIELD_school, value);
        self
    }

    pub fn changed_school_id(&self) -> Option<teaql_core::Value> {
        self.generated_change(Self::__TEAQL_FIELD_school)
    }

    pub fn eval_school_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_school.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: Self::__TEAQL_FIELD_school.query_key.to_string(), attempted_path: Self::__TEAQL_FIELD_school.query_key.to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.school_id())
                }}
    pub fn school(&self) -> Option<&crate::School> {
        if self.changed_school_id().is_some()
            || !self.is_loaded("school") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::School>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "school",
        ) {
            return edge.as_ref();
        }
        self.school.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.school_id())})
    }

    pub fn eval_school(&self) -> teaql_core::eval::EvalResult<&crate::School> {
        match self.school() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "school".to_string(), attempted_path: "school".to_string() },
        }
    }

}

