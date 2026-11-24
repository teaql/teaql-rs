
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/school_type
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
#[teaql(reverse_relation(name = "school_list", target = "School", json_type = "crate::School", local_key = "id", foreign_key = "school_type_id", many))]
#[teaql(entity = "SchoolType", table = "school_type_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct SchoolType {
// @source model.xml:8
#[teaql(id)]
    id: u64,

// @source model.xml:8
#[teaql(max_length = 100)]
    name: String,

// @source model.xml:8
#[teaql(max_length = 100)]
    code: String,
#[teaql(version)]
    version: i64,
// @source model.xml:8
#[teaql(column = "platform")]
    platform_id: u64,
// @source model.xml:8
#[teaql(relation(target = "Platform", local_key = "platform_id", foreign_key = "id"))]
    platform: Option<Box<crate::Platform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl SchoolType {
    pub const ENTITY_NAME: &'static str = "School Type";

    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_platform: crate::GeneratedField = crate::GeneratedField {
        model_key: "platform",
        query_key: "platform_id",
        member_name: "platform_id",
        column_name: "platform",
        load_index: 0,
        reference: true,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_id: crate::GeneratedField = crate::GeneratedField {
        model_key: "id",
        query_key: "id",
        member_name: "id",
        column_name: "id",
        load_index: 1,
        reference: false,
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
    pub(crate) const __TEAQL_FIELD_code: crate::GeneratedField = crate::GeneratedField {
        model_key: "code",
        query_key: "code",
        member_name: "code",
        column_name: "code",
        load_index: 3,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_version: crate::GeneratedField = crate::GeneratedField {
        model_key: "version",
        query_key: "version",
        member_name: "version",
        column_name: "version",
        load_index: 4,
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
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "7dead3ff3d50761605f872635fd603c366e72d352a1866f35d539dba2561af31";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        (Self::__TEAQL_FIELD_platform.model_key, Self::__TEAQL_FIELD_platform.load_index),
        (Self::__TEAQL_FIELD_id.model_key, Self::__TEAQL_FIELD_id.load_index),
        (Self::__TEAQL_FIELD_name.model_key, Self::__TEAQL_FIELD_name.load_index),
        (Self::__TEAQL_FIELD_code.model_key, Self::__TEAQL_FIELD_code.load_index),
        (Self::__TEAQL_FIELD_version.model_key, Self::__TEAQL_FIELD_version.load_index),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        (
            Self::__TEAQL_FIELD_platform.model_key,
            Self::__TEAQL_FIELD_platform.member_name,
            Self::__TEAQL_FIELD_platform.column_name,
        ),
        (
            Self::__TEAQL_FIELD_id.model_key,
            Self::__TEAQL_FIELD_id.member_name,
            Self::__TEAQL_FIELD_id.column_name,
        ),
        (
            Self::__TEAQL_FIELD_name.model_key,
            Self::__TEAQL_FIELD_name.member_name,
            Self::__TEAQL_FIELD_name.column_name,
        ),
        (
            Self::__TEAQL_FIELD_code.model_key,
            Self::__TEAQL_FIELD_code.member_name,
            Self::__TEAQL_FIELD_code.column_name,
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
            code: String::new(),
            version: 0_i64,
            platform_id: 0_u64,
            platform: None,
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


    pub fn code(&self) -> String {
        self.changed_code().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.code.clone())
    }

    pub fn update_code(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.code = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.code.clone());
        self.set_generated_field(Self::__TEAQL_FIELD_code, value);
        self
    }

    pub fn changed_code(&self) -> Option<teaql_core::Value> {
        self.generated_change(Self::__TEAQL_FIELD_code)
    }

    pub fn eval_code(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_code.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: Self::__TEAQL_FIELD_code.query_key.to_string(), attempted_path: Self::__TEAQL_FIELD_code.query_key.to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.code())
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

    pub fn platform_id(&self) -> u64 {
        self.changed_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.platform_id)
    }

    pub fn update_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.platform_id = value.try_u64().unwrap_or(self.platform_id.clone());
        self.set_generated_field(Self::__TEAQL_FIELD_platform, value);
        self
    }

    pub fn changed_platform_id(&self) -> Option<teaql_core::Value> {
        self.generated_change(Self::__TEAQL_FIELD_platform)
    }

    pub fn eval_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_platform.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: Self::__TEAQL_FIELD_platform.query_key.to_string(), attempted_path: Self::__TEAQL_FIELD_platform.query_key.to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.platform_id())
                }}
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
    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn school_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::School>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "school_list",
        )
    }

    pub fn eval_school_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::School>> {
        let relation = self.school_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "school_list".to_string(), attempted_path: "school_list".to_string() },
        }
    }

}

