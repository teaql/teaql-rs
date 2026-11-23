
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/school_capacity_summary
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
#[teaql(entity = "SchoolCapacitySummary", table = "school_capacity_summary_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct SchoolCapacitySummary {
#[teaql(id)]
    id: u64,

// @source main.xml:16
#[teaql(max_length = 100)]
    name: String,

// @source main.xml:16
#[teaql(integer32)]
    total_capacity: i64,

// @source main.xml:16
#[teaql(integer32)]
    school_count: i64,
#[teaql(version)]
    version: i64,
// @source main.xml:16
#[teaql(column = "platform")]
    platform_id: u64,
// @source main.xml:16
#[teaql(relation(target = "Platform", local_key = "platform_id", foreign_key = "id"))]
    platform: Option<Box<crate::Platform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl SchoolCapacitySummary {
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
    pub(crate) const __TEAQL_FIELD_platform: crate::GeneratedField = crate::GeneratedField {
        model_key: "platform",
        query_key: "platform_id",
        member_name: "platform_id",
        column_name: "platform",
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
    pub(crate) const __TEAQL_FIELD_total_capacity: crate::GeneratedField = crate::GeneratedField {
        model_key: "total_capacity",
        query_key: "total_capacity",
        member_name: "total_capacity",
        column_name: "total_capacity",
        load_index: 3,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_school_count: crate::GeneratedField = crate::GeneratedField {
        model_key: "school_count",
        query_key: "school_count",
        member_name: "school_count",
        column_name: "school_count",
        load_index: 4,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_version: crate::GeneratedField = crate::GeneratedField {
        model_key: "version",
        query_key: "version",
        member_name: "version",
        column_name: "version",
        load_index: 5,
        reference: false,
    };

    pub const ENTITY_NAME: &'static str = "School Capacity Summary";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "f571f4dcc37bd33fc29f0aa97e881485bf74a0130eef2c5230fe6c7e15c257f4";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        (Self::__TEAQL_FIELD_id.model_key, Self::__TEAQL_FIELD_id.load_index),
        (Self::__TEAQL_FIELD_platform.model_key, Self::__TEAQL_FIELD_platform.load_index),
        (Self::__TEAQL_FIELD_name.model_key, Self::__TEAQL_FIELD_name.load_index),
        (Self::__TEAQL_FIELD_total_capacity.model_key, Self::__TEAQL_FIELD_total_capacity.load_index),
        (Self::__TEAQL_FIELD_school_count.model_key, Self::__TEAQL_FIELD_school_count.load_index),
        (Self::__TEAQL_FIELD_version.model_key, Self::__TEAQL_FIELD_version.load_index),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        (Self::__TEAQL_FIELD_id.model_key, Self::__TEAQL_FIELD_id.member_name, Self::__TEAQL_FIELD_id.column_name),
        (Self::__TEAQL_FIELD_platform.model_key, Self::__TEAQL_FIELD_platform.member_name, Self::__TEAQL_FIELD_platform.column_name),
        (Self::__TEAQL_FIELD_name.model_key, Self::__TEAQL_FIELD_name.member_name, Self::__TEAQL_FIELD_name.column_name),
        (Self::__TEAQL_FIELD_total_capacity.model_key, Self::__TEAQL_FIELD_total_capacity.member_name, Self::__TEAQL_FIELD_total_capacity.column_name),
        (Self::__TEAQL_FIELD_school_count.model_key, Self::__TEAQL_FIELD_school_count.member_name, Self::__TEAQL_FIELD_school_count.column_name),
        (Self::__TEAQL_FIELD_version.model_key, Self::__TEAQL_FIELD_version.member_name, Self::__TEAQL_FIELD_version.column_name),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            total_capacity: 0_i64,
            school_count: 0_i64,
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_id.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_id.query_key).expect("generated field layout");
        self
    }

    pub fn changed_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_id.model_key)
    }

    pub fn eval_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_id.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_name.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_name.query_key).expect("generated field layout");
        self
    }

    pub fn changed_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_name.model_key)
    }

    pub fn eval_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_name.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "name".to_string(), attempted_path: "name".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.name())
                }}


    pub fn total_capacity(&self) -> i64 {
        self.changed_total_capacity().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.total_capacity)
    }

    pub fn update_total_capacity(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.total_capacity = value.try_i64().map(|value| value as i64).unwrap_or(self.total_capacity.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_total_capacity.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_total_capacity.query_key).expect("generated field layout");
        self
    }

    pub fn changed_total_capacity(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_total_capacity.model_key)
    }

    pub fn eval_total_capacity(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_total_capacity.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "total_capacity".to_string(), attempted_path: "total_capacity".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.total_capacity())
                }}


    pub fn school_count(&self) -> i64 {
        self.changed_school_count().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.school_count)
    }

    pub fn update_school_count(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.school_count = value.try_i64().map(|value| value as i64).unwrap_or(self.school_count.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_school_count.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_school_count.query_key).expect("generated field layout");
        self
    }

    pub fn changed_school_count(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_school_count.model_key)
    }

    pub fn eval_school_count(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_school_count.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "school_count".to_string(), attempted_path: "school_count".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.school_count())
                }}


    pub fn version(&self) -> i64 {
        self.changed_version().and_then(|value| value.try_i64()).unwrap_or(self.version)
    }

    pub fn update_version(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.version = value.try_i64().unwrap_or(self.version.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_version.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_version.query_key).expect("generated field layout");
        self
    }

    pub fn changed_version(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_version.model_key)
    }

    pub fn eval_version(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_version.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "version".to_string(), attempted_path: "version".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.version())
                }}

    pub fn platform_id(&self) -> u64 {
        self.changed_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.platform_id)
    }

    pub fn update_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.platform_id = value.try_u64().unwrap_or(self.platform_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_platform.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_platform.query_key).expect("generated field layout");
        self
    }

    pub fn changed_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_platform.model_key)
    }

    pub fn eval_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_platform.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "platform_id".to_string(), attempted_path: "platform_id".to_string() }
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

}

