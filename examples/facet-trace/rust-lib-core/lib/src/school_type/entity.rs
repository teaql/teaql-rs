
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
#[teaql(reverse_relation(name = "school_list", target = "School", local_key = "id", foreign_key = "school_type_id", many))]
#[teaql(entity = "SchoolType", table = "school_type_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct SchoolType {
// @source main.xml:24
#[teaql(id)]
    id: u64,

// @source main.xml:24
#[teaql(max_length = 100)]
    name: String,

// @source main.xml:24
#[teaql(max_length = 100)]
    code: String,

// @source main.xml:24
#[teaql(numeric_precision = 19)]
#[teaql(numeric_scale = 7)]
    display_order: rust_decimal::Decimal,
#[teaql(version)]
    version: i64,
// @source main.xml:24
#[teaql(column = "platform")]
    platform_id: u64,
// @source main.xml:24
#[teaql(relation(target = "Platform", local_key = "platform_id", foreign_key = "id"))]
    platform: Option<Box<crate::Platform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl SchoolType {
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
    pub(crate) const __TEAQL_FIELD_display_order: crate::GeneratedField = crate::GeneratedField {
        model_key: "display_order",
        query_key: "display_order",
        member_name: "display_order",
        column_name: "display_order",
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

    pub const ENTITY_NAME: &'static str = "School Type";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            code: String::new(),
            display_order: rust_decimal::Decimal::ZERO,
            version: 0_i64,
            platform_id: 0_u64,
            platform: None,
            dynamic: BTreeMap::new(),
            __teaql_runtime_state: root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
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
        self.__load_state = state;
    }

    pub fn id(&self) -> u64 {
        self.changed_id().and_then(|value| value.try_u64()).unwrap_or(self.id)
    }

    pub fn update_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.id = value.try_u64().unwrap_or(self.id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_id.model_key, value);
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


    pub fn code(&self) -> String {
        self.changed_code().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.code.clone())
    }

    pub fn update_code(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.code = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.code.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_code.model_key, value);
        self
    }

    pub fn changed_code(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_code.model_key)
    }

    pub fn eval_code(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_code.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "code".to_string(), attempted_path: "code".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.code())
                }}


    pub fn display_order(&self) -> rust_decimal::Decimal {
        self.changed_display_order().and_then(|value| value.try_decimal()).unwrap_or(self.display_order)
    }

    pub fn update_display_order(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.display_order = value.try_decimal().unwrap_or(self.display_order.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_display_order.model_key, value);
        self
    }

    pub fn changed_display_order(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_display_order.model_key)
    }

    pub fn eval_display_order(&self) -> teaql_core::eval::EvalResult<rust_decimal::Decimal> {
        if !self.is_loaded(Self::__TEAQL_FIELD_display_order.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "display_order".to_string(), attempted_path: "display_order".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.display_order())
                }}


    pub fn version(&self) -> i64 {
        self.changed_version().and_then(|value| value.try_i64()).unwrap_or(self.version)
    }

    pub fn update_version(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.version = value.try_i64().unwrap_or(self.version.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_version.model_key, value);
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_platform.query_key, value);
        self
    }

    pub fn changed_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_platform.query_key)
    }

    pub fn eval_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_platform.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "platform_id".to_string(), attempted_path: "platform_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.platform_id())
                }}
    pub fn platform(&self) -> Option<&crate::Platform> {
        self.platform.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.platform_id())})
    }

    pub fn eval_platform(&self) -> teaql_core::eval::EvalResult<&crate::Platform> {
        match self.platform() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None if self.is_loaded("platform") => teaql_core::eval::EvalResult::Null,
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

