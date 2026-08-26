
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
#[teaql(entity = "SchoolType", table = "school_type_data", data_service = "sqlite")]
pub struct SchoolType {
// @source school_management.xml:24
#[teaql(id)]
    id: u64,

// @source school_management.xml:24
    name: String,

// @source school_management.xml:24
    code: String,

// @source school_management.xml:24
    display_order: rust_decimal::Decimal,
#[teaql(version)]
    version: i64,
// @source school_management.xml:24
#[teaql(column = "platform")]
    platform_id: u64,
// @source school_management.xml:24
#[teaql(relation(target = "Platform", local_key = "platform_id", foreign_key = "id"))]
    platform: Option<Box<crate::Platform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl SchoolType {
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
        self.__teaql_runtime_state().set(self.entity_key(), "id", value);
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

    pub fn code(&self) -> String {
        self.changed_code().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.code.clone())
    }

    pub fn update_code(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.code = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.code.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "code", value);
        self
    }

    pub fn changed_code(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "code")
    }

    pub fn eval_code(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("code") {
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
        self.__teaql_runtime_state().set(self.entity_key(), "display_order", value);
        self
    }

    pub fn changed_display_order(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "display_order")
    }

    pub fn eval_display_order(&self) -> teaql_core::eval::EvalResult<rust_decimal::Decimal> {
        if !self.is_loaded("display_order") {
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
        self.__teaql_runtime_state().set(self.entity_key(), "version", value);
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
    pub fn platform_id(&self) -> u64 {
        self.changed_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.platform_id)
    }

    pub fn update_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.platform_id = value.try_u64().unwrap_or(self.platform_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "platform_id", value);
        self
    }

    pub fn changed_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "platform_id")
    }

    pub fn eval_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("platform_id") {
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
