
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_search_preset
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
#[teaql(entity = "OrderSearchPreset", table = "order_search_preset_data", data_service = "sqlite")]
pub struct OrderSearchPreset {
#[teaql(id)]
    id: u64,

// @source order-management-model.xml:12
    name: String,

// @source order-management-model.xml:12
    filter_json: String,

// @source order-management-model.xml:12
    request_id: String,

// @source order-management-model.xml:12
    owner_user_id: String,

// @source order-management-model.xml:12
    create_time: teaql_core::time::Timestamp,

// @source order-management-model.xml:12
    update_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
// @source order-management-model.xml:12
#[teaql(column = "commerce_platform")]
    commerce_platform_id: u64,
// @source order-management-model.xml:12
#[teaql(relation(target = "CommercePlatform", local_key = "commerce_platform_id", foreign_key = "id"))]
    commerce_platform: Option<Box<crate::CommercePlatform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl OrderSearchPreset {
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
    pub(crate) const __TEAQL_FIELD_name: crate::GeneratedField = crate::GeneratedField {
        model_key: "name",
        query_key: "name",
        member_name: "name",
        column_name: "name",
        load_index: 1,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_filter_json: crate::GeneratedField = crate::GeneratedField {
        model_key: "filter_json",
        query_key: "filter_json",
        member_name: "filter_json",
        column_name: "filter_json",
        load_index: 2,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_request_id: crate::GeneratedField = crate::GeneratedField {
        model_key: "request_id",
        query_key: "request_id",
        member_name: "request_id",
        column_name: "request_id",
        load_index: 3,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_owner_user_id: crate::GeneratedField = crate::GeneratedField {
        model_key: "owner_user_id",
        query_key: "owner_user_id",
        member_name: "owner_user_id",
        column_name: "owner_user_id",
        load_index: 4,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_commerce_platform: crate::GeneratedField = crate::GeneratedField {
        model_key: "commerce_platform",
        query_key: "commerce_platform_id",
        member_name: "commerce_platform_id",
        column_name: "commerce_platform",
        load_index: 5,
        reference: true,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_create_time: crate::GeneratedField = crate::GeneratedField {
        model_key: "create_time",
        query_key: "create_time",
        member_name: "create_time",
        column_name: "create_time",
        load_index: 6,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_update_time: crate::GeneratedField = crate::GeneratedField {
        model_key: "update_time",
        query_key: "update_time",
        member_name: "update_time",
        column_name: "update_time",
        load_index: 7,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_version: crate::GeneratedField = crate::GeneratedField {
        model_key: "version",
        query_key: "version",
        member_name: "version",
        column_name: "version",
        load_index: 8,
        reference: false,
    };

    pub const ENTITY_NAME: &'static str = "order_search_preset";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            filter_json: String::new(),
            request_id: String::new(),
            owner_user_id: String::new(),
            create_time: teaql_core::time::Timestamp::now(),
            update_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            commerce_platform_id: 0_u64,
            commerce_platform: None,
            dynamic: BTreeMap::new(),
            __teaql_runtime_state: root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
        }
    }

    pub fn attach_runtime_state_recursive(&mut self, root: teaql_runtime::EntityRuntimeState) {
        root.adopt_mutations_from(self.__teaql_runtime_state());
        self.__teaql_replace_runtime_state(root.clone());
        if let Some(entity) = &mut self.commerce_platform {
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

    pub fn filter_json(&self) -> String {
        self.changed_filter_json().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.filter_json.clone())
    }

    pub fn update_filter_json(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.filter_json = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.filter_json.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_filter_json.model_key, value);
        self
    }

    pub fn changed_filter_json(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_filter_json.model_key)
    }

    pub fn eval_filter_json(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_filter_json.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "filter_json".to_string(), attempted_path: "filter_json".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.filter_json())
                }}

    pub fn request_id(&self) -> String {
        self.changed_request_id().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.request_id.clone())
    }

    pub fn update_request_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.request_id = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.request_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_request_id.model_key, value);
        self
    }

    pub fn changed_request_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_request_id.model_key)
    }

    pub fn eval_request_id(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_request_id.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "request_id".to_string(), attempted_path: "request_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.request_id())
                }}

    pub fn owner_user_id(&self) -> String {
        self.changed_owner_user_id().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.owner_user_id.clone())
    }

    pub fn update_owner_user_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.owner_user_id = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.owner_user_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_owner_user_id.model_key, value);
        self
    }

    pub fn changed_owner_user_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_owner_user_id.model_key)
    }

    pub fn eval_owner_user_id(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_owner_user_id.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "owner_user_id".to_string(), attempted_path: "owner_user_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.owner_user_id())
                }}

    pub fn create_time(&self) -> teaql_core::time::Timestamp {
        self.changed_create_time().and_then(|value| value.try_timestamp()).unwrap_or(self.create_time)
    }

    pub fn update_create_time(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.create_time = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_create_time.model_key, value);
        self
    }
    pub fn changed_create_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_create_time.model_key)
    }

    pub fn eval_create_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded(Self::__TEAQL_FIELD_create_time.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_update_time.model_key, value);
        self
    }
    pub fn changed_update_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_update_time.model_key)
    }

    pub fn eval_update_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded(Self::__TEAQL_FIELD_update_time.query_key) {
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
    pub fn commerce_platform_id(&self) -> u64 {
        self.changed_commerce_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.commerce_platform_id)
    }

    pub fn update_commerce_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.commerce_platform_id = value.try_u64().unwrap_or(self.commerce_platform_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_commerce_platform.query_key, value);
        self
    }

    pub fn changed_commerce_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_commerce_platform.query_key)
    }

    pub fn eval_commerce_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_commerce_platform.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "commerce_platform_id".to_string(), attempted_path: "commerce_platform_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.commerce_platform_id())
                }}
    pub fn commerce_platform(&self) -> Option<&crate::CommercePlatform> {
        self.commerce_platform.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.commerce_platform_id())})
    }

    pub fn eval_commerce_platform(&self) -> teaql_core::eval::EvalResult<&crate::CommercePlatform> {
        match self.commerce_platform() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None if self.is_loaded("commerce_platform") => teaql_core::eval::EvalResult::Null,
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "commerce_platform".to_string(), attempted_path: "commerce_platform".to_string() },
        }
    }

}
