
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/shipment
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
#[teaql(entity = "Shipment", table = "shipment_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct Shipment {
#[teaql(id)]
    id: u64,

// @source model.xml:16
#[teaql(max_length = 100)]
    reference_code: String,
#[teaql(version)]
    version: i64,
// @source model.xml:16
#[teaql(column = "customer_order")]
    customer_order_id: u64,
// @source model.xml:16
#[teaql(relation(target = "CustomerOrder", local_key = "customer_order_id", foreign_key = "id"))]
    customer_order: Option<Box<crate::CustomerOrder>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl Shipment {
    pub const ENTITY_NAME: &'static str = "Shipment";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            reference_code: String::new(),
            version: 0_i64,
            customer_order_id: 0_u64,
            customer_order: None,
            dynamic: BTreeMap::new(),
            __teaql_runtime_state: root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
        }
    }

    pub fn attach_runtime_state_recursive(&mut self, root: teaql_runtime::EntityRuntimeState) {
        root.adopt_mutations_from(self.__teaql_runtime_state());
        self.__teaql_replace_runtime_state(root.clone());
        if let Some(entity) = &mut self.customer_order {
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


    pub fn reference_code(&self) -> String {
        self.changed_reference_code().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.reference_code.clone())
    }

    pub fn update_reference_code(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.reference_code = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.reference_code.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "reference_code", value);
        self
    }

    pub fn changed_reference_code(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "reference_code")
    }

    pub fn eval_reference_code(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("reference_code") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "reference_code".to_string(), attempted_path: "reference_code".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.reference_code())
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

    pub fn customer_order_id(&self) -> u64 {
        self.changed_customer_order_id().and_then(|value| value.try_u64()).unwrap_or(self.customer_order_id)
    }

    pub fn update_customer_order_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.customer_order_id = value.try_u64().unwrap_or(self.customer_order_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "customer_order_id", value);
        self
    }

    pub fn changed_customer_order_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "customer_order_id")
    }

    pub fn eval_customer_order_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("customer_order_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order_id".to_string(), attempted_path: "customer_order_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.customer_order_id())
                }}
    pub fn customer_order(&self) -> Option<&crate::CustomerOrder> {
        self.customer_order.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.customer_order_id())})
    }

    pub fn eval_customer_order(&self) -> teaql_core::eval::EvalResult<&crate::CustomerOrder> {
        match self.customer_order() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None if self.is_loaded("customer_order") => teaql_core::eval::EvalResult::Null,
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order".to_string(), attempted_path: "customer_order".to_string() },
        }
    }

}

