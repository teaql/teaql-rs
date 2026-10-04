
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/payment_attempt
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
#[teaql(entity = "PaymentAttempt", table = "payment_attempt_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct PaymentAttempt {
#[teaql(id)]
    id: u64,

// @source model.xml:15
#[teaql(max_length = 100)]
    reference_code: String,
#[teaql(version)]
    version: i64,
// @source model.xml:15
#[teaql(column = "payment")]
    payment_id: u64,
// @source model.xml:15
#[teaql(relation(target = "Payment", local_key = "payment_id", foreign_key = "id"))]
    payment: Option<Box<crate::Payment>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl PaymentAttempt {
    pub const ENTITY_NAME: &'static str = "Payment Attempt";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            reference_code: String::new(),
            version: 0_i64,
            payment_id: 0_u64,
            payment: None,
            dynamic: BTreeMap::new(),
            __teaql_runtime_state: root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
        }
    }

    pub fn attach_runtime_state_recursive(&mut self, root: teaql_runtime::EntityRuntimeState) {
        root.adopt_mutations_from(self.__teaql_runtime_state());
        self.__teaql_replace_runtime_state(root.clone());
        if let Some(entity) = &mut self.payment {
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

    pub fn payment_id(&self) -> u64 {
        self.changed_payment_id().and_then(|value| value.try_u64()).unwrap_or(self.payment_id)
    }

    pub fn update_payment_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.payment_id = value.try_u64().unwrap_or(self.payment_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "payment_id", value);
        self
    }

    pub fn changed_payment_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "payment_id")
    }

    pub fn eval_payment_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("payment_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "payment_id".to_string(), attempted_path: "payment_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.payment_id())
                }}
    pub fn payment(&self) -> Option<&crate::Payment> {
        let state = self.__teaql_runtime_state();
        if state.has_relation_view(<Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "payment") {
            let value: Option<&crate::Payment> = state.relation_option(<Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "payment").value();
            if value.is_some_and(|related| related.id() == self.payment_id()) {
                return value;
            }
        }
        self.payment.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.payment_id())})
    }

    pub fn eval_payment(&self) -> teaql_core::eval::EvalResult<&crate::Payment> {
        match self.payment() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None if self.is_loaded("payment") => teaql_core::eval::EvalResult::Null,
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "payment".to_string(), attempted_path: "payment".to_string() },
        }
    }

}

