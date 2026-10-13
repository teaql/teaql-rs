
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
#[teaql(indexed_layout)]
#[teaql(entity = "PaymentAttempt", table = "payment_attempt_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct PaymentAttempt {
#[teaql(id)]
    id: u64,

// @source main.xml:15
#[teaql(max_length = 100)]
    reference_code: String,
#[teaql(version)]
    version: i64,
// @source main.xml:15
#[teaql(column = "payment")]
    payment_id: u64,
// @source main.xml:15
#[teaql(relation(target = "Payment", local_key = "payment_id", foreign_key = "id"))]
    payment: Option<Box<crate::Payment>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl PaymentAttempt {
    pub const ENTITY_NAME: &'static str = "Payment Attempt";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "e8c46db1ccaafe0f9d90335a74f7dd234482731b27d51dbebb2e9f07bebc02eb";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("payment", 1),
        ("reference_code", 2),
        ("version", 3),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("payment", "payment_id", "payment"),
        ("reference_code", "reference_code", "reference_code"),
        ("version", "version", "version"),
    ];

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
            __load_state: teaql_core::eval::LoadState::NotLoaded.into_indexed(
                <Self as teaql_core::TeaqlEntity>::field_layout().expect("generated field layout").expect("indexed layout")
            ).expect("new entity load state"),
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


    pub fn reference_code(&self) -> String {
        self.changed_reference_code().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.reference_code.clone())
    }

    pub fn update_reference_code(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.reference_code = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.reference_code.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "reference_code", value);
        self.__load_state.mark_loaded("reference_code").expect("generated field layout");
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

    pub fn payment_id(&self) -> u64 {
        self.changed_payment_id().and_then(|value| value.try_u64()).unwrap_or(self.payment_id)
    }

    pub fn update_payment_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.payment_id = value.try_u64().unwrap_or(self.payment_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "payment_id", value);
        self.__load_state.mark_loaded("payment_id").expect("generated field layout");
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
        if self.changed_payment_id().is_some()
            || !self.is_loaded("payment") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Payment>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "payment",
        ) {
            return edge.as_ref();
        }
        self.payment.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.payment_id())})
    }

    pub fn eval_payment(&self) -> teaql_core::eval::EvalResult<&crate::Payment> {
        match self.payment() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "payment".to_string(), attempted_path: "payment".to_string() },
        }
    }

}

