
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/customer_profile
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
#[teaql(reverse_relation(name = "customer_order_list", target = "CustomerOrder", json_type = "crate::CustomerOrder", local_key = "id", foreign_key = "customer_profile_id", many))]
#[teaql(entity = "CustomerProfile", table = "customer_profile_data", data_service = "sqlite", audit_mask_fields = "email")]
pub struct CustomerProfile {
#[teaql(id)]
    id: u64,

// @source model.xml:5
#[teaql(max_length = 100)]
    email: String,
#[teaql(version)]
    version: i64,
// @source model.xml:5
#[teaql(column = "merchant")]
    merchant_id: u64,
// @source model.xml:5
#[teaql(relation(target = "Merchant", local_key = "merchant_id", foreign_key = "id"))]
    merchant: Option<Box<crate::Merchant>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl CustomerProfile {
    pub const ENTITY_NAME: &'static str = "Customer Profile";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "5a9cde22ab27c45df77fbacebdb80f8a03759bd10053dc56f76edb4c8b61e319";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("merchant", 1),
        ("email", 2),
        ("version", 3),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("merchant", "merchant_id", "merchant"),
        ("email", "email", "email"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            email: String::new(),
            version: 0_i64,
            merchant_id: 0_u64,
            merchant: None,
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
        if let Some(entity) = &mut self.merchant {
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


    pub fn email(&self) -> String {
        self.changed_email().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.email.clone())
    }

    pub fn update_email(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.email = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.email.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "email", value);
        self.__load_state.mark_loaded("email").expect("generated field layout");
        self
    }

    pub fn changed_email(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "email")
    }

    pub fn eval_email(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("email") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "email".to_string(), attempted_path: "email".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.email())
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

    pub fn merchant_id(&self) -> u64 {
        self.changed_merchant_id().and_then(|value| value.try_u64()).unwrap_or(self.merchant_id)
    }

    pub fn update_merchant_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.merchant_id = value.try_u64().unwrap_or(self.merchant_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "merchant", value);
        self.__load_state.mark_loaded("merchant_id").expect("generated field layout");
        self
    }

    pub fn changed_merchant_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "merchant")
    }

    pub fn eval_merchant_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("merchant_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "merchant_id".to_string(), attempted_path: "merchant_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.merchant_id())
                }}
    pub fn merchant(&self) -> Option<&crate::Merchant> {
        if self.changed_merchant_id().is_some()
            || !self.is_loaded("merchant") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Merchant>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "merchant",
        ) {
            return edge.as_ref();
        }
        self.merchant.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.merchant_id())})
    }

    pub fn eval_merchant(&self) -> teaql_core::eval::EvalResult<&crate::Merchant> {
        match self.merchant() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "merchant".to_string(), attempted_path: "merchant".to_string() },
        }
    }
    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn customer_order_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::CustomerOrder>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "customer_order_list",
        )
    }

    pub fn eval_customer_order_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::CustomerOrder>> {
        let relation = self.customer_order_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order_list".to_string(), attempted_path: "customer_order_list".to_string() },
        }
    }

}

