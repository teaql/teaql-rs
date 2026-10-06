
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/customer_order
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
#[teaql(reverse_relation(name = "order_item_list", target = "OrderItem", local_key = "id", foreign_key = "customer_order_id", many))]
#[teaql(reverse_relation(name = "payment_list", target = "Payment", local_key = "id", foreign_key = "customer_order_id", many))]
#[teaql(reverse_relation(name = "shipment_list", target = "Shipment", local_key = "id", foreign_key = "customer_order_id", many))]
#[teaql(entity = "CustomerOrder", table = "customer_order_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct CustomerOrder {
#[teaql(id)]
    id: u64,

// @source model.xml:8
#[teaql(max_length = 100)]
    order_number: String,

// @source model.xml:8
#[teaql(max_length = 100)]
    description: String,
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

impl CustomerOrder {
    pub const ENTITY_NAME: &'static str = "Customer Order";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            order_number: String::new(),
            description: String::new(),
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


    pub fn order_number(&self) -> String {
        self.changed_order_number().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.order_number.clone())
    }

    pub fn update_order_number(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.order_number = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.order_number.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "order_number", value);
        self
    }

    pub fn changed_order_number(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "order_number")
    }

    pub fn eval_order_number(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("order_number") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_number".to_string(), attempted_path: "order_number".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.order_number())
                }}


    pub fn description(&self) -> String {
        self.changed_description().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.description.clone())
    }

    pub fn update_description(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.description = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.description.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "description", value);
        self
    }

    pub fn changed_description(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "description")
    }

    pub fn eval_description(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("description") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "description".to_string(), attempted_path: "description".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.description())
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
        let state = self.__teaql_runtime_state();
        if state.has_relation_view(<Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "platform") {
            let value: Option<&crate::Platform> = state.relation_option(<Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "platform").value();
            if value.is_some_and(|related| related.id() == self.platform_id()) {
                return value;
            }
        }
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
    pub fn order_item_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::OrderItem>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "order_item_list",
        )
    }

    pub fn eval_order_item_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::OrderItem>> {
        let relation = self.order_item_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_item_list".to_string(), attempted_path: "order_item_list".to_string() },
        }
    }

    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn payment_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::Payment>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "payment_list",
        )
    }

    pub fn eval_payment_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::Payment>> {
        let relation = self.payment_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "payment_list".to_string(), attempted_path: "payment_list".to_string() },
        }
    }

    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn shipment_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::Shipment>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "shipment_list",
        )
    }

    pub fn eval_shipment_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::Shipment>> {
        let relation = self.shipment_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "shipment_list".to_string(), attempted_path: "shipment_list".to_string() },
        }
    }

}

