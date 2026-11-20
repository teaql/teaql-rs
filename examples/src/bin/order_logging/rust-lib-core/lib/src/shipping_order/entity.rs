
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/shipping_order
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
#[teaql(reverse_relation(name = "shipping_item_list", target = "ShippingItem", json_type = "crate::ShippingItem", local_key = "id", foreign_key = "shipping_order_id", many))]
#[teaql(entity = "ShippingOrder", table = "shipping_order_data", data_service = "sqlite", audit_mask_fields = "customer_reference")]
pub struct ShippingOrder {
#[teaql(id)]
    id: u64,

// @source model.xml:9
#[teaql(max_length = 100)]
    reference: String,

// @source model.xml:9
#[teaql(max_length = 100)]
    customer_reference: String,

// @source model.xml:9
#[teaql(max_length = 100)]
    instructions: String,
#[teaql(version)]
    version: i64,
// @source model.xml:9
#[teaql(column = "warehouse")]
    warehouse_id: u64,
// @source model.xml:9
#[teaql(relation(target = "Warehouse", local_key = "warehouse_id", foreign_key = "id"))]
    warehouse: Option<Box<crate::Warehouse>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl ShippingOrder {
    pub const ENTITY_NAME: &'static str = "Shipping Order";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "7cb26cb4dca2b945f676f70a01453cd00440a544fae9d617fd153e37536078ce";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("warehouse", 1),
        ("reference", 2),
        ("customer_reference", 3),
        ("instructions", 4),
        ("version", 5),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("warehouse", "warehouse_id", "warehouse"),
        ("reference", "reference", "reference"),
        ("customer_reference", "customer_reference", "customer_reference"),
        ("instructions", "instructions", "instructions"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            reference: String::new(),
            customer_reference: String::new(),
            instructions: String::new(),
            version: 0_i64,
            warehouse_id: 0_u64,
            warehouse: None,
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
        if let Some(entity) = &mut self.warehouse {
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


    pub fn reference(&self) -> String {
        self.changed_reference().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.reference.clone())
    }

    pub fn update_reference(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.reference = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.reference.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "reference", value);
        self.__load_state.mark_loaded("reference").expect("generated field layout");
        self
    }

    pub fn changed_reference(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "reference")
    }

    pub fn eval_reference(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("reference") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "reference".to_string(), attempted_path: "reference".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.reference())
                }}


    pub fn customer_reference(&self) -> String {
        self.changed_customer_reference().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.customer_reference.clone())
    }

    pub fn update_customer_reference(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.customer_reference = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.customer_reference.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "customer_reference", value);
        self.__load_state.mark_loaded("customer_reference").expect("generated field layout");
        self
    }

    pub fn changed_customer_reference(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "customer_reference")
    }

    pub fn eval_customer_reference(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("customer_reference") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_reference".to_string(), attempted_path: "customer_reference".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.customer_reference())
                }}


    pub fn instructions(&self) -> String {
        self.changed_instructions().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.instructions.clone())
    }

    pub fn update_instructions(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.instructions = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.instructions.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "instructions", value);
        self.__load_state.mark_loaded("instructions").expect("generated field layout");
        self
    }

    pub fn changed_instructions(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "instructions")
    }

    pub fn eval_instructions(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("instructions") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "instructions".to_string(), attempted_path: "instructions".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.instructions())
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

    pub fn warehouse_id(&self) -> u64 {
        self.changed_warehouse_id().and_then(|value| value.try_u64()).unwrap_or(self.warehouse_id)
    }

    pub fn update_warehouse_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.warehouse_id = value.try_u64().unwrap_or(self.warehouse_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "warehouse", value);
        self.__load_state.mark_loaded("warehouse_id").expect("generated field layout");
        self
    }

    pub fn changed_warehouse_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "warehouse")
    }

    pub fn eval_warehouse_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("warehouse_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "warehouse_id".to_string(), attempted_path: "warehouse_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.warehouse_id())
                }}
    pub fn warehouse(&self) -> Option<&crate::Warehouse> {
        if self.changed_warehouse_id().is_some()
            || !self.is_loaded("warehouse") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Warehouse>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "warehouse",
        ) {
            return edge.as_ref();
        }
        self.warehouse.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.warehouse_id())})
    }

    pub fn eval_warehouse(&self) -> teaql_core::eval::EvalResult<&crate::Warehouse> {
        match self.warehouse() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "warehouse".to_string(), attempted_path: "warehouse".to_string() },
        }
    }
    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn shipping_item_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::ShippingItem>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "shipping_item_list",
        )
    }

    pub fn eval_shipping_item_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::ShippingItem>> {
        let relation = self.shipping_item_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "shipping_item_list".to_string(), attempted_path: "shipping_item_list".to_string() },
        }
    }

}

