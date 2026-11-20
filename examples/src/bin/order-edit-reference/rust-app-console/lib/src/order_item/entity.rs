
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_item
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
#[teaql(entity = "OrderItem", table = "order_item_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct OrderItem {
#[teaql(id)]
    id: u64,

// @source model.xml:5
#[teaql(max_length = 100)]
    sku: String,

// @source model.xml:5
#[teaql(integer32)]
    quantity: i64,
#[teaql(version)]
    version: i64,
// @source model.xml:5
#[teaql(column = "sales_order")]
    sales_order_id: u64,
// @source model.xml:5
#[teaql(relation(target = "SalesOrder", local_key = "sales_order_id", foreign_key = "id"))]
    sales_order: Option<Box<crate::SalesOrder>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl OrderItem {
    pub const ENTITY_NAME: &'static str = "order_item";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "8d70859137f59ca1b9c6e9daa84dde3e7300bdbbb95ca56c147662302a8a6aba";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("sales_order", 1),
        ("sku", 2),
        ("quantity", 3),
        ("version", 4),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("sales_order", "sales_order_id", "sales_order"),
        ("sku", "sku", "sku"),
        ("quantity", "quantity", "quantity"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            sku: String::new(),
            quantity: 0_i64,
            version: 0_i64,
            sales_order_id: 0_u64,
            sales_order: None,
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
        if let Some(entity) = &mut self.sales_order {
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


    pub fn sku(&self) -> String {
        self.changed_sku().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.sku.clone())
    }

    pub fn update_sku(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.sku = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.sku.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "sku", value);
        self.__load_state.mark_loaded("sku").expect("generated field layout");
        self
    }

    pub fn changed_sku(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "sku")
    }

    pub fn eval_sku(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("sku") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "sku".to_string(), attempted_path: "sku".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.sku())
                }}


    pub fn quantity(&self) -> i64 {
        self.changed_quantity().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.quantity)
    }

    pub fn update_quantity(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.quantity = value.try_i64().map(|value| value as i64).unwrap_or(self.quantity.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "quantity", value);
        self.__load_state.mark_loaded("quantity").expect("generated field layout");
        self
    }

    pub fn changed_quantity(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "quantity")
    }

    pub fn eval_quantity(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("quantity") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "quantity".to_string(), attempted_path: "quantity".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.quantity())
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

    pub fn sales_order_id(&self) -> u64 {
        self.changed_sales_order_id().and_then(|value| value.try_u64()).unwrap_or(self.sales_order_id)
    }

    pub fn update_sales_order_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.sales_order_id = value.try_u64().unwrap_or(self.sales_order_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "sales_order", value);
        self.__load_state.mark_loaded("sales_order_id").expect("generated field layout");
        self
    }

    pub fn changed_sales_order_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "sales_order")
    }

    pub fn eval_sales_order_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("sales_order_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "sales_order_id".to_string(), attempted_path: "sales_order_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.sales_order_id())
                }}
    pub fn sales_order(&self) -> Option<&crate::SalesOrder> {
        if self.changed_sales_order_id().is_some()
            || !self.is_loaded("sales_order") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::SalesOrder>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "sales_order",
        ) {
            return edge.as_ref();
        }
        self.sales_order.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.sales_order_id())})
    }

    pub fn eval_sales_order(&self) -> teaql_core::eval::EvalResult<&crate::SalesOrder> {
        match self.sales_order() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "sales_order".to_string(), attempted_path: "sales_order".to_string() },
        }
    }

}

