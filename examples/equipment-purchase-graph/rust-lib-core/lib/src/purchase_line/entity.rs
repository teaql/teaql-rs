
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/purchase_line
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
#[teaql(entity = "PurchaseLine", table = "purchase_line_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct PurchaseLine {
#[teaql(id)]
    id: u64,

// @source model.xml:7
#[teaql(max_length = 100)]
    line_code: String,

// @source model.xml:7
#[teaql(integer32)]
    quantity: i64,
#[teaql(version)]
    version: i64,
// @source model.xml:7
#[teaql(column = "purchase_order")]
    purchase_order_id: u64,

// @source model.xml:7
#[teaql(column = "catalog_product")]
    catalog_product_id: u64,
// @source model.xml:7
#[teaql(relation(target = "PurchaseOrder", local_key = "purchase_order_id", foreign_key = "id"))]
    purchase_order: Option<Box<crate::PurchaseOrder>>,

// @source model.xml:7
#[teaql(relation(target = "CatalogProduct", local_key = "catalog_product_id", foreign_key = "id"))]
    catalog_product: Option<Box<crate::CatalogProduct>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl PurchaseLine {
    pub const ENTITY_NAME: &'static str = "purchase_line";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "8db1cd19bfe810b079ddf2d0b01188de0380297d40e08c775aeb61f158ba78c5";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("purchase_order", 1),
        ("catalog_product", 2),
        ("line_code", 3),
        ("quantity", 4),
        ("version", 5),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("purchase_order", "purchase_order_id", "purchase_order"),
        ("catalog_product", "catalog_product_id", "catalog_product"),
        ("line_code", "line_code", "line_code"),
        ("quantity", "quantity", "quantity"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            line_code: String::new(),
            quantity: 0_i64,
            version: 0_i64,
            purchase_order_id: 0_u64,
            catalog_product_id: 0_u64,
            purchase_order: None,
            catalog_product: None,
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
        if let Some(entity) = &mut self.purchase_order {
            entity.attach_runtime_state_recursive(root.clone());
        }
        if let Some(entity) = &mut self.catalog_product {
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


    pub fn line_code(&self) -> String {
        self.changed_line_code().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.line_code.clone())
    }

    pub fn update_line_code(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.line_code = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.line_code.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "line_code", value);
        self.__load_state.mark_loaded("line_code").expect("generated field layout");
        self
    }

    pub fn changed_line_code(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "line_code")
    }

    pub fn eval_line_code(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("line_code") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "line_code".to_string(), attempted_path: "line_code".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.line_code())
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

    pub fn purchase_order_id(&self) -> u64 {
        self.changed_purchase_order_id().and_then(|value| value.try_u64()).unwrap_or(self.purchase_order_id)
    }

    pub fn update_purchase_order_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.purchase_order_id = value.try_u64().unwrap_or(self.purchase_order_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "purchase_order", value);
        self.__load_state.mark_loaded("purchase_order_id").expect("generated field layout");
        self
    }

    pub fn changed_purchase_order_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "purchase_order")
    }

    pub fn eval_purchase_order_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("purchase_order_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "purchase_order_id".to_string(), attempted_path: "purchase_order_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.purchase_order_id())
                }}

    pub fn catalog_product_id(&self) -> u64 {
        self.changed_catalog_product_id().and_then(|value| value.try_u64()).unwrap_or(self.catalog_product_id)
    }

    pub fn update_catalog_product_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.catalog_product_id = value.try_u64().unwrap_or(self.catalog_product_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "catalog_product", value);
        self.__load_state.mark_loaded("catalog_product_id").expect("generated field layout");
        self
    }

    pub fn changed_catalog_product_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "catalog_product")
    }

    pub fn eval_catalog_product_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("catalog_product_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "catalog_product_id".to_string(), attempted_path: "catalog_product_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.catalog_product_id())
                }}
    pub fn purchase_order(&self) -> Option<&crate::PurchaseOrder> {
        if self.changed_purchase_order_id().is_some()
            || !self.is_loaded("purchase_order") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::PurchaseOrder>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "purchase_order",
        ) {
            return edge.as_ref();
        }
        self.purchase_order.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.purchase_order_id())})
    }

    pub fn eval_purchase_order(&self) -> teaql_core::eval::EvalResult<&crate::PurchaseOrder> {
        match self.purchase_order() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "purchase_order".to_string(), attempted_path: "purchase_order".to_string() },
        }
    }

    pub fn catalog_product(&self) -> Option<&crate::CatalogProduct> {
        if self.changed_catalog_product_id().is_some()
            || !self.is_loaded("catalog_product") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::CatalogProduct>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "catalog_product",
        ) {
            return edge.as_ref();
        }
        self.catalog_product.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.catalog_product_id())})
    }

    pub fn eval_catalog_product(&self) -> teaql_core::eval::EvalResult<&crate::CatalogProduct> {
        match self.catalog_product() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "catalog_product".to_string(), attempted_path: "catalog_product".to_string() },
        }
    }

}

