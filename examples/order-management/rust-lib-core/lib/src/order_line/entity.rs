
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_line
use std::collections::BTreeMap;

use teaql_macros::TeaqlEntity;

/// [TEAQL AI WARNING]
/// TeaQL was explicitly designed to PREVENT AI hallucinations and random guessing.
/// DO NOT GUESS METHOD NAMES!
/// The methods listed below are the ONLY valid ways to interact with this entity.
/// If you encounter compilation errors (e.g., method not found), DO NOT guess another method name.
/// Read the method signatures in this file before proceeding.
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "OrderLine", table = "order_line_data", data_service = "sqlite")]
pub struct OrderLine {
#[teaql(id)]
    id: u64,

// @source order-management-model.xml:80
    product_name: String,

// @source order-management-model.xml:80
    sku: String,

// @source order-management-model.xml:80
    quantity: i64,

// @source order-management-model.xml:80
    create_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
// @source order-management-model.xml:80
#[teaql(column = "customer_order")]
    customer_order_id: u64,

// @source order-management-model.xml:80
#[teaql(column = "product")]
    product_id: u64,

// @source order-management-model.xml:80
#[teaql(column = "commerce_platform")]
    commerce_platform_id: u64,
// @source order-management-model.xml:80
#[teaql(relation(target = "CustomerOrder", local_key = "customer_order_id", foreign_key = "id"))]
    customer_order: Option<crate::CustomerOrder>,

// @source order-management-model.xml:80
#[teaql(relation(target = "Product", local_key = "product_id", foreign_key = "id"))]
    product: Option<crate::Product>,

// @source order-management-model.xml:80
#[teaql(relation(target = "CommercePlatform", local_key = "commerce_platform_id", foreign_key = "id"))]
    commerce_platform: Option<crate::CommercePlatform>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    root: teaql_runtime::EntityRoot,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl OrderLine {
    pub const ENTITY_NAME: &'static str = "Order Line";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRoot) -> Self {
        Self {
            id: 0_u64,
            product_name: String::new(),
            sku: String::new(),
            quantity: 0_i64,
            create_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            customer_order_id: 0_u64,
            product_id: 0_u64,
            commerce_platform_id: 0_u64,
            customer_order: None,
            product: None,
            commerce_platform: None,
            dynamic: BTreeMap::new(),
            root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
        }
    }

    pub fn entity_key(&self) -> teaql_runtime::EntityKey {
        teaql_runtime::EntityKey::new("OrderLine", self.id)
    }

    pub fn attach_root_recursive(&mut self, root: teaql_runtime::EntityRoot) {
        self.root = root.clone();
        if let Some(entity) = &mut self.customer_order {
            entity.attach_root_recursive(root.clone());
        }
        if let Some(entity) = &mut self.product {
            entity.attach_root_recursive(root.clone());
        }
        if let Some(entity) = &mut self.commerce_platform {
            entity.attach_root_recursive(root.clone());
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
        self.root.set(self.entity_key(), "id", value);
        self
    }

    pub fn changed_id(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "id")
    }

    pub fn eval_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "id".to_string(), attempted_path: "id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.id())
                }}

    pub fn product_name(&self) -> String {
        self.changed_product_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.product_name.clone())
    }

    pub fn update_product_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.product_name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.product_name.clone());
        self.root.set(self.entity_key(), "product_name", value);
        self
    }

    pub fn changed_product_name(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "product_name")
    }

    pub fn eval_product_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("product_name") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "product_name".to_string(), attempted_path: "product_name".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.product_name())
                }}

    pub fn sku(&self) -> String {
        self.changed_sku().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.sku.clone())
    }

    pub fn update_sku(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.sku = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.sku.clone());
        self.root.set(self.entity_key(), "sku", value);
        self
    }

    pub fn changed_sku(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "sku")
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
        self.root.set(self.entity_key(), "quantity", value);
        self
    }

    pub fn changed_quantity(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "quantity")
    }

    pub fn eval_quantity(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("quantity") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "quantity".to_string(), attempted_path: "quantity".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.quantity())
                }}

    pub fn create_time(&self) -> teaql_core::time::Timestamp {
        self.changed_create_time().and_then(|value| value.try_timestamp()).unwrap_or(self.create_time)
    }

    pub fn update_create_time(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.create_time = value.try_timestamp().unwrap_or(self.create_time.clone());
        self.root.set(self.entity_key(), "create_time", value);
        self
    }

    pub fn changed_create_time(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "create_time")
    }

    pub fn eval_create_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("create_time") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "create_time".to_string(), attempted_path: "create_time".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.create_time())
                }}

    pub fn version(&self) -> i64 {
        self.changed_version().and_then(|value| value.try_i64()).unwrap_or(self.version)
    }

    pub fn update_version(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.version = value.try_i64().unwrap_or(self.version.clone());
        self.root.set(self.entity_key(), "version", value);
        self
    }

    pub fn changed_version(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "version")
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
        self.root.set(self.entity_key(), "customer_order_id", value);
        self
    }

    pub fn changed_customer_order_id(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "customer_order_id")
    }

    pub fn eval_customer_order_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("customer_order_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order_id".to_string(), attempted_path: "customer_order_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.customer_order_id())
                }}

    pub fn product_id(&self) -> u64 {
        self.changed_product_id().and_then(|value| value.try_u64()).unwrap_or(self.product_id)
    }

    pub fn update_product_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.product_id = value.try_u64().unwrap_or(self.product_id.clone());
        self.root.set(self.entity_key(), "product_id", value);
        self
    }

    pub fn changed_product_id(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "product_id")
    }

    pub fn eval_product_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("product_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "product_id".to_string(), attempted_path: "product_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.product_id())
                }}

    pub fn commerce_platform_id(&self) -> u64 {
        self.changed_commerce_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.commerce_platform_id)
    }

    pub fn update_commerce_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.commerce_platform_id = value.try_u64().unwrap_or(self.commerce_platform_id.clone());
        self.root.set(self.entity_key(), "commerce_platform_id", value);
        self
    }

    pub fn changed_commerce_platform_id(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "commerce_platform_id")
    }

    pub fn eval_commerce_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("commerce_platform_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "commerce_platform_id".to_string(), attempted_path: "commerce_platform_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.commerce_platform_id())
                }}
    pub fn customer_order(&self) -> Option<&crate::CustomerOrder> {
        self.customer_order.as_ref()
    }

    pub fn eval_customer_order(&self) -> teaql_core::eval::EvalResult<&crate::CustomerOrder> {
        if !self.is_loaded("customer_order") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order".to_string(), attempted_path: "customer_order".to_string() }
        } else {
            match &self.customer_order {
                Some(v) => teaql_core::eval::EvalResult::Value(v),
                None => teaql_core::eval::EvalResult::Null,
            }
        }
    }

    pub fn product(&self) -> Option<&crate::Product> {
        self.product.as_ref()
    }

    pub fn eval_product(&self) -> teaql_core::eval::EvalResult<&crate::Product> {
        if !self.is_loaded("product") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "product".to_string(), attempted_path: "product".to_string() }
        } else {
            match &self.product {
                Some(v) => teaql_core::eval::EvalResult::Value(v),
                None => teaql_core::eval::EvalResult::Null,
            }
        }
    }

    pub fn commerce_platform(&self) -> Option<&crate::CommercePlatform> {
        self.commerce_platform.as_ref()
    }

    pub fn eval_commerce_platform(&self) -> teaql_core::eval::EvalResult<&crate::CommercePlatform> {
        if !self.is_loaded("commerce_platform") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "commerce_platform".to_string(), attempted_path: "commerce_platform".to_string() }
        } else {
            match &self.commerce_platform {
                Some(v) => teaql_core::eval::EvalResult::Value(v),
                None => teaql_core::eval::EvalResult::Null,
            }
        }
    }

    pub fn mark_as_delete(&mut self) -> &mut Self {
        self.root.mark_as_delete(self.entity_key());
        self
    }

    pub fn set_comment(&mut self, comment: impl Into<String>) -> &mut Self {
        self.root.set_comment(comment);
        self
    }
}

