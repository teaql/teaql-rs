
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_line
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
#[teaql(entity = "OrderLine", table = "order_line_data", data_service = "sqlite")]
pub struct OrderLine {
#[teaql(id)]
    id: u64,

// @source order-management-model.xml:11
    product_name: String,

// @source order-management-model.xml:11
    sku: String,

// @source order-management-model.xml:11
    quantity: i64,

// @source order-management-model.xml:11
    create_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
// @source order-management-model.xml:11
#[teaql(column = "customer_order")]
    customer_order_id: u64,

// @source order-management-model.xml:11
#[teaql(column = "product")]
    product_id: u64,

// @source order-management-model.xml:11
#[teaql(column = "commerce_platform")]
    commerce_platform_id: u64,
// @source order-management-model.xml:11
#[teaql(relation(target = "CustomerOrder", local_key = "customer_order_id", foreign_key = "id"))]
    customer_order: Option<Box<crate::CustomerOrder>>,

// @source order-management-model.xml:11
#[teaql(relation(target = "Product", local_key = "product_id", foreign_key = "id"))]
    product: Option<Box<crate::Product>>,

// @source order-management-model.xml:11
#[teaql(relation(target = "CommercePlatform", local_key = "commerce_platform_id", foreign_key = "id"))]
    commerce_platform: Option<Box<crate::CommercePlatform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl OrderLine {
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
    pub(crate) const __TEAQL_FIELD_customer_order: crate::GeneratedField = crate::GeneratedField {
        model_key: "customer_order",
        query_key: "customer_order_id",
        member_name: "customer_order_id",
        column_name: "customer_order",
        load_index: 1,
        reference: true,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_product: crate::GeneratedField = crate::GeneratedField {
        model_key: "product",
        query_key: "product_id",
        member_name: "product_id",
        column_name: "product",
        load_index: 2,
        reference: true,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_product_name: crate::GeneratedField = crate::GeneratedField {
        model_key: "product_name",
        query_key: "product_name",
        member_name: "product_name",
        column_name: "product_name",
        load_index: 3,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_sku: crate::GeneratedField = crate::GeneratedField {
        model_key: "sku",
        query_key: "sku",
        member_name: "sku",
        column_name: "sku",
        load_index: 4,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_quantity: crate::GeneratedField = crate::GeneratedField {
        model_key: "quantity",
        query_key: "quantity",
        member_name: "quantity",
        column_name: "quantity",
        load_index: 5,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_commerce_platform: crate::GeneratedField = crate::GeneratedField {
        model_key: "commerce_platform",
        query_key: "commerce_platform_id",
        member_name: "commerce_platform_id",
        column_name: "commerce_platform",
        load_index: 6,
        reference: true,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_create_time: crate::GeneratedField = crate::GeneratedField {
        model_key: "create_time",
        query_key: "create_time",
        member_name: "create_time",
        column_name: "create_time",
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

    pub const ENTITY_NAME: &'static str = "order_line";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
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
        if let Some(entity) = &mut self.product {
            entity.attach_runtime_state_recursive(root.clone());
        }
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

    pub fn product_name(&self) -> String {
        self.changed_product_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.product_name.clone())
    }

    pub fn update_product_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.product_name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.product_name.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_product_name.model_key, value);
        self
    }

    pub fn changed_product_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_product_name.model_key)
    }

    pub fn eval_product_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_product_name.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_sku.model_key, value);
        self
    }

    pub fn changed_sku(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_sku.model_key)
    }

    pub fn eval_sku(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_sku.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_quantity.model_key, value);
        self
    }

    pub fn changed_quantity(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_quantity.model_key)
    }

    pub fn eval_quantity(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_quantity.query_key) {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "quantity".to_string(), attempted_path: "quantity".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.quantity())
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
    pub fn customer_order_id(&self) -> u64 {
        self.changed_customer_order_id().and_then(|value| value.try_u64()).unwrap_or(self.customer_order_id)
    }

    pub fn update_customer_order_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.customer_order_id = value.try_u64().unwrap_or(self.customer_order_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_customer_order.query_key, value);
        self
    }

    pub fn changed_customer_order_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_customer_order.query_key)
    }

    pub fn eval_customer_order_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_customer_order.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_product.query_key, value);
        self
    }

    pub fn changed_product_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_product.query_key)
    }

    pub fn eval_product_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_product.query_key) {
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

    pub fn product(&self) -> Option<&crate::Product> {
        self.product.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.product_id())})
    }

    pub fn eval_product(&self) -> teaql_core::eval::EvalResult<&crate::Product> {
        match self.product() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None if self.is_loaded("product") => teaql_core::eval::EvalResult::Null,
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "product".to_string(), attempted_path: "product".to_string() },
        }
    }

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
