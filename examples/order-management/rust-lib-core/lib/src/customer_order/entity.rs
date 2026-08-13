
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/customer_order
use std::collections::BTreeMap;

use teaql_core::SmartList;
use teaql_macros::TeaqlEntity;

/// [TEAQL AI WARNING]
/// TeaQL was explicitly designed to PREVENT AI hallucinations and random guessing.
/// DO NOT GUESS METHOD NAMES!
/// The methods listed below are the ONLY valid ways to interact with this entity.
/// If you encounter compilation errors (e.g., method not found), DO NOT guess another method name.
/// Read the method signatures in this file before proceeding.
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "CustomerOrder", table = "customer_order_data", data_service = "sqlite")]
pub struct CustomerOrder {
#[teaql(id)]
    id: u64,

// @source order-management-model.xml:57
    order_number: String,

// @source order-management-model.xml:57
    order_date: chrono::NaiveDate,

// @source order-management-model.xml:57
    total_amount: rust_decimal::Decimal,

// @source order-management-model.xml:57
    create_time: teaql_core::time::Timestamp,

// @source order-management-model.xml:57
    update_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
// @source order-management-model.xml:57
#[teaql(column = "status")]
    status_id: u64,

// @source order-management-model.xml:57
#[teaql(column = "customer")]
    customer_id: u64,

// @source order-management-model.xml:57
#[teaql(column = "commerce_platform")]
    commerce_platform_id: u64,
// @source order-management-model.xml:57
#[teaql(relation(target = "OrderStatus", local_key = "status_id", foreign_key = "id"))]
    status: Option<crate::OrderStatus>,

// @source order-management-model.xml:57
#[teaql(relation(target = "Customer", local_key = "customer_id", foreign_key = "id"))]
    customer: Option<crate::Customer>,

// @source order-management-model.xml:57
#[teaql(relation(target = "CommercePlatform", local_key = "commerce_platform_id", foreign_key = "id"))]
    commerce_platform: Option<crate::CommercePlatform>,
#[teaql(relation(target = "OrderLine", local_key = "id", foreign_key = "customer_order_id", many))]
    order_line_list: SmartList<crate::OrderLine>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    root: teaql_runtime::EntityRoot,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl CustomerOrder {
    pub const ENTITY_NAME: &'static str = "Customer Order";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRoot) -> Self {
        Self {
            id: 0_u64,
            order_number: String::new(),
            order_date: chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            total_amount: rust_decimal::Decimal::ZERO,
            create_time: teaql_core::time::Timestamp::now(),
            update_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            status_id: 0_u64,
            customer_id: 0_u64,
            commerce_platform_id: 0_u64,
            status: None,
            customer: None,
            commerce_platform: None,
            order_line_list: Default::default(),
            dynamic: BTreeMap::new(),
            root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
        }
    }

    pub fn entity_key(&self) -> teaql_runtime::EntityKey {
        teaql_runtime::EntityKey::new("CustomerOrder", self.id)
    }

    pub fn attach_root_recursive(&mut self, root: teaql_runtime::EntityRoot) {
        self.root = root.clone();
        if let Some(entity) = &mut self.status {
            entity.attach_root_recursive(root.clone());
        }
        if let Some(entity) = &mut self.customer {
            entity.attach_root_recursive(root.clone());
        }
        if let Some(entity) = &mut self.commerce_platform {
            entity.attach_root_recursive(root.clone());
        }
        for entity in &mut self.order_line_list {
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

    pub fn order_number(&self) -> String {
        self.changed_order_number().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.order_number.clone())
    }

    pub fn update_order_number(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.order_number = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.order_number.clone());
        self.root.set(self.entity_key(), "order_number", value);
        self
    }

    pub fn changed_order_number(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "order_number")
    }

    pub fn eval_order_number(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("order_number") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_number".to_string(), attempted_path: "order_number".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.order_number())
                }}

    pub fn order_date(&self) -> chrono::NaiveDate {
        self.changed_order_date().and_then(|value| value.try_date()).unwrap_or(self.order_date)
    }

    pub fn update_order_date(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.order_date = value.try_date().unwrap_or(self.order_date.clone());
        self.root.set(self.entity_key(), "order_date", value);
        self
    }

    pub fn changed_order_date(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "order_date")
    }

    pub fn eval_order_date(&self) -> teaql_core::eval::EvalResult<chrono::NaiveDate> {
        if !self.is_loaded("order_date") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_date".to_string(), attempted_path: "order_date".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.order_date())
                }}

    pub fn total_amount(&self) -> rust_decimal::Decimal {
        self.changed_total_amount().and_then(|value| value.try_decimal()).unwrap_or(self.total_amount)
    }

    pub fn update_total_amount(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.total_amount = value.try_decimal().unwrap_or(self.total_amount.clone());
        self.root.set(self.entity_key(), "total_amount", value);
        self
    }

    pub fn changed_total_amount(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "total_amount")
    }

    pub fn eval_total_amount(&self) -> teaql_core::eval::EvalResult<rust_decimal::Decimal> {
        if !self.is_loaded("total_amount") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "total_amount".to_string(), attempted_path: "total_amount".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.total_amount())
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

    pub fn update_time(&self) -> teaql_core::time::Timestamp {
        self.changed_update_time().and_then(|value| value.try_timestamp()).unwrap_or(self.update_time)
    }

    pub fn update_update_time(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.update_time = value.try_timestamp().unwrap_or(self.update_time.clone());
        self.root.set(self.entity_key(), "update_time", value);
        self
    }

    pub fn changed_update_time(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "update_time")
    }

    pub fn eval_update_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("update_time") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "update_time".to_string(), attempted_path: "update_time".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.update_time())
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
    pub fn status_id(&self) -> u64 {
        self.changed_status_id().and_then(|value| value.try_u64()).unwrap_or(self.status_id)
    }

    pub(crate) fn update_status_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.status_id = value.try_u64().unwrap_or(self.status_id.clone());
        self.root.set(self.entity_key(), "status_id", value);
        self
    }

    pub fn changed_status_id(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "status_id")
    }

    pub fn eval_status_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("status_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "status_id".to_string(), attempted_path: "status_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.status_id())
                }}

    pub fn customer_id(&self) -> u64 {
        self.changed_customer_id().and_then(|value| value.try_u64()).unwrap_or(self.customer_id)
    }

    pub fn update_customer_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.customer_id = value.try_u64().unwrap_or(self.customer_id.clone());
        self.root.set(self.entity_key(), "customer_id", value);
        self
    }

    pub fn changed_customer_id(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "customer_id")
    }

    pub fn eval_customer_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("customer_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_id".to_string(), attempted_path: "customer_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.customer_id())
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
    pub fn update_status_to_pending(&mut self) -> &mut Self {
        self.update_status_id(1001_u64)
    }

    pub fn status_is_pending(&self) -> bool {
        self.status_id() == 1001_u64
    }
    pub fn update_status_to_processing(&mut self) -> &mut Self {
        self.update_status_id(1002_u64)
    }

    pub fn status_is_processing(&self) -> bool {
        self.status_id() == 1002_u64
    }
    pub fn update_status_to_shipped(&mut self) -> &mut Self {
        self.update_status_id(1003_u64)
    }

    pub fn status_is_shipped(&self) -> bool {
        self.status_id() == 1003_u64
    }
    pub fn update_status_to_completed(&mut self) -> &mut Self {
        self.update_status_id(1004_u64)
    }

    pub fn status_is_completed(&self) -> bool {
        self.status_id() == 1004_u64
    }
    pub fn status(&self) -> Option<&crate::OrderStatus> {
        self.status.as_ref()
    }

    pub fn eval_status(&self) -> teaql_core::eval::EvalResult<&crate::OrderStatus> {
        if !self.is_loaded("status") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "status".to_string(), attempted_path: "status".to_string() }
        } else {
            match &self.status {
                Some(v) => teaql_core::eval::EvalResult::Value(v),
                None => teaql_core::eval::EvalResult::Null,
            }
        }
    }

    pub fn customer(&self) -> Option<&crate::Customer> {
        self.customer.as_ref()
    }

    pub fn eval_customer(&self) -> teaql_core::eval::EvalResult<&crate::Customer> {
        if !self.is_loaded("customer") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer".to_string(), attempted_path: "customer".to_string() }
        } else {
            match &self.customer {
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
    pub fn order_line_list(&self) -> &SmartList<crate::OrderLine> {
        &self.order_line_list
    }

    pub fn order_line_list_mut(&mut self) -> &mut SmartList<crate::OrderLine> {
        &mut self.order_line_list
    }

    pub fn eval_order_line_list(&self) -> teaql_core::eval::EvalResult<&SmartList<crate::OrderLine>> {
        if !self.is_loaded("order_line_list") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_line_list".to_string(), attempted_path: "order_line_list".to_string() }
        } else {
            teaql_core::eval::EvalResult::Value(&self.order_line_list)
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

