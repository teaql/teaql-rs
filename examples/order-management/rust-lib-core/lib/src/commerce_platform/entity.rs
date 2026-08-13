
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/commerce_platform
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
#[teaql(entity = "CommercePlatform", table = "commerce_platform_data", data_service = "sqlite")]
pub struct CommercePlatform {
#[teaql(id)]
    id: u64,

// @source order-management-model.xml:14
    name: String,

// @source order-management-model.xml:14
    create_time: teaql_core::time::Timestamp,

// @source order-management-model.xml:14
    update_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
    #[teaql(boxed_relations)]
    pub _relations: Box<CommercePlatformReverseRelations>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    root: teaql_runtime::EntityRoot,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl CommercePlatform {
    pub const ENTITY_NAME: &'static str = "Commerce Platform";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRoot) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            create_time: teaql_core::time::Timestamp::now(),
            update_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            _relations: Box::new(CommercePlatformReverseRelations::new()),
            dynamic: BTreeMap::new(),
            root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
        }
    }

    pub fn entity_key(&self) -> teaql_runtime::EntityKey {
        teaql_runtime::EntityKey::new("CommercePlatform", self.id)
    }

    pub fn attach_root_recursive(&mut self, root: teaql_runtime::EntityRoot) {
        self.root = root.clone();
        self._relations.attach_root_recursive(root.clone());
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

    pub fn name(&self) -> String {
        self.changed_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.name.clone())
    }

    pub fn update_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.name.clone());
        self.root.set(self.entity_key(), "name", value);
        self
    }

    pub fn changed_name(&self) -> Option<teaql_core::Value> {
        self.root.get(&self.entity_key(), "name")
    }

    pub fn eval_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("name") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "name".to_string(), attempted_path: "name".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.name())
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
    pub fn customer_list(&self) -> &SmartList<crate::Customer> {
        &self._relations.customer_list
    }

    pub fn customer_list_mut(&mut self) -> &mut SmartList<crate::Customer> {
        &mut self._relations.customer_list
    }

    pub fn eval_customer_list(&self) -> teaql_core::eval::EvalResult<&SmartList<crate::Customer>> {
        if !self.is_loaded("customer_list") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_list".to_string(), attempted_path: "customer_list".to_string() }
        } else {
            teaql_core::eval::EvalResult::Value(&self._relations.customer_list)
        }
    }

    pub fn order_status_list(&self) -> &SmartList<crate::OrderStatus> {
        &self._relations.order_status_list
    }

    pub fn order_status_list_mut(&mut self) -> &mut SmartList<crate::OrderStatus> {
        &mut self._relations.order_status_list
    }

    pub fn eval_order_status_list(&self) -> teaql_core::eval::EvalResult<&SmartList<crate::OrderStatus>> {
        if !self.is_loaded("order_status_list") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_status_list".to_string(), attempted_path: "order_status_list".to_string() }
        } else {
            teaql_core::eval::EvalResult::Value(&self._relations.order_status_list)
        }
    }

    pub fn customer_order_list(&self) -> &SmartList<crate::CustomerOrder> {
        &self._relations.customer_order_list
    }

    pub fn customer_order_list_mut(&mut self) -> &mut SmartList<crate::CustomerOrder> {
        &mut self._relations.customer_order_list
    }

    pub fn eval_customer_order_list(&self) -> teaql_core::eval::EvalResult<&SmartList<crate::CustomerOrder>> {
        if !self.is_loaded("customer_order_list") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order_list".to_string(), attempted_path: "customer_order_list".to_string() }
        } else {
            teaql_core::eval::EvalResult::Value(&self._relations.customer_order_list)
        }
    }

    pub fn product_list(&self) -> &SmartList<crate::Product> {
        &self._relations.product_list
    }

    pub fn product_list_mut(&mut self) -> &mut SmartList<crate::Product> {
        &mut self._relations.product_list
    }

    pub fn eval_product_list(&self) -> teaql_core::eval::EvalResult<&SmartList<crate::Product>> {
        if !self.is_loaded("product_list") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "product_list".to_string(), attempted_path: "product_list".to_string() }
        } else {
            teaql_core::eval::EvalResult::Value(&self._relations.product_list)
        }
    }

    pub fn order_line_list(&self) -> &SmartList<crate::OrderLine> {
        &self._relations.order_line_list
    }

    pub fn order_line_list_mut(&mut self) -> &mut SmartList<crate::OrderLine> {
        &mut self._relations.order_line_list
    }

    pub fn eval_order_line_list(&self) -> teaql_core::eval::EvalResult<&SmartList<crate::OrderLine>> {
        if !self.is_loaded("order_line_list") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_line_list".to_string(), attempted_path: "order_line_list".to_string() }
        } else {
            teaql_core::eval::EvalResult::Value(&self._relations.order_line_list)
        }
    }

    pub fn order_search_preset_list(&self) -> &SmartList<crate::OrderSearchPreset> {
        &self._relations.order_search_preset_list
    }

    pub fn order_search_preset_list_mut(&mut self) -> &mut SmartList<crate::OrderSearchPreset> {
        &mut self._relations.order_search_preset_list
    }

    pub fn eval_order_search_preset_list(&self) -> teaql_core::eval::EvalResult<&SmartList<crate::OrderSearchPreset>> {
        if !self.is_loaded("order_search_preset_list") {
            teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_search_preset_list".to_string(), attempted_path: "order_search_preset_list".to_string() }
        } else {
            teaql_core::eval::EvalResult::Value(&self._relations.order_search_preset_list)
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

#[derive(Clone, Debug, PartialEq, teaql_macros::TeaqlReverseRelations)]
pub struct CommercePlatformReverseRelations {
#[teaql(relation(target = "Customer", local_key = "id", foreign_key = "commerce_platform_id", many))]
    customer_list: SmartList<crate::Customer>,
#[teaql(relation(target = "OrderStatus", local_key = "id", foreign_key = "commerce_platform_id", many))]
    order_status_list: SmartList<crate::OrderStatus>,
#[teaql(relation(target = "CustomerOrder", local_key = "id", foreign_key = "commerce_platform_id", many))]
    customer_order_list: SmartList<crate::CustomerOrder>,
#[teaql(relation(target = "Product", local_key = "id", foreign_key = "commerce_platform_id", many))]
    product_list: SmartList<crate::Product>,
#[teaql(relation(target = "OrderLine", local_key = "id", foreign_key = "commerce_platform_id", many))]
    order_line_list: SmartList<crate::OrderLine>,
#[teaql(relation(target = "OrderSearchPreset", local_key = "id", foreign_key = "commerce_platform_id", many))]
    order_search_preset_list: SmartList<crate::OrderSearchPreset>,
}

impl CommercePlatformReverseRelations {
    pub fn new() -> Self {
        Self {
            customer_list: Default::default(),
            order_status_list: Default::default(),
            customer_order_list: Default::default(),
            product_list: Default::default(),
            order_line_list: Default::default(),
            order_search_preset_list: Default::default(),
        }
    }

    pub fn attach_root_recursive(&mut self, root: teaql_runtime::EntityRoot) {
        for entity in &mut self.customer_list {
            entity.attach_root_recursive(root.clone());
        }
        for entity in &mut self.order_status_list {
            entity.attach_root_recursive(root.clone());
        }
        for entity in &mut self.customer_order_list {
            entity.attach_root_recursive(root.clone());
        }
        for entity in &mut self.product_list {
            entity.attach_root_recursive(root.clone());
        }
        for entity in &mut self.order_line_list {
            entity.attach_root_recursive(root.clone());
        }
        for entity in &mut self.order_search_preset_list {
            entity.attach_root_recursive(root.clone());
        }
    }
}
