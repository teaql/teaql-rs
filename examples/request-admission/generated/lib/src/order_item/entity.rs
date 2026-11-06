
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_item
use std::collections::BTreeMap;

use teaql_macros::{teaql_entity, TeaqlEntity};

// Never silently compile a contextual model against an unsupported runtime or macro.
const _: () = {
    assert!(teaql_runtime::CONTEXT_ROOT_SAVE_ADMISSION_REVISION >= 1);
    assert!(<OrderItem as teaql_core::TeaqlEntity>::CONTEXT_ROOT_METADATA_REVISION >= 1,
        "TeaQL context-root ownership metadata requires matching teaql-macros");
};


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

// @source model.xml:16
#[teaql(max_length = 80)]
    product_name: String,

// @source model.xml:16
#[teaql(numeric_precision = 19)]
#[teaql(numeric_scale = 7)]
    unit_price: rust_decimal::Decimal,

// @source model.xml:16
    create_time: teaql_core::time::Timestamp,

// @source model.xml:16
    update_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
// @source model.xml:16
#[teaql(column = "purchase_order")]
    purchase_order_id: u64,

// @source model.xml:16
#[teaql(column = "tenant")]
    #[teaql(context_root = "Tenant")]
    tenant_id: u64,
// @source model.xml:16
#[teaql(relation(target = "PurchaseOrder", local_key = "purchase_order_id", foreign_key = "id"))]
    purchase_order: Option<Box<crate::PurchaseOrder>>,

// @source model.xml:16
#[teaql(relation(target = "Tenant", local_key = "tenant_id", foreign_key = "id"))]
    tenant: Option<Box<crate::Tenant>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl OrderItem {
    pub const ENTITY_NAME: &'static str = "order_item";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "7d17fd30f99ff7729c89502083418f85d482d6c6b7448972408e96e73ae79d75";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("purchase_order", 1),
        ("tenant", 2),
        ("product_name", 3),
        ("unit_price", 4),
        ("create_time", 5),
        ("update_time", 6),
        ("version", 7),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("purchase_order", "purchase_order_id", "purchase_order"),
        ("tenant", "tenant_id", "tenant"),
        ("product_name", "product_name", "product_name"),
        ("unit_price", "unit_price", "unit_price"),
        ("create_time", "create_time", "create_time"),
        ("update_time", "update_time", "update_time"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            product_name: String::new(),
            unit_price: rust_decimal::Decimal::ZERO,
            create_time: teaql_core::time::Timestamp::now(),
            update_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            purchase_order_id: 0_u64,
            tenant_id: 0_u64,
            purchase_order: None,
            tenant: None,
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
        if let Some(entity) = &mut self.tenant {
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


    pub fn product_name(&self) -> String {
        self.changed_product_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.product_name.clone())
    }

    pub fn update_product_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.product_name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.product_name.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "product_name", value);
        self.__load_state.mark_loaded("product_name").expect("generated field layout");
        self
    }

    pub fn changed_product_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "product_name")
    }

    pub fn eval_product_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("product_name") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "product_name".to_string(), attempted_path: "product_name".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.product_name())
                }}


    pub fn unit_price(&self) -> rust_decimal::Decimal {
        self.changed_unit_price().and_then(|value| value.try_decimal()).unwrap_or(self.unit_price)
    }

    pub fn update_unit_price(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.unit_price = value.try_decimal().unwrap_or(self.unit_price.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "unit_price", value);
        self.__load_state.mark_loaded("unit_price").expect("generated field layout");
        self
    }

    pub fn changed_unit_price(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "unit_price")
    }

    pub fn eval_unit_price(&self) -> teaql_core::eval::EvalResult<rust_decimal::Decimal> {
        if !self.is_loaded("unit_price") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "unit_price".to_string(), attempted_path: "unit_price".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.unit_price())
                }}


    pub fn create_time(&self) -> teaql_core::time::Timestamp {
        self.changed_create_time().and_then(|value| value.try_timestamp()).unwrap_or(self.create_time)
    }

    pub fn update_create_time(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.create_time = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), "create_time", value);
        self.__load_state.mark_loaded("create_time").expect("generated field layout");
        self
    }
    pub fn changed_create_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "create_time")
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

    pub fn update_update_time(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.update_time = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), "update_time", value);
        self.__load_state.mark_loaded("update_time").expect("generated field layout");
        self
    }
    pub fn changed_update_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "update_time")
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

    pub fn tenant_id(&self) -> u64 {
        self.changed_tenant_id().and_then(|value| value.try_u64()).unwrap_or(self.tenant_id)
    }

    pub(crate) fn update_tenant_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.tenant_id = value.try_u64().unwrap_or(self.tenant_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "tenant", value);
        self.__load_state.mark_loaded("tenant_id").expect("generated field layout");
        self
    }

    pub fn changed_tenant_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "tenant")
    }

    pub fn eval_tenant_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("tenant_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "tenant_id".to_string(), attempted_path: "tenant_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.tenant_id())
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

    pub fn tenant(&self) -> Option<&crate::Tenant> {
        if self.changed_tenant_id().is_some()
            || !self.is_loaded("tenant") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Tenant>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "tenant",
        ) {
            return edge.as_ref();
        }
        self.tenant.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.tenant_id())})
    }

    pub fn eval_tenant(&self) -> teaql_core::eval::EvalResult<&crate::Tenant> {
        match self.tenant() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "tenant".to_string(), attempted_path: "tenant".to_string() },
        }
    }

}

