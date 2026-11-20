
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/purchase_order
use std::collections::BTreeMap;

use teaql_macros::{teaql_entity, TeaqlEntity};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderNumber(String);

impl OrderNumber {
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into().trim().to_owned();
        if value.is_empty() {
            return Err("order_number must not be blank");
        }
        Ok(Self(value))
    }

    pub(crate) fn from_canonical(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str { &self.0 }

    pub fn definition() -> teaql_core::business_id::BusinessIdDefinition {
        teaql_core::business_id::BusinessIdDefinition {
            field_name: "order_number".to_owned(),
            profile: "daily-permuted-v1".to_owned(),
            prefix: "ORD".to_owned(),
            namespace: "order_number".to_owned(),
            split_by_date: true,
            preserve_digits: 6,
            separator: "-".to_owned(),
        }
    }
}

impl From<OrderNumber> for teaql_core::Value {
    fn from(value: OrderNumber) -> Self {
        teaql_core::Value::Text(value.0)
    }
}

impl From<&OrderNumber> for teaql_core::Value {
    fn from(value: &OrderNumber) -> Self {
        teaql_core::Value::Text(value.0.clone())
    }
}

/// [TEAQL AI WARNING]
/// TeaQL was explicitly designed to PREVENT AI hallucinations and random guessing.
/// DO NOT GUESS METHOD NAMES!
/// The methods listed below are the ONLY valid ways to interact with this entity.
/// If you encounter compilation errors (e.g., method not found), DO NOT guess another method name.
/// Read the method signatures in this file before proceeding.
#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(indexed_layout)]
#[teaql(entity = "PurchaseOrder", table = "purchase_order_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct PurchaseOrder {
#[teaql(id)]
    id: u64,

// @source model.xml:5
#[teaql(max_length = 80)]
    supplier: String,

// @source model.xml:5
#[teaql(max_length = 100)]
    order_number: String,
#[teaql(version)]
    version: i64,
// @source model.xml:5
#[teaql(column = "platform")]
    platform_id: u64,
// @source model.xml:5
#[teaql(relation(target = "Platform", local_key = "platform_id", foreign_key = "id"))]
    platform: Option<Box<crate::Platform>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl PurchaseOrder {
    pub const ENTITY_NAME: &'static str = "purchase_order";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "5270760d55c941f82fc30cf0457d609378a6398d6229c0bb5ba44c7dea546787";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("platform", 1),
        ("supplier", 2),
        ("order_number", 3),
        ("version", 4),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("platform", "platform_id", "platform"),
        ("supplier", "supplier", "supplier"),
        ("order_number", "order_number", "order_number"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            supplier: String::new(),
            order_number: String::new(),
            version: 0_i64,
            platform_id: 0_u64,
            platform: None,
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
        if let Some(entity) = &mut self.platform {
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


    pub fn supplier(&self) -> String {
        self.changed_supplier().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.supplier.clone())
    }

    pub fn update_supplier(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.supplier = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.supplier.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "supplier", value);
        self.__load_state.mark_loaded("supplier").expect("generated field layout");
        self
    }

    pub fn changed_supplier(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "supplier")
    }

    pub fn eval_supplier(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("supplier") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "supplier".to_string(), attempted_path: "supplier".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.supplier())
                }}


    pub fn order_number(&self) -> OrderNumber {
        let value = self.changed_order_number()
            .and_then(|value| value.try_text().map(str::to_owned))
            .unwrap_or_else(|| self.order_number.clone());
        OrderNumber::from_canonical(value)
    }

    pub fn update_order_number(&mut self, value: OrderNumber) -> &mut Self {
        let canonical = value.0.clone();
        self.order_number = canonical.clone();
        self.__teaql_runtime_state().set(
            self.entity_key(),
            "order_number",
            teaql_core::Value::Text(canonical),
        );
        self.__load_state.mark_loaded("order_number").expect("generated field layout");
        self
    }

    pub fn changed_order_number(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "order_number")
    }

    pub fn eval_order_number(&self) -> teaql_core::eval::EvalResult<OrderNumber> {
        if !self.is_loaded("order_number") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "order_number".to_string(),
                attempted_path: "order_number".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.order_number())
        }
    }


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

    pub fn platform_id(&self) -> u64 {
        self.changed_platform_id().and_then(|value| value.try_u64()).unwrap_or(self.platform_id)
    }

    pub fn update_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.platform_id = value.try_u64().unwrap_or(self.platform_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "platform", value);
        self.__load_state.mark_loaded("platform_id").expect("generated field layout");
        self
    }

    pub fn changed_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "platform")
    }

    pub fn eval_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("platform_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "platform_id".to_string(), attempted_path: "platform_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.platform_id())
                }}
    pub fn platform(&self) -> Option<&crate::Platform> {
        if self.changed_platform_id().is_some()
            || !self.is_loaded("platform") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Platform>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "platform",
        ) {
            return edge.as_ref();
        }
        self.platform.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.platform_id())})
    }

    pub fn eval_platform(&self) -> teaql_core::eval::EvalResult<&crate::Platform> {
        match self.platform() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "platform".to_string(), attempted_path: "platform".to_string() },
        }
    }

}

pub(crate) struct PurchaseOrderOrderNumberBusinessIdSlot<'a>(
    pub(crate) &'a mut PurchaseOrder,
    pub(crate) bool,
);

impl teaql_core::business_id::BusinessIdSlot for PurchaseOrderOrderNumberBusinessIdSlot<'_> {
    fn current_business_id(&self) -> Option<&str> {
        (!self.0.order_number.is_empty()).then_some(self.0.order_number.as_str())
    }

    fn is_new_aggregate(&self) -> bool {
        self.1
    }

    fn assign_business_id(&mut self, value: teaql_core::business_id::BusinessIdValue) {
        self.0.update_order_number(OrderNumber::from_canonical(value.0));
    }
}
