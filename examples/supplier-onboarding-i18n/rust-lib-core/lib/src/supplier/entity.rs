
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/supplier
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
#[teaql(entity = "Supplier", table = "supplier_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct Supplier {
#[teaql(id)]
    id: u64,

// @source model.xml:6
#[teaql(max_length = 80)]
    name: String,

// @source model.xml:6
#[teaql(integer32)]
    minimum_order_quantity: i64,
#[teaql(version)]
    version: i64,
// @source model.xml:6
#[teaql(column = "warehouse")]
    warehouse_id: u64,
// @source model.xml:6
#[teaql(relation(target = "Warehouse", local_key = "warehouse_id", foreign_key = "id"))]
    warehouse: Option<Box<crate::Warehouse>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl Supplier {
    pub const ENTITY_NAME: &'static str = "Supplier";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "f097b94d1d63a6466f5d9902e229b6332e48071c02492daa09de0a8717d781db";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("warehouse", 1),
        ("name", 2),
        ("minimum_order_quantity", 3),
        ("version", 4),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("warehouse", "warehouse_id", "warehouse"),
        ("name", "name", "name"),
        ("minimum_order_quantity", "minimum_order_quantity", "minimum_order_quantity"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            minimum_order_quantity: 0_i64,
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


    pub fn name(&self) -> String {
        self.changed_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.name.clone())
    }

    pub fn update_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.name.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "name", value);
        self.__load_state.mark_loaded("name").expect("generated field layout");
        self
    }

    pub fn changed_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "name")
    }

    pub fn eval_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("name") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "name".to_string(), attempted_path: "name".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.name())
                }}


    pub fn minimum_order_quantity(&self) -> i64 {
        self.changed_minimum_order_quantity().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.minimum_order_quantity)
    }

    pub fn update_minimum_order_quantity(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.minimum_order_quantity = value.try_i64().map(|value| value as i64).unwrap_or(self.minimum_order_quantity.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "minimum_order_quantity", value);
        self.__load_state.mark_loaded("minimum_order_quantity").expect("generated field layout");
        self
    }

    pub fn changed_minimum_order_quantity(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "minimum_order_quantity")
    }

    pub fn eval_minimum_order_quantity(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("minimum_order_quantity") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "minimum_order_quantity".to_string(), attempted_path: "minimum_order_quantity".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.minimum_order_quantity())
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

}

