
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/supplier_order
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
#[teaql(entity = "SupplierOrder", table = "supplier_order_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct SupplierOrder {
#[teaql(id)]
    id: u64,

// @source model.xml:5
#[teaql(max_length = 100)]
    external_reference: String,

// @source model.xml:5
#[teaql(max_length = 100)]
    sync_status: String,
#[teaql(version)]
    version: i64,
// @source model.xml:5
#[teaql(column = "supplier")]
    supplier_id: u64,
// @source model.xml:5
#[teaql(relation(target = "Supplier", local_key = "supplier_id", foreign_key = "id"))]
    supplier: Option<Box<crate::Supplier>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl SupplierOrder {
    pub const ENTITY_NAME: &'static str = "Supplier Order";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "c0177bc460393fa876fee1a230cdfe3c9269b316b16a102e12484e7b39e1a67f";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("supplier", 1),
        ("external_reference", 2),
        ("sync_status", 3),
        ("version", 4),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("supplier", "supplier_id", "supplier"),
        ("external_reference", "external_reference", "external_reference"),
        ("sync_status", "sync_status", "sync_status"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            external_reference: String::new(),
            sync_status: String::new(),
            version: 0_i64,
            supplier_id: 0_u64,
            supplier: None,
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
        if let Some(entity) = &mut self.supplier {
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


    pub fn external_reference(&self) -> String {
        self.changed_external_reference().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.external_reference.clone())
    }

    pub fn update_external_reference(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.external_reference = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.external_reference.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "external_reference", value);
        self.__load_state.mark_loaded("external_reference").expect("generated field layout");
        self
    }

    pub fn changed_external_reference(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "external_reference")
    }

    pub fn eval_external_reference(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("external_reference") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "external_reference".to_string(), attempted_path: "external_reference".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.external_reference())
                }}


    pub fn sync_status(&self) -> String {
        self.changed_sync_status().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.sync_status.clone())
    }

    pub fn update_sync_status(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.sync_status = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.sync_status.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "sync_status", value);
        self.__load_state.mark_loaded("sync_status").expect("generated field layout");
        self
    }

    pub fn changed_sync_status(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "sync_status")
    }

    pub fn eval_sync_status(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("sync_status") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "sync_status".to_string(), attempted_path: "sync_status".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.sync_status())
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

    pub fn supplier_id(&self) -> u64 {
        self.changed_supplier_id().and_then(|value| value.try_u64()).unwrap_or(self.supplier_id)
    }

    pub fn update_supplier_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.supplier_id = value.try_u64().unwrap_or(self.supplier_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "supplier", value);
        self.__load_state.mark_loaded("supplier_id").expect("generated field layout");
        self
    }

    pub fn changed_supplier_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "supplier")
    }

    pub fn eval_supplier_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("supplier_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "supplier_id".to_string(), attempted_path: "supplier_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.supplier_id())
                }}
    pub fn supplier(&self) -> Option<&crate::Supplier> {
        if self.changed_supplier_id().is_some()
            || !self.is_loaded("supplier") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Supplier>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "supplier",
        ) {
            return edge.as_ref();
        }
        self.supplier.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.supplier_id())})
    }

    pub fn eval_supplier(&self) -> teaql_core::eval::EvalResult<&crate::Supplier> {
        match self.supplier() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "supplier".to_string(), attempted_path: "supplier".to_string() },
        }
    }

}

