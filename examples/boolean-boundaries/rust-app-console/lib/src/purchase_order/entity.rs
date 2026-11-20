
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/purchase_order
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
#[teaql(entity = "PurchaseOrder", table = "purchase_order_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct PurchaseOrder {
#[teaql(id)]
    id: u64,

// @source model.xml:4
#[teaql(max_length = 100)]
    reference: String,

// @source model.xml:4
    approved: Option<bool>,
#[teaql(version)]
    version: i64,
// @source model.xml:4
#[teaql(column = "platform")]
    platform_id: u64,
// @source model.xml:4
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
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "3e9d155e4f307fde4f4278ba5d3ad5fe94db7756f5ff2ba6f116d19a85000504";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("platform", 1),
        ("reference", 2),
        ("approved", 3),
        ("version", 4),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("platform", "platform_id", "platform"),
        ("reference", "reference", "reference"),
        ("approved", "approved", "approved"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            reference: String::new(),
            approved: None,
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


    pub fn reference(&self) -> String {
        self.changed_reference().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.reference.clone())
    }

    pub fn update_reference(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.reference = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.reference.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "reference", value);
        self.__load_state.mark_loaded("reference").expect("generated field layout");
        self
    }

    pub fn changed_reference(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "reference")
    }

    pub fn eval_reference(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("reference") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "reference".to_string(), attempted_path: "reference".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.reference())
                }}


    pub fn approved(&self) -> Option<bool> {
        self.approved.clone()
    }

    pub fn update_approved(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.approved = if matches!(value, teaql_core::Value::Null) { None } else { value.try_bool().map(Some).unwrap_or_else(|| self.approved.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "approved", value);
        self.__load_state.mark_loaded("approved").expect("generated field layout");
        self
    }

    pub fn changed_approved(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "approved")
    }

    pub fn eval_approved(&self) -> teaql_core::eval::EvalResult<Option<bool>> {
        if !self.is_loaded("approved") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "approved".to_string(), attempted_path: "approved".to_string() }
                } else {
                    match &self.approved {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
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

