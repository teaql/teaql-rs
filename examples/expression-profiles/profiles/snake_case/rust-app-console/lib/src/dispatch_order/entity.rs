
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/dispatch_order
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
#[teaql(entity = "DispatchOrder", table = "dispatch_order_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct DispatchOrder {
#[teaql(id)]
    id: u64,

// @source model.xml:4
#[teaql(max_length = 100)]
    order_reference: String,

// @source model.xml:4
#[teaql(max_length = 100)]
    handling_note: String,
#[teaql(version)]
    version: i64,
// @source model.xml:4
#[teaql(column = "distribution_center")]
    distribution_center_id: u64,
// @source model.xml:4
#[teaql(relation(target = "DistributionCenter", local_key = "distribution_center_id", foreign_key = "id"))]
    distribution_center: Option<Box<crate::DistributionCenter>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl DispatchOrder {
    pub const ENTITY_NAME: &'static str = "dispatch_order";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "22bf34df51a4cc1c7b2666998b4fd7e5f109061a14a97fa6728cca038c8e8375";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("distribution_center", 1),
        ("order_reference", 2),
        ("handling_note", 3),
        ("version", 4),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("distribution_center", "distribution_center_id", "distribution_center"),
        ("order_reference", "order_reference", "order_reference"),
        ("handling_note", "handling_note", "handling_note"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            order_reference: String::new(),
            handling_note: String::new(),
            version: 0_i64,
            distribution_center_id: 0_u64,
            distribution_center: None,
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
        if let Some(entity) = &mut self.distribution_center {
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


    pub fn order_reference(&self) -> String {
        self.changed_order_reference().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.order_reference.clone())
    }

    pub fn update_order_reference(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.order_reference = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.order_reference.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "order_reference", value);
        self.__load_state.mark_loaded("order_reference").expect("generated field layout");
        self
    }

    pub fn changed_order_reference(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "order_reference")
    }

    pub fn eval_order_reference(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("order_reference") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_reference".to_string(), attempted_path: "order_reference".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.order_reference())
                }}


    pub fn handling_note(&self) -> String {
        self.changed_handling_note().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.handling_note.clone())
    }

    pub fn update_handling_note(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.handling_note = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.handling_note.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "handling_note", value);
        self.__load_state.mark_loaded("handling_note").expect("generated field layout");
        self
    }

    pub fn changed_handling_note(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "handling_note")
    }

    pub fn eval_handling_note(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("handling_note") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "handling_note".to_string(), attempted_path: "handling_note".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.handling_note())
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

    pub fn distribution_center_id(&self) -> u64 {
        self.changed_distribution_center_id().and_then(|value| value.try_u64()).unwrap_or(self.distribution_center_id)
    }

    pub fn update_distribution_center_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.distribution_center_id = value.try_u64().unwrap_or(self.distribution_center_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "distribution_center", value);
        self.__load_state.mark_loaded("distribution_center_id").expect("generated field layout");
        self
    }

    pub fn changed_distribution_center_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "distribution_center")
    }

    pub fn eval_distribution_center_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("distribution_center_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "distribution_center_id".to_string(), attempted_path: "distribution_center_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.distribution_center_id())
                }}
    pub fn distribution_center(&self) -> Option<&crate::DistributionCenter> {
        if self.changed_distribution_center_id().is_some()
            || !self.is_loaded("distribution_center") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::DistributionCenter>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "distribution_center",
        ) {
            return edge.as_ref();
        }
        self.distribution_center.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.distribution_center_id())})
    }

    pub fn eval_distribution_center(&self) -> teaql_core::eval::EvalResult<&crate::DistributionCenter> {
        match self.distribution_center() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "distribution_center".to_string(), attempted_path: "distribution_center".to_string() },
        }
    }

}

