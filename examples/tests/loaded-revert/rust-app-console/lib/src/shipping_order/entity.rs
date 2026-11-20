
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/shipping_order
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
#[teaql(reverse_relation(name = "shipping_line_list", target = "ShippingLine", json_type = "crate::ShippingLine", local_key = "id", foreign_key = "shipping_order_id", many))]
#[teaql(entity = "ShippingOrder", table = "shipping_order_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct ShippingOrder {
#[teaql(id)]
    id: u64,

// @source order-edit-model.xml:4
#[teaql(max_length = 100)]
    order_number: String,
#[teaql(version)]
    version: i64,
// @source order-edit-model.xml:4
#[teaql(column = "dispatch_desk")]
    dispatch_desk_id: u64,
// @source order-edit-model.xml:4
#[teaql(relation(target = "DispatchDesk", local_key = "dispatch_desk_id", foreign_key = "id"))]
    dispatch_desk: Option<Box<crate::DispatchDesk>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl ShippingOrder {
    pub const ENTITY_NAME: &'static str = "shipping_order";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "ff8c3d9002b09a9da98a1ec79eb8f5d123a8c2a463ac289f6ecf01d7c1b3b35f";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("dispatch_desk", 1),
        ("order_number", 2),
        ("version", 3),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("dispatch_desk", "dispatch_desk_id", "dispatch_desk"),
        ("order_number", "order_number", "order_number"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            order_number: String::new(),
            version: 0_i64,
            dispatch_desk_id: 0_u64,
            dispatch_desk: None,
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
        if let Some(entity) = &mut self.dispatch_desk {
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


    pub fn order_number(&self) -> String {
        self.changed_order_number().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.order_number.clone())
    }

    pub fn update_order_number(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.order_number = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.order_number.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "order_number", value);
        self.__load_state.mark_loaded("order_number").expect("generated field layout");
        self
    }

    pub fn changed_order_number(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "order_number")
    }

    pub fn eval_order_number(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("order_number") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_number".to_string(), attempted_path: "order_number".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.order_number())
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

    pub fn dispatch_desk_id(&self) -> u64 {
        self.changed_dispatch_desk_id().and_then(|value| value.try_u64()).unwrap_or(self.dispatch_desk_id)
    }

    pub fn update_dispatch_desk_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.dispatch_desk_id = value.try_u64().unwrap_or(self.dispatch_desk_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "dispatch_desk", value);
        self.__load_state.mark_loaded("dispatch_desk_id").expect("generated field layout");
        self
    }

    pub fn changed_dispatch_desk_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "dispatch_desk")
    }

    pub fn eval_dispatch_desk_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("dispatch_desk_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "dispatch_desk_id".to_string(), attempted_path: "dispatch_desk_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.dispatch_desk_id())
                }}
    pub fn dispatch_desk(&self) -> Option<&crate::DispatchDesk> {
        if self.changed_dispatch_desk_id().is_some()
            || !self.is_loaded("dispatch_desk") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::DispatchDesk>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "dispatch_desk",
        ) {
            return edge.as_ref();
        }
        self.dispatch_desk.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.dispatch_desk_id())})
    }

    pub fn eval_dispatch_desk(&self) -> teaql_core::eval::EvalResult<&crate::DispatchDesk> {
        match self.dispatch_desk() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "dispatch_desk".to_string(), attempted_path: "dispatch_desk".to_string() },
        }
    }
    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn shipping_line_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::ShippingLine>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "shipping_line_list",
        )
    }

    pub fn eval_shipping_line_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::ShippingLine>> {
        let relation = self.shipping_line_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "shipping_line_list".to_string(), attempted_path: "shipping_line_list".to_string() },
        }
    }

}

