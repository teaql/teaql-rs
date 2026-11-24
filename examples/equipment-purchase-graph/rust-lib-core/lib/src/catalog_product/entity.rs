
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/catalog_product
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
#[teaql(reverse_relation(name = "purchase_line_list", target = "PurchaseLine", json_type = "crate::PurchaseLine", local_key = "id", foreign_key = "catalog_product_id", many))]
#[teaql(entity = "CatalogProduct", table = "catalog_product_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct CatalogProduct {
#[teaql(id)]
    id: u64,

// @source model.xml:4
#[teaql(max_length = 100)]
    sku: String,

// @source model.xml:4
#[teaql(max_length = 100)]
    label: String,
#[teaql(version)]
    version: i64,
// @source model.xml:4
#[teaql(column = "procurement_desk")]
    procurement_desk_id: u64,
// @source model.xml:4
#[teaql(relation(target = "ProcurementDesk", local_key = "procurement_desk_id", foreign_key = "id"))]
    procurement_desk: Option<Box<crate::ProcurementDesk>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl CatalogProduct {
    pub const ENTITY_NAME: &'static str = "catalog_product";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "a2e2a967a9e5f2f996559661445da0a723f97d45048a7dc4ee2758b4e60859bc";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("procurement_desk", 1),
        ("sku", 2),
        ("label", 3),
        ("version", 4),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("procurement_desk", "procurement_desk_id", "procurement_desk"),
        ("sku", "sku", "sku"),
        ("label", "label", "label"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            sku: String::new(),
            label: String::new(),
            version: 0_i64,
            procurement_desk_id: 0_u64,
            procurement_desk: None,
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
        if let Some(entity) = &mut self.procurement_desk {
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


    pub fn sku(&self) -> String {
        self.changed_sku().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.sku.clone())
    }

    pub fn update_sku(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.sku = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.sku.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "sku", value);
        self.__load_state.mark_loaded("sku").expect("generated field layout");
        self
    }

    pub fn changed_sku(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "sku")
    }

    pub fn eval_sku(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("sku") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "sku".to_string(), attempted_path: "sku".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.sku())
                }}


    pub fn label(&self) -> String {
        self.changed_label().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.label.clone())
    }

    pub fn update_label(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.label = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.label.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "label", value);
        self.__load_state.mark_loaded("label").expect("generated field layout");
        self
    }

    pub fn changed_label(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "label")
    }

    pub fn eval_label(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("label") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "label".to_string(), attempted_path: "label".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.label())
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

    pub fn procurement_desk_id(&self) -> u64 {
        self.changed_procurement_desk_id().and_then(|value| value.try_u64()).unwrap_or(self.procurement_desk_id)
    }

    pub fn update_procurement_desk_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.procurement_desk_id = value.try_u64().unwrap_or(self.procurement_desk_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "procurement_desk", value);
        self.__load_state.mark_loaded("procurement_desk_id").expect("generated field layout");
        self
    }

    pub fn changed_procurement_desk_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "procurement_desk")
    }

    pub fn eval_procurement_desk_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("procurement_desk_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "procurement_desk_id".to_string(), attempted_path: "procurement_desk_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.procurement_desk_id())
                }}
    pub fn procurement_desk(&self) -> Option<&crate::ProcurementDesk> {
        if self.changed_procurement_desk_id().is_some()
            || !self.is_loaded("procurement_desk") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::ProcurementDesk>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "procurement_desk",
        ) {
            return edge.as_ref();
        }
        self.procurement_desk.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.procurement_desk_id())})
    }

    pub fn eval_procurement_desk(&self) -> teaql_core::eval::EvalResult<&crate::ProcurementDesk> {
        match self.procurement_desk() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "procurement_desk".to_string(), attempted_path: "procurement_desk".to_string() },
        }
    }
    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn purchase_line_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::PurchaseLine>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "purchase_line_list",
        )
    }

    pub fn eval_purchase_line_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::PurchaseLine>> {
        let relation = self.purchase_line_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "purchase_line_list".to_string(), attempted_path: "purchase_line_list".to_string() },
        }
    }

}

