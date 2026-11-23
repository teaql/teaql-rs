
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/purchase_order
use std::collections::BTreeMap;

use teaql_macros::{teaql_entity, TeaqlEntity};

// Never silently compile a contextual model against an unsupported runtime or macro.
const _: () = {
    assert!(teaql_runtime::CONTEXT_ROOT_SAVE_ADMISSION_REVISION >= 1);
    assert!(<PurchaseOrder as teaql_core::TeaqlEntity>::CONTEXT_ROOT_METADATA_REVISION >= 1,
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
#[teaql(reverse_relation(name = "order_item_list", target = "OrderItem", json_type = "crate::OrderItem", local_key = "id", foreign_key = "purchase_order_id", many))]
#[teaql(entity = "PurchaseOrder", table = "purchase_order_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct PurchaseOrder {
#[teaql(id)]
    id: u64,

// @source main.xml:9
#[teaql(max_length = 32)]
    contact_phone: String,

// @source main.xml:9
#[teaql(max_length = 100)]
    internal_note: Option<String>,

// @source main.xml:9
    create_time: teaql_core::time::Timestamp,

// @source main.xml:9
    update_time: teaql_core::time::Timestamp,
#[teaql(version)]
    version: i64,
// @source main.xml:9
#[teaql(column = "tenant")]
    #[teaql(context_root = "Tenant")]
    tenant_id: u64,
// @source main.xml:9
#[teaql(relation(target = "Tenant", local_key = "tenant_id", foreign_key = "id"))]
    tenant: Option<Box<crate::Tenant>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl PurchaseOrder {
    pub const ENTITY_NAME: &'static str = "purchase_order";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "fc779222657a345b62cacc08e4beb1351e6097a5fd25927366703543a4afdd39";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("tenant", 1),
        ("contact_phone", 2),
        ("internal_note", 3),
        ("create_time", 4),
        ("update_time", 5),
        ("version", 6),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("tenant", "tenant_id", "tenant"),
        ("contact_phone", "contact_phone", "contact_phone"),
        ("internal_note", "internal_note", "internal_note"),
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
            contact_phone: String::new(),
            internal_note: None,
            create_time: teaql_core::time::Timestamp::now(),
            update_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            tenant_id: 0_u64,
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


    pub fn contact_phone(&self) -> String {
        self.changed_contact_phone().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.contact_phone.clone())
    }

    pub fn update_contact_phone(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.contact_phone = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.contact_phone.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "contact_phone", value);
        self.__load_state.mark_loaded("contact_phone").expect("generated field layout");
        self
    }

    pub fn changed_contact_phone(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "contact_phone")
    }

    pub fn eval_contact_phone(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("contact_phone") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "contact_phone".to_string(), attempted_path: "contact_phone".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.contact_phone())
                }}


    pub fn internal_note(&self) -> Option<String> {
        self.internal_note.clone()
    }

    pub fn update_internal_note(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.internal_note = if matches!(value, teaql_core::Value::Null) { None } else { value.try_text().map(|value| value.trim().to_owned()).map(Some).unwrap_or_else(|| self.internal_note.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "internal_note", value);
        self.__load_state.mark_loaded("internal_note").expect("generated field layout");
        self
    }

    pub fn changed_internal_note(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "internal_note")
    }

    pub fn eval_internal_note(&self) -> teaql_core::eval::EvalResult<Option<String>> {
        if !self.is_loaded("internal_note") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "internal_note".to_string(), attempted_path: "internal_note".to_string() }
                } else {
                    match &self.internal_note {
                        Some(v) => teaql_core::eval::EvalResult::Value(Some(v.clone())),
                        None => teaql_core::eval::EvalResult::Null,
                    }
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
    /// Returns the relation view installed by the query that loaded this entity.
    /// This method never performs an implicit database query.
    pub fn order_item_list(&self) -> teaql_runtime::RelationHandle<'_, teaql_core::SmartList<crate::OrderItem>> {
        self.__teaql_runtime_state().relation_list(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME,
            self.id(),
            "order_item_list",
        )
    }

    pub fn eval_order_item_list(&self) -> teaql_core::eval::EvalResult<&teaql_core::SmartList<crate::OrderItem>> {
        let relation = self.order_item_list();
        match relation.state() {
            teaql_runtime::LoadedRelation::Loaded | teaql_runtime::LoadedRelation::Empty => teaql_core::eval::EvalResult::Value(relation.value().expect("loaded list relation must have a value")),
            teaql_runtime::LoadedRelation::NotLoaded => teaql_core::eval::EvalResult::NotLoaded { failed_node: "order_item_list".to_string(), attempted_path: "order_item_list".to_string() },
        }
    }

}

