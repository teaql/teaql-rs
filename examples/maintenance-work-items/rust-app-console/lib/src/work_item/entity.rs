
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/work_item
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
#[teaql(entity = "WorkItem", table = "work_item_data", data_service = "sqlite", audit_mask_fields = "")]
pub struct WorkItem {
#[teaql(id)]
    id: u64,

// @source model.xml:5
#[teaql(max_length = 100)]
    ticket: String,

// @source model.xml:5
#[teaql(max_length = 100)]
    title: String,

// @source model.xml:5
#[teaql(integer32)]
    priority: i64,

// @source model.xml:5
#[teaql(max_length = 100)]
    inspection_note: Option<String>,
#[teaql(version)]
    version: i64,
// @source model.xml:5
#[teaql(column = "maintenance_board")]
    maintenance_board_id: u64,
// @source model.xml:5
#[teaql(relation(target = "MaintenanceBoard", local_key = "maintenance_board_id", foreign_key = "id"))]
    maintenance_board: Option<Box<crate::MaintenanceBoard>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl WorkItem {
    pub const ENTITY_NAME: &'static str = "work_item";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "fee181d2b6f3023f6d380840970ff6d007c9410a5c18664d4a3ed58baf40d31d";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("maintenance_board", 1),
        ("ticket", 2),
        ("title", 3),
        ("priority", 4),
        ("inspection_note", 5),
        ("version", 6),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("maintenance_board", "maintenance_board_id", "maintenance_board"),
        ("ticket", "ticket", "ticket"),
        ("title", "title", "title"),
        ("priority", "priority", "priority"),
        ("inspection_note", "inspection_note", "inspection_note"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            ticket: String::new(),
            title: String::new(),
            priority: 0_i64,
            inspection_note: None,
            version: 0_i64,
            maintenance_board_id: 0_u64,
            maintenance_board: None,
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
        if let Some(entity) = &mut self.maintenance_board {
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


    pub fn ticket(&self) -> String {
        self.changed_ticket().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.ticket.clone())
    }

    pub fn update_ticket(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.ticket = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.ticket.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "ticket", value);
        self.__load_state.mark_loaded("ticket").expect("generated field layout");
        self
    }

    pub fn changed_ticket(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "ticket")
    }

    pub fn eval_ticket(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("ticket") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "ticket".to_string(), attempted_path: "ticket".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.ticket())
                }}


    pub fn title(&self) -> String {
        self.changed_title().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.title.clone())
    }

    pub fn update_title(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.title = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.title.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "title", value);
        self.__load_state.mark_loaded("title").expect("generated field layout");
        self
    }

    pub fn changed_title(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "title")
    }

    pub fn eval_title(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("title") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "title".to_string(), attempted_path: "title".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.title())
                }}


    pub fn priority(&self) -> i64 {
        self.changed_priority().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.priority)
    }

    pub fn update_priority(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.priority = value.try_i64().map(|value| value as i64).unwrap_or(self.priority.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "priority", value);
        self.__load_state.mark_loaded("priority").expect("generated field layout");
        self
    }

    pub fn changed_priority(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "priority")
    }

    pub fn eval_priority(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("priority") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "priority".to_string(), attempted_path: "priority".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.priority())
                }}


    pub fn inspection_note(&self) -> Option<String> {
        self.inspection_note.clone()
    }

    pub fn update_inspection_note(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.inspection_note = if matches!(value, teaql_core::Value::Null) { None } else { value.try_text().map(|value| value.trim().to_owned()).map(Some).unwrap_or_else(|| self.inspection_note.clone()) };
        self.__teaql_runtime_state().set(self.entity_key(), "inspection_note", value);
        self.__load_state.mark_loaded("inspection_note").expect("generated field layout");
        self
    }

    pub fn changed_inspection_note(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "inspection_note")
    }

    pub fn eval_inspection_note(&self) -> teaql_core::eval::EvalResult<Option<String>> {
        if !self.is_loaded("inspection_note") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "inspection_note".to_string(), attempted_path: "inspection_note".to_string() }
                } else {
                    match &self.inspection_note {
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

    pub fn maintenance_board_id(&self) -> u64 {
        self.changed_maintenance_board_id().and_then(|value| value.try_u64()).unwrap_or(self.maintenance_board_id)
    }

    pub fn update_maintenance_board_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.maintenance_board_id = value.try_u64().unwrap_or(self.maintenance_board_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "maintenance_board", value);
        self.__load_state.mark_loaded("maintenance_board_id").expect("generated field layout");
        self
    }

    pub fn changed_maintenance_board_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "maintenance_board")
    }

    pub fn eval_maintenance_board_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("maintenance_board_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "maintenance_board_id".to_string(), attempted_path: "maintenance_board_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.maintenance_board_id())
                }}
    pub fn maintenance_board(&self) -> Option<&crate::MaintenanceBoard> {
        if self.changed_maintenance_board_id().is_some()
            || !self.is_loaded("maintenance_board") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::MaintenanceBoard>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "maintenance_board",
        ) {
            return edge.as_ref();
        }
        self.maintenance_board.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.maintenance_board_id())})
    }

    pub fn eval_maintenance_board(&self) -> teaql_core::eval::EvalResult<&crate::MaintenanceBoard> {
        match self.maintenance_board() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "maintenance_board".to_string(), attempted_path: "maintenance_board".to_string() },
        }
    }

}

