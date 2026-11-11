
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/entry
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
#[teaql(entity = "Entry", table = "entry_data", data_service = "sqlite", audit_mask_fields = "guarded_token")]
pub struct Entry {
#[teaql(id)]
    id: u64,

// @source model.xml:4
#[teaql(max_length = 100)]
    name: String,

// @source model.xml:4
    born_at: teaql_core::time::Timestamp,

// @source model.xml:4
    touched_at: teaql_core::time::Timestamp,

// @source model.xml:4
#[teaql(max_length = 100)]
    source_ip: String,

// @source model.xml:4
#[teaql(max_length = 100)]
    last_ip: String,

// @source model.xml:4
#[teaql(max_length = 100)]
    opened_by: String,

// @source model.xml:4
#[teaql(max_length = 100)]
    changed_by: String,

// @source model.xml:4
#[teaql(max_length = 100)]
    guarded_token: String,
#[teaql(version)]
    version: i64,
// @source model.xml:4
#[teaql(column = "catalog")]
    catalog_id: u64,
// @source model.xml:4
#[teaql(relation(target = "Catalog", local_key = "catalog_id", foreign_key = "id"))]
    catalog: Option<Box<crate::Catalog>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl Entry {
    pub const ENTITY_NAME: &'static str = "entry";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "7ae3f6f25be2681c105a55532a8c60a599eada522a7c70284e22e2172db13492";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("catalog", 1),
        ("name", 2),
        ("born_at", 3),
        ("touched_at", 4),
        ("source_ip", 5),
        ("last_ip", 6),
        ("opened_by", 7),
        ("changed_by", 8),
        ("guarded_token", 9),
        ("version", 10),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("catalog", "catalog_id", "catalog"),
        ("name", "name", "name"),
        ("born_at", "born_at", "born_at"),
        ("touched_at", "touched_at", "touched_at"),
        ("source_ip", "source_ip", "source_ip"),
        ("last_ip", "last_ip", "last_ip"),
        ("opened_by", "opened_by", "opened_by"),
        ("changed_by", "changed_by", "changed_by"),
        ("guarded_token", "guarded_token", "guarded_token"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            born_at: Default::default(),
            touched_at: Default::default(),
            source_ip: String::new(),
            last_ip: String::new(),
            opened_by: String::new(),
            changed_by: String::new(),
            guarded_token: Default::default(),
            version: 0_i64,
            catalog_id: 0_u64,
            catalog: None,
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
        if let Some(entity) = &mut self.catalog {
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


    pub fn born_at(&self) -> teaql_core::time::Timestamp {
        self.changed_born_at().and_then(|value| value.try_timestamp()).unwrap_or(self.born_at)
    }

    pub fn update_born_at(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.born_at = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), "born_at", value);
        self.__load_state.mark_loaded("born_at").expect("generated field layout");
        self
    }
    pub fn changed_born_at(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "born_at")
    }

    pub fn eval_born_at(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("born_at") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "born_at".to_string(), attempted_path: "born_at".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.born_at())
                }}


    pub fn touched_at(&self) -> teaql_core::time::Timestamp {
        self.changed_touched_at().and_then(|value| value.try_timestamp()).unwrap_or(self.touched_at)
    }

    pub fn update_touched_at(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.touched_at = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state().set(self.entity_key(), "touched_at", value);
        self.__load_state.mark_loaded("touched_at").expect("generated field layout");
        self
    }
    pub fn changed_touched_at(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "touched_at")
    }

    pub fn eval_touched_at(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("touched_at") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "touched_at".to_string(), attempted_path: "touched_at".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.touched_at())
                }}


    pub fn source_ip(&self) -> String {
        self.changed_source_ip().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.source_ip.clone())
    }

    pub fn update_source_ip(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.source_ip = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.source_ip.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "source_ip", value);
        self.__load_state.mark_loaded("source_ip").expect("generated field layout");
        self
    }

    pub fn changed_source_ip(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "source_ip")
    }

    pub fn eval_source_ip(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("source_ip") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "source_ip".to_string(), attempted_path: "source_ip".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.source_ip())
                }}


    pub fn last_ip(&self) -> String {
        self.changed_last_ip().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.last_ip.clone())
    }

    pub fn update_last_ip(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.last_ip = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.last_ip.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "last_ip", value);
        self.__load_state.mark_loaded("last_ip").expect("generated field layout");
        self
    }

    pub fn changed_last_ip(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "last_ip")
    }

    pub fn eval_last_ip(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("last_ip") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "last_ip".to_string(), attempted_path: "last_ip".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.last_ip())
                }}


    pub fn opened_by(&self) -> String {
        self.changed_opened_by().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.opened_by.clone())
    }

    pub fn update_opened_by(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.opened_by = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.opened_by.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "opened_by", value);
        self.__load_state.mark_loaded("opened_by").expect("generated field layout");
        self
    }

    pub fn changed_opened_by(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "opened_by")
    }

    pub fn eval_opened_by(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("opened_by") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "opened_by".to_string(), attempted_path: "opened_by".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.opened_by())
                }}


    pub fn changed_by(&self) -> String {
        self.changed_changed_by().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.changed_by.clone())
    }

    pub fn update_changed_by(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.changed_by = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.changed_by.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "changed_by", value);
        self.__load_state.mark_loaded("changed_by").expect("generated field layout");
        self
    }

    pub fn changed_changed_by(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "changed_by")
    }

    pub fn eval_changed_by(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("changed_by") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "changed_by".to_string(), attempted_path: "changed_by".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.changed_by())
                }}


    pub fn guarded_token(&self) -> String {
        self.changed_guarded_token().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.guarded_token.clone())
    }

    pub fn update_guarded_token(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.guarded_token = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.guarded_token.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "guarded_token", value);
        self.__load_state.mark_loaded("guarded_token").expect("generated field layout");
        self
    }

    pub fn changed_guarded_token(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "guarded_token")
    }

    pub fn eval_guarded_token(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("guarded_token") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "guarded_token".to_string(), attempted_path: "guarded_token".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.guarded_token())
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

    pub fn catalog_id(&self) -> u64 {
        self.changed_catalog_id().and_then(|value| value.try_u64()).unwrap_or(self.catalog_id)
    }

    pub fn update_catalog_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.catalog_id = value.try_u64().unwrap_or(self.catalog_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "catalog", value);
        self.__load_state.mark_loaded("catalog_id").expect("generated field layout");
        self
    }

    pub fn changed_catalog_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "catalog")
    }

    pub fn eval_catalog_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("catalog_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "catalog_id".to_string(), attempted_path: "catalog_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.catalog_id())
                }}
    pub fn catalog(&self) -> Option<&crate::Catalog> {
        if self.changed_catalog_id().is_some()
            || !self.is_loaded("catalog") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::Catalog>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "catalog",
        ) {
            return edge.as_ref();
        }
        self.catalog.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.catalog_id())})
    }

    pub fn eval_catalog(&self) -> teaql_core::eval::EvalResult<&crate::Catalog> {
        match self.catalog() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "catalog".to_string(), attempted_path: "catalog".to_string() },
        }
    }

}

