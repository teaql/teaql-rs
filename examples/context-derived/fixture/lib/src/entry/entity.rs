
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
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_id: crate::GeneratedField = crate::GeneratedField {
        model_key: "id",
        query_key: "id",
        member_name: "id",
        column_name: "id",
        load_index: 0,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_catalog: crate::GeneratedField = crate::GeneratedField {
        model_key: "catalog",
        query_key: "catalog_id",
        member_name: "catalog_id",
        column_name: "catalog",
        load_index: 1,
        reference: true,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_name: crate::GeneratedField = crate::GeneratedField {
        model_key: "name",
        query_key: "name",
        member_name: "name",
        column_name: "name",
        load_index: 2,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_born_at: crate::GeneratedField = crate::GeneratedField {
        model_key: "born_at",
        query_key: "born_at",
        member_name: "born_at",
        column_name: "born_at",
        load_index: 3,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_touched_at: crate::GeneratedField = crate::GeneratedField {
        model_key: "touched_at",
        query_key: "touched_at",
        member_name: "touched_at",
        column_name: "touched_at",
        load_index: 4,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_source_ip: crate::GeneratedField = crate::GeneratedField {
        model_key: "source_ip",
        query_key: "source_ip",
        member_name: "source_ip",
        column_name: "source_ip",
        load_index: 5,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_last_ip: crate::GeneratedField = crate::GeneratedField {
        model_key: "last_ip",
        query_key: "last_ip",
        member_name: "last_ip",
        column_name: "last_ip",
        load_index: 6,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_opened_by: crate::GeneratedField = crate::GeneratedField {
        model_key: "opened_by",
        query_key: "opened_by",
        member_name: "opened_by",
        column_name: "opened_by",
        load_index: 7,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_changed_by: crate::GeneratedField = crate::GeneratedField {
        model_key: "changed_by",
        query_key: "changed_by",
        member_name: "changed_by",
        column_name: "changed_by",
        load_index: 8,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_guarded_token: crate::GeneratedField = crate::GeneratedField {
        model_key: "guarded_token",
        query_key: "guarded_token",
        member_name: "guarded_token",
        column_name: "guarded_token",
        load_index: 9,
        reference: false,
    };
    #[allow(non_upper_case_globals)]
    pub(crate) const __TEAQL_FIELD_version: crate::GeneratedField = crate::GeneratedField {
        model_key: "version",
        query_key: "version",
        member_name: "version",
        column_name: "version",
        load_index: 10,
        reference: false,
    };

    pub const ENTITY_NAME: &'static str = "entry";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "7ae3f6f25be2681c105a55532a8c60a599eada522a7c70284e22e2172db13492";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        (Self::__TEAQL_FIELD_id.model_key, Self::__TEAQL_FIELD_id.load_index),
        (Self::__TEAQL_FIELD_catalog.model_key, Self::__TEAQL_FIELD_catalog.load_index),
        (Self::__TEAQL_FIELD_name.model_key, Self::__TEAQL_FIELD_name.load_index),
        (Self::__TEAQL_FIELD_born_at.model_key, Self::__TEAQL_FIELD_born_at.load_index),
        (Self::__TEAQL_FIELD_touched_at.model_key, Self::__TEAQL_FIELD_touched_at.load_index),
        (Self::__TEAQL_FIELD_source_ip.model_key, Self::__TEAQL_FIELD_source_ip.load_index),
        (Self::__TEAQL_FIELD_last_ip.model_key, Self::__TEAQL_FIELD_last_ip.load_index),
        (Self::__TEAQL_FIELD_opened_by.model_key, Self::__TEAQL_FIELD_opened_by.load_index),
        (Self::__TEAQL_FIELD_changed_by.model_key, Self::__TEAQL_FIELD_changed_by.load_index),
        (Self::__TEAQL_FIELD_guarded_token.model_key, Self::__TEAQL_FIELD_guarded_token.load_index),
        (Self::__TEAQL_FIELD_version.model_key, Self::__TEAQL_FIELD_version.load_index),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        (Self::__TEAQL_FIELD_id.model_key, Self::__TEAQL_FIELD_id.member_name, Self::__TEAQL_FIELD_id.column_name),
        (Self::__TEAQL_FIELD_catalog.model_key, Self::__TEAQL_FIELD_catalog.member_name, Self::__TEAQL_FIELD_catalog.column_name),
        (Self::__TEAQL_FIELD_name.model_key, Self::__TEAQL_FIELD_name.member_name, Self::__TEAQL_FIELD_name.column_name),
        (Self::__TEAQL_FIELD_born_at.model_key, Self::__TEAQL_FIELD_born_at.member_name, Self::__TEAQL_FIELD_born_at.column_name),
        (Self::__TEAQL_FIELD_touched_at.model_key, Self::__TEAQL_FIELD_touched_at.member_name, Self::__TEAQL_FIELD_touched_at.column_name),
        (Self::__TEAQL_FIELD_source_ip.model_key, Self::__TEAQL_FIELD_source_ip.member_name, Self::__TEAQL_FIELD_source_ip.column_name),
        (Self::__TEAQL_FIELD_last_ip.model_key, Self::__TEAQL_FIELD_last_ip.member_name, Self::__TEAQL_FIELD_last_ip.column_name),
        (Self::__TEAQL_FIELD_opened_by.model_key, Self::__TEAQL_FIELD_opened_by.member_name, Self::__TEAQL_FIELD_opened_by.column_name),
        (Self::__TEAQL_FIELD_changed_by.model_key, Self::__TEAQL_FIELD_changed_by.member_name, Self::__TEAQL_FIELD_changed_by.column_name),
        (Self::__TEAQL_FIELD_guarded_token.model_key, Self::__TEAQL_FIELD_guarded_token.member_name, Self::__TEAQL_FIELD_guarded_token.column_name),
        (Self::__TEAQL_FIELD_version.model_key, Self::__TEAQL_FIELD_version.member_name, Self::__TEAQL_FIELD_version.column_name),
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_id.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_id.query_key).expect("generated field layout");
        self
    }

    pub fn changed_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_id.model_key)
    }

    pub fn eval_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_id.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_name.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_name.query_key).expect("generated field layout");
        self
    }

    pub fn changed_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_name.model_key)
    }

    pub fn eval_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_name.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_born_at.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_born_at.query_key).expect("generated field layout");
        self
    }
    pub fn changed_born_at(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_born_at.model_key)
    }

    pub fn eval_born_at(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded(Self::__TEAQL_FIELD_born_at.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_touched_at.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_touched_at.query_key).expect("generated field layout");
        self
    }
    pub fn changed_touched_at(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_touched_at.model_key)
    }

    pub fn eval_touched_at(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded(Self::__TEAQL_FIELD_touched_at.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_source_ip.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_source_ip.query_key).expect("generated field layout");
        self
    }

    pub fn changed_source_ip(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_source_ip.model_key)
    }

    pub fn eval_source_ip(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_source_ip.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_last_ip.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_last_ip.query_key).expect("generated field layout");
        self
    }

    pub fn changed_last_ip(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_last_ip.model_key)
    }

    pub fn eval_last_ip(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_last_ip.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_opened_by.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_opened_by.query_key).expect("generated field layout");
        self
    }

    pub fn changed_opened_by(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_opened_by.model_key)
    }

    pub fn eval_opened_by(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_opened_by.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_changed_by.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_changed_by.query_key).expect("generated field layout");
        self
    }

    pub fn changed_changed_by(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_changed_by.model_key)
    }

    pub fn eval_changed_by(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_changed_by.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_guarded_token.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_guarded_token.query_key).expect("generated field layout");
        self
    }

    pub fn changed_guarded_token(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_guarded_token.model_key)
    }

    pub fn eval_guarded_token(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded(Self::__TEAQL_FIELD_guarded_token.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_version.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_version.query_key).expect("generated field layout");
        self
    }

    pub fn changed_version(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_version.model_key)
    }

    pub fn eval_version(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_version.query_key) {
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
        self.__teaql_runtime_state().set(self.entity_key(), Self::__TEAQL_FIELD_catalog.model_key, value);
        self.__load_state.mark_loaded(Self::__TEAQL_FIELD_catalog.query_key).expect("generated field layout");
        self
    }

    pub fn changed_catalog_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), Self::__TEAQL_FIELD_catalog.model_key)
    }

    pub fn eval_catalog_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded(Self::__TEAQL_FIELD_catalog.query_key) {
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

