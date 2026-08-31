// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/school
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
#[teaql(entity = "School", table = "school_data", data_service = "sqlite")]
pub struct School {
    #[teaql(id)]
    id: u64,

    // @source school-model.xml:40
    name: String,

    // @source school-model.xml:40
    address: String,

    // @source school-model.xml:40
    established_date: chrono::NaiveDate,

    // @source school-model.xml:40
    student_capacity: i64,

    // @source school-model.xml:40
    active: bool,

    // @source school-model.xml:40
    create_time: teaql_core::time::Timestamp,

    // @source school-model.xml:40
    update_time: teaql_core::time::Timestamp,
    #[teaql(version)]
    version: i64,
    // @source school-model.xml:40
    #[teaql(column = "platform")]
    platform_id: u64,

    // @source school-model.xml:40
    #[teaql(column = "school_type")]
    school_type_id: u64,
    // @source school-model.xml:40
    #[teaql(relation(target = "Platform", local_key = "platform_id", foreign_key = "id"))]
    platform: Option<Box<crate::Platform>>,

    // @source school-model.xml:40
    #[teaql(relation(
        target = "SchoolType",
        local_key = "school_type_id",
        foreign_key = "id"
    ))]
    school_type: Option<Box<crate::SchoolType>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl School {
    pub const ENTITY_NAME: &'static str = "School";

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            name: String::new(),
            address: String::new(),
            established_date: chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            student_capacity: 0_i64,
            active: false,
            create_time: teaql_core::time::Timestamp::now(),
            update_time: teaql_core::time::Timestamp::now(),
            version: 0_i64,
            platform_id: 0_u64,
            school_type_id: 0_u64,
            platform: None,
            school_type: None,
            dynamic: BTreeMap::new(),
            __teaql_runtime_state: root,
            __load_state: teaql_core::eval::LoadState::FullyLoaded,
        }
    }

    pub fn attach_runtime_state_recursive(&mut self, root: teaql_runtime::EntityRuntimeState) {
        root.adopt_mutations_from(self.__teaql_runtime_state());
        self.__teaql_replace_runtime_state(root.clone());
        if let Some(entity) = &mut self.platform {
            entity.attach_runtime_state_recursive(root.clone());
        }
        if let Some(entity) = &mut self.school_type {
            entity.attach_runtime_state_recursive(root.clone());
        }
    }

    pub fn is_loaded(&self, field_or_relation: &str) -> bool {
        self.__load_state.is_loaded(field_or_relation)
    }

    pub fn set_load_state(&mut self, state: teaql_core::eval::LoadState) {
        self.__load_state = state;
    }

    pub fn id(&self) -> u64 {
        self.changed_id()
            .and_then(|value| value.try_u64())
            .unwrap_or(self.id)
    }

    pub fn update_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.id = value.try_u64().unwrap_or(self.id.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "id", value);
        self
    }

    pub fn changed_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "id")
    }

    pub fn eval_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("id") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "id".to_string(),
                attempted_path: "id".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.id())
        }
    }

    pub fn name(&self) -> String {
        self.changed_name()
            .and_then(|value| value.try_text().map(|value| value.to_owned()))
            .unwrap_or_else(|| self.name.clone())
    }

    pub fn update_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.name = value
            .try_text()
            .map(|value| value.trim().to_owned())
            .unwrap_or_else(|| self.name.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "name", value);
        self
    }

    pub fn changed_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "name")
    }

    pub fn eval_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("name") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "name".to_string(),
                attempted_path: "name".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.name())
        }
    }

    pub fn address(&self) -> String {
        self.changed_address()
            .and_then(|value| value.try_text().map(|value| value.to_owned()))
            .unwrap_or_else(|| self.address.clone())
    }

    pub fn update_address(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.address = value
            .try_text()
            .map(|value| value.trim().to_owned())
            .unwrap_or_else(|| self.address.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "address", value);
        self
    }

    pub fn changed_address(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "address")
    }

    pub fn eval_address(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("address") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "address".to_string(),
                attempted_path: "address".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.address())
        }
    }

    pub fn established_date(&self) -> chrono::NaiveDate {
        self.changed_established_date()
            .and_then(|value| value.try_date())
            .unwrap_or(self.established_date)
    }

    pub fn update_established_date(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.established_date = value.try_date().unwrap_or(self.established_date.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "established_date", value);
        self
    }

    pub fn changed_established_date(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "established_date")
    }

    pub fn eval_established_date(&self) -> teaql_core::eval::EvalResult<chrono::NaiveDate> {
        if !self.is_loaded("established_date") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "established_date".to_string(),
                attempted_path: "established_date".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.established_date())
        }
    }

    pub fn student_capacity(&self) -> i64 {
        self.changed_student_capacity()
            .and_then(|value| value.try_i64())
            .map(|value| value as i64)
            .unwrap_or(self.student_capacity)
    }

    pub fn update_student_capacity(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.student_capacity = value
            .try_i64()
            .map(|value| value as i64)
            .unwrap_or(self.student_capacity.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "student_capacity", value);
        self
    }

    pub fn changed_student_capacity(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "student_capacity")
    }

    pub fn eval_student_capacity(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("student_capacity") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "student_capacity".to_string(),
                attempted_path: "student_capacity".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.student_capacity())
        }
    }

    pub fn active(&self) -> bool {
        self.changed_active()
            .and_then(|value| value.try_bool())
            .unwrap_or(self.active)
    }

    pub fn update_active(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.active = value.try_bool().unwrap_or(self.active.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "active", value);
        self
    }

    pub fn changed_active(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "active")
    }

    pub fn eval_active(&self) -> teaql_core::eval::EvalResult<bool> {
        if !self.is_loaded("active") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "active".to_string(),
                attempted_path: "active".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.active())
        }
    }

    pub fn create_time(&self) -> teaql_core::time::Timestamp {
        self.changed_create_time()
            .and_then(|value| value.try_timestamp())
            .unwrap_or(self.create_time)
    }

    pub fn update_create_time(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.create_time = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state()
            .set(self.entity_key(), "create_time", value);
        self
    }
    pub fn changed_create_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "create_time")
    }

    pub fn eval_create_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("create_time") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "create_time".to_string(),
                attempted_path: "create_time".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.create_time())
        }
    }

    pub fn update_time(&self) -> teaql_core::time::Timestamp {
        self.changed_update_time()
            .and_then(|value| value.try_timestamp())
            .unwrap_or(self.update_time)
    }

    pub fn update_update_time(&mut self, value: teaql_core::time::Timestamp) -> &mut Self {
        self.update_time = value;
        let value = teaql_core::Value::from(value);
        self.__teaql_runtime_state()
            .set(self.entity_key(), "update_time", value);
        self
    }
    pub fn changed_update_time(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "update_time")
    }

    pub fn eval_update_time(&self) -> teaql_core::eval::EvalResult<teaql_core::time::Timestamp> {
        if !self.is_loaded("update_time") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "update_time".to_string(),
                attempted_path: "update_time".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.update_time())
        }
    }

    pub fn version(&self) -> i64 {
        self.changed_version()
            .and_then(|value| value.try_i64())
            .unwrap_or(self.version)
    }

    pub fn update_version(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.version = value.try_i64().unwrap_or(self.version.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "version", value);
        self
    }

    pub fn changed_version(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "version")
    }

    pub fn eval_version(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("version") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "version".to_string(),
                attempted_path: "version".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.version())
        }
    }
    pub fn platform_id(&self) -> u64 {
        self.changed_platform_id()
            .and_then(|value| value.try_u64())
            .unwrap_or(self.platform_id)
    }

    pub fn update_platform_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.platform_id = value.try_u64().unwrap_or(self.platform_id.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "platform_id", value);
        self
    }

    pub fn changed_platform_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "platform_id")
    }

    pub fn eval_platform_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("platform_id") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "platform_id".to_string(),
                attempted_path: "platform_id".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.platform_id())
        }
    }

    pub fn school_type_id(&self) -> u64 {
        self.changed_school_type_id()
            .and_then(|value| value.try_u64())
            .unwrap_or(self.school_type_id)
    }

    pub(crate) fn update_school_type_id(
        &mut self,
        value: impl Into<teaql_core::Value>,
    ) -> &mut Self {
        let value = value.into();
        self.school_type_id = value.try_u64().unwrap_or(self.school_type_id.clone());
        self.__teaql_runtime_state()
            .set(self.entity_key(), "school_type_id", value);
        self
    }

    pub fn changed_school_type_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state()
            .get(&self.entity_key(), "school_type_id")
    }

    pub fn eval_school_type_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("school_type_id") {
            teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "school_type_id".to_string(),
                attempted_path: "school_type_id".to_string(),
            }
        } else {
            teaql_core::eval::EvalResult::Value(self.school_type_id())
        }
    }
    pub fn update_school_type_to_primary(&mut self) -> &mut Self {
        self.update_school_type_id(1001_u64)
    }

    pub fn school_type_is_primary(&self) -> bool {
        self.school_type_id() == 1001_u64
    }
    pub fn update_school_type_to_secondary(&mut self) -> &mut Self {
        self.update_school_type_id(1002_u64)
    }

    pub fn school_type_is_secondary(&self) -> bool {
        self.school_type_id() == 1002_u64
    }
    pub fn platform(&self) -> Option<&crate::Platform> {
        self.platform.as_deref().or_else(|| {
            self.__teaql_runtime_state()
                .resolve_entity(self.platform_id())
        })
    }

    pub fn eval_platform(&self) -> teaql_core::eval::EvalResult<&crate::Platform> {
        match self.platform() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None if self.is_loaded("platform") => teaql_core::eval::EvalResult::Null,
            None => teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "platform".to_string(),
                attempted_path: "platform".to_string(),
            },
        }
    }

    pub fn school_type(&self) -> Option<&crate::SchoolType> {
        self.school_type.as_deref().or_else(|| {
            self.__teaql_runtime_state()
                .resolve_entity(self.school_type_id())
        })
    }

    pub fn eval_school_type(&self) -> teaql_core::eval::EvalResult<&crate::SchoolType> {
        match self.school_type() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None if self.is_loaded("school_type") => teaql_core::eval::EvalResult::Null,
            None => teaql_core::eval::EvalResult::NotLoaded {
                failed_node: "school_type".to_string(),
                attempted_path: "school_type".to_string(),
            },
        }
    }
}
