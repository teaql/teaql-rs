
// ⛔ AI agents: DO NOT read this file for API discovery. Instead run: cargo teaql --input modeling/MODEL.xml rust-assist-query/order_attachment
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
#[teaql(entity = "OrderAttachment", table = "order_attachment_data", data_service = "sqlite", audit_mask_fields = "object_key")]
pub struct OrderAttachment {
#[teaql(id)]
    id: u64,

// @source model.xml:7
#[teaql(max_length = 100)]
    file_name: String,

// @source model.xml:7
#[teaql(max_length = 100)]
    content_type: String,

// @source model.xml:7
#[teaql(integer32)]
    content_length: i64,

// @source model.xml:7
#[teaql(max_length = 100)]
    content_sha256: String,

// @source model.xml:7
#[teaql(max_length = 100)]
    object_key: String,
#[teaql(version)]
    version: i64,
// @source model.xml:7
#[teaql(column = "customer_order")]
    customer_order_id: u64,
// @source model.xml:7
#[teaql(relation(target = "CustomerOrder", local_key = "customer_order_id", foreign_key = "id"))]
    customer_order: Option<Box<crate::CustomerOrder>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, teaql_core::Value>,
    #[teaql(skip)]
    pub __load_state: teaql_core::eval::LoadState,
}

impl OrderAttachment {
    pub const ENTITY_NAME: &'static str = "order_attachment";

    // Framework metadata: complete fixed layout, including overflow slots.
    pub const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "1b7e0fc32cc094c60c46058779d5933a02527a724ffa64e3e722a9670f3883aa";
    pub const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("customer_order", 1),
        ("file_name", 2),
        ("content_type", 3),
        ("content_length", 4),
        ("content_sha256", 5),
        ("object_key", 6),
        ("version", 7),
    ];
    pub const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("customer_order", "customer_order_id", "customer_order"),
        ("file_name", "file_name", "file_name"),
        ("content_type", "content_type", "content_type"),
        ("content_length", "content_length", "content_length"),
        ("content_sha256", "content_sha256", "content_sha256"),
        ("object_key", "object_key", "object_key"),
        ("version", "version", "version"),
    ];

    pub fn with_id(id: u64) -> teaql_core::Value {
        teaql_core::Value::U64(id)
    }

    pub(crate) fn runtime_new(root: teaql_runtime::EntityRuntimeState) -> Self {
        Self {
            id: 0_u64,
            file_name: String::new(),
            content_type: String::new(),
            content_length: 0_i64,
            content_sha256: String::new(),
            object_key: String::new(),
            version: 0_i64,
            customer_order_id: 0_u64,
            customer_order: None,
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
        if let Some(entity) = &mut self.customer_order {
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


    pub fn file_name(&self) -> String {
        self.changed_file_name().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.file_name.clone())
    }

    pub fn update_file_name(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.file_name = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.file_name.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "file_name", value);
        self.__load_state.mark_loaded("file_name").expect("generated field layout");
        self
    }

    pub fn changed_file_name(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "file_name")
    }

    pub fn eval_file_name(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("file_name") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "file_name".to_string(), attempted_path: "file_name".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.file_name())
                }}


    pub fn content_type(&self) -> String {
        self.changed_content_type().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.content_type.clone())
    }

    pub fn update_content_type(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.content_type = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.content_type.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "content_type", value);
        self.__load_state.mark_loaded("content_type").expect("generated field layout");
        self
    }

    pub fn changed_content_type(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "content_type")
    }

    pub fn eval_content_type(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("content_type") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "content_type".to_string(), attempted_path: "content_type".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.content_type())
                }}


    pub fn content_length(&self) -> i64 {
        self.changed_content_length().and_then(|value| value.try_i64()).map(|value| value as i64).unwrap_or(self.content_length)
    }

    pub fn update_content_length(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.content_length = value.try_i64().map(|value| value as i64).unwrap_or(self.content_length.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "content_length", value);
        self.__load_state.mark_loaded("content_length").expect("generated field layout");
        self
    }

    pub fn changed_content_length(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "content_length")
    }

    pub fn eval_content_length(&self) -> teaql_core::eval::EvalResult<i64> {
        if !self.is_loaded("content_length") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "content_length".to_string(), attempted_path: "content_length".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.content_length())
                }}


    pub fn content_sha256(&self) -> String {
        self.changed_content_sha256().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.content_sha256.clone())
    }

    pub fn update_content_sha256(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.content_sha256 = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.content_sha256.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "content_sha256", value);
        self.__load_state.mark_loaded("content_sha256").expect("generated field layout");
        self
    }

    pub fn changed_content_sha256(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "content_sha256")
    }

    pub fn eval_content_sha256(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("content_sha256") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "content_sha256".to_string(), attempted_path: "content_sha256".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.content_sha256())
                }}


    pub fn object_key(&self) -> String {
        self.changed_object_key().and_then(|value| value.try_text().map(|value| value.to_owned())).unwrap_or_else(|| self.object_key.clone())
    }

    pub fn update_object_key(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.object_key = value.try_text().map(|value| value.trim().to_owned()).unwrap_or_else(|| self.object_key.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "object_key", value);
        self.__load_state.mark_loaded("object_key").expect("generated field layout");
        self
    }

    pub fn changed_object_key(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "object_key")
    }

    pub fn eval_object_key(&self) -> teaql_core::eval::EvalResult<String> {
        if !self.is_loaded("object_key") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "object_key".to_string(), attempted_path: "object_key".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.object_key())
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

    pub fn customer_order_id(&self) -> u64 {
        self.changed_customer_order_id().and_then(|value| value.try_u64()).unwrap_or(self.customer_order_id)
    }

    pub fn update_customer_order_id(&mut self, value: impl Into<teaql_core::Value>) -> &mut Self {
        let value = value.into();
        self.customer_order_id = value.try_u64().unwrap_or(self.customer_order_id.clone());
        self.__teaql_runtime_state().set(self.entity_key(), "customer_order", value);
        self.__load_state.mark_loaded("customer_order_id").expect("generated field layout");
        self
    }

    pub fn changed_customer_order_id(&self) -> Option<teaql_core::Value> {
        self.__teaql_runtime_state().get(&self.entity_key(), "customer_order")
    }

    pub fn eval_customer_order_id(&self) -> teaql_core::eval::EvalResult<u64> {
        if !self.is_loaded("customer_order_id") {
                    teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order_id".to_string(), attempted_path: "customer_order_id".to_string() }
                } else {
                    teaql_core::eval::EvalResult::Value(self.customer_order_id())
                }}
    pub fn customer_order(&self) -> Option<&crate::CustomerOrder> {
        if self.changed_customer_order_id().is_some()
            || !self.is_loaded("customer_order") {
            return None;
        }
        if let Some(edge) = self.__teaql_runtime_state().resolve_relation_option::<crate::CustomerOrder>(
            <Self as teaql_core::TeaqlEntity>::ENTITY_NAME, self.id(), "customer_order",
        ) {
            return edge.as_ref();
        }
        self.customer_order.as_deref().or_else(|| {
            self.__teaql_runtime_state().resolve_entity(self.customer_order_id())})
    }

    pub fn eval_customer_order(&self) -> teaql_core::eval::EvalResult<&crate::CustomerOrder> {
        match self.customer_order() {
            Some(v) => teaql_core::eval::EvalResult::Value(v),
            None => teaql_core::eval::EvalResult::NotLoaded { failed_node: "customer_order".to_string(), attempted_path: "customer_order".to_string() },
        }
    }

}

