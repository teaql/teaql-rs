use serde::{Deserialize, Serialize};

use crate::{BaseEntity, BaseEntityData, Entity, Record, SmartList, Value, record_to_json_value};

pub const STYLE_KEY: &str = "style";
pub const ACTION_LIST_KEY: &str = "actionList";
pub const WEB_RESPONSE_VERSION: &str = "1.001";

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebStyle {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background_color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_names: Option<String>,
}

impl WebStyle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_background_color(color: impl Into<String>) -> Self {
        Self::new().background_color(color)
    }

    pub fn with_font_color(color: impl Into<String>) -> Self {
        Self::new().font_color(color)
    }

    pub fn with_class_names(class_names: impl Into<String>) -> Self {
        Self::new().class_names(class_names)
    }

    pub fn background_color(mut self, color: impl Into<String>) -> Self {
        self.background_color = Some(color.into());
        self
    }

    pub fn font_color(mut self, color: impl Into<String>) -> Self {
        self.color = Some(color.into());
        self
    }

    pub fn class_names(mut self, class_names: impl Into<String>) -> Self {
        self.class_names = Some(class_names.into());
        self
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("WebStyle serialization cannot fail")
    }

    pub fn bind_base(&self, entity: &mut BaseEntityData) {
        entity.put_dynamic(STYLE_KEY, self.to_json_value());
    }

    pub fn bind_entity<E>(&self, entity: &mut E)
    where
        E: BaseEntity,
    {
        entity.put_dynamic(STYLE_KEY, self.to_json_value());
    }

    pub fn bind_record(&self, record: &mut Record) {
        record.insert(STYLE_KEY.to_owned(), Value::Json(self.to_json_value()));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebAction {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execute: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_for_list: Option<String>,
    #[serde(rename = "requestURL", skip_serializing_if = "Option::is_none")]
    pub request_url: Option<String>,
}

impl WebAction {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn view_web_action() -> Self {
        Self::new()
            .name("VIEW DETAIL")
            .level("view")
            .execute("switchview")
            .target("detail")
    }

    pub fn view_sub_list_action(
        name: impl Into<String>,
        list_view_name: impl Into<String>,
        role_for_list: impl Into<String>,
    ) -> Self {
        Self::new()
            .name(name)
            .level("view")
            .execute("gotoList")
            .role_for_list(role_for_list)
            .target(list_view_name)
    }

    pub fn simple_component_action(
        name: impl Into<String>,
        component_name: impl Into<String>,
    ) -> Self {
        Self::new().name(name).component(component_name)
    }

    pub fn modify_web_action(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self::modify_web_action_with_warning(name, url, None::<String>)
    }

    pub fn modify_web_action_with_warning(
        name: impl Into<String>,
        url: impl Into<String>,
        warning_message: Option<impl Into<String>>,
    ) -> Self {
        let name = name.into();
        Self::new()
            .name(name.clone())
            .key(name)
            .level("modify")
            .execute("switchview")
            .target("modify")
            .request_url(url)
            .optional_warning_message(warning_message)
    }

    pub fn default_modify_web_action() -> Self {
        Self::new()
            .name("UPDATE")
            .level("modify")
            .execute("switchview")
            .target("modify")
    }

    pub fn delete_web_action() -> Self {
        Self::new()
            .name("DELETE")
            .level("delete")
            .execute("switchview")
            .target("deleteview")
    }

    pub fn delete_web_action_with_warning(
        url: impl Into<String>,
        warning_message: impl Into<String>,
    ) -> Self {
        Self::modify_web_action_with_warning("web.action.delete", url, Some(warning_message.into()))
    }

    pub fn audit_web_action(url: impl Into<String>, warning_message: impl Into<String>) -> Self {
        Self::modify_web_action_with_warning("AUDIT", url, Some(warning_message.into()))
    }

    pub fn discard_web_action(url: impl Into<String>, warning_message: impl Into<String>) -> Self {
        Self::modify_web_action_with_warning("DISCARD", url, Some(warning_message.into()))
    }

    pub fn goto_action(
        name: impl Into<String>,
        target: impl Into<String>,
        url: impl Into<String>,
    ) -> Self {
        Self::new()
            .name(name)
            .level("modify")
            .execute("gotoview")
            .target(target)
            .request_url(url)
    }

    pub fn switch_view_action(view_name: impl Into<String>, target: impl Into<String>) -> Self {
        Self::new()
            .name(view_name)
            .level("modify")
            .execute("switchview")
            .target(target)
    }

    pub fn add_new_web_action(object_display_name: impl Into<String>) -> Self {
        Self::new()
            .name(format!("NEW {}", object_display_name.into()))
            .level("modify")
            .execute("switchview")
            .target("addnew")
    }

    pub fn batch_upload_web_action() -> Self {
        Self::new()
            .name("BATCH UPLOAD")
            .level("modify")
            .execute("switchview")
            .target("batchupload")
    }

    pub fn common_web_actions() -> Vec<Self> {
        vec![Self::view_web_action(), Self::default_modify_web_action()]
    }

    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn level(mut self, level: impl Into<String>) -> Self {
        self.level = Some(level.into());
        self
    }

    pub fn execute(mut self, execute: impl Into<String>) -> Self {
        self.execute = Some(execute.into());
        self
    }

    pub fn target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(target.into());
        self
    }

    pub fn component(mut self, component: impl Into<String>) -> Self {
        self.component = Some(component.into());
        self
    }

    pub fn warning_message(mut self, warning_message: impl Into<String>) -> Self {
        self.warning_message = Some(warning_message.into());
        self
    }

    pub fn optional_warning_message(mut self, warning_message: Option<impl Into<String>>) -> Self {
        self.warning_message = warning_message.map(Into::into);
        self
    }

    pub fn role_for_list(mut self, role_for_list: impl Into<String>) -> Self {
        self.role_for_list = Some(role_for_list.into());
        self
    }

    pub fn request_url(mut self, request_url: impl Into<String>) -> Self {
        self.request_url = Some(request_url.into());
        self
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("WebAction serialization cannot fail")
    }

    pub fn bind_base(&self, entity: &mut BaseEntityData) {
        append_action(&mut entity.dynamic, self.to_json_value());
    }

    pub fn bind_entity<E>(&self, entity: &mut E)
    where
        E: BaseEntity,
    {
        append_action(&mut entity.base_mut().dynamic, self.to_json_value());
    }

    pub fn bind_record(&self, record: &mut Record) {
        append_record_action(record, self.to_json_value());
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebResponse {
    pub data: Vec<serde_json::Value>,
    pub result_code: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub record_count: u64,
    pub version: String,
}

impl WebResponse {
    pub fn success() -> Self {
        Self {
            data: Vec::new(),
            result_code: 0,
            status: Some("YES".to_owned()),
            message: None,
            record_count: 0,
            version: WEB_RESPONSE_VERSION.to_owned(),
        }
    }

    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            data: Vec::new(),
            result_code: 1,
            status: Some("NO".to_owned()),
            message: Some(message.into()),
            record_count: 0,
            version: WEB_RESPONSE_VERSION.to_owned(),
        }
    }

    pub fn empty_list(message: impl Into<String>) -> Self {
        Self {
            data: Vec::new(),
            result_code: 0,
            status: None,
            message: Some(message.into()),
            record_count: 0,
            version: WEB_RESPONSE_VERSION.to_owned(),
        }
    }

    pub fn from_records(records: impl IntoIterator<Item = Record>) -> Self {
        let data: Vec<_> = records
            .into_iter()
            .map(|record| record_to_json_value(&record))
            .collect();
        Self::success().with_data(data)
    }

    pub fn from_entity<E>(entity: &E) -> Self
    where
        E: Entity + Clone,
    {
        Self::from_records([entity.clone().into_record()])
    }

    pub fn from_entities<E>(entities: impl IntoIterator<Item = E>) -> Self
    where
        E: Entity,
    {
        Self::from_records(entities.into_iter().map(Entity::into_record))
    }

    pub fn from_smart_list<E>(smart_list: SmartList<E>) -> Self
    where
        E: Entity,
    {
        let total_count = smart_list.total_count_or_len();
        Self::from_entities(smart_list).with_record_count(total_count)
    }

    pub fn with_data(mut self, data: Vec<serde_json::Value>) -> Self {
        self.record_count = data.len() as u64;
        self.data = data;
        self
    }

    pub fn with_record_count(mut self, record_count: u64) -> Self {
        self.record_count = record_count;
        self
    }

    pub fn push_json(mut self, value: impl Into<serde_json::Value>) -> Self {
        self.data.push(value.into());
        self.record_count = self.data.len() as u64;
        self
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("WebResponse serialization cannot fail")
    }
}

fn append_action(
    dynamic: &mut std::collections::BTreeMap<String, serde_json::Value>,
    action: serde_json::Value,
) {
    match dynamic.get_mut(ACTION_LIST_KEY) {
        Some(serde_json::Value::Array(actions)) => actions.push(action),
        Some(existing) => {
            let previous = std::mem::take(existing);
            *existing = serde_json::Value::Array(vec![previous, action]);
        }
        None => {
            dynamic.insert(
                ACTION_LIST_KEY.to_owned(),
                serde_json::Value::Array(vec![action]),
            );
        }
    }
}

fn append_record_action(record: &mut Record, action: serde_json::Value) {
    match record.get_mut(ACTION_LIST_KEY) {
        Some(Value::Json(serde_json::Value::Array(actions))) => actions.push(action),
        Some(existing) => {
            let previous = std::mem::replace(existing, Value::Null);
            *existing = Value::Json(serde_json::Value::Array(vec![
                previous.to_json_value(),
                action,
            ]));
        }
        None => {
            record.insert(
                ACTION_LIST_KEY.to_owned(),
                Value::Json(serde_json::Value::Array(vec![action])),
            );
        }
    }
}
