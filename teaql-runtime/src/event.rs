use std::sync::Arc;

use teaql_core::{Record, Value};

use crate::{RuntimeError, UserContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawAuditEventKind {
    Created,
    Updated,
    Deleted,
    Recovered,
    /// Emitted when a new table is created during schema bootstrap.
    SchemaCreated,
    /// Emitted when an existing table is verified during schema bootstrap.
    SchemaVerified,
    /// Emitted when a new column is added to an existing table (schema migration).
    FieldAdded,
    /// Emitted when initial seed data is inserted or updated during bootstrap.
    DataSeeded,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntityPropertyChange {
    pub field: String,
    pub old_value: Option<Value>,
    pub new_value: Option<Value>,
}

impl EntityPropertyChange {
    pub fn new(
        field: impl Into<String>,
        old_value: Option<Value>,
        new_value: Option<Value>,
    ) -> Self {
        Self {
            field: field.into(),
            old_value,
            new_value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawAuditEvent {
    pub kind: RawAuditEventKind,
    pub entity: String,
    pub values: Record,
    pub updated_fields: Vec<String>,
    pub old_values: Option<Record>,
    pub new_values: Option<Record>,
    pub changes: Vec<EntityPropertyChange>,
    /// Annotation trace chain from the graph save scope chain.
    pub trace_chain: Vec<teaql_core::TraceNode>,
}

impl RawAuditEvent {
    pub fn created(entity: impl Into<String>, values: Record) -> Self {
        let changes = values
            .iter()
            .map(|(field, value)| {
                EntityPropertyChange::new(field.clone(), None, Some(value.clone()))
            })
            .collect();
        Self {
            kind: RawAuditEventKind::Created,
            entity: entity.into(),
            values: values.clone(),
            updated_fields: Vec::new(),
            old_values: None,
            new_values: Some(values),
            changes,
            trace_chain: Vec::new(),
        }
    }

    pub fn updated(entity: impl Into<String>, values: Record) -> Self {
        let updated_fields = values.keys().cloned().collect::<Vec<_>>();
        let changes = Self::changes_for_fields(None, Some(&values), &updated_fields);
        Self {
            kind: RawAuditEventKind::Updated,
            entity: entity.into(),
            values: values.clone(),
            updated_fields,
            old_values: None,
            new_values: Some(values),
            changes,
            trace_chain: Vec::new(),
        }
    }

    pub fn updated_with_old_values(
        entity: impl Into<String>,
        values: Record,
        old_values: Option<Record>,
        new_values: Record,
        updated_fields: Vec<String>,
    ) -> Self {
        let changes =
            Self::changes_for_fields(old_values.as_ref(), Some(&new_values), &updated_fields);
        Self {
            kind: RawAuditEventKind::Updated,
            entity: entity.into(),
            values,
            updated_fields,
            old_values,
            new_values: Some(new_values),
            changes,
            trace_chain: Vec::new(),
        }
    }

    pub fn deleted(entity: impl Into<String>, id: Value, expected_version: Option<i64>) -> Self {
        let mut values = Record::from([("id".to_owned(), id)]);
        if let Some(version) = expected_version {
            values.insert("version".to_owned(), Value::I64(version));
        }
        Self {
            kind: RawAuditEventKind::Deleted,
            entity: entity.into(),
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
            trace_chain: Vec::new(),
        }
    }

    pub fn deleted_with_old_values(
        entity: impl Into<String>,
        id: Value,
        expected_version: Option<i64>,
        old_values: Option<Record>,
    ) -> Self {
        let mut event = Self::deleted(entity, id, expected_version);
        event.changes = old_values
            .as_ref()
            .map(|values| {
                values
                    .iter()
                    .map(|(field, value)| {
                        EntityPropertyChange::new(field.clone(), Some(value.clone()), None)
                    })
                    .collect()
            })
            .unwrap_or_default();
        event.old_values = old_values;
        event
    }

    pub fn recovered(entity: impl Into<String>, id: Value, expected_version: i64) -> Self {
        let values = Record::from([
            ("id".to_owned(), id),
            ("version".to_owned(), Value::I64(expected_version)),
        ]);
        Self {
            kind: RawAuditEventKind::Recovered,
            entity: entity.into(),
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
            trace_chain: Vec::new(),
        }
    }

    pub fn recovered_with_old_values(
        entity: impl Into<String>,
        id: Value,
        expected_version: i64,
        old_values: Option<Record>,
    ) -> Self {
        let recovered_version = -expected_version + 1;
        let mut new_values = old_values.clone().unwrap_or_default();
        new_values.insert("id".to_owned(), id.clone());
        new_values.insert("version".to_owned(), Value::I64(recovered_version));
        let mut event = Self::recovered(entity, id, expected_version);
        event.old_values = old_values;
        event.new_values = Some(new_values.clone());
        event.changes = Self::changes_for_fields(
            event.old_values.as_ref(),
            Some(&new_values),
            &["version".to_owned()],
        );
        event
    }

    /// A new table was created during schema bootstrap.
    pub fn schema_created(
        entity: impl Into<String>,
        table_name: impl Into<String>,
        field_count: usize,
    ) -> Self {
        let entity = entity.into();
        let values = Record::from([
            ("table_name".to_owned(), Value::Text(table_name.into())),
            ("field_count".to_owned(), Value::I64(field_count as i64)),
        ]);
        let changes = values
            .iter()
            .map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone())))
            .collect();
        Self {
            kind: RawAuditEventKind::SchemaCreated,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes,
            trace_chain: Vec::new(),
        }
    }

    /// An existing table was verified during schema bootstrap.
    pub fn schema_verified(
        entity: impl Into<String>,
        table_name: impl Into<String>,
        field_count: usize,
    ) -> Self {
        let entity = entity.into();
        let values = Record::from([
            ("table_name".to_owned(), Value::Text(table_name.into())),
            ("field_count".to_owned(), Value::I64(field_count as i64)),
        ]);
        let changes = values
            .iter()
            .map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone())))
            .collect();
        Self {
            kind: RawAuditEventKind::SchemaVerified,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes,
            trace_chain: Vec::new(),
        }
    }

    /// A new column was added to an existing table (schema migration).
    pub fn field_added(
        entity: impl Into<String>,
        table_name: impl Into<String>,
        field_name: impl Into<String>,
    ) -> Self {
        let entity = entity.into();
        let values = Record::from([
            ("table_name".to_owned(), Value::Text(table_name.into())),
            ("field_name".to_owned(), Value::Text(field_name.into())),
        ]);
        let changes = values
            .iter()
            .map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone())))
            .collect();
        Self {
            kind: RawAuditEventKind::FieldAdded,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes,
            trace_chain: Vec::new(),
        }
    }

    /// Initial seed data was inserted or updated during bootstrap.
    pub fn data_seeded(
        entity: impl Into<String>,
        table_name: impl Into<String>,
        inserted: usize,
        updated: usize,
    ) -> Self {
        let entity = entity.into();
        let values = Record::from([
            ("table_name".to_owned(), Value::Text(table_name.into())),
            ("inserted".to_owned(), Value::I64(inserted as i64)),
            ("updated".to_owned(), Value::I64(updated as i64)),
        ]);
        let changes = values
            .iter()
            .map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone())))
            .collect();
        Self {
            kind: RawAuditEventKind::DataSeeded,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes,
            trace_chain: Vec::new(),
        }
    }

    fn changes_for_fields(
        old_values: Option<&Record>,
        new_values: Option<&Record>,
        fields: &[String],
    ) -> Vec<EntityPropertyChange> {
        fields
            .iter()
            .map(|field| {
                EntityPropertyChange::new(
                    field.clone(),
                    old_values.and_then(|values| values.get(field).cloned()),
                    new_values.and_then(|values| values.get(field).cloned()),
                )
            })
            .collect()
    }

    pub fn build_safe_event(
        &self,
        audit_mask_fields: &[String],
        audit_value_max_len: Option<usize>,
    ) -> SafeAuditEvent {
        let mut safe_fields = Vec::new();
        for change in &self.changes {
            if change.field.starts_with('_') {
                continue;
            }
            // For audit, if it's masked or we just want the new/old values, we should represent it stringified.
            // Usually we care about the new value in SafeAuditEvent. Or maybe we want to represent the change.
            // Based on design doc, we stringify the value and apply masks.
            let raw_val_str = change.new_value.as_ref().map(|v| format!("{:?}", v));
            let safe_field = build_safe_audit_field(
                &change.field,
                raw_val_str.as_deref(),
                audit_mask_fields,
                audit_value_max_len,
            );
            safe_fields.push(safe_field);
        }

        SafeAuditEvent {
            kind: self.kind,
            entity: self.entity.clone(),
            fields: safe_fields,
            trace_chain: self.trace_chain.clone(),
        }
    }
}

pub fn mask_audit_value(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let len = chars.len();

    if len == 0 {
        return String::new();
    }

    if chars.iter().all(|c| c.is_ascii_digit()) {
        return "*".repeat(len);
    }

    if len < 8 {
        return "*".repeat(len);
    }

    let prefix: String = chars[0..2].iter().collect();
    let suffix: String = chars[len - 2..len].iter().collect();
    let middle = "*".repeat(len - 4);

    format!("{}{}{}", prefix, middle, suffix)
}

pub fn limit_audit_value(value: &str, max_len: usize) -> (String, bool) {
    let chars: Vec<char> = value.chars().collect();
    let len = chars.len();

    if len <= max_len {
        return (value.to_string(), false);
    }

    if max_len <= 3 {
        return ("*".repeat(max_len), true);
    }

    let marker = "...";
    let keep_len = max_len - marker.len();
    let head_len = keep_len / 2;
    let tail_len = keep_len - head_len;

    let head: String = chars[0..head_len].iter().collect();
    let tail: String = chars[len - tail_len..len].iter().collect();

    (format!("{}{}{}", head, marker, tail), true)
}

pub fn build_safe_audit_field(
    field_name: &str,
    raw_value: Option<&str>,
    audit_mask_fields: &[String],
    audit_value_max_len: Option<usize>,
) -> SafeAuditField {
    match raw_value {
        None => SafeAuditField {
            name: field_name.to_string(),
            value: None,
            masked: false,
            truncated: false,
            raw_length: None,
            output_length: None,
            mask_reason: None,
            truncate_reason: None,
        },
        Some(raw) => {
            let raw_length = raw.chars().count();
            let should_mask = audit_mask_fields.iter().any(|f| f == field_name);

            let mut value = match should_mask {
                true => mask_audit_value(raw),
                false => raw.to_string(),
            };

            let mut truncated = false;
            if let Some(max_len) = audit_value_max_len {
                let result = limit_audit_value(&value, max_len);
                value = result.0;
                truncated = result.1;
            }

            let output_length = value.chars().count();

            SafeAuditField {
                name: field_name.to_string(),
                value: Some(value),
                masked: should_mask,
                truncated,
                raw_length: Some(raw_length),
                output_length: Some(output_length),
                mask_reason: should_mask.then(|| "_audit_mask_fields".to_string()),
                truncate_reason: truncated.then(|| "_audit_value_max_len".to_string()),
            }
        }
    }
}

pub trait RawAuditEventSink: Send + Sync {
    fn on_event(&self, ctx: &UserContext, event: &RawAuditEvent) -> Result<(), RuntimeError>;
}

#[derive(Default, Clone)]
pub struct InMemoryRawAuditEventSink {
    sinks: Vec<Arc<dyn RawAuditEventSink>>,
}

impl InMemoryRawAuditEventSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, sink: impl RawAuditEventSink + 'static) {
        self.sinks.push(Arc::new(sink));
    }

    pub fn with_sink(mut self, sink: impl RawAuditEventSink + 'static) -> Self {
        self.register(sink);
        self
    }
}

impl RawAuditEventSink for InMemoryRawAuditEventSink {
    fn on_event(&self, ctx: &UserContext, event: &RawAuditEvent) -> Result<(), RuntimeError> {
        for sink in &self.sinks {
            sink.on_event(ctx, event)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SafeAuditField {
    pub name: String,
    pub value: Option<String>,
    pub masked: bool,
    pub truncated: bool,
    pub raw_length: Option<usize>,
    pub output_length: Option<usize>,
    pub mask_reason: Option<String>,
    pub truncate_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SafeAuditEvent {
    pub kind: RawAuditEventKind,
    pub entity: String,
    pub fields: Vec<SafeAuditField>,
    pub trace_chain: Vec<teaql_core::TraceNode>,
}

pub trait SafeAuditEventSink: Send + Sync {
    fn on_safe_event(
        &self,
        ctx: &crate::UserContext,
        event: &SafeAuditEvent,
    ) -> Result<(), crate::RuntimeError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RuntimeError, UserContext};
    use teaql_core::{Record, Value};

    #[test]
    fn test_entity_property_change() {
        let change = EntityPropertyChange::new("field1", None, Some(Value::I64(1)));
        assert_eq!(change.field, "field1");
        assert_eq!(change.old_value, None);
        assert_eq!(change.new_value, Some(Value::I64(1)));
    }

    #[test]
    fn test_raw_audit_event_created() {
        let mut values = Record::new();
        values.insert("a".to_owned(), Value::I64(1));
        let event = RawAuditEvent::created("User", values.clone());
        assert_eq!(event.kind, RawAuditEventKind::Created);
        assert_eq!(event.entity, "User");
        assert_eq!(event.values, values);
        assert_eq!(event.updated_fields.len(), 0);
        assert_eq!(event.old_values, None);
        assert_eq!(event.new_values, Some(values.clone()));
        assert_eq!(event.changes.len(), 1);
        assert_eq!(event.changes[0].field, "a");
        assert_eq!(event.changes[0].old_value, None);
        assert_eq!(event.changes[0].new_value, Some(Value::I64(1)));
    }

    #[test]
    fn test_raw_audit_event_updated() {
        let mut values = Record::new();
        values.insert("a".to_owned(), Value::I64(2));
        let event = RawAuditEvent::updated("User", values.clone());
        assert_eq!(event.kind, RawAuditEventKind::Updated);
        assert_eq!(event.entity, "User");
        assert_eq!(event.values, values);
        assert_eq!(event.updated_fields, vec!["a".to_owned()]);
        assert_eq!(event.old_values, None);
        assert_eq!(event.new_values, Some(values.clone()));
        assert_eq!(event.changes.len(), 1);
    }

    #[test]
    fn test_raw_audit_event_updated_with_old_values() {
        let mut values = Record::new();
        values.insert("a".to_owned(), Value::I64(2));
        let mut old_values = Record::new();
        old_values.insert("a".to_owned(), Value::I64(1));
        let mut new_values = Record::new();
        new_values.insert("a".to_owned(), Value::I64(2));

        let event = RawAuditEvent::updated_with_old_values(
            "User",
            values.clone(),
            Some(old_values),
            new_values.clone(),
            vec!["a".to_owned()],
        );
        assert_eq!(event.kind, RawAuditEventKind::Updated);
        assert_eq!(event.changes.len(), 1);
        assert_eq!(event.changes[0].old_value, Some(Value::I64(1)));
        assert_eq!(event.changes[0].new_value, Some(Value::I64(2)));
    }

    #[test]
    fn test_raw_audit_event_deleted() {
        let event = RawAuditEvent::deleted("User", Value::I64(10), Some(1));
        assert_eq!(event.kind, RawAuditEventKind::Deleted);
        assert_eq!(event.values.get("id"), Some(&Value::I64(10)));
        assert_eq!(event.values.get("version"), Some(&Value::I64(1)));
    }

    #[test]
    fn test_raw_audit_event_deleted_with_old_values() {
        let mut old_values = Record::new();
        old_values.insert("name".to_owned(), Value::Text("Alice".to_owned()));
        let event =
            RawAuditEvent::deleted_with_old_values("User", Value::I64(10), None, Some(old_values));
        assert_eq!(event.kind, RawAuditEventKind::Deleted);
        assert_eq!(event.changes.len(), 1);
        assert_eq!(
            event.changes[0].old_value,
            Some(Value::Text("Alice".to_owned()))
        );
        assert_eq!(event.changes[0].new_value, None);
    }

    #[test]
    fn test_raw_audit_event_recovered() {
        let event = RawAuditEvent::recovered("User", Value::I64(10), 1);
        assert_eq!(event.kind, RawAuditEventKind::Recovered);
        assert_eq!(event.values.get("version"), Some(&Value::I64(1)));
    }

    #[test]
    fn test_raw_audit_event_recovered_with_old_values() {
        let mut old_values = Record::new();
        old_values.insert("name".to_owned(), Value::Text("Alice".to_owned()));
        let event =
            RawAuditEvent::recovered_with_old_values("User", Value::I64(10), 2, Some(old_values));
        assert_eq!(event.kind, RawAuditEventKind::Recovered);
        assert_eq!(event.changes.len(), 1);
        assert_eq!(event.changes[0].field, "version");
        assert_eq!(event.changes[0].old_value, None);
        assert_eq!(event.changes[0].new_value, Some(Value::I64(-1)));
    }

    #[test]
    fn test_raw_audit_event_schema_created() {
        let event = RawAuditEvent::schema_created("System", "users", 5);
        assert_eq!(event.kind, RawAuditEventKind::SchemaCreated);
        assert_eq!(
            event.values.get("table_name"),
            Some(&Value::Text("users".to_owned()))
        );
        assert_eq!(event.values.get("field_count"), Some(&Value::I64(5)));
    }

    #[test]
    fn test_raw_audit_event_schema_verified() {
        let event = RawAuditEvent::schema_verified("System", "users", 5);
        assert_eq!(event.kind, RawAuditEventKind::SchemaVerified);
    }

    #[test]
    fn test_raw_audit_event_field_added() {
        let event = RawAuditEvent::field_added("System", "users", "age");
        assert_eq!(event.kind, RawAuditEventKind::FieldAdded);
        assert_eq!(
            event.values.get("field_name"),
            Some(&Value::Text("age".to_owned()))
        );
    }

    #[test]
    fn test_raw_audit_event_data_seeded() {
        let event = RawAuditEvent::data_seeded("System", "users", 10, 2);
        assert_eq!(event.kind, RawAuditEventKind::DataSeeded);
        assert_eq!(event.values.get("inserted"), Some(&Value::I64(10)));
        assert_eq!(event.values.get("updated"), Some(&Value::I64(2)));
    }

    #[test]
    fn test_mask_audit_value() {
        assert_eq!(mask_audit_value(""), "");
        assert_eq!(mask_audit_value("123456"), "******");
        assert_eq!(mask_audit_value("short"), "*****");
        assert_eq!(mask_audit_value("password123"), "pa*******23");
    }

    #[test]
    fn test_limit_audit_value() {
        assert_eq!(limit_audit_value("hello", 10), ("hello".to_string(), false));
        assert_eq!(limit_audit_value("abc", 2), ("**".to_string(), true));
        assert_eq!(
            limit_audit_value("this is a very long string", 10),
            ("thi...ring".to_string(), true)
        );
    }

    #[test]
    fn test_build_safe_audit_field() {
        let field = build_safe_audit_field(
            "password",
            Some("mysecret"),
            &["password".to_string()],
            None,
        );
        assert_eq!(field.masked, true);
        assert_eq!(field.value, Some("my****et".to_string()));

        let field_unmasked =
            build_safe_audit_field("username", Some("alice"), &["password".to_string()], None);
        assert_eq!(field_unmasked.masked, false);
        assert_eq!(field_unmasked.value, Some("alice".to_string()));

        let field_truncated =
            build_safe_audit_field("desc", Some("long description here"), &[], Some(10));
        assert_eq!(field_truncated.truncated, true);
        assert_eq!(field_truncated.value, Some("lon...here".to_string()));

        let field_none = build_safe_audit_field("empty", None, &[], None);
        assert_eq!(field_none.value, None);
    }

    #[test]
    fn test_build_safe_event() {
        let mut values = Record::new();
        values.insert("pwd".to_owned(), Value::Text("12345678".to_owned()));
        values.insert("age".to_owned(), Value::I64(30));
        values.insert("_hidden".to_owned(), Value::I64(1));

        let event = RawAuditEvent::created("User", values);
        let safe_event = event.build_safe_event(&["pwd".to_string()], Some(20));

        assert_eq!(safe_event.kind, RawAuditEventKind::Created);
        assert_eq!(safe_event.fields.len(), 2);

        let pwd_field = safe_event.fields.iter().find(|f| f.name == "pwd").unwrap();
        assert_eq!(pwd_field.masked, true);
    }

    struct DummySink {
        called: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }
    impl RawAuditEventSink for DummySink {
        fn on_event(&self, _ctx: &UserContext, _event: &RawAuditEvent) -> Result<(), RuntimeError> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_in_memory_raw_audit_event_sink() {
        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sink1 = DummySink {
            called: called.clone(),
        };
        let mut in_memory = InMemoryRawAuditEventSink::new();
        in_memory.register(sink1);

        let ctx = UserContext::default();
        let event = RawAuditEvent::schema_verified("Sys", "t", 1);
        let _ = in_memory.on_event(&ctx, &event);

        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_in_memory_with_sink() {
        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sink1 = DummySink {
            called: called.clone(),
        };
        let in_memory = InMemoryRawAuditEventSink::new().with_sink(sink1);

        let ctx = UserContext::default();
        let event = RawAuditEvent::schema_verified("Sys", "t", 1);
        let _ = in_memory.on_event(&ctx, &event);

        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_derived_traits() {
        let kind1 = RawAuditEventKind::Created;
        let kind2 = kind1.clone();
        assert_eq!(kind1, kind2);
        assert_eq!(format!("{:?}", kind1), "Created");

        let change1 = EntityPropertyChange::new("a", None, None);
        let change2 = change1.clone();
        assert_eq!(change1, change2);
        assert_eq!(format!("{:?}", change1).contains("EntityPropertyChange"), true);

        let event1 = RawAuditEvent::created("Entity", Record::new());
        let event2 = event1.clone();
        assert_eq!(event1, event2);
        assert_eq!(format!("{:?}", event1).contains("RawAuditEvent"), true);

        let safe_field1 = SafeAuditField {
            name: "f".to_string(),
            value: None,
            masked: false,
            truncated: false,
            raw_length: None,
            output_length: None,
            mask_reason: None,
            truncate_reason: None,
        };
        let safe_field2 = safe_field1.clone();
        assert_eq!(safe_field1, safe_field2);
        assert_eq!(format!("{:?}", safe_field1).contains("SafeAuditField"), true);

        let safe_event1 = SafeAuditEvent {
            kind: RawAuditEventKind::Created,
            entity: "Entity".to_string(),
            fields: vec![],
            trace_chain: vec![],
        };
        let safe_event2 = safe_event1.clone();
        assert_eq!(safe_event1, safe_event2);
        assert_eq!(format!("{:?}", safe_event1).contains("SafeAuditEvent"), true);

        let sink1 = InMemoryRawAuditEventSink::default();
        let sink2 = sink1.clone();
        let _ = sink2;
    }

    #[test]
    fn test_deleted_event_edges() {
        let event1 = RawAuditEvent::deleted("User", Value::I64(1), None);
        assert_eq!(event1.values.get("version"), None);

        let event2 = RawAuditEvent::deleted_with_old_values("User", Value::I64(1), None, None);
        assert_eq!(event2.old_values, None);
        assert_eq!(event2.changes.len(), 0);
    }

    #[test]
    fn test_recovered_with_old_values_none() {
        let event = RawAuditEvent::recovered_with_old_values("User", Value::I64(1), 2, None);
        assert_eq!(event.old_values, None);
        assert_eq!(event.new_values.as_ref().unwrap().get("version"), Some(&Value::I64(-1)));
    }

    #[test]
    fn test_limit_audit_value_edges() {
        assert_eq!(limit_audit_value("abcd", 4), ("abcd".to_string(), false));
        assert_eq!(limit_audit_value("abcd", 3), ("***".to_string(), true));
    }

    #[test]
    fn test_build_safe_event_deleted() {
        // A deleted event has changes where new_value is None.
        let mut old_values = Record::new();
        old_values.insert("name".to_owned(), Value::Text("Alice".to_owned()));
        let event = RawAuditEvent::deleted_with_old_values("User", Value::I64(1), None, Some(old_values));
        
        let safe_event = event.build_safe_event(&[], None);
        assert_eq!(safe_event.fields.len(), 1);
        assert_eq!(safe_event.fields[0].name, "name");
        assert_eq!(safe_event.fields[0].value, None);
    }

    #[test]
    fn test_changes_for_fields_none() {
        let changes = RawAuditEvent::changes_for_fields(None, None, &["missing".to_string()]);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].old_value, None);
        assert_eq!(changes[0].new_value, None);
    }
}
