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
        let changes = values.iter().map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone()))).collect();
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
        let changes = values.iter().map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone()))).collect();
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
        let changes = values.iter().map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone()))).collect();
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
        let changes = values.iter().map(|(k, v)| EntityPropertyChange::new(k.clone(), None, Some(v.clone()))).collect();
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

            let mut value = if should_mask {
                mask_audit_value(raw)
            } else {
                raw.to_string()
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
                mask_reason: if should_mask {
                    Some("_audit_mask_fields".to_string())
                } else {
                    None
                },
                truncate_reason: if truncated {
                    Some("_audit_value_max_len".to_string())
                } else {
                    None
                },
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
    fn on_safe_event(&self, ctx: &crate::UserContext, event: &SafeAuditEvent) -> Result<(), crate::RuntimeError>;
}
