use std::sync::Arc;

use teaql_core::{Record, Value};

use crate::{RuntimeError, UserContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityEventKind {
    Created,
    Updated,
    Deleted,
    Recovered,
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
pub struct EntityEvent {
    pub kind: EntityEventKind,
    pub entity: String,
    pub values: Record,
    pub updated_fields: Vec<String>,
    pub old_values: Option<Record>,
    pub new_values: Option<Record>,
    pub changes: Vec<EntityPropertyChange>,
    /// Annotation comment lineage from the graph save scope chain.
    pub comment: Option<String>,
}

impl EntityEvent {
    pub fn created(entity: impl Into<String>, values: Record) -> Self {
        let changes = values
            .iter()
            .map(|(field, value)| {
                EntityPropertyChange::new(field.clone(), None, Some(value.clone()))
            })
            .collect();
        Self {
            kind: EntityEventKind::Created,
            entity: entity.into(),
            values: values.clone(),
            updated_fields: Vec::new(),
            old_values: None,
            new_values: Some(values),
            changes,
            comment: None,
        }
    }

    pub fn updated(entity: impl Into<String>, values: Record) -> Self {
        let updated_fields = values.keys().cloned().collect::<Vec<_>>();
        let changes = Self::changes_for_fields(None, Some(&values), &updated_fields);
        Self {
            kind: EntityEventKind::Updated,
            entity: entity.into(),
            values: values.clone(),
            updated_fields,
            old_values: None,
            new_values: Some(values),
            changes,
            comment: None,
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
            kind: EntityEventKind::Updated,
            entity: entity.into(),
            values,
            updated_fields,
            old_values,
            new_values: Some(new_values),
            changes,
            comment: None,
        }
    }

    pub fn deleted(entity: impl Into<String>, id: Value, expected_version: Option<i64>) -> Self {
        let mut values = Record::from([("id".to_owned(), id)]);
        if let Some(version) = expected_version {
            values.insert("version".to_owned(), Value::I64(version));
        }
        Self {
            kind: EntityEventKind::Deleted,
            entity: entity.into(),
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
            comment: None,
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
            kind: EntityEventKind::Recovered,
            entity: entity.into(),
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
            comment: None,
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
}

pub trait EntityEventSink: Send + Sync {
    fn on_event(&self, ctx: &UserContext, event: &EntityEvent) -> Result<(), RuntimeError>;
}

#[derive(Default, Clone)]
pub struct InMemoryEntityEventSink {
    sinks: Vec<Arc<dyn EntityEventSink>>,
}

impl InMemoryEntityEventSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, sink: impl EntityEventSink + 'static) {
        self.sinks.push(Arc::new(sink));
    }

    pub fn with_sink(mut self, sink: impl EntityEventSink + 'static) -> Self {
        self.register(sink);
        self
    }
}

impl EntityEventSink for InMemoryEntityEventSink {
    fn on_event(&self, ctx: &UserContext, event: &EntityEvent) -> Result<(), RuntimeError> {
        for sink in &self.sinks {
            sink.on_event(ctx, event)?;
        }
        Ok(())
    }
}
