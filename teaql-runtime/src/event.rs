use std::sync::Arc;

use teaql_core::{Record, Value};

use crate::{RuntimeError, UserContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityEventKind {
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
pub struct EntityEvent {
    pub kind: EntityEventKind,
    pub entity: String,
    pub values: Record,
    pub updated_fields: Vec<String>,
    pub old_values: Option<Record>,
    pub new_values: Option<Record>,
    pub changes: Vec<EntityPropertyChange>,
    /// Annotation trace chain from the graph save scope chain.
    pub trace_chain: Vec<teaql_core::TraceNode>,
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
            trace_chain: Vec::new(),
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
            kind: EntityEventKind::Updated,
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
            kind: EntityEventKind::Deleted,
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
            kind: EntityEventKind::Recovered,
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
        Self {
            kind: EntityEventKind::SchemaCreated,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
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
        Self {
            kind: EntityEventKind::SchemaVerified,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
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
        Self {
            kind: EntityEventKind::FieldAdded,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
            trace_chain: Vec::new(),
        }
    }

    /// Initial seed data was inserted or updated during bootstrap.
    ///
    /// - `inserted`: number of new records inserted
    /// - `updated`: number of existing records updated
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
        Self {
            kind: EntityEventKind::DataSeeded,
            entity,
            values,
            updated_fields: Vec::new(),
            old_values: None,
            new_values: None,
            changes: Vec::new(),
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
