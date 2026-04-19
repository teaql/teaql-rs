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
pub struct EntityEvent {
    pub kind: EntityEventKind,
    pub entity: String,
    pub values: Record,
    pub updated_fields: Vec<String>,
}

impl EntityEvent {
    pub fn created(entity: impl Into<String>, values: Record) -> Self {
        Self {
            kind: EntityEventKind::Created,
            entity: entity.into(),
            values,
            updated_fields: Vec::new(),
        }
    }

    pub fn updated(entity: impl Into<String>, values: Record) -> Self {
        let updated_fields = values.keys().cloned().collect();
        Self {
            kind: EntityEventKind::Updated,
            entity: entity.into(),
            values,
            updated_fields,
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
        }
    }

    pub fn recovered(entity: impl Into<String>, id: Value, expected_version: i64) -> Self {
        Self {
            kind: EntityEventKind::Recovered,
            entity: entity.into(),
            values: Record::from([
                ("id".to_owned(), id),
                ("version".to_owned(), Value::I64(expected_version)),
            ]),
            updated_fields: Vec::new(),
        }
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
