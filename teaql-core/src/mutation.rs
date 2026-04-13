use crate::{Record, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
    Recover,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertCommand {
    pub entity: String,
    pub values: Record,
}

impl InsertCommand {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            values: Record::new(),
        }
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: Option<i64>,
    pub values: Record,
}

impl UpdateCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            values: Record::new(),
        }
    }

    pub fn expected_version(mut self, version: i64) -> Self {
        self.expected_version = Some(version);
        self
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: Option<i64>,
    pub soft_delete: bool,
}

impl DeleteCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            soft_delete: true,
        }
    }

    pub fn expected_version(mut self, version: i64) -> Self {
        self.expected_version = Some(version);
        self
    }

    pub fn hard_delete(mut self) -> Self {
        self.soft_delete = false;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoverCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: i64,
}

impl RecoverCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>, expected_version: i64) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version,
        }
    }
}
