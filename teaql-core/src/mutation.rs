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
    pub trace_chain: Vec<crate::TraceNode>,
}

impl InsertCommand {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            values: Record::new(),
            trace_chain: Vec::new(),
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
    pub trace_chain: Vec<crate::TraceNode>,
    pub old_values: Option<Record>,
}

impl UpdateCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            values: Record::new(),
            trace_chain: Vec::new(),
            old_values: None,
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
pub struct BatchInsertCommand {
    pub entity: String,
    pub batch_values: Vec<Record>,
    pub trace_chains: Vec<Vec<crate::TraceNode>>,
}

impl BatchInsertCommand {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            batch_values: Vec::new(),
            trace_chains: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchUpdateCommand {
    pub entity: String,
    pub batch_ids: Vec<Value>,
    pub batch_expected_versions: Vec<Option<i64>>,
    pub batch_values: Vec<Record>,
    pub update_fields: Vec<String>,
    pub trace_chains: Vec<Vec<crate::TraceNode>>,
    pub batch_old_values: Vec<Option<Record>>,
}

impl BatchUpdateCommand {
    pub fn new(entity: impl Into<String>, update_fields: Vec<String>) -> Self {
        Self {
            entity: entity.into(),
            batch_ids: Vec::new(),
            batch_expected_versions: Vec::new(),
            batch_values: Vec::new(),
            update_fields,
            trace_chains: Vec::new(),
            batch_old_values: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: Option<i64>,
    pub soft_delete: bool,
    pub trace_chain: Vec<crate::TraceNode>,
}

impl DeleteCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            soft_delete: true,
            trace_chain: Vec::new(),
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
    pub trace_chain: Vec<crate::TraceNode>,
}

impl RecoverCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>, expected_version: i64) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version,
            trace_chain: Vec::new(),
        }
    }
}
