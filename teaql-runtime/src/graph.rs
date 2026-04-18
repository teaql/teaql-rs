use std::collections::BTreeMap;

use teaql_core::{Record, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphOperation {
    Upsert,
    Reference,
    Remove,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphNode {
    pub entity: String,
    pub values: Record,
    pub relations: BTreeMap<String, Vec<GraphNode>>,
    pub operation: GraphOperation,
}

impl GraphNode {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            values: Record::new(),
            relations: BTreeMap::new(),
            operation: GraphOperation::Upsert,
        }
    }

    pub fn operation(mut self, operation: GraphOperation) -> Self {
        self.operation = operation;
        self
    }

    pub fn reference(mut self) -> Self {
        self.operation = GraphOperation::Reference;
        self
    }

    pub fn remove(mut self) -> Self {
        self.operation = GraphOperation::Remove;
        self
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }

    pub fn relation(mut self, name: impl Into<String>, node: GraphNode) -> Self {
        self.relations.entry(name.into()).or_default().push(node);
        self
    }

    pub fn relations(
        mut self,
        name: impl Into<String>,
        nodes: impl IntoIterator<Item = GraphNode>,
    ) -> Self {
        self.relations.entry(name.into()).or_default().extend(nodes);
        self
    }

    pub fn id(&self) -> Option<&Value> {
        self.values.get("id")
    }
}
