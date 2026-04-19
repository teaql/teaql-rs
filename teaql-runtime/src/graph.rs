use std::collections::{BTreeMap, BTreeSet};

use teaql_core::{Record, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphOperation {
    Upsert,
    Reference,
    Remove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphMutationKind {
    Create,
    Update,
    Delete,
    Reference,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphMutationPlanItem {
    pub entity: String,
    pub kind: GraphMutationKind,
    pub values: Record,
    pub update_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphMutationBatch {
    pub entity: String,
    pub kind: GraphMutationKind,
    pub update_fields: Vec<String>,
    pub items: Vec<GraphMutationPlanItem>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GraphMutationPlan {
    pub planned_root: Option<GraphNode>,
    pub items: Vec<GraphMutationPlanItem>,
    pub batches: Vec<GraphMutationBatch>,
}

impl GraphMutationPlan {
    pub fn push(
        &mut self,
        entity: impl Into<String>,
        kind: GraphMutationKind,
        values: Record,
        update_fields: Vec<String>,
    ) {
        self.items.push(GraphMutationPlanItem {
            entity: entity.into(),
            kind,
            values,
            update_fields,
        });
    }

    pub fn rebuild_batches(&mut self) {
        let mut grouped: BTreeMap<
            (String, GraphMutationKind, Vec<String>),
            Vec<GraphMutationPlanItem>,
        > = BTreeMap::new();
        for item in &self.items {
            let update_fields = match item.kind {
                GraphMutationKind::Update => item.update_fields.clone(),
                _ => Vec::new(),
            };
            grouped
                .entry((item.entity.clone(), item.kind, update_fields))
                .or_default()
                .push(item.clone());
        }
        self.batches = grouped
            .into_iter()
            .map(
                |((entity, kind, update_fields), items)| GraphMutationBatch {
                    entity,
                    kind,
                    update_fields,
                    items,
                },
            )
            .collect();
    }

    pub fn grouped_counts(&self) -> BTreeMap<(String, GraphMutationKind), usize> {
        let mut counts = BTreeMap::new();
        for batch in &self.batches {
            *counts
                .entry((batch.entity.clone(), batch.kind))
                .or_insert(0) += batch.items.len();
        }
        counts
    }

    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

pub fn sorted_update_fields(
    values: &Record,
    excluded: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let excluded = excluded.into_iter().collect::<BTreeSet<_>>();
    values
        .keys()
        .filter(|field| !excluded.contains(*field))
        .cloned()
        .collect()
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
