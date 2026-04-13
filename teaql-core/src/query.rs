use std::collections::BTreeMap;

use crate::{Expr, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    pub field: String,
    pub direction: SortDirection,
}

impl OrderBy {
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Desc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub function: AggregateFunction,
    pub field: String,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slice {
    pub limit: Option<u64>,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationLoad {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub entity: String,
    pub projection: Vec<String>,
    pub filter: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub slice: Option<Slice>,
    pub aggregates: Vec<Aggregate>,
    pub group_by: Vec<String>,
    pub relations: Vec<RelationLoad>,
}

impl SelectQuery {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            projection: Vec::new(),
            filter: None,
            order_by: Vec::new(),
            slice: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            relations: Vec::new(),
        }
    }

    pub fn project(mut self, field: impl Into<String>) -> Self {
        self.projection.push(field.into());
        self
    }

    pub fn filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        let slice = self.slice.get_or_insert(Slice {
            limit: None,
            offset: 0,
        });
        slice.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        let slice = self.slice.get_or_insert(Slice {
            limit: None,
            offset: 0,
        });
        slice.offset = offset;
        self
    }
}

pub type Record = BTreeMap<String, Value>;

pub fn record_to_json_value(record: &Record) -> serde_json::Value {
    serde_json::Value::Object(
        record
            .iter()
            .map(|(key, value)| (key.clone(), value.to_json_value()))
            .collect(),
    )
}
