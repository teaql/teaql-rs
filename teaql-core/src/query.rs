use std::collections::BTreeMap;

use crate::{Expr, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedExpr {
    pub alias: String,
    pub expr: Expr,
}

impl NamedExpr {
    pub fn new(alias: impl Into<String>, expr: Expr) -> Self {
        Self {
            alias: alias.into(),
            expr,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub field: String,
    pub expr: Option<Expr>,
    pub direction: SortDirection,
}

impl OrderBy {
    pub fn new(field: impl Into<String>, direction: SortDirection) -> Self {
        Self {
            field: field.into(),
            expr: None,
            direction,
        }
    }

    pub fn expr(expr: Expr, direction: SortDirection) -> Self {
        Self {
            field: String::new(),
            expr: Some(expr),
            direction,
        }
    }

    pub fn asc(field: impl Into<String>) -> Self {
        Self::new(field, SortDirection::Asc)
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self::new(field, SortDirection::Desc)
    }

    pub fn asc_expr(expr: Expr) -> Self {
        Self::expr(expr, SortDirection::Asc)
    }

    pub fn desc_expr(expr: Expr) -> Self {
        Self::expr(expr, SortDirection::Desc)
    }

    pub fn asc_gbk(field: impl Into<String>) -> Self {
        Self::asc_expr(Expr::gbk(Expr::column(field)))
    }

    pub fn desc_gbk(field: impl Into<String>) -> Self {
        Self::desc_expr(Expr::gbk(Expr::column(field)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Stddev,
    StddevPop,
    VarSamp,
    VarPop,
    BitAnd,
    BitOr,
    BitXor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub function: AggregateFunction,
    pub field: String,
    pub alias: String,
}

impl Aggregate {
    pub fn new(
        function: AggregateFunction,
        field: impl Into<String>,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            function,
            field: field.into(),
            alias: alias.into(),
        }
    }

    pub fn count(alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Count, "*", alias)
    }

    pub fn count_field(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Count, field, alias)
    }

    pub fn sum(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Sum, field, alias)
    }

    pub fn avg(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Avg, field, alias)
    }

    pub fn min(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Min, field, alias)
    }

    pub fn max(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Max, field, alias)
    }

    pub fn stddev(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Stddev, field, alias)
    }

    pub fn stddev_pop(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::StddevPop, field, alias)
    }

    pub fn var_samp(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::VarSamp, field, alias)
    }

    pub fn var_pop(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::VarPop, field, alias)
    }

    pub fn bit_and(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::BitAnd, field, alias)
    }

    pub fn bit_or(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::BitOr, field, alias)
    }

    pub fn bit_xor(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::BitXor, field, alias)
    }
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

impl RelationLoad {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub entity: String,
    pub projection: Vec<String>,
    pub expr_projection: Vec<NamedExpr>,
    pub filter: Option<Expr>,
    pub having: Option<Expr>,
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
            expr_projection: Vec::new(),
            filter: None,
            having: None,
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

    pub fn projects(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.projection.extend(fields.into_iter().map(Into::into));
        self
    }

    pub fn project_expr(mut self, alias: impl Into<String>, expr: Expr) -> Self {
        self.expr_projection.push(NamedExpr::new(alias, expr));
        self
    }

    pub fn filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn and_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(existing) => existing.and_expr(filter),
            None => filter,
        });
        self
    }

    pub fn or_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(existing) => existing.or_expr(filter),
            None => filter,
        });
        self
    }

    pub fn having(mut self, having: Expr) -> Self {
        self.having = Some(having);
        self
    }

    pub fn and_having(mut self, having: Expr) -> Self {
        self.having = Some(match self.having.take() {
            Some(existing) => existing.and_expr(having),
            None => having,
        });
        self
    }

    pub fn or_having(mut self, having: Expr) -> Self {
        self.having = Some(match self.having.take() {
            Some(existing) => existing.or_expr(having),
            None => having,
        });
        self
    }

    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    pub fn order_asc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::asc(field))
    }

    pub fn order_desc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::desc(field))
    }

    pub fn order_expr_asc(self, expr: Expr) -> Self {
        self.order_by(OrderBy::asc_expr(expr))
    }

    pub fn order_expr_desc(self, expr: Expr) -> Self {
        self.order_by(OrderBy::desc_expr(expr))
    }

    pub fn order_gbk_asc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::asc_gbk(field))
    }

    pub fn order_gbk_desc(self, field: impl Into<String>) -> Self {
        self.order_by(OrderBy::desc_gbk(field))
    }

    pub fn group_by(mut self, field: impl Into<String>) -> Self {
        self.group_by.push(field.into());
        self
    }

    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregates.push(aggregate);
        self
    }

    pub fn count(self, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::count(alias))
    }

    pub fn count_field(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::count_field(field, alias))
    }

    pub fn sum(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::sum(field, alias))
    }

    pub fn avg(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::avg(field, alias))
    }

    pub fn min(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::min(field, alias))
    }

    pub fn max(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::max(field, alias))
    }

    pub fn stddev(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::stddev(field, alias))
    }

    pub fn stddev_pop(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::stddev_pop(field, alias))
    }

    pub fn var_samp(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::var_samp(field, alias))
    }

    pub fn var_pop(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::var_pop(field, alias))
    }

    pub fn bit_and(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::bit_and(field, alias))
    }

    pub fn bit_or(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::bit_or(field, alias))
    }

    pub fn bit_xor(self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregate(Aggregate::bit_xor(field, alias))
    }

    pub fn relation(mut self, name: impl Into<String>) -> Self {
        self.relations.push(RelationLoad::new(name));
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

    pub fn page(self, offset: u64, limit: u64) -> Self {
        self.offset(offset).limit(limit)
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
