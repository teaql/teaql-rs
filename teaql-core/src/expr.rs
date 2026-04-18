use crate::{EntityDescriptor, SelectQuery, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    NotLike,
    In,
    NotIn,
    InLarge,
    NotInLarge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprFunction {
    Soundex,
    Gbk,
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

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Value(Value),
    Function {
        function: ExprFunction,
        args: Vec<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    SubQuery {
        left: Box<Expr>,
        op: BinaryOp,
        entity: EntityDescriptor,
        query: Box<SelectQuery>,
    },
    Between {
        expr: Box<Expr>,
        lower: Box<Expr>,
        upper: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    pub fn value(value: impl Into<Value>) -> Self {
        Self::Value(value.into())
    }

    pub fn function(function: ExprFunction, args: impl IntoIterator<Item = Expr>) -> Self {
        Self::Function {
            function,
            args: args.into_iter().collect(),
        }
    }

    pub fn soundex(expr: Expr) -> Self {
        Self::function(ExprFunction::Soundex, [expr])
    }

    pub fn gbk(expr: Expr) -> Self {
        Self::function(ExprFunction::Gbk, [expr])
    }

    pub fn count_all() -> Self {
        Self::function(ExprFunction::Count, [])
    }

    pub fn count_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::Count, [expr])
    }

    pub fn sum_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::Sum, [expr])
    }

    pub fn avg_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::Avg, [expr])
    }

    pub fn min_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::Min, [expr])
    }

    pub fn max_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::Max, [expr])
    }

    pub fn stddev_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::Stddev, [expr])
    }

    pub fn stddev_pop_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::StddevPop, [expr])
    }

    pub fn var_samp_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::VarSamp, [expr])
    }

    pub fn var_pop_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::VarPop, [expr])
    }

    pub fn bit_and_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::BitAnd, [expr])
    }

    pub fn bit_or_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::BitOr, [expr])
    }

    pub fn bit_xor_expr(expr: Expr) -> Self {
        Self::function(ExprFunction::BitXor, [expr])
    }

    pub fn sound_like(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::binary(
            Self::soundex(Self::column(column)),
            BinaryOp::Eq,
            Self::soundex(Self::value(value)),
        )
    }

    pub fn eq(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::binary(Self::column(column), BinaryOp::Eq, Self::value(value))
    }

    pub fn ne(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::binary(Self::column(column), BinaryOp::Ne, Self::value(value))
    }

    pub fn gt(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::binary(Self::column(column), BinaryOp::Gt, Self::value(value))
    }

    pub fn gte(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::binary(Self::column(column), BinaryOp::Gte, Self::value(value))
    }

    pub fn lt(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::binary(Self::column(column), BinaryOp::Lt, Self::value(value))
    }

    pub fn lte(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::binary(Self::column(column), BinaryOp::Lte, Self::value(value))
    }

    pub fn like(column: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::binary(
            Self::column(column),
            BinaryOp::Like,
            Self::value(pattern.into()),
        )
    }

    pub fn not_like(column: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::binary(
            Self::column(column),
            BinaryOp::NotLike,
            Self::value(pattern.into()),
        )
    }

    pub fn contain(column: impl Into<String>, value: impl Into<String>) -> Self {
        Self::like(column, format!("%{}%", value.into()))
    }

    pub fn not_contain(column: impl Into<String>, value: impl Into<String>) -> Self {
        Self::not_like(column, format!("%{}%", value.into()))
    }

    pub fn begin_with(column: impl Into<String>, value: impl Into<String>) -> Self {
        Self::like(column, format!("{}%", value.into()))
    }

    pub fn not_begin_with(column: impl Into<String>, value: impl Into<String>) -> Self {
        Self::not_like(column, format!("{}%", value.into()))
    }

    pub fn end_with(column: impl Into<String>, value: impl Into<String>) -> Self {
        Self::like(column, format!("%{}", value.into()))
    }

    pub fn not_end_with(column: impl Into<String>, value: impl Into<String>) -> Self {
        Self::not_like(column, format!("%{}", value.into()))
    }

    pub fn binary(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn in_list(column: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Self {
        Self::binary(
            Self::column(column),
            BinaryOp::In,
            Self::Value(Value::List(values.into_iter().collect())),
        )
    }

    pub fn not_in_list(column: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Self {
        Self::binary(
            Self::column(column),
            BinaryOp::NotIn,
            Self::Value(Value::List(values.into_iter().collect())),
        )
    }

    pub fn in_large(column: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Self {
        Self::binary(
            Self::column(column),
            BinaryOp::InLarge,
            Self::Value(Value::List(values.into_iter().collect())),
        )
    }

    pub fn not_in_large(
        column: impl Into<String>,
        values: impl IntoIterator<Item = Value>,
    ) -> Self {
        Self::binary(
            Self::column(column),
            BinaryOp::NotInLarge,
            Self::Value(Value::List(values.into_iter().collect())),
        )
    }

    pub fn in_subquery(
        column: impl Into<String>,
        entity: EntityDescriptor,
        query: SelectQuery,
        field: impl Into<String>,
    ) -> Self {
        Self::subquery(Self::column(column), BinaryOp::In, entity, query, field)
    }

    pub fn not_in_subquery(
        column: impl Into<String>,
        entity: EntityDescriptor,
        query: SelectQuery,
        field: impl Into<String>,
    ) -> Self {
        Self::subquery(Self::column(column), BinaryOp::NotIn, entity, query, field)
    }

    pub fn subquery(
        left: Expr,
        op: BinaryOp,
        entity: EntityDescriptor,
        mut query: SelectQuery,
        field: impl Into<String>,
    ) -> Self {
        query.projection = vec![field.into()];
        Self::SubQuery {
            left: Box::new(left),
            op,
            entity,
            query: Box::new(query),
        }
    }

    pub fn between(
        column: impl Into<String>,
        lower: impl Into<Value>,
        upper: impl Into<Value>,
    ) -> Self {
        Self::Between {
            expr: Box::new(Self::column(column)),
            lower: Box::new(Self::value(lower)),
            upper: Box::new(Self::value(upper)),
        }
    }

    pub fn is_null(column: impl Into<String>) -> Self {
        Self::IsNull(Box::new(Self::column(column)))
    }

    pub fn is_not_null(column: impl Into<String>) -> Self {
        Self::IsNotNull(Box::new(Self::column(column)))
    }

    pub fn and(parts: impl IntoIterator<Item = Expr>) -> Self {
        Self::And(parts.into_iter().collect())
    }

    pub fn or(parts: impl IntoIterator<Item = Expr>) -> Self {
        Self::Or(parts.into_iter().collect())
    }

    pub fn negate(expr: Expr) -> Self {
        Self::Not(Box::new(expr))
    }

    pub fn and_expr(self, other: Expr) -> Self {
        match self {
            Self::And(mut parts) => {
                parts.push(other);
                Self::And(parts)
            }
            expr => Self::And(vec![expr, other]),
        }
    }

    pub fn or_expr(self, other: Expr) -> Self {
        match self {
            Self::Or(mut parts) => {
                parts.push(other);
                Self::Or(parts)
            }
            expr => Self::Or(vec![expr, other]),
        }
    }
}
