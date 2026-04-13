use crate::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    In,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Value(Value),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
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

    pub fn eq(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Binary {
            left: Box::new(Self::column(column)),
            op: BinaryOp::Eq,
            right: Box::new(Self::value(value)),
        }
    }

    pub fn in_list(column: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Self {
        Self::Binary {
            left: Box::new(Self::column(column)),
            op: BinaryOp::In,
            right: Box::new(Self::Value(Value::List(values.into_iter().collect()))),
        }
    }

    pub fn and(parts: impl IntoIterator<Item = Expr>) -> Self {
        Self::And(parts.into_iter().collect())
    }
}
