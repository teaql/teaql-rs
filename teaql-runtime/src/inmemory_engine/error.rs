use std::fmt;

/// A SUM input or result cannot be represented without changing its domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InMemoryQueryErrorKind {
    NonNumericValue,
    MixedFloatingAndExact,
    NonFiniteValue,
    NumericOverflow,
    DecimalPrecisionLoss,
}

impl InMemoryQueryErrorKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::NonNumericValue => "MEMORY_SUM_NON_NUMERIC",
            Self::MixedFloatingAndExact => "MEMORY_SUM_MIXED_NUMERIC_DOMAINS",
            Self::NonFiniteValue => "MEMORY_SUM_NON_FINITE",
            Self::NumericOverflow => "MEMORY_SUM_OVERFLOW",
            Self::DecimalPrecisionLoss => "MEMORY_SUM_PRECISION_LOSS",
        }
    }
}

/// Structured in-memory aggregate error. Never contains an input row or value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryQueryError {
    pub field: String,
    pub alias: String,
    pub kind: InMemoryQueryErrorKind,
}

impl InMemoryQueryError {
    pub(crate) fn sum(aggregate: &teaql_core::Aggregate, kind: InMemoryQueryErrorKind) -> Self {
        Self {
            field: aggregate.field.clone(),
            alias: aggregate.alias.clone(),
            kind,
        }
    }

    pub const fn code(&self) -> &'static str {
        self.kind.code()
    }
}

impl fmt::Display for InMemoryQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: SUM({}) AS {}", self.code(), self.field, self.alias)
    }
}

impl std::error::Error for InMemoryQueryError {}
