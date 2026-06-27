#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphTransactionBoundary {
    Started,
    AlreadyActive,
    Unsupported,
}
