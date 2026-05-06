use teaql_core::Record;
use teaql_sql::CompiledQuery;

pub trait QueryExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error>;
    fn execute(&self, query: &CompiledQuery) -> Result<u64, Self::Error>;

    fn begin_transaction(&self) -> Result<GraphTransactionBoundary, Self::Error> {
        Ok(GraphTransactionBoundary::Unsupported)
    }

    fn commit_transaction(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn rollback_transaction(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphTransactionBoundary {
    Started,
    AlreadyActive,
    Unsupported,
}
