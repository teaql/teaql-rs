use teaql_core::{Record, SmartList, TeaqlEntity};
use teaql_dialect_pg::PostgresDialect;
use teaql_dialect_sqlite::SqliteDialect;
use teaql_macros::TeaqlEntity;
use teaql_runtime::sqlx_support::{
    MutationExecutorError, PgMutationExecutor, SqliteMutationExecutor,
};
use teaql_runtime::{
    InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry,
    QueryExecutor, RepositoryBehavior, RuntimeModule, UserContext,
};
use tokio::runtime::Handle;
use tokio::task::block_in_place;

#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "Product", table = "example_product")]
pub struct Product {
    #[teaql(id)]
    pub id: u64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "OrderLine", table = "example_orderline")]
pub struct OrderLine {
    #[teaql(id)]
    pub id: u64,
    #[teaql(column = "order_id")]
    pub order_id: u64,
    pub name: String,
    #[teaql(column = "product_id")]
    pub product_id: u64,
    #[teaql(relation(target = "Product", local_key = "product_id", foreign_key = "id"))]
    pub product: Option<Product>,
}

#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "Order", table = "example_orders")]
pub struct Order {
    #[teaql(id)]
    pub id: u64,
    #[teaql(version)]
    pub version: i64,
    pub name: String,
    #[teaql(relation(target = "OrderLine", local_key = "id", foreign_key = "order_id", many))]
    pub lines: SmartList<OrderLine>,
}

pub struct OrderRelations;

impl RepositoryBehavior for OrderRelations {
    fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
        vec!["lines.product".to_owned()]
    }
}

#[derive(Clone)]
pub struct SqliteSyncExecutor {
    inner: SqliteMutationExecutor,
}

impl SqliteSyncExecutor {
    pub fn new(inner: SqliteMutationExecutor) -> Self {
        Self { inner }
    }
}

impl QueryExecutor for SqliteSyncExecutor {
    type Error = MutationExecutorError;

    fn fetch_all(&self, query: &teaql_sql::CompiledQuery) -> Result<Vec<Record>, Self::Error> {
        let handle = Handle::current();
        block_in_place(|| handle.block_on(self.inner.fetch_all(query)))
    }

    fn execute(&self, query: &teaql_sql::CompiledQuery) -> Result<u64, Self::Error> {
        let handle = Handle::current();
        block_in_place(|| handle.block_on(self.inner.execute(query)))
    }
}

#[derive(Clone)]
pub struct PgSyncExecutor {
    inner: PgMutationExecutor,
}

impl PgSyncExecutor {
    pub fn new(inner: PgMutationExecutor) -> Self {
        Self { inner }
    }
}

impl QueryExecutor for PgSyncExecutor {
    type Error = MutationExecutorError;

    fn fetch_all(&self, query: &teaql_sql::CompiledQuery) -> Result<Vec<Record>, Self::Error> {
        let handle = Handle::current();
        block_in_place(|| handle.block_on(self.inner.fetch_all(query)))
    }

    fn execute(&self, query: &teaql_sql::CompiledQuery) -> Result<u64, Self::Error> {
        let handle = Handle::current();
        block_in_place(|| handle.block_on(self.inner.execute(query)))
    }
}

pub fn module() -> RuntimeModule {
    RuntimeModule::new()
        .entity_with_behavior::<Order, _>(OrderRelations)
        .entity::<OrderLine>()
        .entity::<Product>()
}

pub fn metadata() -> InMemoryMetadataStore {
    InMemoryMetadataStore::new()
        .with_entity(Order::entity_descriptor())
        .with_entity(OrderLine::entity_descriptor())
        .with_entity(Product::entity_descriptor())
}

pub fn repository_registry() -> InMemoryRepositoryRegistry {
    InMemoryRepositoryRegistry::new().with_entity("Order")
}

pub fn behavior_registry() -> InMemoryRepositoryBehaviorRegistry {
    InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderRelations)
}

pub fn sqlite_context(executor: SqliteMutationExecutor) -> UserContext {
    let mut ctx = UserContext::new()
        .with_metadata(metadata())
        .with_repository_registry(repository_registry())
        .with_repository_behavior_registry(behavior_registry());
    ctx.insert_resource(SqliteDialect);
    ctx.insert_resource(SqliteSyncExecutor::new(executor));
    ctx
}

pub fn postgres_context(executor: PgMutationExecutor) -> UserContext {
    let mut ctx = UserContext::new()
        .with_metadata(metadata())
        .with_repository_registry(repository_registry())
        .with_repository_behavior_registry(behavior_registry());
    ctx.insert_resource(PostgresDialect);
    ctx.insert_resource(PgSyncExecutor::new(executor));
    ctx
}

pub async fn reset_sqlite_schema(
    pool: &sqlx::SqlitePool,
    executor: &SqliteMutationExecutor,
) -> Result<(), MutationExecutorError> {
    sqlx::query("DROP TABLE IF EXISTS example_orderline")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS example_product")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS example_orders")
        .execute(pool)
        .await?;
    executor
        .ensure_schema(
            &SqliteDialect,
            &[
                &Order::entity_descriptor(),
                &OrderLine::entity_descriptor(),
                &Product::entity_descriptor(),
            ],
        )
        .await
}

pub async fn reset_postgres_schema(
    pool: &sqlx::PgPool,
    executor: &PgMutationExecutor,
) -> Result<(), MutationExecutorError> {
    sqlx::query("DROP TABLE IF EXISTS example_orderline")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS example_product")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS example_orders")
        .execute(pool)
        .await?;
    executor
        .ensure_schema(
            &PostgresDialect,
            &[
                &Order::entity_descriptor(),
                &OrderLine::entity_descriptor(),
                &Product::entity_descriptor(),
            ],
        )
        .await
}
