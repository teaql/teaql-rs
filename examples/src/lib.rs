use teaql_core::{SmartList, TeaqlEntity};
use teaql_macros::TeaqlEntity;
use teaql_provider_sqlx_postgres::{
    MutationExecutorError as PgMutationExecutorError, PgMutationExecutor, PostgresDialect,
    PostgresProviderExt,
};
use teaql_provider_sqlx_sqlite::{
    MutationExecutorError as SqliteMutationExecutorError, SqliteDialect, SqliteMutationExecutor,
    SqliteProviderExt,
};
use teaql_runtime::{
    InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry,
    InMemoryRepositoryRegistry, RepositoryBehavior, RuntimeModule, UserContext,
};
use teaql_sql::SqlDataServiceExecutor;

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
    ctx.use_sqlite_provider(executor.clone());
    
    // Build and inject SqlDataServiceExecutor instead of the old QueryExecutor
    let data_service = SqlDataServiceExecutor::new(
        SqliteDialect,
        executor,
        metadata()
    );
    ctx.insert_resource(data_service);
    ctx
}

pub fn postgres_context(executor: PgMutationExecutor) -> UserContext {
    let mut ctx = UserContext::new()
        .with_metadata(metadata())
        .with_repository_registry(repository_registry())
        .with_repository_behavior_registry(behavior_registry());
    ctx.use_postgres_provider(executor.clone());
    
    // Build and inject SqlDataServiceExecutor instead of the old QueryExecutor
    let data_service = SqlDataServiceExecutor::new(
        PostgresDialect,
        executor,
        metadata()
    );
    ctx.insert_resource(data_service);
    ctx
}

pub async fn reset_sqlite_schema(
    pool: &sqlx::SqlitePool,
    executor: &SqliteMutationExecutor,
) -> Result<(), SqliteMutationExecutorError> {
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
) -> Result<(), PgMutationExecutorError> {
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
