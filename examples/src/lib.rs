use teaql_core::{SmartList, TeaqlEntity};
use teaql_macros::TeaqlEntity;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    EntityDataServiceBehavior, InMemoryEntityDataServiceBehaviorRegistry, InMemoryEntityRegistry,
    InMemoryMetadataStore, RuntimeModule, UserContext,
};
use teaql_sql::SqlDataServiceExecutor;

#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "Product", table = "example_product")]
pub struct Product {
    #[teaql(skip)]
    pub root: teaql_runtime::EntityRuntimeState,
    #[teaql(id)]
    pub id: u64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "OrderLine", table = "example_orderline")]
pub struct OrderLine {
    #[teaql(skip)]
    pub root: teaql_runtime::EntityRuntimeState,
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
    #[teaql(skip)]
    pub root: teaql_runtime::EntityRuntimeState,
    #[teaql(id)]
    pub id: u64,
    #[teaql(version)]
    pub version: i64,
    pub name: String,
    #[teaql(relation(target = "OrderLine", local_key = "id", foreign_key = "order_id", many))]
    pub lines: SmartList<OrderLine>,
}

pub struct OrderRelations;

impl EntityDataServiceBehavior for OrderRelations {
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

pub fn entity_registry() -> InMemoryEntityRegistry {
    InMemoryEntityRegistry::new().with_entity("Order")
}

pub fn behavior_registry() -> InMemoryEntityDataServiceBehaviorRegistry {
    InMemoryEntityDataServiceBehaviorRegistry::new().with_behavior("Order", OrderRelations)
}

pub fn sqlite_context(executor: SqliteMutationExecutor) -> UserContext {
    let mut context = UserContext::new()
        .with_metadata(metadata())
        .with_entity_registry(entity_registry())
        .with_entity_data_service_behavior_registry(behavior_registry());
    context.use_sqlite_provider(executor.clone());

    // Register the executor and the audited graph-save capability.
    let data_service = SqlDataServiceExecutor::new(SqliteDialect, executor, metadata());
    context.register_executor(data_service);
    context
}
