
use crate::*;
use teaql_core::TeaqlEntity;

use teaql_provider_sqlite::SqliteProviderExt as _;

pub type DataServiceDialect = teaql_provider_sqlite::SqliteDialect;
pub type DataServiceMutationExecutor = teaql_provider_sqlite::SqliteMutationExecutor;
pub type DataServiceMutationError = teaql_provider_sqlite::MutationExecutorError;
pub type DataServiceIdGenerator = teaql_provider_sqlite::SqliteIdSpaceGenerator;
pub type DataServicePool = std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>;
pub type DataServiceExecutor = ServiceRuntimeExecutor;
pub type ServiceRuntime = teaql_runtime::UserContext;

pub const DATABASE_URL_ENV: &str = "ORDER_MANAGEMENT_SERVICE_CORE_DATABASE_URL";
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceRuntimeConfig {
    pub database_url: String,
}

impl ServiceRuntimeConfig {
    pub fn from_env() -> Result<Self, ServiceRuntimeError> {
        Ok(Self {
            database_url: env_value(DATABASE_URL_ENV)?,
        })
    }
}

#[derive(Debug)]
pub enum ServiceRuntimeError {
    MissingEnv {
        name: &'static str,
        source: std::env::VarError,
    },
    ConnectionError(String),
    Rusqlite(rusqlite::Error),
    Runtime(teaql_runtime::RuntimeError),
}

impl std::fmt::Display for ServiceRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceRuntimeError::MissingEnv { name, source } => {
                write!(f, "missing environment variable {name}: {source}")
            }
            ServiceRuntimeError::ConnectionError(err) => write!(f, "connection error: {err}"),
            ServiceRuntimeError::Rusqlite(err) => write!(f, "rusqlite error: {err}"),
            ServiceRuntimeError::Runtime(err) => write!(f, "runtime error: {err}"),
        }
    }
}

impl std::error::Error for ServiceRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ServiceRuntimeError::MissingEnv { source, .. } => Some(source),
            ServiceRuntimeError::ConnectionError(_) => None,
            ServiceRuntimeError::Rusqlite(err) => Some(err),
            ServiceRuntimeError::Runtime(err) => Some(err),
        }
    }
}

impl From<rusqlite::Error> for ServiceRuntimeError {
    fn from(err: rusqlite::Error) -> Self {
        ServiceRuntimeError::Rusqlite(err)
    }
}
impl From<teaql_runtime::RuntimeError> for ServiceRuntimeError {
    fn from(err: teaql_runtime::RuntimeError) -> Self {
        ServiceRuntimeError::Runtime(err)
    }
}

#[derive(Clone)]
pub struct LocalSchemaProvider;

impl teaql_data_service::SchemaProvider for LocalSchemaProvider {
    fn get_entity(&self, name: &str) -> Option<std::sync::Arc<teaql_core::EntityDescriptor>> {
        match name {
            "CommercePlatform" => Some(std::sync::Arc::new(crate::CommercePlatform::entity_descriptor())),
            "Customer" => Some(std::sync::Arc::new(crate::Customer::entity_descriptor())),
            "OrderStatus" => Some(std::sync::Arc::new(crate::OrderStatus::entity_descriptor())),
            "CustomerOrder" => Some(std::sync::Arc::new(crate::CustomerOrder::entity_descriptor())),
            "Product" => Some(std::sync::Arc::new(crate::Product::entity_descriptor())),
            "OrderLine" => Some(std::sync::Arc::new(crate::OrderLine::entity_descriptor())),
            "OrderSearchPreset" => Some(std::sync::Arc::new(crate::OrderSearchPreset::entity_descriptor())),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ServiceRuntimeExecutor {
    inner: teaql_sql::SqlDataServiceExecutor<
        DataServiceDialect,
        DataServiceMutationExecutor,
        LocalSchemaProvider
    >,
}

impl ServiceRuntimeExecutor {
    pub fn new(inner: DataServiceMutationExecutor) -> Self {
        Self {
            inner: teaql_sql::SqlDataServiceExecutor::new(
                DataServiceDialect::default(),
                inner,
                LocalSchemaProvider
            ),
        }
    }

}

impl teaql_data_service::DataServiceExecutor for ServiceRuntimeExecutor {
    type Error = teaql_sql::SqlExecutorError<DataServiceMutationError>;
    fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
        teaql_data_service::DataServiceExecutor::capabilities(&self.inner)
    }
}

impl teaql_data_service::QueryExecutor for ServiceRuntimeExecutor {
    async fn query(&self, request: teaql_data_service::QueryRequest) -> Result<teaql_data_service::QueryResult, Self::Error> {
        teaql_data_service::QueryExecutor::query(&self.inner, request).await
    }
}

impl teaql_data_service::StreamQueryExecutor for ServiceRuntimeExecutor {
    fn query_stream(&self, request: teaql_data_service::QueryRequest, chunk_size: usize) -> teaql_data_service::QueryStream<'_, Self::Error> {
        teaql_data_service::StreamQueryExecutor::query_stream(&self.inner, request, chunk_size)
    }
}

impl teaql_data_service::MutationExecutor for ServiceRuntimeExecutor {
    async fn mutate(&self, request: teaql_data_service::MutationRequest) -> Result<teaql_data_service::MutationResult, Self::Error> {
        teaql_data_service::MutationExecutor::mutate(&self.inner, request).await
    }
}

impl teaql_data_service::TransactionExecutor for ServiceRuntimeExecutor {
    type Tx<'a> = teaql_sql::SqlDataServiceTransaction<'a, DataServiceDialect, <DataServiceMutationExecutor as teaql_sql::SqlTransactionTransport>::Tx<'a>, LocalSchemaProvider> where Self: 'a;

    async fn begin(&self) -> Result<Self::Tx<'_ >, Self::Error> {
        teaql_data_service::TransactionExecutor::begin(&self.inner).await
    }
}

pub async fn service_runtime_from_env() -> Result<ServiceRuntime, ServiceRuntimeError> {
    service_runtime(ServiceRuntimeConfig::from_env()?).await
}

pub async fn service_runtime(config: ServiceRuntimeConfig) -> Result<ServiceRuntime, ServiceRuntimeError> {
    let pool = connect_data_service_pool(&config).await?;
    service_runtime_from_pool(pool).await
}

pub async fn service_runtime_from_pool(pool: DataServicePool) -> Result<ServiceRuntime, ServiceRuntimeError> {
    let mutation_executor = DataServiceMutationExecutor::new(pool);
    let id_generator = DataServiceIdGenerator::from_executor(mutation_executor.clone());let mut context = module_with_behaviors_and_checkers().into_context();
    context.set_internal_id_generator(id_generator);
    context.use_sqlite_provider(mutation_executor.clone());
    let executor = ServiceRuntimeExecutor::new(mutation_executor);
    context.register_executor(executor.clone());
    context.insert_resource(executor);

    // Load runtime configuration only. Schema installation is an explicit application action.
    let env_config = teaql_tool_core::audit_config_from_env(&[
        "commerce_platform_data", "customer_data", "order_status_data", "customer_order_data", "product_data", "order_line_data", "order_search_preset_data"
    ]);
    context.insert_resource(env_config.config.clone());
    context.insert_resource(env_config);

    Ok(context)
}



fn env_value(name: &'static str) -> Result<String, ServiceRuntimeError> {
    std::env::var(name).map_err(|source| ServiceRuntimeError::MissingEnv { name, source })
}

async fn connect_data_service_pool(config: &ServiceRuntimeConfig) -> Result<DataServicePool, ServiceRuntimeError> {
    let url = &config.database_url;
    let sanitized_url = if url.starts_with("sqlite:") { url.strip_prefix("sqlite:").unwrap().trim_start_matches("//") } else { url };
    let pure_file_path = sanitized_url.split('?').next().unwrap_or(sanitized_url);
    let path = std::path::Path::new(pure_file_path);
    if let Some(parent) = path.parent() { if !parent.as_os_str().is_empty() { std::fs::create_dir_all(parent).map_err(|e| ServiceRuntimeError::ConnectionError(e.to_string()))?; } }
    Ok(std::sync::Arc::new(std::sync::Mutex::new(rusqlite::Connection::open(pure_file_path).map_err(|e| ServiceRuntimeError::ConnectionError(e.to_string()))?)))
}

pub fn repository_registry() -> teaql_runtime::InMemoryEntityRegistry {
    teaql_runtime::InMemoryEntityRegistry::new()
        .with_entity("CommercePlatform")
        .with_entity("Customer")
        .with_entity("OrderStatus")
        .with_entity("CustomerOrder")
        .with_entity("Product")
        .with_entity("OrderLine")
        .with_entity("OrderSearchPreset")
}

pub fn behavior_registry() -> teaql_runtime::InMemoryEntityDataServiceBehaviorRegistry {
    teaql_runtime::InMemoryEntityDataServiceBehaviorRegistry::new()
        .with_behavior("CommercePlatform", CommercePlatformBehavior::default())
        .with_behavior("Customer", CustomerBehavior::default())
        .with_behavior("OrderStatus", OrderStatusBehavior::default())
        .with_behavior("CustomerOrder", CustomerOrderBehavior::default())
        .with_behavior("Product", ProductBehavior::default())
        .with_behavior("OrderLine", OrderLineBehavior::default())
        .with_behavior("OrderSearchPreset", OrderSearchPresetBehavior::default())
}

pub fn checker_registry() -> teaql_runtime::InMemoryCheckerRegistry {
    teaql_runtime::InMemoryCheckerRegistry::new()
        .with_checker(teaql_runtime::TypedEntityChecker::<CommercePlatform, _>::new(CommercePlatformChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<Customer, _>::new(CustomerChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<OrderStatus, _>::new(OrderStatusChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<CustomerOrder, _>::new(CustomerOrderChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<Product, _>::new(ProductChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<OrderLine, _>::new(OrderLineChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<OrderSearchPreset, _>::new(OrderSearchPresetChecker::default()))
}

fn ensure_generated_bootstrap<'a>(context: &'a teaql_runtime::UserContext) -> teaql_runtime::GeneratedSchemaBootstrapFuture<'a> {
    Box::pin(async move {
        use teaql_core::Entity as _;
        let root_rows = crate::Q::commerce_platforms().select_self_fields().with_id_is(1_u64).comment("what: locate generated Domain Root").purpose("why: idempotent runtime bootstrap").execute_for_list(context).await.map_err(|e| teaql_runtime::RuntimeError::Graph(e.to_string()))?;
        let domain_root = if let Some(entity) = root_rows.data.into_iter().next() { entity } else {
            let mut entity = CommercePlatform::runtime_new(context.entity_runtime_state());
            entity.update_id(1_u64);
            context.initialize_generated_bootstrap_entity(&mut entity, CommercePlatform::ENTITY_NAME, 1_u64)?;
            entity.update_name("Northwind Demo");
            teaql_runtime::AuditedSaveExt::save(entity.audit_as("create generated Domain Root CommercePlatform"), context).await?
        };
        context.set_generated_bootstrap_active_root(CommercePlatform::ENTITY_NAME, domain_root.id())?;
        let rows_constant_order_status_1001 = crate::Q::order_statuses().select_self_fields().with_id_is(1001_u64).comment("what: locate generated constant").purpose("why: idempotent runtime bootstrap").execute_for_list(context).await.map_err(|e| teaql_runtime::RuntimeError::Graph(e.to_string()))?;
        if let Some(mut constant_order_status_1001) = rows_constant_order_status_1001.data.into_iter().next() {
            let mut changed = false;
            if constant_order_status_1001.name() != "Pending" { constant_order_status_1001.update_name("Pending"); changed = true; }
            if constant_order_status_1001.code() != "PENDING" { constant_order_status_1001.update_code("PENDING"); changed = true; }
            if constant_order_status_1001.color() != Some(("#F59E0B").into()) { constant_order_status_1001.update_color("#F59E0B"); changed = true; }
            if constant_order_status_1001.display_order() != Some((rust_decimal::Decimal::from_str_exact("1").unwrap()).into()) { constant_order_status_1001.update_display_order(rust_decimal::Decimal::from_str_exact("1").unwrap()); changed = true; }
            if constant_order_status_1001.commerce_platform_id() != 1_u64 { constant_order_status_1001.update_commerce_platform_id(1_u64); changed = true; }
            if changed { let _ = teaql_runtime::AuditedSaveExt::save(constant_order_status_1001.audit_as("reconcile model constant OrderStatus(1001)"), context).await?; }
        } else {
            let mut constant_order_status_1001 = OrderStatus::runtime_new(context.entity_runtime_state());
            constant_order_status_1001.update_id(1001_u64);
            context.initialize_generated_bootstrap_entity(&mut constant_order_status_1001, OrderStatus::ENTITY_NAME, 1001_u64)?;
            constant_order_status_1001.update_name("Pending");
            constant_order_status_1001.update_code("PENDING");
            constant_order_status_1001.update_color("#F59E0B");
            constant_order_status_1001.update_display_order(rust_decimal::Decimal::from_str_exact("1").unwrap());
            constant_order_status_1001.update_commerce_platform_id(1_u64);
            let _ = teaql_runtime::AuditedSaveExt::save(constant_order_status_1001.audit_as("create model constant OrderStatus(1001)"), context).await?;
        }
        let rows_constant_order_status_1002 = crate::Q::order_statuses().select_self_fields().with_id_is(1002_u64).comment("what: locate generated constant").purpose("why: idempotent runtime bootstrap").execute_for_list(context).await.map_err(|e| teaql_runtime::RuntimeError::Graph(e.to_string()))?;
        if let Some(mut constant_order_status_1002) = rows_constant_order_status_1002.data.into_iter().next() {
            let mut changed = false;
            if constant_order_status_1002.name() != "Confirmed" { constant_order_status_1002.update_name("Confirmed"); changed = true; }
            if constant_order_status_1002.code() != "CONFIRMED" { constant_order_status_1002.update_code("CONFIRMED"); changed = true; }
            if constant_order_status_1002.color() != Some(("#10B981").into()) { constant_order_status_1002.update_color("#10B981"); changed = true; }
            if constant_order_status_1002.display_order() != Some((rust_decimal::Decimal::from_str_exact("2").unwrap()).into()) { constant_order_status_1002.update_display_order(rust_decimal::Decimal::from_str_exact("2").unwrap()); changed = true; }
            if constant_order_status_1002.commerce_platform_id() != 1_u64 { constant_order_status_1002.update_commerce_platform_id(1_u64); changed = true; }
            if changed { let _ = teaql_runtime::AuditedSaveExt::save(constant_order_status_1002.audit_as("reconcile model constant OrderStatus(1002)"), context).await?; }
        } else {
            let mut constant_order_status_1002 = OrderStatus::runtime_new(context.entity_runtime_state());
            constant_order_status_1002.update_id(1002_u64);
            context.initialize_generated_bootstrap_entity(&mut constant_order_status_1002, OrderStatus::ENTITY_NAME, 1002_u64)?;
            constant_order_status_1002.update_name("Confirmed");
            constant_order_status_1002.update_code("CONFIRMED");
            constant_order_status_1002.update_color("#10B981");
            constant_order_status_1002.update_display_order(rust_decimal::Decimal::from_str_exact("2").unwrap());
            constant_order_status_1002.update_commerce_platform_id(1_u64);
            let _ = teaql_runtime::AuditedSaveExt::save(constant_order_status_1002.audit_as("create model constant OrderStatus(1002)"), context).await?;
        }
        Ok(())
    })
}


pub fn module() -> teaql_runtime::RuntimeModule {
    teaql_runtime::RuntimeModule::new()
        .entity::<CommercePlatform>()
        .entity::<Customer>()
        .entity::<OrderStatus>()
        .entity::<CustomerOrder>()
        .entity::<Product>()
        .entity::<OrderLine>()
        .entity::<OrderSearchPreset>()
        .generated_schema_bootstrap(ensure_generated_bootstrap)
}

pub fn module_with_checkers() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new();
    module = module.entity::<CommercePlatform>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<CommercePlatform, _>::new(CommercePlatformChecker::default()));
    module = module.entity::<Customer>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<Customer, _>::new(CustomerChecker::default()));
    module = module.entity::<OrderStatus>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<OrderStatus, _>::new(OrderStatusChecker::default()));
    module = module.entity::<CustomerOrder>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<CustomerOrder, _>::new(CustomerOrderChecker::default()));
    module = module.entity::<Product>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<Product, _>::new(ProductChecker::default()));
    module = module.entity::<OrderLine>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<OrderLine, _>::new(OrderLineChecker::default()));
    module = module.entity::<OrderSearchPreset>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<OrderSearchPreset, _>::new(OrderSearchPresetChecker::default()));
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}

pub fn module_with_behaviors() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new();
    module = module.entity_with_behavior::<CommercePlatform, _>(CommercePlatformBehavior::default());
    module = module.entity_with_behavior::<Customer, _>(CustomerBehavior::default());
    module = module.entity_with_behavior::<OrderStatus, _>(OrderStatusBehavior::default());
    module = module.entity_with_behavior::<CustomerOrder, _>(CustomerOrderBehavior::default());
    module = module.entity_with_behavior::<Product, _>(ProductBehavior::default());
    module = module.entity_with_behavior::<OrderLine, _>(OrderLineBehavior::default());
    module = module.entity_with_behavior::<OrderSearchPreset, _>(OrderSearchPresetBehavior::default());
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}

pub fn module_with_behaviors_and_checkers() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new();
    module = module.entity_with_behavior::<CommercePlatform, _>(CommercePlatformBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<CommercePlatform, _>::new(CommercePlatformChecker::default()));
    module = module.entity_with_behavior::<Customer, _>(CustomerBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<Customer, _>::new(CustomerChecker::default()));
    module = module.entity_with_behavior::<OrderStatus, _>(OrderStatusBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<OrderStatus, _>::new(OrderStatusChecker::default()));
    module = module.entity_with_behavior::<CustomerOrder, _>(CustomerOrderBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<CustomerOrder, _>::new(CustomerOrderChecker::default()));
    module = module.entity_with_behavior::<Product, _>(ProductBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<Product, _>::new(ProductChecker::default()));
    module = module.entity_with_behavior::<OrderLine, _>(OrderLineBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<OrderLine, _>::new(OrderLineChecker::default()));
    module = module.entity_with_behavior::<OrderSearchPreset, _>(OrderSearchPresetBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<OrderSearchPreset, _>::new(OrderSearchPresetChecker::default()));
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}
