
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
    Configuration(String),
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
            ServiceRuntimeError::Configuration(err) => write!(f, "configuration error: {err}"),
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
            ServiceRuntimeError::Configuration(_) => None,
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
    query_metadata_observer: Option<std::sync::Arc<dyn Fn(&teaql_data_service::ExecutionMetadata) + Send + Sync>>,
}

impl ServiceRuntimeExecutor {
    pub fn new(inner: DataServiceMutationExecutor) -> Self {
        Self {
            inner: teaql_sql::SqlDataServiceExecutor::new(
                DataServiceDialect::default(),
                inner,
                LocalSchemaProvider
            ),
            query_metadata_observer: None,
        }
    }

    /// Trusted provider/test SPI, disabled by default. Metadata can contain raw
    /// binds: never send it to operator logs or an untrusted transport. Normal
    /// SQL/audit sinks retain their runtime-owned safe projections.
    #[doc(hidden)]
    pub fn with_query_metadata_observer(mut self, observer: std::sync::Arc<dyn Fn(&teaql_data_service::ExecutionMetadata) + Send + Sync>) -> Self {
        self.query_metadata_observer = Some(observer);
        self
    }

    fn notify_query_metadata(&self, metadata: &teaql_data_service::ExecutionMetadata) {
        if let Some(observer) = &self.query_metadata_observer {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(metadata)));
        }
    }

    fn query_metadata_observer<'a>(
        &self, upstream: Option<teaql_data_service::ExecutionObserver<'a>>,
    ) -> Option<teaql_data_service::ExecutionObserver<'a>> {
        match &self.query_metadata_observer {
            Some(callback) => {
                let callback = callback.clone();
                Some(std::sync::Arc::new(move |metadata| {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(&metadata)));
                    if let Some(upstream) = &upstream { upstream(metadata); }
                }))
            }
            None => upstream,
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
    fn query_log_intent(&self, query: &teaql_core::SelectQuery) -> teaql_data_service::SqlIntentRedactions {
        teaql_data_service::QueryExecutor::query_log_intent(&self.inner, query)
    }

    fn dynamic_field_store(&self) -> Option<&dyn teaql_data_service::dynamic_fields::DynamicFieldStore> {
        teaql_data_service::QueryExecutor::dynamic_field_store(&self.inner)
    }

    async fn query(&self, request: teaql_data_service::QueryRequest) -> Result<teaql_data_service::QueryResult, Self::Error> {
        teaql_data_service::QueryExecutor::query_observed(self, request, None).await
    }

    fn query_observed<'a>(
        &'a self,
        request: teaql_data_service::QueryRequest,
        observer: Option<teaql_data_service::ExecutionObserver<'a>>,
    ) -> impl std::future::Future<Output = Result<teaql_data_service::QueryResult, Self::Error>> + Send + 'a {
        async move {
        let observer = self.query_metadata_observer(observer);
        let result = teaql_data_service::QueryExecutor::query_observed(&self.inner, request, observer).await?;
        self.notify_query_metadata(&result.metadata);
        Ok(result)
        }
    }
}

impl teaql_data_service::StreamQueryExecutor for ServiceRuntimeExecutor {
    fn query_stream_with_dynamic_fields<'a>(
        &'a self,
        request: teaql_data_service::QueryRequest,
        chunk_size: usize,
        plan: teaql_data_service::dynamic_fields::DynamicFieldStreamPlan,
        observer: Option<teaql_data_service::ExecutionObserver<'a>>,
    ) -> Result<teaql_data_service::QueryStream<'a, Self::Error>, teaql_core::dynamic_fields::DynamicFieldError> {
        let observer = self.query_metadata_observer(observer);
        teaql_data_service::StreamQueryExecutor::query_stream_with_dynamic_fields(
            &self.inner, request, chunk_size, plan, observer,
        )
    }

    fn query_stream_observed<'a>(
        &'a self,
        request: teaql_data_service::QueryRequest,
        chunk_size: usize,
        observer: teaql_data_service::ExecutionObserver<'a>,
    ) -> teaql_data_service::QueryStream<'a, Self::Error> {
        let observer = self.query_metadata_observer(Some(observer)).expect("caller supplied stream observer");
        teaql_data_service::StreamQueryExecutor::query_stream_observed(&self.inner, request, chunk_size, observer)
    }

    fn query_stream(&self, request: teaql_data_service::QueryRequest, chunk_size: usize) -> teaql_data_service::QueryStream<'_, Self::Error> {
        if let Some(observer) = self.query_metadata_observer(None) {
            teaql_data_service::StreamQueryExecutor::query_stream_observed(&self.inner, request, chunk_size, observer)
        } else {
            teaql_data_service::StreamQueryExecutor::query_stream(&self.inner, request, chunk_size)
        }
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

    // SQL logging is owned by teaql-runtime. The legacy teaql-tool-core
    // environment whitelist rejects runtime logging variables and must not
    // gate generated application startup. Schema remains explicit.

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

fn is_generated_bootstrap_retryable_conflict(error: &teaql_runtime::RuntimeError) -> bool {
    if matches!(error, teaql_runtime::RuntimeError::OptimisticLockConflict { .. }) { return true }
    let teaql_runtime::RuntimeError::Graph(message) = error else { return false };
    (message.contains("UNIQUE constraint failed:") && message.contains("_data.id"))
        || (message.contains("duplicate key value violates unique constraint") && message.contains("_data_pkey"))
        || (message.contains("Duplicate entry") && message.contains("PRIMARY"))
}

fn ensure_generated_bootstrap<'a>(context: &'a teaql_runtime::UserContext) -> teaql_runtime::GeneratedSchemaBootstrapFuture<'a> {
    Box::pin(async move {
        for attempt in 0..5 {
            match ensure_generated_bootstrap_once(context).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt < 4 && is_generated_bootstrap_retryable_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        unreachable!("bootstrap attempts always return or fail")
    })
}

async fn ensure_generated_bootstrap_once(context: &teaql_runtime::UserContext) -> Result<(), teaql_runtime::RuntimeError> {
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
}


/// Canonical KSML field to selected JSON wire name, consumed by HTTP/TFP adapters.
pub fn generated_wire_field_mappings() -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>> {
    std::collections::BTreeMap::from([
        ("CommercePlatform".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("create_time".to_owned(), "createTime".to_owned()),
            ("update_time".to_owned(), "updateTime".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("Customer".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("email".to_owned(), "email".to_owned()),
            ("commerce_platform".to_owned(), "commercePlatform".to_owned()),
            ("create_time".to_owned(), "createTime".to_owned()),
            ("update_time".to_owned(), "updateTime".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("OrderStatus".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("code".to_owned(), "code".to_owned()),
            ("color".to_owned(), "color".to_owned()),
            ("display_order".to_owned(), "displayOrder".to_owned()),
            ("commerce_platform".to_owned(), "commercePlatform".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("CustomerOrder".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("order_number".to_owned(), "orderNumber".to_owned()),
            ("order_date".to_owned(), "orderDate".to_owned()),
            ("total_amount".to_owned(), "totalAmount".to_owned()),
            ("status".to_owned(), "status".to_owned()),
            ("customer".to_owned(), "customer".to_owned()),
            ("commerce_platform".to_owned(), "commercePlatform".to_owned()),
            ("create_time".to_owned(), "createTime".to_owned()),
            ("update_time".to_owned(), "updateTime".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("Product".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("sku".to_owned(), "sku".to_owned()),
            ("image_url".to_owned(), "imageUrl".to_owned()),
            ("commerce_platform".to_owned(), "commercePlatform".to_owned()),
            ("create_time".to_owned(), "createTime".to_owned()),
            ("update_time".to_owned(), "updateTime".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("OrderLine".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("customer_order".to_owned(), "customerOrder".to_owned()),
            ("product".to_owned(), "product".to_owned()),
            ("product_name".to_owned(), "productName".to_owned()),
            ("sku".to_owned(), "sku".to_owned()),
            ("quantity".to_owned(), "quantity".to_owned()),
            ("commerce_platform".to_owned(), "commercePlatform".to_owned()),
            ("create_time".to_owned(), "createTime".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("OrderSearchPreset".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("filter_json".to_owned(), "filterJson".to_owned()),
            ("request_id".to_owned(), "requestId".to_owned()),
            ("owner_user_id".to_owned(), "ownerUserId".to_owned()),
            ("commerce_platform".to_owned(), "commercePlatform".to_owned()),
            ("create_time".to_owned(), "createTime".to_owned()),
            ("update_time".to_owned(), "updateTime".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ]))
    ])
}

/// Accepted legacy aliases; empty until explicitly declared by the model.
pub fn generated_wire_field_aliases() -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>> {
    std::collections::BTreeMap::new()
}

/// One immutable model schema shared across contexts, never across row values.
pub fn generated_entity_json_model() -> &'static teaql_runtime::entity_json::Model {
    use teaql_runtime::entity_json::{Definition, Field, Kind};
    static MODEL: std::sync::OnceLock<teaql_runtime::entity_json::Model> = std::sync::OnceLock::new();
    MODEL.get_or_init(|| teaql_runtime::entity_json::Model::new(vec![
        Definition::new("CommercePlatform", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("name", "name", Kind::Text).nullable(false),
            Field::new("create_time", "create_time", Kind::DateTime).nullable(false),
            Field::new("update_time", "update_time", Kind::DateTime).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("customer_list", "customer_list", Kind::Many { target: "Customer".to_owned() }),
            Field::new("order_status_list", "order_status_list", Kind::Many { target: "OrderStatus".to_owned() }),
            Field::new("customer_order_list", "customer_order_list", Kind::Many { target: "CustomerOrder".to_owned() }),
            Field::new("product_list", "product_list", Kind::Many { target: "Product".to_owned() }),
            Field::new("order_line_list", "order_line_list", Kind::Many { target: "OrderLine".to_owned() }),
            Field::new("order_search_preset_list", "order_search_preset_list", Kind::Many { target: "OrderSearchPreset".to_owned() }),
        ]),
        Definition::new("Customer", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("name", "name", Kind::Text).nullable(false),
            Field::new("email", "email", Kind::Text).nullable(false),
            Field::new("commerce_platform", "commerce_platform_id", Kind::Reference { target: "CommercePlatform".to_owned(), detail_key: "commerce_platform".to_owned() }).nullable(false),
            Field::new("create_time", "create_time", Kind::DateTime).nullable(false),
            Field::new("update_time", "update_time", Kind::DateTime).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("customer_order_list", "customer_order_list", Kind::Many { target: "CustomerOrder".to_owned() }),
        ]),
        Definition::new("OrderStatus", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("name", "name", Kind::Text).nullable(false),
            Field::new("code", "code", Kind::Text).nullable(false),
            Field::new("color", "color", Kind::Text).nullable(true),
            Field::new("display_order", "display_order", Kind::Decimal).nullable(true),
            Field::new("commerce_platform", "commerce_platform_id", Kind::Reference { target: "CommercePlatform".to_owned(), detail_key: "commerce_platform".to_owned() }).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("customer_order_list", "customer_order_list", Kind::Many { target: "CustomerOrder".to_owned() }),
        ]),
        Definition::new("CustomerOrder", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("order_number", "order_number", Kind::Text).nullable(false),
            Field::new("order_date", "order_date", Kind::Date).nullable(false),
            Field::new("total_amount", "total_amount", Kind::Decimal).nullable(false),
            Field::new("status", "status_id", Kind::Reference { target: "OrderStatus".to_owned(), detail_key: "status".to_owned() }).nullable(false),
            Field::new("customer", "customer_id", Kind::Reference { target: "Customer".to_owned(), detail_key: "customer".to_owned() }).nullable(false),
            Field::new("commerce_platform", "commerce_platform_id", Kind::Reference { target: "CommercePlatform".to_owned(), detail_key: "commerce_platform".to_owned() }).nullable(false),
            Field::new("create_time", "create_time", Kind::DateTime).nullable(false),
            Field::new("update_time", "update_time", Kind::DateTime).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("order_line_list", "order_line_list", Kind::Many { target: "OrderLine".to_owned() }),
        ]),
        Definition::new("Product", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("name", "name", Kind::Text).nullable(false),
            Field::new("sku", "sku", Kind::Text).nullable(false),
            Field::new("image_url", "image_url", Kind::Text).nullable(true),
            Field::new("commerce_platform", "commerce_platform_id", Kind::Reference { target: "CommercePlatform".to_owned(), detail_key: "commerce_platform".to_owned() }).nullable(false),
            Field::new("create_time", "create_time", Kind::DateTime).nullable(false),
            Field::new("update_time", "update_time", Kind::DateTime).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("order_line_list", "order_line_list", Kind::Many { target: "OrderLine".to_owned() }),
        ]),
        Definition::new("OrderLine", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("customer_order", "customer_order_id", Kind::Reference { target: "CustomerOrder".to_owned(), detail_key: "customer_order".to_owned() }).nullable(false),
            Field::new("product", "product_id", Kind::Reference { target: "Product".to_owned(), detail_key: "product".to_owned() }).nullable(false),
            Field::new("product_name", "product_name", Kind::Text).nullable(false),
            Field::new("sku", "sku", Kind::Text).nullable(false),
            Field::new("quantity", "quantity", Kind::Integer).nullable(false),
            Field::new("commerce_platform", "commerce_platform_id", Kind::Reference { target: "CommercePlatform".to_owned(), detail_key: "commerce_platform".to_owned() }).nullable(false),
            Field::new("create_time", "create_time", Kind::DateTime).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
        ]),
        Definition::new("OrderSearchPreset", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("name", "name", Kind::Text).nullable(false),
            Field::new("filter_json", "filter_json", Kind::Text).nullable(false),
            Field::new("request_id", "request_id", Kind::Text).nullable(false),
            Field::new("owner_user_id", "owner_user_id", Kind::Text).nullable(false),
            Field::new("commerce_platform", "commerce_platform_id", Kind::Reference { target: "CommercePlatform".to_owned(), detail_key: "commerce_platform".to_owned() }).nullable(false),
            Field::new("create_time", "create_time", Kind::DateTime).nullable(false),
            Field::new("update_time", "update_time", Kind::DateTime).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
        ])
    ]).expect("invalid generated entity JSON metadata"))
}

pub fn generated_json_field_naming_profile() -> teaql_runtime::JsonFieldNamingProfile {
    teaql_runtime::JsonFieldNamingProfile::CamelCase
}

fn generated_checker_presentation() -> std::sync::Arc<teaql_runtime::checker_messages::CheckerPresentationModel> {
    static MODEL: std::sync::OnceLock<std::sync::Arc<teaql_runtime::checker_messages::CheckerPresentationModel>> = std::sync::OnceLock::new();
    MODEL.get_or_init(|| std::sync::Arc::new(
        teaql_runtime::checker_messages::CheckerPresentationModel::from_json("{\"schema\":\"teaql.checker-model/v1\",\"revision\":\"sha256:feb0a6c97f37d35282317bf77fbd60ffde5a4dc18d6f6a371fd40d2c37d6c975\",\"entities\":{\"CommercePlatform\":{\"labels\":{\"en\":\"commerce platform\"},\"fields\":[{\"name\":\"create_time\",\"labels\":{\"en\":\"create time\"},\"aliases\":[\"CreateTime\",\"createTime\"]},{\"name\":\"customer_list\",\"targetType\":\"Customer\",\"labels\":{\"en\":\"customer\"},\"aliases\":[\"CustomerList\",\"customerList\"]},{\"name\":\"customer_order_list\",\"targetType\":\"CustomerOrder\",\"labels\":{\"en\":\"customer order\"},\"aliases\":[\"CustomerOrderList\",\"customerOrderList\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"name\",\"labels\":{\"en\":\"name\"},\"aliases\":[\"Name\"]},{\"name\":\"order_line_list\",\"targetType\":\"OrderLine\",\"labels\":{\"en\":\"order line\"},\"aliases\":[\"OrderLineList\",\"orderLineList\"]},{\"name\":\"order_search_preset_list\",\"targetType\":\"OrderSearchPreset\",\"labels\":{\"en\":\"order search preset\"},\"aliases\":[\"OrderSearchPresetList\",\"orderSearchPresetList\"]},{\"name\":\"order_status_list\",\"targetType\":\"OrderStatus\",\"labels\":{\"en\":\"order status\"},\"aliases\":[\"OrderStatusList\",\"orderStatusList\"]},{\"name\":\"product_list\",\"targetType\":\"Product\",\"labels\":{\"en\":\"product\"},\"aliases\":[\"ProductList\",\"productList\"]},{\"name\":\"update_time\",\"labels\":{\"en\":\"update time\"},\"aliases\":[\"UpdateTime\",\"updateTime\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"Customer\":{\"labels\":{\"en\":\"customer\"},\"fields\":[{\"name\":\"commerce_platform\",\"targetType\":\"CommercePlatform\",\"labels\":{\"en\":\"commerce platform\"},\"aliases\":[\"CommercePlatform\",\"commercePlatform\"]},{\"name\":\"create_time\",\"labels\":{\"en\":\"create time\"},\"aliases\":[\"CreateTime\",\"createTime\"]},{\"name\":\"customer_order_list\",\"targetType\":\"CustomerOrder\",\"labels\":{\"en\":\"customer order\"},\"aliases\":[\"CustomerOrderList\",\"customerOrderList\"]},{\"name\":\"email\",\"labels\":{\"en\":\"email\"},\"aliases\":[\"Email\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"name\",\"labels\":{\"en\":\"name\"},\"aliases\":[\"Name\"]},{\"name\":\"update_time\",\"labels\":{\"en\":\"update time\"},\"aliases\":[\"UpdateTime\",\"updateTime\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"CustomerOrder\":{\"labels\":{\"en\":\"customer order\"},\"fields\":[{\"name\":\"commerce_platform\",\"targetType\":\"CommercePlatform\",\"labels\":{\"en\":\"commerce platform\"},\"aliases\":[\"CommercePlatform\",\"commercePlatform\"]},{\"name\":\"create_time\",\"labels\":{\"en\":\"create time\"},\"aliases\":[\"CreateTime\",\"createTime\"]},{\"name\":\"customer\",\"targetType\":\"Customer\",\"labels\":{\"en\":\"customer\"},\"aliases\":[\"Customer\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"order_date\",\"labels\":{\"en\":\"order date\"},\"aliases\":[\"OrderDate\",\"orderDate\"]},{\"name\":\"order_line_list\",\"targetType\":\"OrderLine\",\"labels\":{\"en\":\"order line\"},\"aliases\":[\"OrderLineList\",\"orderLineList\"]},{\"name\":\"order_number\",\"labels\":{\"en\":\"order number\"},\"aliases\":[\"OrderNumber\",\"orderNumber\"]},{\"name\":\"status\",\"targetType\":\"OrderStatus\",\"labels\":{\"en\":\"status\"},\"aliases\":[\"Status\"]},{\"name\":\"total_amount\",\"labels\":{\"en\":\"total amount\"},\"aliases\":[\"TotalAmount\",\"totalAmount\"]},{\"name\":\"update_time\",\"labels\":{\"en\":\"update time\"},\"aliases\":[\"UpdateTime\",\"updateTime\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"OrderLine\":{\"labels\":{\"en\":\"order line\"},\"fields\":[{\"name\":\"commerce_platform\",\"targetType\":\"CommercePlatform\",\"labels\":{\"en\":\"commerce platform\"},\"aliases\":[\"CommercePlatform\",\"commercePlatform\"]},{\"name\":\"create_time\",\"labels\":{\"en\":\"create time\"},\"aliases\":[\"CreateTime\",\"createTime\"]},{\"name\":\"customer_order\",\"targetType\":\"CustomerOrder\",\"labels\":{\"en\":\"customer order\"},\"aliases\":[\"CustomerOrder\",\"customerOrder\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"product\",\"targetType\":\"Product\",\"labels\":{\"en\":\"product\"},\"aliases\":[\"Product\"]},{\"name\":\"product_name\",\"labels\":{\"en\":\"product name\"},\"aliases\":[\"ProductName\",\"productName\"]},{\"name\":\"quantity\",\"labels\":{\"en\":\"quantity\"},\"aliases\":[\"Quantity\"]},{\"name\":\"sku\",\"labels\":{\"en\":\"sku\"},\"aliases\":[\"Sku\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"OrderSearchPreset\":{\"labels\":{\"en\":\"order search preset\"},\"fields\":[{\"name\":\"commerce_platform\",\"targetType\":\"CommercePlatform\",\"labels\":{\"en\":\"commerce platform\"},\"aliases\":[\"CommercePlatform\",\"commercePlatform\"]},{\"name\":\"create_time\",\"labels\":{\"en\":\"create time\"},\"aliases\":[\"CreateTime\",\"createTime\"]},{\"name\":\"filter_json\",\"labels\":{\"en\":\"filter json\"},\"aliases\":[\"FilterJson\",\"filterJson\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"name\",\"labels\":{\"en\":\"name\"},\"aliases\":[\"Name\"]},{\"name\":\"owner_user_id\",\"labels\":{\"en\":\"owner user id\"},\"aliases\":[\"OwnerUserId\",\"ownerUserId\"]},{\"name\":\"request_id\",\"labels\":{\"en\":\"request id\"},\"aliases\":[\"RequestId\",\"requestId\"]},{\"name\":\"update_time\",\"labels\":{\"en\":\"update time\"},\"aliases\":[\"UpdateTime\",\"updateTime\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"OrderStatus\":{\"labels\":{\"en\":\"order status\"},\"fields\":[{\"name\":\"code\",\"labels\":{\"en\":\"code\"},\"aliases\":[\"Code\"]},{\"name\":\"color\",\"labels\":{\"en\":\"color\"},\"aliases\":[\"Color\"]},{\"name\":\"commerce_platform\",\"targetType\":\"CommercePlatform\",\"labels\":{\"en\":\"commerce platform\"},\"aliases\":[\"CommercePlatform\",\"commercePlatform\"]},{\"name\":\"customer_order_list\",\"targetType\":\"CustomerOrder\",\"labels\":{\"en\":\"customer order\"},\"aliases\":[\"CustomerOrderList\",\"customerOrderList\"]},{\"name\":\"display_order\",\"labels\":{\"en\":\"display order\"},\"aliases\":[\"DisplayOrder\",\"displayOrder\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"name\",\"labels\":{\"en\":\"name\"},\"aliases\":[\"Name\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"Product\":{\"labels\":{\"en\":\"product\"},\"fields\":[{\"name\":\"commerce_platform\",\"targetType\":\"CommercePlatform\",\"labels\":{\"en\":\"commerce platform\"},\"aliases\":[\"CommercePlatform\",\"commercePlatform\"]},{\"name\":\"create_time\",\"labels\":{\"en\":\"create time\"},\"aliases\":[\"CreateTime\",\"createTime\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"image_url\",\"labels\":{\"en\":\"image url\"},\"aliases\":[\"ImageUrl\",\"imageUrl\"]},{\"name\":\"name\",\"labels\":{\"en\":\"name\"},\"aliases\":[\"Name\"]},{\"name\":\"order_line_list\",\"targetType\":\"OrderLine\",\"labels\":{\"en\":\"order line\"},\"aliases\":[\"OrderLineList\",\"orderLineList\"]},{\"name\":\"sku\",\"labels\":{\"en\":\"sku\"},\"aliases\":[\"Sku\"]},{\"name\":\"update_time\",\"labels\":{\"en\":\"update time\"},\"aliases\":[\"UpdateTime\",\"updateTime\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]}}}")
            .expect("Invalid generated Checker presentation metadata")
    )).clone()
}

pub fn module() -> teaql_runtime::RuntimeModule {
    teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation())        .entity::<CommercePlatform>()
        .entity::<Customer>()
        .entity::<OrderStatus>()
        .entity::<CustomerOrder>()
        .entity::<Product>()
        .entity::<OrderLine>()
        .entity::<OrderSearchPreset>()
        .generated_schema_bootstrap(ensure_generated_bootstrap)
}

pub fn module_with_checkers() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation());
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
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation());
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
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation());
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