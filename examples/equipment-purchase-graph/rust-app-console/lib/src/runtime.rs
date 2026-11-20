
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

pub const DATABASE_URL_ENV: &str = "EQUIPMENT_PROCUREMENT_CORE_DATABASE_URL";
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
            "ProcurementDesk" => Some(std::sync::Arc::new(crate::ProcurementDesk::entity_descriptor())),
            "CatalogProduct" => Some(std::sync::Arc::new(crate::CatalogProduct::entity_descriptor())),
            "PurchaseOrder" => Some(std::sync::Arc::new(crate::PurchaseOrder::entity_descriptor())),
            "PurchaseLine" => Some(std::sync::Arc::new(crate::PurchaseLine::entity_descriptor())),
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
        .with_entity("ProcurementDesk")
        .with_entity("CatalogProduct")
        .with_entity("PurchaseOrder")
        .with_entity("PurchaseLine")
}

pub fn behavior_registry() -> teaql_runtime::InMemoryEntityDataServiceBehaviorRegistry {
    teaql_runtime::InMemoryEntityDataServiceBehaviorRegistry::new()
        .with_behavior("ProcurementDesk", ProcurementDeskBehavior::default())
        .with_behavior("CatalogProduct", CatalogProductBehavior::default())
        .with_behavior("PurchaseOrder", PurchaseOrderBehavior::default())
        .with_behavior("PurchaseLine", PurchaseLineBehavior::default())
}

pub fn checker_registry() -> teaql_runtime::InMemoryCheckerRegistry {
    teaql_runtime::InMemoryCheckerRegistry::new()
        .with_checker(teaql_runtime::TypedEntityChecker::<ProcurementDesk, _>::new(ProcurementDeskChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<CatalogProduct, _>::new(CatalogProductChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<PurchaseOrder, _>::new(PurchaseOrderChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<PurchaseLine, _>::new(PurchaseLineChecker::default()))
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
        let root_rows = crate::Q::procurement_desks().select_self_fields().with_id_is(1_u64).comment("what: locate generated Domain Root").purpose("why: idempotent runtime bootstrap").execute_for_list(context).await.map_err(|e| teaql_runtime::RuntimeError::Graph(e.to_string()))?;
        let domain_root = if let Some(entity) = root_rows.data.into_iter().next() { entity } else {
            let mut entity = ProcurementDesk::runtime_new(context.entity_runtime_state());
            entity.update_id(1_u64);
            context.initialize_generated_bootstrap_entity(&mut entity, ProcurementDesk::ENTITY_NAME, 1_u64)?;
            entity.update_name("Operations Procurement");
            teaql_runtime::AuditedSaveExt::save(entity.audit_as("create generated Domain Root ProcurementDesk"), context).await?
        };
        context.set_generated_bootstrap_active_root(ProcurementDesk::ENTITY_NAME, domain_root.id())?;
        Ok(())
}


/// Canonical KSML field to selected JSON wire name, consumed by HTTP/TFP adapters.
pub fn generated_wire_field_mappings() -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>> {
    std::collections::BTreeMap::from([
        ("ProcurementDesk".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("CatalogProduct".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("procurement_desk".to_owned(), "procurementDesk".to_owned()),
            ("sku".to_owned(), "sku".to_owned()),
            ("label".to_owned(), "label".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("PurchaseOrder".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("procurement_desk".to_owned(), "procurementDesk".to_owned()),
            ("reference".to_owned(), "reference".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("PurchaseLine".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("purchase_order".to_owned(), "purchaseOrder".to_owned()),
            ("catalog_product".to_owned(), "catalogProduct".to_owned()),
            ("line_code".to_owned(), "lineCode".to_owned()),
            ("quantity".to_owned(), "quantity".to_owned()),
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
        Definition::new("ProcurementDesk", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("name", "name", Kind::Text).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("catalog_product_list", "catalog_product_list", Kind::Many { target: "CatalogProduct".to_owned() }),
            Field::new("purchase_order_list", "purchase_order_list", Kind::Many { target: "PurchaseOrder".to_owned() }),
        ]),
        Definition::new("CatalogProduct", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("procurement_desk", "procurement_desk_id", Kind::Reference { target: "ProcurementDesk".to_owned(), detail_key: "procurement_desk".to_owned() }).nullable(false),
            Field::new("sku", "sku", Kind::Text).nullable(false),
            Field::new("label", "label", Kind::Text).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("purchase_line_list", "purchase_line_list", Kind::Many { target: "PurchaseLine".to_owned() }),
        ]),
        Definition::new("PurchaseOrder", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("procurement_desk", "procurement_desk_id", Kind::Reference { target: "ProcurementDesk".to_owned(), detail_key: "procurement_desk".to_owned() }).nullable(false),
            Field::new("reference", "reference", Kind::Text).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("purchase_line_list", "purchase_line_list", Kind::Many { target: "PurchaseLine".to_owned() }),
        ]),
        Definition::new("PurchaseLine", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("purchase_order", "purchase_order_id", Kind::Reference { target: "PurchaseOrder".to_owned(), detail_key: "purchase_order".to_owned() }).nullable(false),
            Field::new("catalog_product", "catalog_product_id", Kind::Reference { target: "CatalogProduct".to_owned(), detail_key: "catalog_product".to_owned() }).nullable(false),
            Field::new("line_code", "line_code", Kind::Text).nullable(false),
            Field::new("quantity", "quantity", Kind::Integer).nullable(false),
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
        teaql_runtime::checker_messages::CheckerPresentationModel::from_json("{\"schema\":\"teaql.checker-model/v1\",\"revision\":\"sha256:84f8d612c09698c3a9bef01f6aac72a324e4d99e88566c7e8ddc12f07edc5cd7\",\"entities\":{\"CatalogProduct\":{\"labels\":{\"en\":\"catalog product\"},\"fields\":[{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"label\",\"labels\":{\"en\":\"label\"},\"aliases\":[\"Label\"]},{\"name\":\"procurement_desk\",\"targetType\":\"ProcurementDesk\",\"labels\":{\"en\":\"procurement desk\"},\"aliases\":[\"ProcurementDesk\",\"procurementDesk\"]},{\"name\":\"purchase_line_list\",\"targetType\":\"PurchaseLine\",\"labels\":{\"en\":\"purchase line\"},\"aliases\":[\"PurchaseLineList\",\"purchaseLineList\"]},{\"name\":\"sku\",\"labels\":{\"en\":\"sku\"},\"aliases\":[\"Sku\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"ProcurementDesk\":{\"labels\":{\"en\":\"procurement desk\"},\"fields\":[{\"name\":\"catalog_product_list\",\"targetType\":\"CatalogProduct\",\"labels\":{\"en\":\"catalog product\"},\"aliases\":[\"CatalogProductList\",\"catalogProductList\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"name\",\"labels\":{\"en\":\"name\"},\"aliases\":[\"Name\"]},{\"name\":\"purchase_order_list\",\"targetType\":\"PurchaseOrder\",\"labels\":{\"en\":\"purchase order\"},\"aliases\":[\"PurchaseOrderList\",\"purchaseOrderList\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"PurchaseLine\":{\"labels\":{\"en\":\"purchase line\"},\"fields\":[{\"name\":\"catalog_product\",\"targetType\":\"CatalogProduct\",\"labels\":{\"en\":\"catalog product\"},\"aliases\":[\"CatalogProduct\",\"catalogProduct\"]},{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"line_code\",\"labels\":{\"en\":\"line code\"},\"aliases\":[\"LineCode\",\"lineCode\"]},{\"name\":\"purchase_order\",\"targetType\":\"PurchaseOrder\",\"labels\":{\"en\":\"purchase order\"},\"aliases\":[\"PurchaseOrder\",\"purchaseOrder\"]},{\"name\":\"quantity\",\"labels\":{\"en\":\"quantity\"},\"aliases\":[\"Quantity\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]},\"PurchaseOrder\":{\"labels\":{\"en\":\"purchase order\"},\"fields\":[{\"name\":\"id\",\"labels\":{\"en\":\"id\"},\"aliases\":[\"Id\"]},{\"name\":\"procurement_desk\",\"targetType\":\"ProcurementDesk\",\"labels\":{\"en\":\"procurement desk\"},\"aliases\":[\"ProcurementDesk\",\"procurementDesk\"]},{\"name\":\"purchase_line_list\",\"targetType\":\"PurchaseLine\",\"labels\":{\"en\":\"purchase line\"},\"aliases\":[\"PurchaseLineList\",\"purchaseLineList\"]},{\"name\":\"reference\",\"labels\":{\"en\":\"reference\"},\"aliases\":[\"Reference\"]},{\"name\":\"version\",\"labels\":{\"en\":\"version\"},\"aliases\":[\"Version\"]}]}}}")
            .expect("Invalid generated Checker presentation metadata")
    )).clone()
}

pub fn module() -> teaql_runtime::RuntimeModule {
    teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation())        .entity::<ProcurementDesk>()
        .entity::<CatalogProduct>()
        .entity::<PurchaseOrder>()
        .entity::<PurchaseLine>()
        .generated_schema_bootstrap(ensure_generated_bootstrap)
}

pub fn module_with_checkers() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation());
    module = module.entity::<ProcurementDesk>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<ProcurementDesk, _>::new(ProcurementDeskChecker::default()));
    module = module.entity::<CatalogProduct>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<CatalogProduct, _>::new(CatalogProductChecker::default()));
    module = module.entity::<PurchaseOrder>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<PurchaseOrder, _>::new(PurchaseOrderChecker::default()));
    module = module.entity::<PurchaseLine>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<PurchaseLine, _>::new(PurchaseLineChecker::default()));
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}

pub fn module_with_behaviors() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation());
    module = module.entity_with_behavior::<ProcurementDesk, _>(ProcurementDeskBehavior::default());
    module = module.entity_with_behavior::<CatalogProduct, _>(CatalogProductBehavior::default());
    module = module.entity_with_behavior::<PurchaseOrder, _>(PurchaseOrderBehavior::default());
    module = module.entity_with_behavior::<PurchaseLine, _>(PurchaseLineBehavior::default());
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}

pub fn module_with_behaviors_and_checkers() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .checker_presentation_model(generated_checker_presentation());
    module = module.entity_with_behavior::<ProcurementDesk, _>(ProcurementDeskBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<ProcurementDesk, _>::new(ProcurementDeskChecker::default()));
    module = module.entity_with_behavior::<CatalogProduct, _>(CatalogProductBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<CatalogProduct, _>::new(CatalogProductChecker::default()));
    module = module.entity_with_behavior::<PurchaseOrder, _>(PurchaseOrderBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<PurchaseOrder, _>::new(PurchaseOrderChecker::default()));
    module = module.entity_with_behavior::<PurchaseLine, _>(PurchaseLineBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<PurchaseLine, _>::new(PurchaseLineChecker::default()));
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}