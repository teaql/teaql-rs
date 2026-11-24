
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

pub const DATABASE_URL_ENV: &str = "SCALAR_TYPE_CONFORMANCE_SERVICE_CORE_DATABASE_URL";
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
            "TypePlatform" => Some(std::sync::Arc::new(crate::TypePlatform::entity_descriptor())),
            "ScalarTypeProbe" => Some(std::sync::Arc::new(crate::ScalarTypeProbe::entity_descriptor())),
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
        .with_entity("TypePlatform")
        .with_entity("ScalarTypeProbe")
}

pub fn behavior_registry() -> teaql_runtime::InMemoryEntityDataServiceBehaviorRegistry {
    teaql_runtime::InMemoryEntityDataServiceBehaviorRegistry::new()
        .with_behavior("TypePlatform", TypePlatformBehavior::default())
        .with_behavior("ScalarTypeProbe", ScalarTypeProbeBehavior::default())
}

pub fn checker_registry() -> teaql_runtime::InMemoryCheckerRegistry {
    teaql_runtime::InMemoryCheckerRegistry::new()
        .with_checker(teaql_runtime::TypedEntityChecker::<TypePlatform, _>::new(TypePlatformChecker::default()))
        .with_checker(teaql_runtime::TypedEntityChecker::<ScalarTypeProbe, _>::new(ScalarTypeProbeChecker::default()))
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
        let root_rows = crate::Q::type_platforms().select_self_fields().with_id_is(1_u64).comment("what: locate generated Domain Root").purpose("why: idempotent runtime bootstrap").execute_for_list(context).await.map_err(|e| teaql_runtime::RuntimeError::Graph(e.to_string()))?;
        let domain_root = if let Some(entity) = root_rows.data.into_iter().next() { entity } else {
            let mut entity = TypePlatform::runtime_new(context.entity_runtime_state());
            entity.update_id(1_u64);
            context.initialize_generated_bootstrap_entity(&mut entity, TypePlatform::ENTITY_NAME, 1_u64)?;
            entity.update_name("Scalar Type Conformance");
            teaql_runtime::AuditedSaveExt::save(entity.audit_as("create generated Domain Root TypePlatform"), context).await?
        };
        context.set_generated_bootstrap_active_root(TypePlatform::ENTITY_NAME, domain_root.id())?;
        Ok(())
}


/// Canonical KSML field to selected JSON wire name, consumed by HTTP/TFP adapters.
pub fn generated_wire_field_mappings() -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>> {
    std::collections::BTreeMap::from([
        ("TypePlatform".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("version".to_owned(), "version".to_owned())
        ])),
        ("ScalarTypeProbe".to_owned(), std::collections::BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("type_platform".to_owned(), "type_platform".to_owned()),
            ("required_string".to_owned(), "required_string".to_owned()),
            ("optional_string".to_owned(), "optional_string".to_owned()),
            ("large_text".to_owned(), "large_text".to_owned()),
            ("required_boolean".to_owned(), "required_boolean".to_owned()),
            ("optional_boolean".to_owned(), "optional_boolean".to_owned()),
            ("required_integer".to_owned(), "required_integer".to_owned()),
            ("optional_integer".to_owned(), "optional_integer".to_owned()),
            ("required_long".to_owned(), "required_long".to_owned()),
            ("optional_long".to_owned(), "optional_long".to_owned()),
            ("required_decimal".to_owned(), "required_decimal".to_owned()),
            ("optional_decimal".to_owned(), "optional_decimal".to_owned()),
            ("required_float".to_owned(), "required_float".to_owned()),
            ("optional_float".to_owned(), "optional_float".to_owned()),
            ("required_double".to_owned(), "required_double".to_owned()),
            ("optional_double".to_owned(), "optional_double".to_owned()),
            ("required_date".to_owned(), "required_date".to_owned()),
            ("optional_date".to_owned(), "optional_date".to_owned()),
            ("required_time".to_owned(), "required_time".to_owned()),
            ("optional_time".to_owned(), "optional_time".to_owned()),
            ("required_datetime".to_owned(), "required_datetime".to_owned()),
            ("optional_datetime".to_owned(), "optional_datetime".to_owned()),
            ("create_time".to_owned(), "create_time".to_owned()),
            ("update_time".to_owned(), "update_time".to_owned()),
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
        Definition::new("TypePlatform", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("name", "name", Kind::Text).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
            Field::new("scalar_type_probe_list", "scalar_type_probe_list", Kind::Many { target: "ScalarTypeProbe".to_owned() }),
        ]),
        Definition::new("ScalarTypeProbe", vec![
            Field::new("id", "id", Kind::Id).nullable(false),
            Field::new("type_platform", "type_platform_id", Kind::Reference { target: "TypePlatform".to_owned(), detail_key: "type_platform".to_owned() }).nullable(false),
            Field::new("required_string", "required_string", Kind::Text).nullable(false),
            Field::new("optional_string", "optional_string", Kind::Text).nullable(true),
            Field::new("large_text", "large_text", Kind::Text).nullable(true),
            Field::new("required_boolean", "required_boolean", Kind::Boolean).nullable(false),
            Field::new("optional_boolean", "optional_boolean", Kind::Boolean).nullable(true),
            Field::new("required_integer", "required_integer", Kind::Integer).nullable(false),
            Field::new("optional_integer", "optional_integer", Kind::Integer).nullable(true),
            Field::new("required_long", "required_long", Kind::Long).nullable(false),
            Field::new("optional_long", "optional_long", Kind::Long).nullable(true),
            Field::new("required_decimal", "required_decimal", Kind::Decimal).nullable(false),
            Field::new("optional_decimal", "optional_decimal", Kind::Decimal).nullable(true),
            Field::new("required_float", "required_float", Kind::Float).nullable(false),
            Field::new("optional_float", "optional_float", Kind::Float).nullable(true),
            Field::new("required_double", "required_double", Kind::Double).nullable(false),
            Field::new("optional_double", "optional_double", Kind::Double).nullable(true),
            Field::new("required_date", "required_date", Kind::Date).nullable(false),
            Field::new("optional_date", "optional_date", Kind::Date).nullable(true),
            Field::new("required_time", "required_time", Kind::Time).nullable(false),
            Field::new("optional_time", "optional_time", Kind::Time).nullable(true),
            Field::new("required_datetime", "required_datetime", Kind::DateTime).nullable(false),
            Field::new("optional_datetime", "optional_datetime", Kind::DateTime).nullable(true),
            Field::new("create_time", "create_time", Kind::DateTime).nullable(false),
            Field::new("update_time", "update_time", Kind::DateTime).nullable(false),
            Field::new("version", "version", Kind::Version).nullable(false),
        ])
    ]).expect("invalid generated entity JSON metadata"))
}

pub fn generated_json_field_naming_profile() -> teaql_runtime::JsonFieldNamingProfile {
    teaql_runtime::JsonFieldNamingProfile::SnakeCase
}

pub fn module() -> teaql_runtime::RuntimeModule {
    teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone())
        .entity::<TypePlatform>()
        .entity::<ScalarTypeProbe>()
        .generated_schema_bootstrap(ensure_generated_bootstrap)
}

pub fn module_with_checkers() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone());
    module = module.entity::<TypePlatform>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<TypePlatform, _>::new(TypePlatformChecker::default()));
    module = module.entity::<ScalarTypeProbe>();
    module = module.checker(teaql_runtime::TypedEntityChecker::<ScalarTypeProbe, _>::new(ScalarTypeProbeChecker::default()));
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}

pub fn module_with_behaviors() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone());
    module = module.entity_with_behavior::<TypePlatform, _>(TypePlatformBehavior::default());
    module = module.entity_with_behavior::<ScalarTypeProbe, _>(ScalarTypeProbeBehavior::default());
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}

pub fn module_with_behaviors_and_checkers() -> teaql_runtime::RuntimeModule {
    let mut module = teaql_runtime::RuntimeModule::new()
        .entity_json_model(generated_entity_json_model().clone());
    module = module.entity_with_behavior::<TypePlatform, _>(TypePlatformBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<TypePlatform, _>::new(TypePlatformChecker::default()));
    module = module.entity_with_behavior::<ScalarTypeProbe, _>(ScalarTypeProbeBehavior::default());
    module = module.checker(teaql_runtime::TypedEntityChecker::<ScalarTypeProbe, _>::new(ScalarTypeProbeChecker::default()));
    module = module.generated_schema_bootstrap(ensure_generated_bootstrap);
    module
}