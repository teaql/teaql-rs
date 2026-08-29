#![allow(warnings)]
extern crate self as teaql_runtime;
mod checker;
mod context;
mod data_service;
mod entity_runtime;
pub mod entity_save;
mod entity_status;
mod error;
mod event;
pub mod generated_support;
mod graph;
mod i18n;
mod id;
pub mod inmemory_engine;
mod language;
pub mod log_formatter;
mod memory;
mod registry;
mod telemetry;
#[cfg(feature = "opentelemetry")]
mod telemetry_opentelemetry;

pub use context::{
    ContextEntityRef, ContextRootError, ContinuousPageCursor, ContinuousPageCursorStore, DataStore,
    IdSetStore, InMemoryContinuousPageCursorStore, InMemoryDataStore, InMemoryIdSetStore,
    InfoLogEntry, LogPayload, RemoteLockProvider, RetainedIdSet, SchemaInvocation, SchemaProvider,
    SqlLogEntry, SqlLogOperation, SqlLogOptions, UnifiedLogBuffer, UnifiedLogEntry, UserContext,
};
pub use data_service::{
    AggregationCacheBackend, EntityDataService, GraphTransactionBoundary, InMemoryAggregationCache,
    RelationLoadPlan,
};
pub use entity_runtime::{
    ChangeSetStack, EntityChangeSet, EntityGraphBuilder, EntityKey, EntityRuntimeState,
    LedgerEntity, LoadedRelation, RelationHandle,
};
pub use entity_save::{AuditedSaveExt, graph_node_from_entity, save_audited_ledger_entity};
pub use entity_status::{EntityAction, EntityStatus};
pub use error::{ContextError, DataServiceError, RuntimeError};
pub use event::{
    EntityPropertyChange, InMemoryRawAuditEventSink, RawAuditEvent, RawAuditEventKind,
    RawAuditEventSink, SafeAuditEvent, SafeAuditEventSink, SafeAuditField,
};
pub use generated_support::*;
pub use graph::{
    EntityValues, GraphMutationBatch, GraphMutationKind, GraphMutationPlan, GraphMutationPlanItem,
    GraphNode, GraphOperation, ScopedCommentNode, TraceScopeToken, sorted_update_fields,
};
pub use i18n::I18nCatalog;
pub(crate) use id::local_id_generator;
pub use id::{
    AtomicCounterIdGenerator, InternalIdGenerator, SnowflakeIdGenerator, canonical_id_space_entity,
};
pub use inmemory_engine::{ExprEvaluator, InMemoryQueryEngine};
pub use language::{
    BuiltinTranslator, Language, Locale, MessageTranslator, translate_check_result,
    translate_location,
};
pub(crate) use memory::MemoryDataService;
pub use registry::{
    EntityDataServiceBehavior, EntityDataServiceBehaviorRegistry, EntityRegistry,
    InMemoryEntityDataServiceBehaviorRegistry, InMemoryEntityGraphDecoderRegistry,
    InMemoryEntityRegistry, InMemoryMetadataStore, MetadataStore, RequestPolicy, RuntimeModule,
};
pub use telemetry::{
    FailOpenRuntimeTelemetryPropagationContext, FailOpenRuntimeTelemetryScope,
    NoopRuntimeTelemetry, RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry,
    RuntimeTelemetryPropagationContext, RuntimeTelemetryScope, extract_runtime_context,
    runtime_error_category, start_runtime_operation,
};
#[cfg(feature = "opentelemetry")]
pub use telemetry_opentelemetry::OpenTelemetryRuntimeTelemetry;

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use super::{
        AggregationCacheBackend, CHECK_OBJECT_STATUS_FIELD, CheckObjectStatus, CheckResult,
        CheckResults, CheckRule, Checker, DataServiceError, EntityDataServiceBehavior,
        EntityRuntimeState, EntityValues, GraphMutationKind, GraphNode, I18nCatalog,
        InMemoryAggregationCache, InMemoryCheckerRegistry,
        InMemoryEntityDataServiceBehaviorRegistry, InMemoryEntityRegistry, InMemoryMetadataStore,
        InternalIdGenerator, Language, MemoryDataService, MetadataStore, ObjectLocation,
        RawAuditEvent, RawAuditEventKind, RawAuditEventSink, RemoteLockProvider, RequestPolicy,
        RuntimeError, RuntimeModule, RuntimeOperation, RuntimeTelemetry, RuntimeTelemetryScope,
        SafeAuditEvent, SafeAuditEventSink, SqlLogOperation, SqlLogOptions, TypedChecker,
        TypedEntityChecker, UserContext, translate_check_result,
    };
    use crate::data_service::RuntimeDataService;
    use teaql_core::{
        Aggregate, AggregateFunction, BinaryOp, DataType, Decimal, DeleteCommand, Entity,
        EntityDescriptor, EntityError, Expr, GeneratedValues, InsertCommand, OrderBy,
        PropertyDescriptor, Record, RecoverCommand, RelationAggregate, SelectQuery, TeaqlEntity,
        UpdateCommand, Value,
    };
    use teaql_data_service::{
        DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
        MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest,
        QueryResult,
    };
    use teaql_macros::TeaqlEntity as DeriveTeaqlEntity;
    use teaql_sql::{
        CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, quote_identifier_if_needed,
    };

    const ORDER_DEFAULT_PROJECTION: &str = "id, version, name";

    #[derive(Debug, Default, Clone, Copy)]
    struct PostgresDialect;

    impl SqlDialect for PostgresDialect {
        fn kind(&self) -> DatabaseKind {
            DatabaseKind::PostgreSql
        }

        fn quote_ident(&self, ident: &str) -> String {
            quote_identifier_if_needed(ident, '"')
        }

        fn placeholder(&self, index: usize) -> String {
            format!("${index}")
        }

        fn schema_type_sql(
            &self,
            data_type: DataType,
            _property: &PropertyDescriptor,
        ) -> Result<&'static str, SqlCompileError> {
            match data_type {
                DataType::Bool => Ok("BOOLEAN"),
                DataType::I64 | DataType::U64 => Ok("BIGINT"),
                DataType::F64 => Ok("DOUBLE PRECISION"),
                DataType::Decimal => Ok("NUMERIC"),
                DataType::Text => Ok("VARCHAR(255)"),
                DataType::LargeText => Ok("TEXT"),
                DataType::Json => Ok("JSONB"),
                DataType::Date => Ok("DATE"),
                DataType::Timestamp => Ok("TIMESTAMPTZ"),
            }
        }
    }

    fn entity() -> EntityDescriptor {
        EntityDescriptor::new("Order")
            .table_name("orders")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("version")
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
            .relation(
                teaql_core::RelationDescriptor::new("lines", "OrderLine")
                    .local_key("id")
                    .foreign_key("order_id")
                    .many(),
            )
    }

    fn line_entity() -> EntityDescriptor {
        EntityDescriptor::new("OrderLine")
            .table_name("orderline")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("version")
                    .version(),
            )
            .property(
                PropertyDescriptor::new("order_id", DataType::U64)
                    .column_name("order_id")
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
            .property(
                PropertyDescriptor::new("product_id", DataType::U64)
                    .column_name("product_id")
                    .not_null(),
            )
            .relation(
                teaql_core::RelationDescriptor::new("product", "Product")
                    .local_key("product_id")
                    .foreign_key("id"),
            )
    }

    fn product_entity() -> EntityDescriptor {
        EntityDescriptor::new("Product")
            .table_name("product")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
    }

    #[derive(Debug, Default)]
    struct StubExecutor {
        affected: u64,
        rows: Vec<Record>,
    }

    #[derive(Debug, Default)]
    struct QueueExecutor {
        affected: u64,
        rows: Mutex<VecDeque<Vec<Record>>>,
        queries: Mutex<Vec<String>>,
    }

    #[derive(Debug, Default)]
    struct IdSetQueueExecutor {
        rows: Mutex<VecDeque<Vec<Record>>>,
        queries: Mutex<Vec<SelectQuery>>,
    }

    #[derive(Debug, Clone, Default)]
    struct ConcurrentIdSetExecutor {
        id_queries: Arc<std::sync::atomic::AtomicUsize>,
    }

    struct UnavailableIdSetStore;

    #[async_trait::async_trait]
    impl crate::IdSetStore for UnavailableIdSetStore {
        async fn get(&self, _query_key: &str) -> Result<Option<crate::RetainedIdSet>, String> {
            Err("unavailable".to_owned())
        }

        async fn put(&self, _id_set: crate::RetainedIdSet) -> Result<(), String> {
            Err("unavailable".to_owned())
        }

        async fn invalidate(&self, _query_key: &str) -> Result<(), String> {
            Err("unavailable".to_owned())
        }
    }

    #[derive(Debug, Default)]
    struct CapturingQueryExecutor {
        rows: Vec<Record>,
        queries: Mutex<Vec<SelectQuery>>,
    }

    struct OrderBehavior;

    #[allow(dead_code)]
    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "CatalogProduct", table = "catalog_product")]
    struct CatalogProductRow {
        #[teaql(id)]
        id: u64,
        name: String,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "OrderAggregate", table = "orders")]
    struct OrderAggregateDynamic {
        #[teaql(id)]
        id: u64,
        #[teaql(dynamic)]
        dynamic: BTreeMap<String, Value>,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Product", table = "product")]
    struct ProductEntityRow {
        #[teaql(id)]
        id: u64,
        name: String,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "OrderLine", table = "orderline")]
    struct OrderLineEntityRow {
        #[teaql(id)]
        id: u64,
        #[teaql(column = "order_id")]
        order_id: u64,
        name: String,
        #[teaql(column = "product_id")]
        product_id: u64,
        #[teaql(relation(target = "Product", local_key = "product_id", foreign_key = "id"))]
        product: Option<ProductEntityRow>,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "OrderLine", table = "orderline")]
    struct ProductLineEntityRow {
        #[teaql(id)]
        id: u64,
        #[teaql(column = "order_id")]
        order_id: u64,
        name: String,
        #[teaql(column = "product_id")]
        product_id: u64,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Product", table = "product")]
    struct ProductWithLinesEntityRow {
        #[teaql(id)]
        id: u64,
        name: String,
        #[teaql(relation(
            target = "OrderLine",
            local_key = "id",
            foreign_key = "product_id",
            many
        ))]
        lines: teaql_core::SmartList<ProductLineEntityRow>,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(
        entity = "DetachedProduct",
        table = "detached_product",
        reverse_relation(
            name = "line_list",
            target = "DetachedLine",
            local_key = "id",
            foreign_key = "product_id",
            many
        )
    )]
    struct ProductWithDetachedLinesRow {
        #[teaql(id)]
        id: u64,
        name: String,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "OrderLine", table = "orderline")]
    struct OrderLineWithProductEntityRow {
        #[teaql(id)]
        id: u64,
        #[teaql(column = "order_id")]
        order_id: u64,
        name: String,
        #[teaql(column = "product_id")]
        product_id: u64,
        #[teaql(relation(target = "Product", local_key = "product_id", foreign_key = "id"))]
        product: Option<ProductWithLinesEntityRow>,
    }

    #[derive(Debug, DeriveTeaqlEntity)]
    #[teaql(entity = "FlatVendor", table = "flat_vendor")]
    struct FlatVendorRow {
        #[teaql(id)]
        id: u64,
        name: String,
        #[teaql(skip)]
        root: EntityRuntimeState,
    }

    #[derive(Debug, DeriveTeaqlEntity)]
    #[teaql(entity = "FlatTrip", table = "flat_trip")]
    struct FlatTripRow {
        #[teaql(id)]
        id: u64,
        vendor_id: u64,
        #[teaql(relation(target = "FlatVendor", local_key = "vendor_id", foreign_key = "id"))]
        vendor: Option<FlatVendorRow>,
        #[teaql(skip)]
        root: EntityRuntimeState,
    }

    impl FlatTripRow {
        fn vendor(&self) -> Option<&FlatVendorRow> {
            self.vendor
                .as_ref()
                .or_else(|| self.root.resolve_entity::<FlatVendorRow>(self.vendor_id))
        }
    }

    #[derive(Clone, Debug, DeriveTeaqlEntity)]
    #[teaql(entity = "FlatFleet", table = "flat_fleet")]
    struct FlatFleetRow {
        #[teaql(id)]
        id: u64,
        #[teaql(relation(
            target = "FlatFleetTrip",
            local_key = "id",
            foreign_key = "fleet_id",
            many
        ))]
        trip_list: teaql_core::SmartList<FlatFleetTripRow>,
        #[teaql(skip)]
        root: EntityRuntimeState,
    }

    impl FlatFleetRow {
        fn trip_list(&self) -> &teaql_core::SmartList<FlatFleetTripRow> {
            if self.trip_list.is_loaded {
                &self.trip_list
            } else {
                self.root
                    .resolve_relation_list(Self::ENTITY_NAME, self.id, "trip_list")
                    .unwrap_or(&self.trip_list)
            }
        }

        fn trip_list_mut(&mut self) -> &mut teaql_core::SmartList<FlatFleetTripRow> {
            if !self.trip_list.is_loaded {
                if let Some(loaded) = self
                    .root
                    .resolve_relation_list(Self::ENTITY_NAME, self.id, "trip_list")
                    .cloned()
                {
                    self.trip_list = loaded;
                }
            }
            &mut self.trip_list
        }
    }

    #[derive(Clone, Debug, DeriveTeaqlEntity)]
    #[teaql(entity = "FlatFleetTrip", table = "flat_fleet_trip")]
    struct FlatFleetTripRow {
        #[teaql(id)]
        id: u64,
        fleet_id: u64,
        name: String,
        #[teaql(skip)]
        root: EntityRuntimeState,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Order", table = "orders")]
    struct OrderAggregateRow {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        name: String,
        #[teaql(relation(target = "OrderLine", local_key = "id", foreign_key = "order_id", many))]
        lines: teaql_core::SmartList<OrderLineEntityRow>,
    }

    #[derive(Debug, Clone, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Order", table = "orders")]
    struct Order {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "TimestampedEntity", table = "timestamped_entity")]
    struct TimestampedEntity {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        happened_at: teaql_core::time::Timestamp,
    }

    struct NoopTimestampedChecker;

    impl TypedChecker<TimestampedEntity> for NoopTimestampedChecker {
        fn check_and_fix_typed(
            &self,
            _context: &UserContext,
            _entity: &mut TimestampedEntity,
            _status: CheckObjectStatus,
            _location: &ObjectLocation,
            _results: &mut CheckResults,
        ) {
        }
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Product", table = "product")]
    struct TypedGraphProduct {
        #[teaql(id)]
        id: u64,
        name: String,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "OrderLine", table = "orderline")]
    struct TypedGraphLine {
        #[teaql(id)]
        id: u64,
        #[teaql(column = "order_id")]
        order_id: Option<u64>,
        name: String,
        #[teaql(column = "product_id")]
        product_id: Option<u64>,
        #[teaql(relation(target = "Product", local_key = "product_id", foreign_key = "id"))]
        product: Option<TypedGraphProduct>,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Order", table = "orders")]
    struct TypedGraphOrder {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        name: String,
        #[teaql(relation(target = "OrderLine", local_key = "id", foreign_key = "order_id", many))]
        lines: teaql_core::SmartList<TypedGraphLine>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct OrderEntity {
        id: u64,
        version: i64,
        name: String,
    }

    impl teaql_core::TeaqlEntity for OrderEntity {
        const ENTITY_NAME: &'static str = "Order";

        fn entity_descriptor() -> EntityDescriptor {
            entity()
        }
    }

    impl Entity for OrderEntity {
        fn from_compact_row(row: teaql_core::CompactRow) -> Result<Self, EntityError> {
            let record = row.into_map();
            let id = match record.get("id") {
                Some(Value::U64(v)) => *v,
                Some(Value::I64(v)) if *v >= 0 => *v as u64,
                other => {
                    return Err(EntityError::new(
                        "Order",
                        format!("invalid id field: {other:?}"),
                    ));
                }
            };
            let version = match record.get("version") {
                Some(Value::I64(v)) => *v,
                other => {
                    return Err(EntityError::new(
                        "Order",
                        format!("invalid version field: {other:?}"),
                    ));
                }
            };
            let name = match record.get("name") {
                Some(Value::Text(v)) => v.clone(),
                other => {
                    return Err(EntityError::new(
                        "Order",
                        format!("invalid name field: {other:?}"),
                    ));
                }
            };
            Ok(Self { id, version, name })
        }

        fn into_values(self) -> teaql_core::MutationValues {
            Record::from([
                (String::from("id"), Value::U64(self.id)),
                (String::from("version"), Value::I64(self.version)),
                (String::from("name"), Value::Text(self.name)),
            ])
            .into()
        }
    }

    #[derive(Debug)]
    struct StubError;

    struct RecordingRuntimeTelemetry(Arc<Mutex<Vec<String>>>);

    impl RuntimeTelemetry for RecordingRuntimeTelemetry {
        fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
            self.0
                .lock()
                .unwrap()
                .push(format!("start:{}", operation.family));
            Box::new(RecordingRuntimeTelemetryScope(self.0.clone()))
        }
    }

    struct RecordingRuntimeTelemetryScope(Arc<Mutex<Vec<String>>>);

    impl RuntimeTelemetryScope for RecordingRuntimeTelemetryScope {
        fn success(&mut self, _attributes: BTreeMap<String, crate::RuntimeAttributeValue>) {
            self.0.lock().unwrap().push("success".to_owned());
        }

        fn failure(&mut self, _error_type: &str) {
            self.0.lock().unwrap().push("failure".to_owned());
        }
    }

    impl std::fmt::Display for StubError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "stub error")
        }
    }

    impl std::error::Error for StubError {}

    impl DataServiceExecutor for StubExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for StubExecutor {
        async fn query(&self, _request: QueryRequest) -> Result<QueryResult, Self::Error> {
            Ok(QueryResult {
                rows: self
                    .rows
                    .clone()
                    .into_iter()
                    .map(teaql_core::CompactRow::from_map)
                    .collect(),
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "stub".to_owned(),
                    operation: DataServiceOperation::Query,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: None,
                    result_count: Some(self.rows.len()),
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: None,
                    params: Vec::new(),
                },
            })
        }
    }

    impl MutationExecutor for StubExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: self.affected,
                generated_values: GeneratedValues::new(),
                persisted_snapshot: None,
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "stub".to_owned(),
                    operation: DataServiceOperation::Update,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: Some(self.affected),
                    result_count: None,
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: None,
                    params: Vec::new(),
                },
            })
        }
    }

    impl DataServiceExecutor for CapturingQueryExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for CapturingQueryExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            self.queries.lock().unwrap().push(request.query);
            Ok(QueryResult {
                rows: self
                    .rows
                    .clone()
                    .into_iter()
                    .map(teaql_core::CompactRow::from_map)
                    .collect(),
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "capture".to_owned(),
                    operation: DataServiceOperation::Query,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: None,
                    result_count: Some(self.rows.len()),
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: None,
                    params: Vec::new(),
                },
            })
        }
    }

    impl MutationExecutor for CapturingQueryExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            unreachable!("relation query test does not mutate")
        }
    }

    impl DataServiceExecutor for QueueExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for QueueExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let sql_approx = format!("SELECT ... FROM {} ...", request.query.entity);
            self.queries.lock().unwrap().push(sql_approx);
            Ok(QueryResult {
                rows: self
                    .rows
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_default()
                    .into_iter()
                    .map(teaql_core::CompactRow::from_map)
                    .collect(),
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "queue".to_owned(),
                    operation: DataServiceOperation::Query,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: None,
                    result_count: Some(0),
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: None,
                    params: Vec::new(),
                },
            })
        }
    }

    impl MutationExecutor for QueueExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: self.affected,
                generated_values: GeneratedValues::new(),
                persisted_snapshot: None,
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "queue".to_owned(),
                    operation: DataServiceOperation::Update,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: Some(self.affected),
                    result_count: None,
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: None,
                    params: Vec::new(),
                },
            })
        }
    }

    impl DataServiceExecutor for IdSetQueueExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for IdSetQueueExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            self.queries.lock().unwrap().push(request.query);
            let rows = self.rows.lock().unwrap().pop_front().unwrap_or_default();
            Ok(QueryResult {
                rows: rows
                    .into_iter()
                    .map(teaql_core::CompactRow::from_map)
                    .collect(),
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "id-set-queue".to_owned(),
                    operation: DataServiceOperation::Query,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: None,
                    result_count: None,
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: None,
                    params: Vec::new(),
                },
            })
        }
    }

    impl MutationExecutor for IdSetQueueExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            unreachable!("ID set query test does not mutate")
        }
    }

    impl DataServiceExecutor for ConcurrentIdSetExecutor {
        type Error = StubError;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for ConcurrentIdSetExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let id_only = request.query.projection == ["id"];
            let rows = if id_only {
                self.id_queries
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                vec![
                    Record::from([(String::from("id"), Value::U64(1))]),
                    Record::from([(String::from("id"), Value::U64(2))]),
                ]
            } else {
                vec![Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text("order-1".to_owned())),
                ])]
            };
            Ok(QueryResult {
                rows: rows
                    .into_iter()
                    .map(teaql_core::CompactRow::from_map)
                    .collect(),
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "concurrent-id-set".to_owned(),
                    operation: DataServiceOperation::Query,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: None,
                    result_count: None,
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: None,
                    params: Vec::new(),
                },
            })
        }
    }

    impl MutationExecutor for ConcurrentIdSetExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            unreachable!("ID set concurrency test does not mutate")
        }
    }

    impl EntityDataServiceBehavior for OrderBehavior {
        fn before_select(
            &self,
            _ctx: &UserContext,
            query: &mut teaql_core::SelectQuery,
        ) -> Result<(), RuntimeError> {
            query.filter = Some(Expr::eq("version", 1_i64));
            Ok(())
        }

        fn before_insert(
            &self,
            _ctx: &UserContext,
            command: &mut InsertCommand,
        ) -> Result<(), RuntimeError> {
            command
                .values
                .entry("version".to_owned())
                .or_insert(Value::I64(1));
            Ok(())
        }

        fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
            vec!["lines".to_owned()]
        }
    }

    struct ContextAwareOrderBehavior;
    struct TenantRequestPolicy;
    struct OrderChecker;
    struct TypedOrderChecker;
    #[derive(Clone)]
    struct RecordingEventSink {
        events: Arc<Mutex<Vec<RawAuditEvent>>>,
    }
    #[derive(Clone)]
    struct RecordingSafeEventSink {
        events: Arc<Mutex<Vec<SafeAuditEvent>>>,
    }

    impl EntityDataServiceBehavior for ContextAwareOrderBehavior {
        fn before_insert(
            &self,
            context: &UserContext,
            command: &mut InsertCommand,
        ) -> Result<(), RuntimeError> {
            let tenant = context
                .get_named_resource::<String>("tenant")
                .cloned()
                .ok_or_else(|| RuntimeError::Behavior("missing tenant resource".to_owned()))?;
            let version = *context
                .get_named_resource::<i64>("initial_version")
                .ok_or_else(|| {
                    RuntimeError::Behavior("missing initial_version resource".to_owned())
                })?;
            let trace_id = match context.local("trace_id") {
                Some(Value::Text(value)) => value.clone(),
                other => {
                    return Err(RuntimeError::Behavior(format!(
                        "missing trace_id local, got {other:?}"
                    )));
                }
            };

            command
                .values
                .entry("name".to_owned())
                .or_insert(Value::Text(format!("{tenant}:{trace_id}")));
            command
                .values
                .entry("version".to_owned())
                .or_insert(Value::I64(version));
            Ok(())
        }
    }

    impl RequestPolicy for TenantRequestPolicy {
        fn enforce_select(
            &self,
            context: &UserContext,
            query: &mut SelectQuery,
        ) -> Result<(), RuntimeError> {
            if query.entity == "Order" {
                let tenant_id = context
                    .get_named_resource::<u64>("tenant_id")
                    .copied()
                    .ok_or_else(|| RuntimeError::Policy("missing tenant_id".to_owned()))?;
                query.filter = Some(match query.filter.take() {
                    Some(filter) => filter.and_expr(Expr::eq("id", tenant_id)),
                    None => Expr::eq("id", tenant_id),
                });
            }
            Ok(())
        }

        fn enforce_insert(
            &self,
            context: &UserContext,
            command: &mut InsertCommand,
        ) -> Result<(), RuntimeError> {
            if command.entity == "Order" {
                let tenant_id = context
                    .get_named_resource::<u64>("tenant_id")
                    .copied()
                    .ok_or_else(|| RuntimeError::Policy("missing tenant_id".to_owned()))?;
                command
                    .values
                    .insert("version".to_owned(), Value::I64(tenant_id as i64));
            }
            Ok(())
        }
    }

    impl Checker for OrderChecker {
        fn entity(&self) -> &str {
            "Order"
        }

        fn check_and_fix(
            &self,
            _ctx: &UserContext,
            values: &mut EntityValues,
            location: &ObjectLocation,
            results: &mut CheckResults,
        ) {
            let status = CheckObjectStatus::from_values(values);
            if status.is_create() {
                self.required(values, "name", location, results);
                values.entry("version".to_owned()).or_insert(Value::I64(1));
            }
            if status.is_update()
                && values.get("name") == Some(&Value::Text("graph-update".to_owned()))
            {
                values.insert(
                    "name".to_owned(),
                    Value::Text("graph-update-checked".to_owned()),
                );
            }
            self.min_string_length(values, "name", 3, location, results);
        }
    }

    impl TypedChecker<Order> for TypedOrderChecker {
        fn check_and_fix_typed(
            &self,
            _ctx: &UserContext,
            entity: &mut Order,
            status: CheckObjectStatus,
            location: &ObjectLocation,
            results: &mut CheckResults,
        ) {
            if status.is_create() {
                if entity.name.is_empty() {
                    results.push(CheckResult::required(location.clone().member("name")));
                }
            }
            if entity.name.chars().count() < 3 {
                results.push(CheckResult::min_str(
                    location.clone().member("name"),
                    3,
                    entity.name.clone(),
                ));
            }
            if entity.name == "fix" {
                entity.name = "fixed".to_owned();
            }
        }
    }

    impl RawAuditEventSink for RecordingEventSink {
        fn on_event(&self, _ctx: &UserContext, event: &RawAuditEvent) -> Result<(), RuntimeError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    impl SafeAuditEventSink for RecordingSafeEventSink {
        fn on_safe_event(
            &self,
            _ctx: &UserContext,
            event: &SafeAuditEvent,
        ) -> Result<(), RuntimeError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct FixedIdGenerator(u64);

    impl InternalIdGenerator for FixedIdGenerator {
        fn generate_id(&self, _entity: &str) -> Result<u64, RuntimeError> {
            Ok(self.0)
        }
    }

    struct SequentialIdGenerator {
        next: Mutex<u64>,
    }

    impl SequentialIdGenerator {
        fn new(next: u64) -> Self {
            Self {
                next: Mutex::new(next),
            }
        }
    }

    impl InternalIdGenerator for SequentialIdGenerator {
        fn generate_id(&self, _entity: &str) -> Result<u64, RuntimeError> {
            let mut next = self
                .next
                .lock()
                .map_err(|err| RuntimeError::IdGeneration(err.to_string()))?;
            let id = *next;
            *next += 1;
            Ok(id)
        }
    }

    #[test]
    fn detached_reverse_relation_remains_in_entity_metadata() {
        let descriptor = ProductWithDetachedLinesRow::entity_descriptor();
        assert_eq!(descriptor.properties.len(), 2);
        assert_eq!(descriptor.relations.len(), 1);
        let relation = &descriptor.relations[0];
        assert_eq!(relation.name, "line_list");
        assert_eq!(relation.target_entity, "DetachedLine");
        assert_eq!(relation.local_key, "id");
        assert_eq!(relation.foreign_key, "product_id");
        assert!(relation.many);
    }

    #[tokio::test]
    async fn metadata_store_registers_entities() {
        let store = InMemoryMetadataStore::new().with_entity(entity());
        assert!(store.entity("Order").is_some());
    }

    #[tokio::test]
    async fn runtime_module_registers_descriptor_into_context() {
        let context = UserContext::new().with_module(RuntimeModule::new().descriptor(entity()));
        assert!(context.entity("Order").is_some());
        assert!(context.has_entity_data_service("Order"));
    }

    #[tokio::test]
    async fn runtime_module_registers_derived_entity_and_behavior() {
        let context = UserContext::new().with_module(
            RuntimeModule::new().entity_with_behavior::<CatalogProductRow, _>(OrderBehavior),
        );
        assert!(context.entity("CatalogProduct").is_some());
        assert!(context.has_entity_data_service("CatalogProduct"));
        assert!(
            context
                .entity_data_service_behavior("CatalogProduct")
                .is_some()
        );
    }

    #[tokio::test]
    async fn module_macro_registers_multiple_entities() {
        let context = UserContext::new().with_module(crate::module!(CatalogProductRow));
        assert!(context.entity("CatalogProduct").is_some());
        assert!(context.has_entity_data_service("CatalogProduct"));
    }

    #[tokio::test]
    async fn module_macro_registers_entity_behavior_pairs() {
        let context =
            UserContext::new().with_module(crate::module!(CatalogProductRow => OrderBehavior));
        assert!(context.entity("CatalogProduct").is_some());
        assert!(
            context
                .entity_data_service_behavior("CatalogProduct")
                .is_some()
        );
    }

    #[tokio::test]
    async fn data_service_returns_optimistic_lock_conflict() {
        let store = InMemoryMetadataStore::new().with_entity(entity());
        let executor = StubExecutor {
            affected: 0,
            rows: Vec::new(),
        };
        let repo = RuntimeDataService::new(&store, &executor);

        let err = repo
            .update(
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "next"),
            )
            .await
            .unwrap_err();

        match err {
            DataServiceError::Runtime(RuntimeError::OptimisticLockConflict { .. }) => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn user_context_indexes_resources_and_locals() {
        let mut context =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(entity()));
        context.insert_resource::<u64>(42);
        context.insert_named_resource("tenant", String::from("acme"));
        context.put_local("trace_id", "req-1");

        assert!(context.entity("Order").is_some());
        assert_eq!(context.get_resource::<u64>(), Some(&42));
        assert_eq!(
            context.get_named_resource::<String>("tenant"),
            Some(&String::from("acme"))
        );
        assert_eq!(
            context.local("trace_id"),
            Some(&Value::Text("req-1".to_owned()))
        );
    }

    #[tokio::test]
    async fn user_context_builds_context_data_service() {
        let telemetry_events = Arc::new(Mutex::new(Vec::new()));
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_runtime_telemetry(Arc::new(RecordingRuntimeTelemetry(
                telemetry_events.clone(),
            )));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context.data_service_internal::<StubExecutor>().unwrap();
        let affected = repo
            .update(
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "next"),
            )
            .await
            .unwrap();

        assert_eq!(affected, 1);
        assert_eq!(
            telemetry_events.lock().unwrap().as_slice(),
            ["start:mutation", "start:provider", "success", "success"]
        );
    }

    #[tokio::test]
    async fn user_context_resolves_entity_data_service_by_entity_type() {
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        assert_eq!(repo.entity(), "Order");
        assert_eq!(repo.select().entity, "Order");

        let affected = repo
            .insert_internal(
                &repo
                    .insert_command()
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "n"),
            )
            .await
            .unwrap();
        assert_eq!(affected, 1);
    }

    #[tokio::test]
    async fn entity_data_service_applies_behavior_hooks() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_entity_data_service_behavior_registry(
                InMemoryEntityDataServiceBehaviorRegistry::new()
                    .with_behavior("Order", OrderBehavior),
            );
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();

        // let compiled = repo.compile(&repo.select()).unwrap();
        // assert!(compiled.sql.contains("WHERE (version = $1)"));

        let insert = repo.insert_command().value("id", 1_u64).value("name", "n");
        let affected = repo.insert_internal(&insert).await.unwrap();
        assert_eq!(affected, 1);
        assert_eq!(repo.relation_loads(), vec!["lines".to_owned()]);
    }

    #[tokio::test]
    async fn entity_data_service_applies_request_policy_after_behavior_hooks() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_entity_data_service_behavior_registry(
                InMemoryEntityDataServiceBehaviorRegistry::new()
                    .with_behavior("Order", OrderBehavior),
            )
            .with_request_policy(TenantRequestPolicy);
        context.insert_named_resource("tenant_id", 9_u64);
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();

        // let compiled = repo.compile(&repo.select()).unwrap();
        // assert!(compiled.sql.contains("version = $1"));
        // assert!(compiled.sql.contains("id = $2"));

        let insert = repo.insert_command().value("id", 1_u64).value("name", "n");
        let command = repo.prepare_insert_command(&insert).unwrap();
        assert_eq!(command.values.get("version"), Some(&Value::I64(9)));
    }

    #[tokio::test]
    async fn entity_data_service_prepares_insert_command_with_generated_id() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_entity_data_service_behavior_registry(
                InMemoryEntityDataServiceBehaviorRegistry::new()
                    .with_behavior("Order", OrderBehavior),
            )
            .with_internal_id_generator(FixedIdGenerator(42));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();

        let prepared = repo
            .prepare_insert_command(&repo.insert_command().value("id", 0_u64).value("name", "n"))
            .unwrap();

        assert_eq!(prepared.values.get("id"), Some(&Value::U64(42)));
        assert_eq!(prepared.values.get("version"), Some(&Value::I64(1)));
        assert_eq!(
            prepared.values.get("name"),
            Some(&Value::Text("n".to_owned()))
        );

        let prepared_zero_version = repo
            .prepare_insert_command(
                &repo
                    .insert_command()
                    .value("id", 0_u64)
                    .value("version", 0_i64)
                    .value("name", "zero-version"),
            )
            .unwrap();
        assert_eq!(
            prepared_zero_version.values.get("version"),
            Some(&Value::I64(1))
        );
    }

    #[tokio::test]
    async fn custom_user_context_can_drive_insert_preparation() {
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_entity_data_service_behavior_registry(
                InMemoryEntityDataServiceBehaviorRegistry::new()
                    .with_behavior("Order", ContextAwareOrderBehavior),
            )
            .with_internal_id_generator(FixedIdGenerator(99));
        context.insert_named_resource("tenant", String::from("acme"));
        context.insert_named_resource("initial_version", 7_i64);
        context.put_local("trace_id", "req-9");
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let prepared = repo.prepare_insert_command(&repo.insert_command()).unwrap();

        assert_eq!(prepared.values.get("id"), Some(&Value::U64(99)));
        assert_eq!(prepared.values.get("version"), Some(&Value::I64(7)));
        assert_eq!(
            prepared.values.get("name"),
            Some(&Value::Text("acme:req-9".to_owned()))
        );
    }

    #[tokio::test]
    async fn checker_registry_validates_and_fixes_insert_commands() {
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker))
            .with_internal_id_generator(FixedIdGenerator(77));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let prepared = repo
            .prepare_insert_command(&repo.insert_command().value("name", "valid"))
            .unwrap();

        assert_eq!(prepared.values.get("id"), Some(&Value::U64(77)));
        assert_eq!(prepared.values.get("version"), Some(&Value::I64(1)));
        assert!(!prepared.values.contains_key(CHECK_OBJECT_STATUS_FIELD));

        let error = repo
            .prepare_insert_command(&repo.insert_command().value("name", "no"))
            .unwrap_err();
        match error {
            RuntimeError::Check(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].location.to_string(), "name");
            }
            other => panic!("unexpected checker error: {other:?}"),
        }
    }

    #[test]
    fn metadata_not_null_constraints_are_checked_without_a_custom_checker() {
        let context = UserContext::new().with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("School")
                    .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                    .property(PropertyDescriptor::new("contact_phone", DataType::Text).not_null()),
            ),
        );
        let mut values = EntityValues::from(Record::from([
            ("id".to_owned(), Value::U64(1)),
            (
                CHECK_OBJECT_STATUS_FIELD.to_owned(),
                Value::from(CheckObjectStatus::Create),
            ),
        ]));

        let error = context
            .check_and_fix_values("School", &mut values)
            .unwrap_err();

        match error {
            RuntimeError::Check(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].rule, CheckRule::Required);
                assert_eq!(results[0].location.to_string(), "contact_phone");
            }
            other => panic!("unexpected validation error: {other:?}"),
        }
    }

    #[test]
    fn metadata_validation_does_not_require_runtime_managed_version_on_create() {
        let context = UserContext::new().with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("School")
                    .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                    .property(
                        PropertyDescriptor::new("version", DataType::I64)
                            .version()
                            .not_null(),
                    )
                    .property(PropertyDescriptor::new("name", DataType::Text).not_null()),
            ),
        );
        let mut values = EntityValues::from(Record::from([
            ("id".to_owned(), Value::U64(1)),
            ("name".to_owned(), Value::Text("TeaQL School".to_owned())),
            (
                CHECK_OBJECT_STATUS_FIELD.to_owned(),
                Value::from(CheckObjectStatus::Create),
            ),
        ]));

        context.check_and_fix_values("School", &mut values).unwrap();
        assert!(!values.contains_key("version"));
    }

    #[test]
    fn typed_checker_preserves_values_and_reports_timestamp_type_error() {
        let context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new().with_entity(TimestampedEntity::entity_descriptor()),
            )
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(
                TypedEntityChecker::<TimestampedEntity, _>::new(NoopTimestampedChecker),
            ));
        let mut values = EntityValues::from(Record::from([
            ("id".to_owned(), Value::U64(7)),
            ("version".to_owned(), Value::I64(1)),
            (
                "happened_at".to_owned(),
                Value::Text("2026-08-25".to_owned()),
            ),
            (
                CHECK_OBJECT_STATUS_FIELD.to_owned(),
                Value::from(CheckObjectStatus::Update),
            ),
        ]));

        let error = context
            .check_and_fix_values("TimestampedEntity", &mut values)
            .unwrap_err();

        assert_eq!(
            values.get("happened_at"),
            Some(&Value::Text("2026-08-25".to_owned()))
        );
        match error {
            RuntimeError::Check(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].rule, CheckRule::InvalidType);
                let message = results[0].message.as_deref().unwrap_or_default();
                assert!(message.contains("happened_at"), "{message}");
                assert!(message.contains("2026-08-25"), "{message}");
            }
            other => panic!("unexpected checker error: {other:?}"),
        }
    }

    #[test]
    fn metadata_not_null_constraints_allow_omitted_fields_on_partial_update() {
        let context = UserContext::new().with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("School")
                    .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                    .property(PropertyDescriptor::new("contact_phone", DataType::Text).not_null()),
            ),
        );
        let mut values = EntityValues::from(Record::from([
            ("id".to_owned(), Value::U64(1)),
            (
                CHECK_OBJECT_STATUS_FIELD.to_owned(),
                Value::from(CheckObjectStatus::Update),
            ),
        ]));

        context.check_and_fix_values("School", &mut values).unwrap();

        values.insert("contact_phone".to_owned(), Value::Null);
        assert!(matches!(
            context.check_and_fix_values("School", &mut values),
            Err(RuntimeError::Check(_))
        ));
    }

    #[tokio::test]
    async fn typed_checker_validates_and_fixes_derived_entities_without_record_access() {
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(Order::entity_descriptor()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_checker_registry(
                InMemoryCheckerRegistry::new()
                    .with_checker(TypedEntityChecker::<Order, _>::new(TypedOrderChecker)),
            )
            .with_internal_id_generator(FixedIdGenerator(79));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let prepared = repo
            .prepare_insert_command(&repo.insert_command().value("name", "fix"))
            .unwrap();
        assert_eq!(
            prepared.values.get("name"),
            Some(&Value::Text("fixed".to_owned()))
        );
        assert_eq!(prepared.values.get("id"), Some(&Value::U64(79)));
        assert_eq!(prepared.values.get("version"), Some(&Value::I64(1)));
        assert!(!prepared.values.contains_key(CHECK_OBJECT_STATUS_FIELD));

        let error = repo
            .prepare_insert_command(&repo.insert_command().value("version", 1_i64))
            .unwrap_err();
        match error {
            RuntimeError::Check(results) => {
                assert!(
                    results
                        .iter()
                        .any(|result| result.rule == CheckRule::Required
                            && result.location.to_string() == "name")
                );
            }
            other => panic!("unexpected typed checker error: {other:?}"),
        }
    }

    #[test]
    fn typed_checker_preserves_sparse_update_boundary() {
        let context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(Order::entity_descriptor()))
            .with_checker_registry(
                InMemoryCheckerRegistry::new()
                    .with_checker(TypedEntityChecker::<Order, _>::new(TypedOrderChecker)),
            );
        let mut values = EntityValues::from(Record::from([
            ("id".to_owned(), Value::U64(7)),
            ("name".to_owned(), Value::Text("valid".to_owned())),
            (
                CHECK_OBJECT_STATUS_FIELD.to_owned(),
                Value::from(CheckObjectStatus::Update),
            ),
        ]));

        context.check_and_fix_values("Order", &mut values).unwrap();

        assert_eq!(values.get("id"), Some(&Value::U64(7)));
        assert_eq!(values.get("name"), Some(&Value::Text("valid".to_owned())));
        assert!(
            !values.contains_key("version"),
            "a defaulted typed-checker field became update intent"
        );

        values.insert("name".to_owned(), Value::Text("fix".to_owned()));
        context.check_and_fix_values("Order", &mut values).unwrap();
        assert_eq!(values.get("name"), Some(&Value::Text("fixed".to_owned())));
        assert!(
            !values.contains_key("version"),
            "checker fix expanded the sparse update"
        );
    }

    #[tokio::test]
    async fn checker_registry_reports_nested_create_locations_and_fixes_records() {
        let context = UserContext::new()
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker));

        let mut child = EntityValues::from(Record::from([
            (String::from("id"), Value::U64(10)),
            (
                String::from(CHECK_OBJECT_STATUS_FIELD),
                Value::from(CheckObjectStatus::Create),
            ),
        ]));
        let error = context
            .check_and_fix_values_at(
                "Order",
                &mut child,
                &ObjectLocation::hash_root("lines").element(0),
            )
            .unwrap_err();

        assert_eq!(child.get("version"), Some(&Value::I64(1)));
        match error {
            RuntimeError::Check(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].rule, CheckRule::Required);
                assert_eq!(results[0].location.to_string(), "lines[0].name");
            }
            other => panic!("unexpected checker error: {other:?}"),
        }

        child.insert("name".to_owned(), Value::Text("valid child".to_owned()));
        context
            .check_and_fix_values_at(
                "Order",
                &mut child,
                &ObjectLocation::hash_root("lines").element(0),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn built_in_language_translators_cover_fifteen_languages() {
        assert_eq!(Language::ALL.len(), 15);
        let results = [
            super::CheckResult::required(ObjectLocation::hash_root("name")),
            super::CheckResult::min(ObjectLocation::hash_root("age"), 18_i64, 12_i64),
            super::CheckResult::max(ObjectLocation::hash_root("age"), 65_i64, 70_i64),
            super::CheckResult::min_str(ObjectLocation::hash_root("name"), 2, "x"),
            super::CheckResult::max_str(ObjectLocation::hash_root("name"), 8, "too long name"),
        ];
        let messages = Language::ALL
            .iter()
            .flat_map(|language| {
                results
                    .iter()
                    .map(|result| translate_check_result(*language, result))
            })
            .collect::<Vec<_>>();

        assert_eq!(messages.len(), 75);
        assert!(messages.iter().all(|message| !message.is_empty()));
        assert!(messages.iter().all(|message| !message.contains('{')));
        assert!(messages.iter().any(|message| message.contains("required")));
        assert!(messages.iter().any(|message| message.contains("必填")));
        assert!(
            messages
                .iter()
                .any(|message| message.contains("obligatoire"))
        );
        assert_eq!(Language::from_code("zh-CN"), Some(Language::Chinese));
        assert_eq!(
            Language::from_code("zh-TW"),
            Some(Language::TraditionalChinese)
        );
    }

    #[tokio::test]
    async fn user_context_language_switch_translates_checker_errors() {
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker))
            .with_internal_id_generator(FixedIdGenerator(77))
            .with_language(Language::Chinese);
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let error = repo
            .prepare_insert_command(&repo.insert_command())
            .unwrap_err();
        match error {
            RuntimeError::Check(results) => {
                assert_eq!(results.len(), 1);
                assert!(
                    results[0]
                        .message
                        .as_ref()
                        .is_some_and(|message| message.contains("必填"))
                );
            }
            other => panic!("unexpected checker error: {other:?}"),
        }

        let mut context = UserContext::new().with_language(Language::English);
        context.set_language_code("es").unwrap();
        assert_eq!(context.language(), Language::Spanish);
        assert!(context.set_locale_code("invalid-code").is_err());
        assert_eq!(context.language(), Language::Spanish);

        let catalog = I18nCatalog::from_json(
            r#"{
                "schema":"teaql.i18n/v1",
                "defaultLocale":"en",
                "locales":{
                    "en":{"messages":{"checker.required":"EN {location}"},"vocabulary":{}},
                    "es":{"messages":{"checker.required":"ES {location}"},"vocabulary":{}}
                }
            }"#,
        )
        .unwrap();
        context.set_i18n_catalog(Arc::new(catalog));
        let mut results = vec![super::CheckResult::required(ObjectLocation::hash_root(
            "name",
        ))];
        context.translate_check_results(&mut results);
        assert_eq!(results[0].message.as_deref(), Some("ES Name"));
    }

    #[tokio::test]
    async fn user_context_event_sink_receives_data_service_mutation_events() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let safe_events = Arc::new(Mutex::new(Vec::new()));
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity().audit_mask_fields(vec!["name".to_owned()])),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_internal_id_generator(FixedIdGenerator(88))
            .with_event_sink(RecordingEventSink {
                events: events.clone(),
            })
            .with_custom_event_sink(RecordingSafeEventSink {
                events: safe_events.clone(),
            });
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                ("id".to_owned(), Value::U64(88)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("old".to_owned())),
            ])],
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        repo.insert_internal(&repo.insert_command().value("name", "created"))
            .await
            .unwrap();
        repo.update_internal(
            &repo
                .update_command(88_u64)
                .expected_version(1)
                .value("name", "updated"),
        )
        .await
        .unwrap();
        repo.delete_internal(&repo.delete_command(88_u64).expected_version(2))
            .await
            .unwrap();
        repo.recover_internal(&repo.recover_command(88_u64, -3))
            .await
            .unwrap();

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].kind, RawAuditEventKind::Created);
        assert_eq!(events[0].entity, "Order");
        assert_eq!(events[0].values.get("id"), Some(&Value::U64(88)));
        assert_eq!(events[1].kind, RawAuditEventKind::Updated);
        assert_eq!(events[1].values.get("id"), Some(&Value::U64(88)));
        assert_eq!(events[1].values.get("version"), Some(&Value::I64(2)));
        assert_eq!(events[1].updated_fields, vec!["name".to_owned()]);
        assert_eq!(
            events[1]
                .old_values
                .as_ref()
                .and_then(|values| values.get("name")),
            None // We no longer fetch old_values dynamically
        );
        assert_eq!(
            events[1]
                .new_values
                .as_ref()
                .and_then(|values| values.get("name")),
            Some(&Value::Text("updated".to_owned()))
        );
        assert_eq!(events[1].changes.len(), 1);
        assert_eq!(events[1].changes[0].field, "name");
        assert_eq!(
            events[1].changes[0].old_value,
            None // Old value is now absent during blind updates
        );
        assert_eq!(
            events[1].changes[0].new_value,
            Some(Value::Text("updated".to_owned()))
        );
        assert_eq!(events[2].kind, RawAuditEventKind::Deleted);
        assert!(events[2].old_values.is_none()); // No longer fetched
        assert!(events[2].new_values.is_none());
        assert_eq!(events[3].kind, RawAuditEventKind::Recovered);
        assert_eq!(
            events[3]
                .old_values
                .as_ref()
                .and_then(|values| values.get("version")),
            None // No longer fetched
        );
        assert_eq!(
            events[3]
                .new_values
                .as_ref()
                .and_then(|values| values.get("version")),
            Some(&Value::I64(4))
        );
        assert_eq!(events[3].changes[0].field, "version");
        drop(events);

        let safe_events = safe_events.lock().unwrap();
        assert_eq!(safe_events.len(), 4);
        assert_eq!(safe_events[0].kind, RawAuditEventKind::Created);
        let name = safe_events[0]
            .fields
            .iter()
            .find(|field| field.name == "name")
            .expect("application audit event should contain the changed name field");
        assert!(name.masked);
        assert_ne!(name.value.as_deref(), Some("created"));
    }

    #[tokio::test]
    async fn entity_data_service_builds_relation_plans() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_entity_data_service_behavior_registry(
                InMemoryEntityDataServiceBehaviorRegistry::new()
                    .with_behavior("Order", OrderBehavior),
            );
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let plans = repo.relation_plans().unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].relation_name, "lines");
        assert_eq!(plans[0].target_entity, "OrderLine");
        assert_eq!(plans[0].local_key, "id");
        assert_eq!(plans[0].foreign_key, "order_id");
        assert!(plans[0].many);
    }

    #[tokio::test]
    async fn entity_data_service_builds_relation_query_from_parent_rows() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_entity_data_service_behavior_registry(
                InMemoryEntityDataServiceBehaviorRegistry::new()
                    .with_behavior("Order", OrderBehavior),
            );
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let parent_rows = vec![
            teaql_core::CompactRow::from_map(Record::from([(String::from("id"), Value::U64(11))])),
            teaql_core::CompactRow::from_map(Record::from([(String::from("id"), Value::U64(12))])),
            teaql_core::CompactRow::from_map(Record::from([(String::from("id"), Value::U64(11))])),
        ];

        let query = repo.relation_query("lines", &parent_rows).unwrap();
        let Some(Expr::Binary { right, .. }) = query.filter else {
            panic!("relation query should contain an IN filter")
        };
        let Expr::Value(Value::List(ids)) = *right else {
            panic!("relation IN filter should contain identity values")
        };
        assert_eq!(ids, vec![Value::U64(11), Value::U64(12)]);
        // let compiled = repo.compile(&query).unwrap();
        // assert!(compiled.sql.contains("FROM orderline"));
        // assert!(compiled.sql.contains("order_id IN ($1, $2)"));
        // assert_eq!(compiled.params, vec![Value::U64(11), Value::U64(12)]);
    }

    #[tokio::test]
    async fn entity_data_service_enhances_parent_rows_with_relations() {
        let telemetry_events = Arc::new(Mutex::new(Vec::new()));
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_entity_data_service_behavior_registry(
                InMemoryEntityDataServiceBehaviorRegistry::new()
                    .with_behavior("Order", OrderBehavior),
            )
            .with_runtime_telemetry(Arc::new(RecordingRuntimeTelemetry(
                telemetry_events.clone(),
            )));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![
                Record::from([
                    (String::from("id"), Value::U64(101)),
                    (String::from("order_id"), Value::U64(11)),
                    (String::from("name"), Value::Text(String::from("l1"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(102)),
                    (String::from("order_id"), Value::U64(11)),
                    (String::from("name"), Value::Text(String::from("l2"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(201)),
                    (String::from("order_id"), Value::U64(12)),
                    (String::from("name"), Value::Text(String::from("l3"))),
                ]),
            ],
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let mut parents = vec![
            teaql_core::CompactRow::from_map(Record::from([(String::from("id"), Value::U64(11))])),
            teaql_core::CompactRow::from_map(Record::from([(String::from("id"), Value::U64(12))])),
        ];

        repo.enhance_relations_internal(&mut parents).await.unwrap();

        match parents[0].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 2),
            other => panic!("unexpected lines payload: {other:?}"),
        }
        match parents[1].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 1),
            other => panic!("unexpected lines payload: {other:?}"),
        }
        assert!(
            telemetry_events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "start:relation_load")
        );
    }

    #[tokio::test]
    async fn relation_limit_is_partitioned_per_parent_and_rank_is_internal() {
        let mut rows = Vec::new();
        for (order_id, first_line_id) in [(11_u64, 101_u64), (12_u64, 201_u64)] {
            for rank in 1_u64..=3 {
                rows.push(Record::from([
                    (String::from("id"), Value::U64(first_line_id + rank - 1)),
                    (String::from("order_id"), Value::U64(order_id)),
                    (
                        String::from(teaql_core::PARTITION_RANK_PROPERTY),
                        Value::U64(rank),
                    ),
                ]));
            }
        }

        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(CapturingQueryExecutor {
            rows,
            queries: Mutex::new(Vec::new()),
        });

        let repo = context
            .entity_data_service::<CapturingQueryExecutor>("Order")
            .unwrap();
        let mut parents = vec![
            teaql_core::CompactRow::from_map(Record::from([(String::from("id"), Value::U64(11))])),
            teaql_core::CompactRow::from_map(Record::from([(String::from("id"), Value::U64(12))])),
        ];
        let query = SelectQuery::new("Order").relation_query(
            "lines",
            SelectQuery::new("OrderLine")
                .order_by(OrderBy::desc("id"))
                .limit(3),
        );

        repo.enhance_query_relations_internal(&mut parents, &query)
            .await
            .unwrap();

        let captured = &context
            .get_resource::<CapturingQueryExecutor>()
            .unwrap()
            .queries
            .lock()
            .unwrap()[0];
        assert_eq!(captured.partition_by.as_deref(), Some("order_id"));
        assert_eq!(captured.slice.and_then(|slice| slice.limit), Some(3));
        for parent in &parents {
            let Some(Value::List(lines)) = parent.get("lines") else {
                panic!("missing relation lines")
            };
            assert_eq!(lines.len(), 3);
            assert!(lines.iter().all(|line| match line {
                Value::Object(line) => !line.contains_key(teaql_core::PARTITION_RANK_PROPERTY),
                _ => false,
            }));
        }
    }

    #[tokio::test]
    async fn relation_enhancement_wraps_inverse_many_relation_as_list() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(OrderLineWithProductEntityRow::entity_descriptor())
                    .with_entity(ProductWithLinesEntityRow::entity_descriptor()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("OrderLine"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([
                vec![Record::from([
                    (String::from("id"), Value::U64(11)),
                    (String::from("order_id"), Value::U64(7)),
                    (String::from("name"), Value::Text(String::from("line"))),
                    (String::from("product_id"), Value::U64(101)),
                ])],
                vec![Record::from([
                    (String::from("id"), Value::U64(101)),
                    (String::from("name"), Value::Text(String::from("sku"))),
                ])],
            ])),
            queries: Mutex::new(Vec::new()),
        });

        let repo = context
            .entity_data_service::<QueueExecutor>("OrderLine")
            .unwrap();
        let rows = repo
            .fetch_enhanced_entities_internal::<OrderLineWithProductEntityRow>(
                &SelectQuery::new("OrderLine").relation("product"),
            )
            .await
            .unwrap();

        let product = rows.data[0].product.as_ref().unwrap();
        assert_eq!(product.lines.data.len(), 1);
        assert_eq!(product.lines.data[0].id, 11);
    }

    #[tokio::test]
    async fn generated_to_one_getter_resolves_from_runtime_module_identity_graph() {
        let mut context = RuntimeModule::new()
            .entity::<FlatTripRow>()
            .entity::<FlatVendorRow>()
            .into_context();
        context.insert_resource(PostgresDialect);
        context.insert_resource(QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([
                vec![Record::from([
                    (String::from("id"), Value::U64(11)),
                    (String::from("vendor_id"), Value::U64(101)),
                ])],
                vec![Record::from([
                    (String::from("id"), Value::U64(101)),
                    (String::from("name"), Value::Text(String::from("Acme"))),
                ])],
            ])),
            queries: Mutex::new(Vec::new()),
        });

        let repo = context
            .entity_data_service::<QueueExecutor>("FlatTrip")
            .unwrap();
        let rows = repo
            .fetch_enhanced_entities_internal::<FlatTripRow>(
                &SelectQuery::new("FlatTrip").relation("vendor"),
            )
            .await
            .unwrap();

        assert!(rows.data[0].vendor.is_none());
        assert_eq!(rows.data[0].vendor().unwrap().name, "Acme");
    }

    #[tokio::test]
    async fn generated_to_many_getter_uses_adjacency_and_mutation_copies_on_write() {
        let mut context = RuntimeModule::new()
            .entity::<FlatFleetRow>()
            .entity::<FlatFleetTripRow>()
            .into_context();
        context.insert_resource(PostgresDialect);
        context.insert_resource(QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([
                vec![Record::from([(String::from("id"), Value::U64(7))])],
                vec![
                    Record::from([
                        (String::from("id"), Value::U64(11)),
                        (String::from("fleet_id"), Value::U64(7)),
                        (String::from("name"), Value::Text(String::from("first"))),
                    ]),
                    Record::from([
                        (String::from("id"), Value::U64(12)),
                        (String::from("fleet_id"), Value::U64(7)),
                        (String::from("name"), Value::Text(String::from("second"))),
                    ]),
                ],
            ])),
            queries: Mutex::new(Vec::new()),
        });

        let repo = context
            .entity_data_service::<QueueExecutor>("FlatFleet")
            .unwrap();
        let mut rows = repo
            .fetch_enhanced_entities_internal::<FlatFleetRow>(
                &SelectQuery::new("FlatFleet").relation("trip_list"),
            )
            .await
            .unwrap();
        let fleet = &mut rows.data[0];

        assert!(!fleet.trip_list.is_loaded);
        assert_eq!(fleet.trip_list().data.len(), 2);
        assert_eq!(fleet.trip_list().data[1].name, "second");
        fleet.trip_list_mut().push(FlatFleetTripRow {
            id: 13,
            fleet_id: 7,
            name: "third".to_owned(),
            root: EntityRuntimeState::default(),
        });
        assert!(fleet.trip_list.is_loaded);
        assert_eq!(fleet.trip_list().data.len(), 3);
        assert_eq!(
            fleet
                .root
                .resolve_relation_list::<FlatFleetTripRow>("FlatFleet", 7, "trip_list")
                .unwrap()
                .data
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn entity_data_service_fetches_smart_list_of_entities() {
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                (String::from("id"), Value::U64(7)),
                (String::from("version"), Value::I64(2)),
                (String::from("name"), Value::Text(String::from("typed"))),
            ])],
        });

        let repo = context
            .entity_data_service::<StubExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_entities_internal::<OrderEntity>(&repo.select())
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.first(),
            Some(&OrderEntity {
                id: 7,
                version: 2,
                name: String::from("typed"),
            })
        );
    }

    #[tokio::test]
    async fn typed_entity_fetch_restores_id_and_version_to_reduced_projection() {
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(CapturingQueryExecutor {
            rows: vec![Record::from([
                (String::from("id"), Value::U64(7)),
                (String::from("version"), Value::I64(2)),
                (String::from("name"), Value::Text(String::from("typed"))),
            ])],
            ..Default::default()
        });

        let repo = context
            .entity_data_service::<CapturingQueryExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_entities_internal::<OrderEntity>(&SelectQuery::new("Order").project("name"))
            .await
            .unwrap();
        let enhanced_rows = repo
            .fetch_enhanced_entities_internal::<OrderEntity>(
                &SelectQuery::new("Order").project("name"),
            )
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(enhanced_rows.len(), 1);
        let executor = context
            .get_resource::<CapturingQueryExecutor>()
            .expect("capturing executor");
        let queries = executor.queries.lock().unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].projection, vec!["name", "id", "version"]);
        assert_eq!(queries[1].projection, vec!["name", "id", "version"]);
    }

    #[tokio::test]
    async fn entity_data_service_fetches_smart_list_of_derived_entities() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new().with_entity(CatalogProductRow::entity_descriptor()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("CatalogProduct"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                (String::from("id"), Value::U64(9)),
                (String::from("name"), Value::Text(String::from("derived"))),
            ])],
        });

        let repo = context
            .entity_data_service::<StubExecutor>("CatalogProduct")
            .unwrap();
        let rows = repo
            .fetch_entities_internal::<CatalogProductRow>(&repo.select())
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.first(),
            Some(&CatalogProductRow {
                id: 9,
                name: String::from("derived"),
            })
        );
    }

    #[tokio::test]
    async fn entity_data_service_collects_dynamic_properties_for_aggregate_output() {
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(OrderAggregateDynamic::entity_descriptor()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("OrderAggregate"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                (String::from("id"), Value::U64(1)),
                (String::from("lineCount"), Value::I64(3)),
                (String::from("amount"), Value::F64(18.5)),
            ])],
        });

        let repo = context
            .entity_data_service::<StubExecutor>("OrderAggregate")
            .unwrap();
        let rows = repo
            .fetch_entities_internal::<OrderAggregateDynamic>(&repo.select())
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows.data[0].id, 1);
        assert_eq!(rows.data[0].dynamic.get("lineCount"), Some(&Value::I64(3)));
        assert_eq!(rows.data[0].dynamic.get("amount"), Some(&Value::F64(18.5)));
        assert_eq!(
            rows.into_vec().into_iter().next().unwrap().into_json(),
            serde_json::json!({
                "id": 1,
                "lineCount": 3,
                "amount": 18.5
            })
        );
    }

    #[tokio::test]
    async fn entity_data_service_executes_relation_aggregates_into_dynamic_properties() {
        let executor = QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([
                vec![
                    Record::from([
                        (String::from("id"), Value::U64(1)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("name"), Value::Text(String::from("first"))),
                    ]),
                    Record::from([
                        (String::from("id"), Value::U64(2)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("name"), Value::Text(String::from("second"))),
                    ]),
                ],
                vec![Record::from([
                    (String::from("order_id"), Value::U64(1)),
                    (String::from("lineCount"), Value::I64(3)),
                ])],
            ])),
            queries: Mutex::new(Vec::new()),
        };
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(executor);

        let repo = context
            .entity_data_service::<QueueExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_all_with_relation_aggregates_internal(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name"),
                &[RelationAggregate::new(
                    "lines",
                    "lineCount",
                    SelectQuery::new("OrderLine"),
                    true,
                )],
            )
            .await
            .unwrap();

        assert_eq!(rows[0].get("lineCount"), Some(&Value::I64(3)));
        assert_eq!(rows[1].get("lineCount"), Some(&Value::U64(0)));

        let executor = context.get_resource::<QueueExecutor>().unwrap();
        let queries = executor.queries.lock().unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[1], "SELECT ... FROM OrderLine ...");
    }

    #[tokio::test]
    async fn entity_data_service_maps_relation_aggregate_storage_key_to_property_key() {
        let mut line = line_entity();
        line.properties
            .iter_mut()
            .find(|property| property.name == "order_id")
            .unwrap()
            .column_name = "order_ref".to_owned();
        let executor = QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([
                vec![Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("first"))),
                ])],
                vec![Record::from([
                    (String::from("order_ref"), Value::I64(1)),
                    (String::from("lineCount"), Value::I64(3)),
                ])],
            ])),
            queries: Mutex::new(Vec::new()),
        };
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(executor);

        let repo = context
            .entity_data_service::<QueueExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_all_with_relation_aggregates_internal(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name"),
                &[RelationAggregate::new(
                    "lines",
                    "lineCount",
                    SelectQuery::new("OrderLine"),
                    true,
                )],
            )
            .await
            .unwrap();

        assert_eq!(rows[0].get("lineCount"), Some(&Value::I64(3)));
        let executor = context.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(
            executor.queries.lock().unwrap()[1],
            "SELECT ... FROM OrderLine ..."
        );
    }

    #[tokio::test]
    async fn entity_data_service_uses_aggregation_cache_when_resource_is_registered() {
        let telemetry_events = Arc::new(Mutex::new(Vec::new()));
        let executor = QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([vec![Record::from([(
                String::from("count"),
                Value::I64(2),
            )])]])),
            queries: Mutex::new(Vec::new()),
        };
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"))
            .with_runtime_telemetry(Arc::new(RecordingRuntimeTelemetry(
                telemetry_events.clone(),
            )));
        context.insert_resource(PostgresDialect);
        context.insert_resource(executor);
        context.insert_resource(InMemoryAggregationCache::default());

        let repo = context
            .entity_data_service::<QueueExecutor>("Order")
            .unwrap();
        let query = repo
            .select()
            .count("count")
            .enable_aggregation_cache_for(60_000);

        let first = repo.fetch_all_internal(&query).await.unwrap();
        let second = repo.fetch_all_internal(&query).await.unwrap();

        assert_eq!(first, second);
        let executor = context.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(executor.queries.lock().unwrap().len(), 1);
        let events = telemetry_events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.as_str() == "start:cache")
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.as_str() == "start:provider")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn continuous_page_fetch_uses_id_seek_for_the_next_page() {
        let rows = (91_u64..=100)
            .rev()
            .map(|id| {
                Record::from([
                    (String::from("id"), Value::U64(id)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(format!("order-{id}"))),
                ])
            })
            .collect();
        let mut context = UserContext::new()
            .with_user_identifier("tenant-1:user-1")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(CapturingQueryExecutor {
            rows,
            queries: Mutex::new(Vec::new()),
        });
        let repo = context
            .entity_data_service::<CapturingQueryExecutor>("Order")
            .unwrap();

        let first = SelectQuery::new("Order")
            .order_desc("id")
            .page(0, 10)
            .optimize_for_continuous_page_fetch_with("recent-orders", 60);
        repo.fetch_all_internal(&first).await.unwrap();
        assert_eq!(
            context.continuous_page_plan().as_deref(),
            Some("OFFSET_FALLBACK:FIRST_PAGE")
        );

        let second = SelectQuery::new("Order")
            .order_desc("id")
            .page(10, 10)
            .optimize_for_continuous_page_fetch_with("recent-orders", 60);
        repo.fetch_all_internal(&second).await.unwrap();
        assert_eq!(
            context.continuous_page_plan().as_deref(),
            Some("CURSOR_SEEK")
        );
        assert!(context.continuous_page_cursor_id().is_some());

        let captured = context
            .get_resource::<CapturingQueryExecutor>()
            .unwrap()
            .queries
            .lock()
            .unwrap();
        assert_eq!(
            captured[1].slice.as_ref().map(|slice| slice.offset),
            Some(0)
        );
        assert!(format!("{:?}", captured[1].filter).contains("Lt"));
        assert!(format!("{:?}", captured[1].filter).contains("U64(91)"));
    }

    #[tokio::test]
    async fn id_set_pagination_reuses_ordered_ids_and_returns_exact_count() {
        let id_rows = (1_u64..=100)
            .map(|id| Record::from([(String::from("id"), Value::U64(id))]))
            .collect::<Vec<_>>();
        let entity_rows = |range: std::ops::RangeInclusive<u64>| {
            range
                .map(|id| {
                    Record::from([
                        (String::from("id"), Value::U64(id)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("name"), Value::Text(format!("order-{id}"))),
                    ])
                })
                .collect::<Vec<_>>()
        };
        let mut context = UserContext::new()
            .with_user_identifier("id-set-test:tenant-1:user-1")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(IdSetQueueExecutor {
            rows: Mutex::new(VecDeque::from([
                id_rows,
                entity_rows(21..=30),
                entity_rows(51..=60),
            ])),
            queries: Mutex::new(Vec::new()),
        });
        let repo = context
            .entity_data_service::<IdSetQueueExecutor>("Order")
            .unwrap();

        let first = repo
            .fetch_enhanced_entities_with_relation_aggregates_internal::<Order>(
                &SelectQuery::new("Order")
                    .projects(["id", "version", "name"])
                    .order_asc("name")
                    .page(20, 10)
                    .optimize_pagination_with_id_set_config("orders", 60, 1_000),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(first.total_count, Some(100));
        assert_eq!(first.first().map(|entity| entity.id), Some(21));
        assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_BUILD"));

        let second = repo
            .fetch_enhanced_entities_with_relation_aggregates_internal::<Order>(
                &SelectQuery::new("Order")
                    .projects(["id", "version", "name"])
                    .order_asc("name")
                    .page(50, 10)
                    .optimize_pagination_with_id_set_config("orders", 60, 1_000),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(second.total_count, Some(100));
        assert_eq!(second.first().map(|entity| entity.id), Some(51));
        assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_HIT"));

        let queries = &context
            .get_resource::<IdSetQueueExecutor>()
            .unwrap()
            .queries
            .lock()
            .unwrap();
        assert_eq!(
            queries.len(),
            3,
            "the second page must not rebuild the ID set"
        );
        assert_eq!(queries[0].projection, vec!["id"]);
        assert_eq!(
            queries[0].slice.as_ref().and_then(|slice| slice.limit),
            Some(1_001)
        );
        assert_eq!(
            queries[0].order_by.last().map(|order| order.field.as_str()),
            Some("id")
        );
        assert_eq!(queries[1].slice.as_ref().map(|slice| slice.offset), Some(0));
        assert!(format!("{:?}", queries[1].filter).contains("U64(21)"));
        assert!(format!("{:?}", queries[2].filter).contains("U64(51)"));
    }

    #[tokio::test]
    async fn id_set_pagination_limit_overflow_falls_back_without_false_count() {
        let mut context = UserContext::new()
            .with_user_identifier("id-set-overflow-test")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(IdSetQueueExecutor {
            rows: Mutex::new(VecDeque::from([
                (1_u64..=4)
                    .map(|id| Record::from([(String::from("id"), Value::U64(id))]))
                    .collect(),
                vec![Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text("order-1".to_owned())),
                ])],
            ])),
            queries: Mutex::new(Vec::new()),
        });
        let repo = context
            .entity_data_service::<IdSetQueueExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_enhanced_entities_with_relation_aggregates_internal::<Order>(
                &SelectQuery::new("Order")
                    .projects(["id", "version", "name"])
                    .order_asc("id")
                    .page(0, 1)
                    .optimize_pagination_with_id_set_config("overflow", 60, 3),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(rows.total_count, None);
        assert_eq!(
            context.id_set_plan().as_deref(),
            Some("ID_SET_FALLBACK_LIMIT_EXCEEDED")
        );
        assert_eq!(context.id_set_count(), Some(4));
    }

    #[tokio::test]
    async fn id_set_pagination_coalesces_concurrent_cache_misses() {
        let executor = ConcurrentIdSetExecutor::default();
        let id_queries = executor.id_queries.clone();
        let store: Arc<dyn crate::IdSetStore> = Arc::new(crate::InMemoryIdSetStore::default());
        let make_context = |executor: ConcurrentIdSetExecutor| {
            let mut context = UserContext::new()
                .with_user_identifier("id-set-single-flight-user")
                .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
                .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
            context.set_id_set_store(store.clone());
            context.insert_resource(PostgresDialect);
            context.insert_resource(executor);
            context
        };
        let first_context = make_context(executor.clone());
        let second_context = make_context(executor);
        let query = SelectQuery::new("Order")
            .projects(["id", "version", "name"])
            .order_asc("id")
            .page(0, 1)
            .optimize_pagination_with_id_set_config("single-flight", 60, 100);

        let first = async {
            first_context
                .entity_data_service::<ConcurrentIdSetExecutor>("Order")
                .unwrap()
                .fetch_enhanced_entities_internal::<Order>(&query)
                .await
                .unwrap()
        };
        let second = async {
            second_context
                .entity_data_service::<ConcurrentIdSetExecutor>("Order")
                .unwrap()
                .fetch_enhanced_entities_internal::<Order>(&query)
                .await
                .unwrap()
        };
        let (first, second) = tokio::join!(first, second);

        assert_eq!(first.total_count, Some(2));
        assert_eq!(second.total_count, Some(2));
        assert_eq!(
            id_queries.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "concurrent misses must share one ID-only build"
        );
    }

    #[tokio::test]
    async fn id_set_pagination_rebuilds_after_ttl_expiry() {
        let executor = ConcurrentIdSetExecutor::default();
        let id_queries = executor.id_queries.clone();
        let mut context = UserContext::new()
            .with_user_identifier("id-set-ttl-user")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.set_id_set_store(Arc::new(crate::InMemoryIdSetStore::default()));
        context.insert_resource(PostgresDialect);
        context.insert_resource(executor);
        let query = SelectQuery::new("Order")
            .projects(["id", "version", "name"])
            .order_asc("id")
            .page(0, 1)
            .optimize_pagination_with_id_set_config("ttl", 1, 100);
        let repo = context
            .entity_data_service::<ConcurrentIdSetExecutor>("Order")
            .unwrap();

        repo.fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1_050)).await;
        repo.fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();

        assert_eq!(id_queries.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_BUILD"));
    }

    #[tokio::test]
    async fn id_set_pagination_isolates_principals_in_a_shared_store() {
        let executor = ConcurrentIdSetExecutor::default();
        let id_queries = executor.id_queries.clone();
        let store: Arc<dyn crate::IdSetStore> = Arc::new(crate::InMemoryIdSetStore::default());
        let make_context = |user: &str| {
            let mut context = UserContext::new()
                .with_user_identifier(user)
                .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
                .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
            context.set_id_set_store(store.clone());
            context.insert_resource(PostgresDialect);
            context.insert_resource(executor.clone());
            context
        };
        let first_context = make_context("tenant-1:user-1");
        let second_context = make_context("tenant-1:user-2");
        let query = SelectQuery::new("Order")
            .projects(["id", "version", "name"])
            .order_asc("id")
            .page(0, 1)
            .optimize_pagination_with_id_set_config("principal-isolation", 60, 100);

        first_context
            .entity_data_service::<ConcurrentIdSetExecutor>("Order")
            .unwrap()
            .fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();
        second_context
            .entity_data_service::<ConcurrentIdSetExecutor>("Order")
            .unwrap()
            .fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();

        assert_eq!(
            id_queries.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "different principals must not share retained IDs"
        );
    }

    #[tokio::test]
    async fn id_set_pagination_retains_empty_exact_result() {
        let mut context = UserContext::new()
            .with_user_identifier("id-set-empty-user")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(IdSetQueueExecutor {
            rows: Mutex::new(VecDeque::from([Vec::new(), Vec::new()])),
            queries: Mutex::new(Vec::new()),
        });
        let query = SelectQuery::new("Order")
            .projects(["id", "version", "name"])
            .order_asc("id")
            .page(0, 10)
            .optimize_pagination_with_id_set_config("empty", 60, 100);

        let rows = context
            .entity_data_service::<IdSetQueueExecutor>("Order")
            .unwrap()
            .fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();

        assert!(rows.is_empty());
        assert_eq!(rows.total_count, Some(0));
        assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_BUILD"));
    }

    #[tokio::test]
    async fn id_set_pagination_store_failure_falls_back_without_changing_rows() {
        let mut context = UserContext::new()
            .with_user_identifier("id-set-store-failure-user")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.set_id_set_store(Arc::new(UnavailableIdSetStore));
        context.insert_resource(PostgresDialect);
        context.insert_resource(IdSetQueueExecutor {
            rows: Mutex::new(VecDeque::from([vec![Record::from([
                (String::from("id"), Value::U64(7)),
                (String::from("version"), Value::I64(1)),
                (String::from("name"), Value::Text("order-7".to_owned())),
            ])]])),
            queries: Mutex::new(Vec::new()),
        });
        let query = SelectQuery::new("Order")
            .projects(["id", "version", "name"])
            .order_asc("id")
            .page(0, 10)
            .optimize_pagination_with_id_set_config("unavailable", 60, 100);

        let rows = context
            .entity_data_service::<IdSetQueueExecutor>("Order")
            .unwrap()
            .fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();

        assert_eq!(rows.first().map(|row| row.id), Some(7));
        assert_eq!(rows.total_count, None);
        assert_eq!(
            context.id_set_plan().as_deref(),
            Some("ID_SET_FALLBACK_STORE_UNAVAILABLE")
        );
    }

    #[tokio::test]
    async fn id_set_pagination_does_not_shift_page_when_an_entity_disappears() {
        let mut context = UserContext::new()
            .with_user_identifier("id-set-delete-user")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(IdSetQueueExecutor {
            rows: Mutex::new(VecDeque::from([
                vec![
                    Record::from([(String::from("id"), Value::U64(1))]),
                    Record::from([(String::from("id"), Value::U64(2))]),
                ],
                vec![Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text("order-2".to_owned())),
                ])],
            ])),
            queries: Mutex::new(Vec::new()),
        });
        let query = SelectQuery::new("Order")
            .projects(["id", "version", "name"])
            .order_asc("id")
            .page(0, 2)
            .optimize_pagination_with_id_set_config("delete", 60, 100);

        let rows = context
            .entity_data_service::<IdSetQueueExecutor>("Order")
            .unwrap()
            .fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();

        assert_eq!(rows.total_count, Some(2));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.first().map(|row| row.id), Some(2));
    }

    #[tokio::test]
    async fn id_set_pagination_unsupported_shape_falls_back_visibly() {
        let mut context = UserContext::new()
            .with_user_identifier("id-set-unsupported-user")
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(IdSetQueueExecutor {
            rows: Mutex::new(VecDeque::from([vec![Record::from([
                (String::from("id"), Value::U64(9)),
                (String::from("version"), Value::I64(1)),
                (String::from("name"), Value::Text("order-9".to_owned())),
            ])]])),
            queries: Mutex::new(Vec::new()),
        });
        let query = SelectQuery::new("Order")
            .projects(["id", "version", "name"])
            .order_expr_asc(Expr::column("name"))
            .page(0, 10)
            .optimize_pagination_with_id_set_config("unsupported", 60, 100);

        let rows = context
            .entity_data_service::<IdSetQueueExecutor>("Order")
            .unwrap()
            .fetch_enhanced_entities_internal::<Order>(&query)
            .await
            .unwrap();

        assert_eq!(rows.first().map(|row| row.id), Some(9));
        assert_eq!(
            context.id_set_plan().as_deref(),
            Some("ID_SET_FALLBACK_UNSUPPORTED_SHAPE")
        );
    }

    #[tokio::test]
    async fn aggregation_cache_is_namespaced_and_invalidated_after_write() {
        let executor = QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([
                vec![Record::from([(String::from("count"), Value::I64(2))])],
                vec![Record::from([(String::from("count"), Value::I64(3))])],
            ])),
            queries: Mutex::new(Vec::new()),
        };
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(executor);
        context.insert_resource(
            Arc::new(InMemoryAggregationCache::with_namespace("tenant-a"))
                as Arc<dyn AggregationCacheBackend>,
        );

        let repo = context
            .entity_data_service::<QueueExecutor>("Order")
            .unwrap();
        let query = repo
            .select()
            .count("count")
            .enable_aggregation_cache_for(60_000);

        let first = repo.fetch_all_internal(&query).await.unwrap();
        let cached = repo.fetch_all_internal(&query).await.unwrap();
        repo.insert_internal(
            &InsertCommand::new("Order")
                .value("id", 9_u64)
                .value("version", 1_i64)
                .value("name", "new"),
        )
        .await
        .unwrap();
        let refreshed = repo.fetch_all_internal(&query).await.unwrap();

        assert_eq!(first, cached);
        assert_ne!(cached, refreshed);
        let executor = context.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(executor.queries.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn aggregation_cache_propagates_to_relation_aggregates() {
        let parent_rows = vec![
            Record::from([
                (String::from("id"), Value::U64(1)),
                (String::from("version"), Value::I64(1)),
                (String::from("name"), Value::Text(String::from("first"))),
            ]),
            Record::from([
                (String::from("id"), Value::U64(2)),
                (String::from("version"), Value::I64(1)),
                (String::from("name"), Value::Text(String::from("second"))),
            ]),
        ];
        let aggregate_rows = vec![Record::from([
            (String::from("order_id"), Value::U64(1)),
            (String::from("lineCount"), Value::I64(3)),
        ])];
        let executor = QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([parent_rows, aggregate_rows])),
            queries: Mutex::new(Vec::new()),
        };
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity()),
            )
            .with_entity_registry(InMemoryEntityRegistry::new().with_entity("Order"));
        context.insert_resource(PostgresDialect);
        context.insert_resource(executor);
        context.insert_resource(InMemoryAggregationCache::default());

        let repo = context
            .entity_data_service::<QueueExecutor>("Order")
            .unwrap();
        let query = repo
            .select()
            .project("id")
            .project("version")
            .project("name")
            .enable_aggregation_cache_for(60_000)
            .propagate_aggregation_cache(60_000);
        let aggregate =
            RelationAggregate::new("lines", "lineCount", SelectQuery::new("OrderLine"), true);

        let first = repo
            .fetch_all_with_relation_aggregates_internal(&query, &[aggregate.clone()])
            .await
            .unwrap();
        let second = repo
            .fetch_all_with_relation_aggregates_internal(&query, &[aggregate])
            .await
            .unwrap();

        let executor = context.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(executor.queries.lock().unwrap().len(), 2);
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn memory_data_service_fetches_smart_list_entities_with_query_features() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata).with_rows(
            "Order",
            vec![
                Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("alpha"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("beta"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(3)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("gamma"))),
                ]),
            ],
        );

        let query = teaql_core::SelectQuery::new("Order")
            .filter(Expr::Binary {
                left: Box::new(Expr::column("id")),
                op: teaql_core::BinaryOp::Gte,
                right: Box::new(Expr::value(2_u64)),
            })
            .order_by(OrderBy::desc("id"))
            .limit(1);

        let orders = data_service.fetch_entities::<Order>(&query).unwrap();

        assert_eq!(orders.ids(), vec![Value::U64(3)]);
        assert_eq!(orders.versions(), vec![1]);
        assert_eq!(orders.first().unwrap().name, "gamma");
    }

    #[tokio::test]
    async fn memory_data_service_runs_relation_aggregates() {
        let metadata = InMemoryMetadataStore::new()
            .with_entity(entity())
            .with_entity(line_entity());

        let data_service = MemoryDataService::new(metadata)
            .with_rows(
                "Order",
                vec![
                    Record::from([
                        (String::from("id"), Value::U64(1)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("name"), Value::Text(String::from("first"))),
                    ]),
                    Record::from([
                        (String::from("id"), Value::U64(2)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("name"), Value::Text(String::from("second"))),
                    ]),
                ],
            )
            .with_rows(
                "OrderLine",
                vec![
                    Record::from([
                        (String::from("id"), Value::U64(10)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("order_id"), Value::U64(1)),
                        (String::from("name"), Value::Text(String::from("line1"))),
                    ]),
                    Record::from([
                        (String::from("id"), Value::U64(11)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("order_id"), Value::U64(1)),
                        (String::from("name"), Value::Text(String::from("line2"))),
                    ]),
                    Record::from([
                        (String::from("id"), Value::U64(12)),
                        (String::from("version"), Value::I64(1)),
                        (String::from("order_id"), Value::U64(2)),
                        (String::from("name"), Value::Text(String::from("line3"))),
                    ]),
                ],
            );

        let query = SelectQuery::new("Order").project("id").project("name");
        let aggregate =
            RelationAggregate::new("lines", "lineCount", SelectQuery::new("OrderLine"), true);

        let rows = data_service
            .fetch_all_with_relation_aggregates(&query, &[aggregate])
            .unwrap();

        assert_eq!(rows.len(), 2);

        let first_order = rows
            .iter()
            .find(|r| r.get("id") == Some(&Value::U64(1)))
            .unwrap();
        assert_eq!(first_order.get("lineCount"), Some(&Value::U64(2)));

        let second_order = rows
            .iter()
            .find(|r| r.get("id") == Some(&Value::U64(2)))
            .unwrap();
        assert_eq!(second_order.get("lineCount"), Some(&Value::U64(1)));
    }

    #[tokio::test]
    async fn memory_data_service_runs_aggregates() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata).with_rows(
            "Order",
            vec![
                Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("alpha"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(2)),
                    (String::from("name"), Value::Text(String::from("beta"))),
                ]),
            ],
        );

        let query = teaql_core::SelectQuery {
            hard_limit: 10_000,
            entity: String::from("Order"),
            projection: Vec::new(),
            expr_projection: Vec::new(),
            filter: None,
            having: None,
            order_by: Vec::new(),
            slice: None,
            partition_by: None,
            trace_chain: Vec::new(),
            aggregates: vec![
                Aggregate {
                    function: AggregateFunction::Count,
                    field: String::from("id"),
                    alias: String::from("count"),
                },
                Aggregate {
                    function: AggregateFunction::Sum,
                    field: String::from("version"),
                    alias: String::from("versionSum"),
                },
            ],
            group_by: Vec::new(),
            relations: Vec::new(),
            aggregation_cache: None,
            comment: None,
            raw_sql: None,
            raw_sql_search_criteria: Vec::new(),
            dynamic_properties: Vec::new(),
            raw_projections: Vec::new(),
            object_group_bys: Vec::new(),
            search_with_text: None,
            child_enhancements: Vec::new(),
            stream_config: None,
            continuous_page_fetch: None,
            id_set_pagination: None,
        };

        let rows = data_service.fetch_all(&query).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("count"), Some(&Value::U64(2)));
        assert_eq!(rows[0].get("versionSum"), Some(&Value::U64(3)));
    }

    #[tokio::test]
    async fn memory_data_service_runs_grouped_aggregates_and_extended_filters() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata).with_rows(
            "Order",
            vec![
                Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("alpha"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(2)),
                    (String::from("name"), Value::Text(String::from("alpha"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(3)),
                    (String::from("version"), Value::I64(3)),
                    (String::from("name"), Value::Text(String::from("tmp-beta"))),
                ]),
            ],
        );

        let rows = data_service
            .fetch_all(
                &teaql_core::SelectQuery::new("Order")
                    .filter(
                        Expr::between("version", 1_i64, 3_i64)
                            .and_expr(Expr::not_like("name", "tmp%"))
                            .and_expr(Expr::not_in_list("name", vec![Value::from("deleted")])),
                    )
                    .group_by("name")
                    .count("total")
                    .sum("version", "versionSum"),
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("name"),
            Some(&Value::Text(String::from("alpha")))
        );
        assert_eq!(rows[0].get("total"), Some(&Value::U64(2)));
        assert_eq!(rows[0].get("versionSum"), Some(&Value::U64(3)));
    }

    #[tokio::test]
    async fn memory_data_service_runs_extended_aggregates_and_having() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata).with_rows(
            "Order",
            vec![
                Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("alpha"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(3)),
                    (String::from("name"), Value::Text(String::from("alpha"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(3)),
                    (String::from("version"), Value::I64(7)),
                    (String::from("name"), Value::Text(String::from("beta"))),
                ]),
            ],
        );

        let rows = data_service
            .fetch_all(
                &teaql_core::SelectQuery::new("Order")
                    .group_by("name")
                    .count("total")
                    .stddev("version", "stddevVersion")
                    .var_pop("version", "varPopVersion")
                    .bit_or("version", "bitOrVersion")
                    .having(Expr::gt("total", 1_i64)),
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("name"),
            Some(&Value::Text(String::from("alpha")))
        );
        assert_eq!(rows[0].get("total"), Some(&Value::U64(2)));
        assert_eq!(
            rows[0].get("stddevVersion").map(Value::to_json_value),
            Some(serde_json::Value::String(
                "1.4142135623730951454746218583".to_owned()
            ))
        );
        assert_eq!(
            rows[0].get("varPopVersion"),
            Some(&Value::Decimal(Decimal::ONE))
        );
        assert_eq!(rows[0].get("bitOrVersion"), Some(&Value::I64(3)));
    }

    #[tokio::test]
    async fn memory_data_service_runs_sound_like_filter() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata).with_rows(
            "Order",
            vec![
                Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("Robert"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("Rupert"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(3)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("Ashcraft"))),
                ]),
            ],
        );

        let rows = data_service
            .fetch_all(
                &teaql_core::SelectQuery::new("Order")
                    .filter(Expr::sound_like("name", "Robert"))
                    .order_asc("id"),
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name"), Some(&Value::Text("Robert".to_owned())));
        assert_eq!(rows[1].get("name"), Some(&Value::Text("Rupert".to_owned())));
    }

    #[tokio::test]
    async fn memory_data_service_runs_java_style_string_match_filters() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata).with_rows(
            "Order",
            vec![
                Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("tea-order"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(1)),
                    (
                        String::from("name"),
                        Value::Text(String::from("coffee-order")),
                    ),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(3)),
                    (String::from("version"), Value::I64(1)),
                    (
                        String::from("name"),
                        Value::Text(String::from("tea-archived")),
                    ),
                ]),
            ],
        );

        let rows = data_service
            .fetch_all(
                &teaql_core::SelectQuery::new("Order")
                    .filter(
                        Expr::contain("name", "tea")
                            .and_expr(Expr::begin_with("name", "tea"))
                            .and_expr(Expr::end_with("name", "order"))
                            .and_expr(Expr::not_contain("name", "coffee"))
                            .and_expr(Expr::not_begin_with("name", "archived"))
                            .and_expr(Expr::not_end_with("name", "draft")),
                    )
                    .order_asc("id"),
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("name"),
            Some(&Value::Text("tea-order".to_owned()))
        );
    }

    #[tokio::test]
    async fn memory_data_service_runs_property_to_property_filters() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata).with_rows(
            "Order",
            vec![
                Record::from([
                    (String::from("id"), Value::U64(1)),
                    (String::from("version"), Value::I64(2)),
                    (String::from("name"), Value::Text(String::from("keep"))),
                ]),
                Record::from([
                    (String::from("id"), Value::U64(2)),
                    (String::from("version"), Value::I64(1)),
                    (String::from("name"), Value::Text(String::from("skip"))),
                ]),
            ],
        );

        let rows = data_service
            .fetch_all(
                &teaql_core::SelectQuery::new("Order")
                    .filter(Expr::compare_columns("version", BinaryOp::Gte, "id"))
                    .order_asc("id"),
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name"), Some(&Value::Text("keep".to_owned())));
    }

    #[tokio::test]
    async fn memory_data_service_supports_mutations_and_optimistic_locking() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = MemoryDataService::new(metadata);

        data_service
            .insert(
                &InsertCommand::new("Order")
                    .value("id", 10_u64)
                    .value("version", 1_i64)
                    .value("name", "draft"),
            )
            .unwrap();
        data_service
            .update(
                &UpdateCommand::new("Order", 10_u64)
                    .expected_version(1)
                    .value("name", "submitted"),
            )
            .unwrap();

        let row = data_service
            .fetch_all(&teaql_core::SelectQuery::new("Order").filter(Expr::eq("id", 10_u64)))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            row.get("name"),
            Some(&Value::Text(String::from("submitted")))
        );
        assert_eq!(row.get("version"), Some(&Value::I64(2)));

        let conflict = data_service
            .update(
                &UpdateCommand::new("Order", 10_u64)
                    .expected_version(1)
                    .value("name", "stale"),
            )
            .unwrap_err();
        assert!(matches!(
            conflict,
            DataServiceError::Runtime(RuntimeError::OptimisticLockConflict { .. })
        ));

        data_service
            .delete(&DeleteCommand::new("Order", 10_u64).expected_version(2))
            .unwrap();
        let row = data_service
            .fetch_all(&teaql_core::SelectQuery::new("Order").filter(Expr::eq("id", 10_u64)))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(row.get("version"), Some(&Value::I64(-3)));

        data_service
            .recover(&RecoverCommand::new("Order", 10_u64, -3))
            .unwrap();
        let row = data_service
            .fetch_all(&teaql_core::SelectQuery::new("Order").filter(Expr::eq("id", 10_u64)))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(row.get("version"), Some(&Value::I64(4)));
    }

    #[tokio::test]
    async fn user_context_reports_missing_schema_provider() {
        let err = UserContext::new().ensure_schema().await.unwrap_err();
        assert!(
            matches!(err, RuntimeError::Schema(message) if message == "missing schema provider")
        );
    }

    #[tokio::test]
    async fn user_context_stores_and_exposes_user_identifier() {
        let mut context = UserContext::new();
        let pid = std::process::id();
        let thread_id_str = format!("{:?}", std::thread::current().id());
        let numeric_thread_id = thread_id_str
            .strip_prefix("ThreadId(")
            .and_then(|s| s.strip_suffix(")"))
            .unwrap_or(&thread_id_str);
        let os_user = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "main".to_owned());
        let expected_default = format!("{os_user}@pid-{pid}.tid-{numeric_thread_id}");
        assert_eq!(context.user_identifier(), Some(expected_default.as_str()));

        context.set_user_identifier("user-123");
        assert_eq!(context.user_identifier(), Some("user-123"));

        let ctx2 = UserContext::new().with_user_identifier("user-456");
        assert_eq!(ctx2.user_identifier(), Some("user-456"));

        let mut ctx3 = UserContext::new();
        ctx3.set_user_identifier_option(Some("user-789".to_owned()));
        assert_eq!(ctx3.user_identifier(), Some("user-789"));
        ctx3.set_user_identifier_option(None);
        assert_eq!(ctx3.user_identifier(), None);

        let ctx4 = UserContext::new().with_user_identifier_option(Some("user-abc".to_owned()));
        assert_eq!(ctx4.user_identifier(), Some("user-abc"));
    }

    #[test]
    fn local_lock_enforces_ownership_timeout_and_lease_expiry() {
        let first = UserContext::new();
        let second = UserContext::new();
        let key = format!("local-lock-{:?}", std::time::SystemTime::now());

        assert!(first.try_local_lock(&key, 0, 50));
        assert!(!second.try_local_lock(&key, 0, 50));
        second.unlock_local(&key);
        assert!(!second.try_local_lock(&key, 0, 50));
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert!(second.try_local_lock(&key, 0, 50));
        second.unlock_local(&key);
        assert!(first.try_local_lock(&key, 0, 50));
        first.unlock_local(&key);
    }

    #[derive(Default)]
    struct TestRemoteLockProvider {
        owners: Mutex<std::collections::HashMap<String, String>>,
    }

    #[async_trait::async_trait]
    impl RemoteLockProvider for TestRemoteLockProvider {
        async fn try_remote_lock(
            &self,
            key: &str,
            owner_token: &str,
            _timeout_millis: u64,
            _expire_millis: u64,
        ) -> bool {
            let mut owners = self.owners.lock().expect("remote lock state");
            if owners.contains_key(key) {
                return false;
            }
            owners.insert(key.to_owned(), owner_token.to_owned());
            true
        }

        async fn unlock_remote(&self, key: &str, owner_token: &str) -> bool {
            let mut owners = self.owners.lock().expect("remote lock state");
            if owners.get(key).is_some_and(|owner| owner == owner_token) {
                owners.remove(key);
                return true;
            }
            false
        }
    }

    #[tokio::test]
    async fn remote_lock_delegates_and_preserves_context_ownership() {
        let provider: Arc<dyn RemoteLockProvider> = Arc::new(TestRemoteLockProvider::default());
        let mut first = UserContext::new();
        first.insert_resource(provider.clone());
        let mut second = UserContext::new();
        second.insert_resource(provider);
        let key = format!("remote-lock-{:?}", std::time::SystemTime::now());

        assert!(first.try_remote_lock(&key, 0, 1_000).await);
        assert!(!second.try_remote_lock(&key, 0, 1_000).await);
        assert!(!second.unlock_remote(&key).await);
        assert!(!second.try_remote_lock(&key, 0, 1_000).await);
        assert!(first.unlock_remote(&key).await);
        assert!(second.try_remote_lock(&key, 0, 1_000).await);
        assert!(second.unlock_remote(&key).await);

        assert!(UserContext::new().try_remote_lock("optional", 0, 0).await);
    }
}

pub use checker::{
    CHECK_OBJECT_STATUS_FIELD, CheckObjectStatus, CheckResult, CheckResults, CheckRule, Checker,
    CheckerRegistry, InMemoryCheckerRegistry, LocationSegment, ObjectLocation, TypedChecker,
    TypedEntityChecker, clear_entity_status, mark_entity_status,
};
