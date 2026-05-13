mod checker;
mod context;
mod entity_runtime;
mod error;
mod event;
mod graph;
mod id;
mod language;
mod memory;
mod registry;
mod repository;

pub use context::{SchemaProvider, SqlLogEntry, SqlLogOperation, SqlLogOptions, UserContext};
pub use entity_runtime::{ChangeSetStack, EntityChangeSet, EntityKey, EntityRoot, RootContext};
pub use error::{ContextError, RepositoryError, RuntimeError};
pub use event::{
    EntityEvent, EntityEventKind, EntityEventSink, EntityPropertyChange, InMemoryEntityEventSink,
};
pub use graph::{
    GraphMutationBatch, GraphMutationKind, GraphMutationPlan, GraphMutationPlanItem, GraphNode,
    GraphOperation, sorted_update_fields,
};
pub(crate) use id::local_id_generator;
pub use id::{InternalIdGenerator, SnowflakeIdGenerator};
pub use language::{
    BuiltinTranslator, Language, MessageTranslator, translate_check_result, translate_location,
};
pub use memory::{MemoryRepository, MemoryRepositoryError};
pub use registry::{
    InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry,
    MetadataStore, RepositoryBehavior, RepositoryBehaviorRegistry, RepositoryRegistry,
    RuntimeModule,
};
pub use repository::{
    AggregationCacheBackend, ContextRepository, GraphTransactionBoundary, InMemoryAggregationCache,
    QueryExecutor, RelationLoadPlan, Repository, ResolvedRepository,
};

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use super::{
        AggregationCacheBackend, CHECK_OBJECT_STATUS_FIELD, CheckObjectStatus, CheckResults,
        CheckRule, Checker, EntityEvent, EntityEventKind, EntityEventSink, GraphMutationKind,
        GraphNode, GraphTransactionBoundary, InMemoryAggregationCache, InMemoryCheckerRegistry,
        InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry,
        InternalIdGenerator, Language, MemoryRepository, MetadataStore, ObjectLocation,
        QueryExecutor, Repository, RepositoryBehavior, RepositoryError, RuntimeError,
        RuntimeModule, SqlLogOperation, SqlLogOptions, TypedChecker, TypedEntityChecker,
        UserContext, translate_check_result,
    };
    use teaql_core::{
        Aggregate, AggregateFunction, BinaryOp, DataType, Decimal, DeleteCommand, Entity,
        EntityDescriptor, EntityError, Expr, InsertCommand, OrderBy, PropertyDescriptor, Record,
        RecoverCommand, RelationAggregate, SelectQuery, TeaqlEntity, UpdateCommand, Value,
    };
    use teaql_macros::TeaqlEntity as DeriveTeaqlEntity;
    use teaql_sql::{CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect};

    const ORDER_DEFAULT_PROJECTION: &str = "\"id\", \"version\", \"name\"";

    #[derive(Debug, Default, Clone, Copy)]
    struct PostgresDialect;

    impl SqlDialect for PostgresDialect {
        fn kind(&self) -> DatabaseKind {
            DatabaseKind::PostgreSql
        }

        fn quote_ident(&self, ident: &str) -> String {
            format!("\"{}\"", ident.replace('"', "\"\""))
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
                DataType::Text => Ok("TEXT"),
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

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Order", table = "orders")]
    struct Order {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        name: String,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "Product", table = "product")]
    struct TypedGraphProduct {
        #[teaql(id)]
        id: Option<u64>,
        name: String,
    }

    #[derive(Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "OrderLine", table = "orderline")]
    struct TypedGraphLine {
        #[teaql(id)]
        id: Option<u64>,
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
        id: Option<u64>,
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
        fn entity_descriptor() -> EntityDescriptor {
            entity()
        }
    }

    impl Entity for OrderEntity {
        fn from_record(record: Record) -> Result<Self, EntityError> {
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

        fn into_record(self) -> Record {
            Record::from([
                (String::from("id"), Value::U64(self.id)),
                (String::from("version"), Value::I64(self.version)),
                (String::from("name"), Value::Text(self.name)),
            ])
        }
    }

    #[derive(Debug)]
    struct StubError;

    impl std::fmt::Display for StubError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "stub error")
        }
    }

    impl std::error::Error for StubError {}

    impl QueryExecutor for StubExecutor {
        type Error = StubError;

        fn fetch_all(&self, _query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
            Ok(self.rows.clone())
        }

        fn execute(&self, _query: &CompiledQuery) -> Result<u64, Self::Error> {
            Ok(self.affected)
        }

        fn begin_transaction(&self) -> Result<GraphTransactionBoundary, Self::Error> {
            Ok(GraphTransactionBoundary::Started)
        }
    }

    impl QueryExecutor for QueueExecutor {
        type Error = StubError;

        fn fetch_all(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
            self.queries.lock().unwrap().push(query.sql.clone());
            Ok(self.rows.lock().unwrap().pop_front().unwrap_or_default())
        }

        fn execute(&self, _query: &CompiledQuery) -> Result<u64, Self::Error> {
            Ok(self.affected)
        }
    }

    #[test]
    fn user_context_records_configured_sql_logs() {
        let mut ctx = UserContext::new()
            .with_module(crate::module!(Order))
            .with_sql_log_options(SqlLogOptions::select_only());
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        {
            let repo = ctx
                .resolve_repository::<PostgresDialect, StubExecutor>("Order")
                .unwrap();
            repo.fetch_all(&SelectQuery::new("Order").filter(Expr::eq("name", "Bob's Shop")))
                .unwrap();
            repo.insert(&InsertCommand::new("Order").value("name", "created"))
                .unwrap();
        }

        let logs = ctx.sql_logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].operation, SqlLogOperation::Select);
        assert_eq!(
            logs[0].debug_sql,
            format!(
                "SELECT {ORDER_DEFAULT_PROJECTION} FROM \"orders\" WHERE (\"name\" = 'Bob''s Shop')"
            )
        );

        ctx.set_sql_log_options(SqlLogOptions::mutation_only());
        ctx.clear_sql_logs();
        ctx.resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap()
            .update(
                &UpdateCommand::new("Order", 1_u64)
                    .value("name", "updated")
                    .expected_version(1),
            )
            .unwrap();

        let logs = ctx.sql_logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].operation, SqlLogOperation::Update);
        assert!(logs[0].debug_sql.contains("UPDATE \"orders\" SET"));
        assert!(logs[0].debug_sql.contains("'updated'"));
    }

    impl RepositoryBehavior for OrderBehavior {
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
    struct OrderChecker;
    struct TypedOrderChecker;
    #[derive(Clone)]
    struct RecordingEventSink {
        events: Arc<Mutex<Vec<EntityEvent>>>,
    }

    impl RepositoryBehavior for ContextAwareOrderBehavior {
        fn before_insert(
            &self,
            ctx: &UserContext,
            command: &mut InsertCommand,
        ) -> Result<(), RuntimeError> {
            let tenant = ctx
                .get_named_resource::<String>("tenant")
                .cloned()
                .ok_or_else(|| RuntimeError::Behavior("missing tenant resource".to_owned()))?;
            let version = *ctx
                .get_named_resource::<i64>("initial_version")
                .ok_or_else(|| {
                    RuntimeError::Behavior("missing initial_version resource".to_owned())
                })?;
            let trace_id = match ctx.local("trace_id") {
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

    impl Checker for OrderChecker {
        fn entity(&self) -> &str {
            "Order"
        }

        fn check_and_fix(
            &self,
            _ctx: &UserContext,
            record: &mut Record,
            location: &ObjectLocation,
            results: &mut CheckResults,
        ) {
            let status = CheckObjectStatus::from_record(record);
            if status.is_create() {
                self.required(record, "name", location, results);
                record.entry("version".to_owned()).or_insert(Value::I64(1));
            }
            if status.is_update()
                && record.get("name") == Some(&Value::Text("graph-update".to_owned()))
            {
                record.insert(
                    "name".to_owned(),
                    Value::Text("graph-update-checked".to_owned()),
                );
            }
            self.min_string_length(record, "name", 3, location, results);
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
                self.required_text(&entity.name, "name", location, results);
            }
            self.min_string_length(&entity.name, "name", 3, location, results);
            if entity.name == "fix" {
                entity.name = "fixed".to_owned();
            }
        }
    }

    impl EntityEventSink for RecordingEventSink {
        fn on_event(&self, _ctx: &UserContext, event: &EntityEvent) -> Result<(), RuntimeError> {
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
    fn metadata_store_registers_entities() {
        let store = InMemoryMetadataStore::new().with_entity(entity());
        assert!(store.entity("Order").is_some());
    }

    #[test]
    fn runtime_module_registers_descriptor_into_context() {
        let ctx = UserContext::new().with_module(RuntimeModule::new().descriptor(entity()));
        assert!(ctx.entity("Order").is_some());
        assert!(ctx.has_repository("Order"));
    }

    #[test]
    fn runtime_module_registers_derived_entity_and_behavior() {
        let ctx = UserContext::new().with_module(
            RuntimeModule::new().entity_with_behavior::<CatalogProductRow, _>(OrderBehavior),
        );
        assert!(ctx.entity("CatalogProduct").is_some());
        assert!(ctx.has_repository("CatalogProduct"));
        assert!(ctx.repository_behavior("CatalogProduct").is_some());
    }

    #[test]
    fn module_macro_registers_multiple_entities() {
        let ctx = UserContext::new().with_module(crate::module!(CatalogProductRow));
        assert!(ctx.entity("CatalogProduct").is_some());
        assert!(ctx.has_repository("CatalogProduct"));
    }

    #[test]
    fn module_macro_registers_entity_behavior_pairs() {
        let ctx =
            UserContext::new().with_module(crate::module!(CatalogProductRow => OrderBehavior));
        assert!(ctx.entity("CatalogProduct").is_some());
        assert!(ctx.repository_behavior("CatalogProduct").is_some());
    }

    #[test]
    fn repository_returns_optimistic_lock_conflict() {
        let store = InMemoryMetadataStore::new().with_entity(entity());
        let executor = StubExecutor {
            affected: 0,
            rows: Vec::new(),
        };
        let repo = Repository::new(&PostgresDialect, &store, &executor);

        let err = repo
            .update(
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "next"),
            )
            .unwrap_err();

        match err {
            RepositoryError::Runtime(RuntimeError::OptimisticLockConflict { .. }) => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn user_context_indexes_resources_and_locals() {
        let mut ctx =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(entity()));
        ctx.insert_resource::<u64>(42);
        ctx.insert_named_resource("tenant", String::from("acme"));
        ctx.put_local("trace_id", "req-1");

        assert!(ctx.entity("Order").is_some());
        assert_eq!(ctx.get_resource::<u64>(), Some(&42));
        assert_eq!(
            ctx.get_named_resource::<String>("tenant"),
            Some(&String::from("acme"))
        );
        assert_eq!(
            ctx.local("trace_id"),
            Some(&Value::Text("req-1".to_owned()))
        );
    }

    #[test]
    fn user_context_builds_context_repository() {
        let mut ctx =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(entity()));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx.repository::<PostgresDialect, StubExecutor>().unwrap();
        let affected = repo
            .update(
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "next"),
            )
            .unwrap();

        assert_eq!(affected, 1);
    }

    #[test]
    fn user_context_resolves_repository_by_entity_type() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        assert_eq!(repo.entity(), "Order");
        assert_eq!(repo.select().entity, "Order");

        let affected = repo
            .insert(
                &repo
                    .insert_command()
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "n"),
            )
            .unwrap();
        assert_eq!(affected, 1);
    }

    #[test]
    fn resolved_repository_applies_behavior_hooks() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();

        let compiled = repo.compile(&repo.select()).unwrap();
        assert!(compiled.sql.contains("WHERE (\"version\" = $1)"));

        let insert = repo.insert_command().value("id", 1_u64).value("name", "n");
        let affected = repo.insert(&insert).unwrap();
        assert_eq!(affected, 1);
        assert_eq!(repo.relation_loads(), vec!["lines".to_owned()]);
    }

    #[test]
    fn resolved_repository_prepares_insert_command_with_generated_id() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            )
            .with_internal_id_generator(FixedIdGenerator(42));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();

        let prepared = repo
            .prepare_insert_command(&repo.insert_command().value("name", "n"))
            .unwrap();

        assert_eq!(prepared.values.get("id"), Some(&Value::U64(42)));
        assert_eq!(prepared.values.get("version"), Some(&Value::I64(1)));
        assert_eq!(
            prepared.values.get("name"),
            Some(&Value::Text("n".to_owned()))
        );
    }

    #[test]
    fn resolved_repository_saves_create_graph_and_maintains_relation_keys() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            )
            .with_internal_id_generator(SequentialIdGenerator::new(500));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let graph = GraphNode::new("Order").value("name", "root").relation(
            "lines",
            GraphNode::new("OrderLine")
                .value("name", "line-1")
                .relation("product", GraphNode::new("Product").value("name", "sku-1")),
        );

        let saved = repo.save_graph(graph).unwrap();

        assert_eq!(saved.values.get("id"), Some(&Value::U64(500)));
        assert_eq!(saved.values.get("version"), Some(&Value::I64(1)));
        let lines = saved.relations.get("lines").unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].values.get("id"), Some(&Value::U64(501)));
        assert_eq!(lines[0].values.get("order_id"), Some(&Value::U64(500)));
        assert_eq!(lines[0].values.get("product_id"), Some(&Value::U64(502)));
        let product = lines[0].relations.get("product").unwrap();
        assert_eq!(product[0].values.get("id"), Some(&Value::U64(502)));
    }

    #[test]
    fn resolved_repository_extracts_and_saves_typed_entity_graph() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_internal_id_generator(SequentialIdGenerator::new(700));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let order = TypedGraphOrder {
            id: None,
            version: 1,
            name: "typed-root".to_owned(),
            lines: teaql_core::SmartList::from(vec![TypedGraphLine {
                id: None,
                order_id: None,
                name: "typed-line".to_owned(),
                product_id: None,
                product: Some(TypedGraphProduct {
                    id: None,
                    name: "typed-product".to_owned(),
                }),
            }]),
        };

        let extracted = repo.graph_node_from_entity(order).unwrap();
        assert_eq!(extracted.entity, "Order");
        assert_eq!(
            extracted.values.get("name"),
            Some(&Value::Text("typed-root".to_owned()))
        );
        assert_eq!(extracted.values.get("id"), Some(&Value::Null));
        assert_eq!(extracted.relations["lines"].len(), 1);
        assert_eq!(
            extracted.relations["lines"][0].values.get("name"),
            Some(&Value::Text("typed-line".to_owned()))
        );
        assert_eq!(
            extracted.relations["lines"][0].relations["product"].len(),
            1
        );

        let saved = repo.save_graph(extracted).unwrap();
        assert_eq!(saved.values.get("id"), Some(&Value::U64(700)));
        let lines = saved.relations.get("lines").unwrap();
        assert_eq!(lines[0].values.get("id"), Some(&Value::U64(701)));
        assert_eq!(lines[0].values.get("order_id"), Some(&Value::U64(700)));
        assert_eq!(lines[0].values.get("product_id"), Some(&Value::U64(702)));
        assert_eq!(
            lines[0].relations["product"][0].values.get("id"),
            Some(&Value::U64(702))
        );
    }

    #[test]
    fn resolved_repository_saves_typed_entity_graph_directly() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_internal_id_generator(SequentialIdGenerator::new(800));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let saved = repo
            .save_entity_graph(TypedGraphOrder {
                id: None,
                version: 1,
                name: "typed-direct".to_owned(),
                lines: teaql_core::SmartList::from(vec![TypedGraphLine {
                    id: None,
                    order_id: None,
                    name: "typed-line".to_owned(),
                    product_id: None,
                    product: Some(TypedGraphProduct {
                        id: None,
                        name: "typed-product".to_owned(),
                    }),
                }]),
            })
            .unwrap();

        assert_eq!(saved.values.get("id"), Some(&Value::U64(800)));
        assert_eq!(
            saved.relations["lines"][0].values.get("order_id"),
            Some(&Value::U64(800))
        );
        assert_eq!(
            saved.relations["lines"][0].values.get("product_id"),
            Some(&Value::U64(802))
        );
    }

    #[test]
    fn custom_user_context_can_drive_insert_preparation() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new()
                    .with_behavior("Order", ContextAwareOrderBehavior),
            )
            .with_internal_id_generator(FixedIdGenerator(99));
        ctx.insert_named_resource("tenant", String::from("acme"));
        ctx.insert_named_resource("initial_version", 7_i64);
        ctx.put_local("trace_id", "req-9");
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let prepared = repo.prepare_insert_command(&repo.insert_command()).unwrap();

        assert_eq!(prepared.values.get("id"), Some(&Value::U64(99)));
        assert_eq!(prepared.values.get("version"), Some(&Value::I64(7)));
        assert_eq!(
            prepared.values.get("name"),
            Some(&Value::Text("acme:req-9".to_owned()))
        );
    }

    #[test]
    fn checker_registry_validates_and_fixes_insert_commands() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker))
            .with_internal_id_generator(FixedIdGenerator(77));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
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
    fn typed_checker_validates_and_fixes_derived_entities_without_record_access() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(Order::entity_descriptor()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_checker_registry(
                InMemoryCheckerRegistry::new()
                    .with_checker(TypedEntityChecker::<Order, _>::new(TypedOrderChecker)),
            )
            .with_internal_id_generator(FixedIdGenerator(79));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let prepared = repo
            .prepare_insert_command(
                &repo
                    .insert_command()
                    .value("name", "fix")
                    .value("version", 1_i64),
            )
            .unwrap();
        assert_eq!(
            prepared.values.get("name"),
            Some(&Value::Text("fixed".to_owned()))
        );
        assert_eq!(prepared.values.get("id"), Some(&Value::U64(79)));
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
    fn checker_registry_validates_update_commands_without_required_insert_checks() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        repo.update(&repo.update_command(1_u64).value("version", 1_i64))
            .unwrap();

        let error = repo
            .update(&repo.update_command(1_u64).value("name", "no"))
            .unwrap_err();
        match error {
            RepositoryError::Runtime(RuntimeError::Check(results)) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].location.to_string(), "name");
            }
            other => panic!("unexpected checker error: {other:?}"),
        }
    }

    #[test]
    fn checker_registry_reports_nested_create_locations_and_fixes_records() {
        let ctx = UserContext::new()
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker));

        let mut child = Record::from([
            (String::from("id"), Value::U64(10)),
            (
                String::from(CHECK_OBJECT_STATUS_FIELD),
                Value::from(CheckObjectStatus::Create),
            ),
        ]);
        let error = ctx
            .check_and_fix_record_at(
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
        ctx.check_and_fix_record_at(
            "Order",
            &mut child,
            &ObjectLocation::hash_root("lines").element(0),
        )
        .unwrap();
    }

    #[test]
    fn built_in_language_translators_cover_fifteen_languages() {
        assert_eq!(Language::ALL.len(), 15);
        let result = super::CheckResult::required(ObjectLocation::hash_root("name"));
        let messages = Language::ALL
            .iter()
            .map(|language| translate_check_result(*language, &result))
            .collect::<Vec<_>>();

        assert!(messages.iter().all(|message| !message.is_empty()));
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

    #[test]
    fn user_context_language_switch_translates_checker_errors() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker))
            .with_internal_id_generator(FixedIdGenerator(77))
            .with_language(Language::Chinese);
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
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

        let mut ctx = UserContext::new().with_language(Language::English);
        ctx.set_language_code("es").unwrap();
        assert_eq!(ctx.language(), Language::Spanish);
    }

    #[test]
    fn checker_registry_merges_graph_update_fixes_by_object_status() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(OrderChecker));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                ("id".to_owned(), Value::U64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("old".to_owned())),
            ])],
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let saved = repo
            .save_graph(
                GraphNode::new("Order")
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "graph-update"),
            )
            .unwrap();

        assert_eq!(
            saved.values.get("name"),
            Some(&Value::Text("graph-update-checked".to_owned()))
        );
        assert_eq!(saved.values.get("version"), Some(&Value::I64(2)));
        assert!(!saved.values.contains_key(CHECK_OBJECT_STATUS_FIELD));
    }

    #[test]
    fn user_context_event_sink_receives_repository_mutation_events() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_internal_id_generator(FixedIdGenerator(88))
            .with_event_sink(RecordingEventSink {
                events: events.clone(),
            });
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                ("id".to_owned(), Value::U64(88)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("old".to_owned())),
            ])],
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        repo.insert(&repo.insert_command().value("name", "created"))
            .unwrap();
        repo.update(
            &repo
                .update_command(88_u64)
                .expected_version(1)
                .value("name", "updated"),
        )
        .unwrap();
        repo.delete(&repo.delete_command(88_u64).expected_version(2))
            .unwrap();
        repo.recover(&repo.recover_command(88_u64, -3)).unwrap();

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].kind, EntityEventKind::Created);
        assert_eq!(events[0].entity, "Order");
        assert_eq!(events[0].values.get("id"), Some(&Value::U64(88)));
        assert_eq!(events[1].kind, EntityEventKind::Updated);
        assert_eq!(events[1].values.get("id"), Some(&Value::U64(88)));
        assert_eq!(events[1].values.get("version"), Some(&Value::I64(2)));
        assert_eq!(events[1].updated_fields, vec!["name".to_owned()]);
        assert_eq!(
            events[1]
                .old_values
                .as_ref()
                .and_then(|values| values.get("name")),
            Some(&Value::Text("old".to_owned()))
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
            Some(Value::Text("old".to_owned()))
        );
        assert_eq!(
            events[1].changes[0].new_value,
            Some(Value::Text("updated".to_owned()))
        );
        assert_eq!(events[2].kind, EntityEventKind::Deleted);
        assert!(events[2].old_values.is_some());
        assert!(events[2].new_values.is_none());
        assert_eq!(events[3].kind, EntityEventKind::Recovered);
        assert_eq!(
            events[3]
                .old_values
                .as_ref()
                .and_then(|values| values.get("version")),
            Some(&Value::I64(1))
        );
        assert_eq!(
            events[3]
                .new_values
                .as_ref()
                .and_then(|values| values.get("version")),
            Some(&Value::I64(4))
        );
        assert_eq!(events[3].changes[0].field, "version");
    }

    #[test]
    fn user_context_event_sink_receives_mixed_graph_mutation_events() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_event_sink(RecordingEventSink {
                events: events.clone(),
            });
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                ("id".to_owned(), Value::U64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("old".to_owned())),
            ])],
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        repo.save_graph(
            GraphNode::new("Order")
                .value("id", 1_u64)
                .value("version", 1_i64)
                .value("name", "updated")
                .relation(
                    "lines",
                    GraphNode::new("OrderLine")
                        .value("name", "line")
                        .value("product_id", 3_u64),
                ),
        )
        .unwrap();

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind, EntityEventKind::Updated);
        assert_eq!(events[0].entity, "Order");
        assert_eq!(events[1].kind, EntityEventKind::Updated);
        assert_eq!(events[1].entity, "OrderLine");
        assert_eq!(events[1].values.get("order_id"), Some(&Value::U64(1)));
        assert_eq!(events[2].kind, EntityEventKind::Deleted);
        assert_eq!(events[2].entity, "OrderLine");
    }

    #[test]
    fn save_graph_builds_plan_grouped_by_entity_and_operation() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_internal_id_generator(SequentialIdGenerator::new(500));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                ("id".to_owned(), Value::U64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("old".to_owned())),
            ])],
        });

        let plan = ctx
            .plan_for_save_graph::<PostgresDialect, StubExecutor>(
                GraphNode::new("Order")
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "updated")
                    .relation(
                        "lines",
                        GraphNode::new("OrderLine")
                            .value("name", "new-line-a")
                            .value("product_id", 2_u64),
                    )
                    .relation(
                        "lines",
                        GraphNode::new("OrderLine")
                            .value("name", "new-line-b")
                            .value("product_id", 2_u64),
                    )
                    .relation(
                        "lines",
                        GraphNode::new("OrderLine")
                            .value("id", 5_u64)
                            .value("version", 1_i64)
                            .value("name", "same-update-a"),
                    )
                    .relation(
                        "lines",
                        GraphNode::new("OrderLine")
                            .value("id", 6_u64)
                            .value("version", 1_i64)
                            .value("name", "same-update-b"),
                    )
                    .relation(
                        "lines",
                        GraphNode::new("OrderLine").value("id", 3_u64).remove(),
                    )
                    .relation(
                        "lines",
                        GraphNode::new("OrderLine").value("id", 4_u64).reference(),
                    ),
            )
            .unwrap();
        let counts = plan.grouped_counts();

        assert_eq!(
            counts.get(&("Order".to_owned(), GraphMutationKind::Update)),
            Some(&1)
        );
        assert_eq!(
            counts.get(&("OrderLine".to_owned(), GraphMutationKind::Create)),
            Some(&2)
        );
        assert_eq!(
            counts.get(&("OrderLine".to_owned(), GraphMutationKind::Update)),
            Some(&2)
        );
        assert_eq!(
            counts.get(&("OrderLine".to_owned(), GraphMutationKind::Delete)),
            Some(&1)
        );
        assert_eq!(
            counts.get(&("OrderLine".to_owned(), GraphMutationKind::Reference)),
            Some(&1)
        );
        let create_batch = plan
            .batches
            .iter()
            .find(|batch| batch.entity == "OrderLine" && batch.kind == GraphMutationKind::Create)
            .unwrap();
        assert_eq!(create_batch.items.len(), 2);
        assert_eq!(
            create_batch.items[0].values.get("id"),
            Some(&Value::U64(500))
        );
        assert_eq!(
            create_batch.items[1].values.get("id"),
            Some(&Value::U64(501))
        );
        let update_batch = plan
            .batches
            .iter()
            .find(|batch| {
                batch.entity == "OrderLine"
                    && batch.kind == GraphMutationKind::Update
                    && batch.update_fields == vec!["name".to_owned()]
            })
            .unwrap();
        assert_eq!(update_batch.items.len(), 2);
    }

    #[test]
    fn resolved_repository_builds_relation_plans() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let plans = repo.relation_plans().unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].relation_name, "lines");
        assert_eq!(plans[0].target_entity, "OrderLine");
        assert_eq!(plans[0].local_key, "id");
        assert_eq!(plans[0].foreign_key, "order_id");
        assert!(plans[0].many);
    }

    #[test]
    fn resolved_repository_builds_relation_query_from_parent_rows() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: Vec::new(),
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let parent_rows = vec![
            Record::from([(String::from("id"), Value::U64(11))]),
            Record::from([(String::from("id"), Value::U64(12))]),
        ];

        let query = repo.relation_query("lines", &parent_rows).unwrap();
        let compiled = repo.compile(&query).unwrap();
        assert!(compiled.sql.contains("FROM \"orderline\""));
        assert!(compiled.sql.contains("\"order_id\" IN ($1, $2)"));
        assert_eq!(compiled.params, vec![Value::U64(11), Value::U64(12)]);
    }

    #[test]
    fn resolved_repository_enhances_parent_rows_with_relations() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity())
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
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

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let mut parents = vec![
            Record::from([(String::from("id"), Value::U64(11))]),
            Record::from([(String::from("id"), Value::U64(12))]),
        ];

        repo.enhance_relations(&mut parents).unwrap();

        match parents[0].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 2),
            other => panic!("unexpected lines payload: {other:?}"),
        }
        match parents[1].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 1),
            other => panic!("unexpected lines payload: {other:?}"),
        }
    }

    #[test]
    fn resolved_repository_fetches_smart_list_of_entities() {
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                (String::from("id"), Value::U64(7)),
                (String::from("version"), Value::I64(2)),
                (String::from("name"), Value::Text(String::from("typed"))),
            ])],
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("Order")
            .unwrap();
        let rows = repo.fetch_entities::<OrderEntity>(&repo.select()).unwrap();

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

    #[test]
    fn resolved_repository_fetches_smart_list_of_derived_entities() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new().with_entity(CatalogProductRow::entity_descriptor()),
            )
            .with_repository_registry(
                InMemoryRepositoryRegistry::new().with_entity("CatalogProduct"),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                (String::from("id"), Value::U64(9)),
                (String::from("name"), Value::Text(String::from("derived"))),
            ])],
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("CatalogProduct")
            .unwrap();
        let rows = repo
            .fetch_entities::<CatalogProductRow>(&repo.select())
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

    #[test]
    fn resolved_repository_collects_dynamic_properties_for_aggregate_output() {
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(OrderAggregateDynamic::entity_descriptor()),
            )
            .with_repository_registry(
                InMemoryRepositoryRegistry::new().with_entity("OrderAggregate"),
            );
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(StubExecutor {
            affected: 1,
            rows: vec![Record::from([
                (String::from("id"), Value::U64(1)),
                (String::from("lineCount"), Value::I64(3)),
                (String::from("amount"), Value::F64(18.5)),
            ])],
        });

        let repo = ctx
            .resolve_repository::<PostgresDialect, StubExecutor>("OrderAggregate")
            .unwrap();
        let rows = repo
            .fetch_entities::<OrderAggregateDynamic>(&repo.select())
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

    #[test]
    fn resolved_repository_executes_relation_aggregates_into_dynamic_properties() {
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
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<PostgresDialect, QueueExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_all_with_relation_aggregates(
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
            .unwrap();

        assert_eq!(rows[0].get("lineCount"), Some(&Value::I64(3)));
        assert_eq!(rows[1].get("lineCount"), Some(&Value::U64(0)));

        let executor = ctx.get_resource::<QueueExecutor>().unwrap();
        let queries = executor.queries.lock().unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(
            queries[1],
            "SELECT \"order_id\", COUNT(*) AS \"lineCount\" FROM \"orderline\" WHERE (\"order_id\" IN ($1, $2)) GROUP BY \"order_id\""
        );
    }

    #[test]
    fn resolved_repository_maps_relation_aggregate_storage_key_to_property_key() {
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
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<PostgresDialect, QueueExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_all_with_relation_aggregates(
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
            .unwrap();

        assert_eq!(rows[0].get("lineCount"), Some(&Value::I64(3)));
        let executor = ctx.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(
            executor.queries.lock().unwrap()[1],
            "SELECT \"order_ref\", COUNT(*) AS \"lineCount\" FROM \"orderline\" WHERE (\"order_ref\" IN ($1)) GROUP BY \"order_ref\""
        );
    }

    #[test]
    fn resolved_repository_uses_aggregation_cache_when_resource_is_registered() {
        let executor = QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([vec![Record::from([(
                String::from("count"),
                Value::I64(2),
            )])]])),
            queries: Mutex::new(Vec::new()),
        };
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(executor);
        ctx.insert_resource(InMemoryAggregationCache::default());

        let repo = ctx
            .resolve_repository::<PostgresDialect, QueueExecutor>("Order")
            .unwrap();
        let query = repo
            .select()
            .count("count")
            .enable_aggregation_cache_for(60_000);

        let first = repo.fetch_all(&query).unwrap();
        let second = repo.fetch_all(&query).unwrap();

        assert_eq!(first, second);
        let executor = ctx.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(executor.queries.lock().unwrap().len(), 1);
    }

    #[test]
    fn aggregation_cache_is_namespaced_and_invalidated_after_write() {
        let executor = QueueExecutor {
            affected: 1,
            rows: Mutex::new(VecDeque::from([
                vec![Record::from([(String::from("count"), Value::I64(2))])],
                vec![Record::from([(String::from("count"), Value::I64(3))])],
            ])),
            queries: Mutex::new(Vec::new()),
        };
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity()))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(executor);
        ctx.insert_resource(
            Arc::new(InMemoryAggregationCache::with_namespace("tenant-a"))
                as Arc<dyn AggregationCacheBackend>,
        );

        let repo = ctx
            .resolve_repository::<PostgresDialect, QueueExecutor>("Order")
            .unwrap();
        let query = repo
            .select()
            .count("count")
            .enable_aggregation_cache_for(60_000);

        let first = repo.fetch_all(&query).unwrap();
        let cached = repo.fetch_all(&query).unwrap();
        repo.insert(
            &InsertCommand::new("Order")
                .value("id", 9_u64)
                .value("version", 1_i64)
                .value("name", "new"),
        )
        .unwrap();
        let refreshed = repo.fetch_all(&query).unwrap();

        assert_eq!(first, cached);
        assert_ne!(cached, refreshed);
        let executor = ctx.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(executor.queries.lock().unwrap().len(), 2);
    }

    #[test]
    fn aggregation_cache_propagates_to_relation_aggregates() {
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
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(entity())
                    .with_entity(line_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(executor);
        ctx.insert_resource(InMemoryAggregationCache::default());

        let repo = ctx
            .resolve_repository::<PostgresDialect, QueueExecutor>("Order")
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
            .fetch_all_with_relation_aggregates(&query, &[aggregate.clone()])
            .unwrap();
        let second = repo
            .fetch_all_with_relation_aggregates(&query, &[aggregate])
            .unwrap();

        assert_eq!(first, second);
        let executor = ctx.get_resource::<QueueExecutor>().unwrap();
        assert_eq!(executor.queries.lock().unwrap().len(), 2);
    }

    #[test]
    fn memory_repository_fetches_smart_list_entities_with_query_features() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata).with_rows(
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

        let orders = repository.fetch_entities::<Order>(&query).unwrap();

        assert_eq!(orders.ids(), vec![Value::U64(3)]);
        assert_eq!(orders.versions(), vec![1]);
        assert_eq!(orders.first().unwrap().name, "gamma");
    }

    #[test]
    fn memory_repository_runs_aggregates() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata).with_rows(
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
            entity: String::from("Order"),
            projection: Vec::new(),
            expr_projection: Vec::new(),
            filter: None,
            having: None,
            order_by: Vec::new(),
            slice: None,
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
            json_expr: None,
            dynamic_properties: Vec::new(),
            raw_projections: Vec::new(),
            object_group_bys: Vec::new(),
            child_enhancements: Vec::new(),
        };

        let rows = repository.fetch_all(&query).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("count"), Some(&Value::U64(2)));
        assert_eq!(rows[0].get("versionSum"), Some(&Value::U64(3)));
    }

    #[test]
    fn memory_repository_runs_grouped_aggregates_and_extended_filters() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata).with_rows(
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

        let rows = repository
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

    #[test]
    fn memory_repository_runs_extended_aggregates_and_having() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata).with_rows(
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

        let rows = repository
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

    #[test]
    fn memory_repository_runs_sound_like_filter() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata).with_rows(
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

        let rows = repository
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

    #[test]
    fn memory_repository_runs_java_style_string_match_filters() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata).with_rows(
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

        let rows = repository
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

    #[test]
    fn memory_repository_runs_property_to_property_filters() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata).with_rows(
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

        let rows = repository
            .fetch_all(
                &teaql_core::SelectQuery::new("Order")
                    .filter(Expr::compare_columns("version", BinaryOp::Gte, "id"))
                    .order_asc("id"),
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name"), Some(&Value::Text("keep".to_owned())));
    }

    #[test]
    fn memory_repository_supports_mutations_and_optimistic_locking() {
        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let repository = MemoryRepository::new(metadata);

        repository
            .insert(
                &InsertCommand::new("Order")
                    .value("id", 10_u64)
                    .value("version", 1_i64)
                    .value("name", "draft"),
            )
            .unwrap();
        repository
            .update(
                &UpdateCommand::new("Order", 10_u64)
                    .expected_version(1)
                    .value("name", "submitted"),
            )
            .unwrap();

        let row = repository
            .fetch_all(&teaql_core::SelectQuery::new("Order").filter(Expr::eq("id", 10_u64)))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            row.get("name"),
            Some(&Value::Text(String::from("submitted")))
        );
        assert_eq!(row.get("version"), Some(&Value::I64(2)));

        let conflict = repository
            .update(
                &UpdateCommand::new("Order", 10_u64)
                    .expected_version(1)
                    .value("name", "stale"),
            )
            .unwrap_err();
        assert!(matches!(
            conflict,
            RepositoryError::Runtime(RuntimeError::OptimisticLockConflict { .. })
        ));

        repository
            .delete(&DeleteCommand::new("Order", 10_u64).expected_version(2))
            .unwrap();
        let row = repository
            .fetch_all(&teaql_core::SelectQuery::new("Order").filter(Expr::eq("id", 10_u64)))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(row.get("version"), Some(&Value::I64(-3)));

        repository
            .recover(&RecoverCommand::new("Order", 10_u64, -3))
            .unwrap();
        let row = repository
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
}

pub use checker::{
    CHECK_OBJECT_STATUS_FIELD, CheckObjectStatus, CheckResult, CheckResults, CheckRule, Checker,
    CheckerRegistry, InMemoryCheckerRegistry, LocationSegment, ObjectLocation, TypedChecker,
    TypedEntityChecker, clear_record_status, mark_record_status,
};
