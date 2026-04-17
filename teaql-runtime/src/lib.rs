mod context;
mod error;
mod id;
mod memory;
mod registry;
mod repository;

pub use context::UserContext;
pub use error::{ContextError, RepositoryError, RuntimeError};
pub(crate) use id::local_id_generator;
pub use id::{InternalIdGenerator, SnowflakeIdGenerator};
pub use memory::{MemoryRepository, MemoryRepositoryError};
pub use registry::{
    InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry,
    MetadataStore, RepositoryBehavior, RepositoryBehaviorRegistry, RepositoryRegistry,
    RuntimeModule,
};
pub use repository::{
    ContextRepository, QueryExecutor, RelationLoadPlan, Repository, ResolvedRepository,
};

#[cfg(feature = "sqlx")]
pub mod sqlx_support;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry,
        InternalIdGenerator, MemoryRepository, MetadataStore, QueryExecutor, Repository,
        RepositoryBehavior, RepositoryError, RuntimeError, RuntimeModule, UserContext,
    };
    use teaql_core::{
        Aggregate, AggregateFunction, DataType, DeleteCommand, Entity, EntityDescriptor,
        EntityError, Expr, InsertCommand, OrderBy, PropertyDescriptor, Record, RecoverCommand,
        TeaqlEntity, UpdateCommand, Value,
    };
    use teaql_dialect_pg::PostgresDialect;
    use teaql_macros::TeaqlEntity as DeriveTeaqlEntity;
    use teaql_sql::CompiledQuery;

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
        dynamic: BTreeMap<String, serde_json::Value>,
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

    struct FixedIdGenerator(u64);

    impl InternalIdGenerator for FixedIdGenerator {
        fn generate_id(&self, _entity: &str) -> Result<u64, RuntimeError> {
            Ok(self.0)
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
        assert_eq!(
            rows.data[0].dynamic.get("lineCount"),
            Some(&serde_json::json!(3))
        );
        assert_eq!(
            rows.data[0].dynamic.get("amount"),
            Some(&serde_json::json!(18.5))
        );
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
            filter: None,
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
        };

        let rows = repository.fetch_all(&query).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("count"), Some(&Value::U64(2)));
        assert_eq!(rows[0].get("versionSum"), Some(&Value::U64(3)));
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
}

#[cfg(all(test, feature = "sqlx"))]
mod sqlx_integration_tests {
    use super::sqlx_support::{MutationExecutorError, PgMutationExecutor, SqliteMutationExecutor};
    use super::{
        InMemoryMetadataStore, InMemoryRepositoryBehaviorRegistry, InMemoryRepositoryRegistry,
        QueryExecutor, RepositoryBehavior, UserContext,
    };
    use chrono::{NaiveDate, TimeZone, Utc};
    use teaql_core::{
        DataType, DeleteCommand, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record,
        RecoverCommand, SelectQuery, UpdateCommand, Value,
    };
    use teaql_dialect_pg::PostgresDialect;
    use teaql_dialect_sqlite::SqliteDialect;
    use teaql_macros::TeaqlEntity as DeriveTeaqlEntity;
    use teaql_sql::SqlDialect;
    use tokio::runtime::Handle;
    use tokio::task::block_in_place;

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
            .property(
                PropertyDescriptor::new("name", DataType::Text)
                    .column_name("name")
                    .not_null(),
            )
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
                PropertyDescriptor::new("order_id", DataType::U64)
                    .column_name("order_id")
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("name", DataType::Text)
                    .column_name("name")
                    .not_null(),
            )
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
            .property(
                PropertyDescriptor::new("name", DataType::Text)
                    .column_name("name")
                    .not_null(),
            )
    }

    fn typed_entity() -> EntityDescriptor {
        EntityDescriptor::new("TypedValue")
            .table_name("typed_value")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("payload", DataType::Json)
                    .column_name("payload")
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("birthday", DataType::Date)
                    .column_name("birthday")
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("happened_at", DataType::Timestamp)
                    .column_name("happened_at")
                    .not_null(),
            )
    }

    struct OrderBehavior;
    struct NestedOrderBehavior;

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

    impl RepositoryBehavior for OrderBehavior {
        fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
            vec!["lines".to_owned()]
        }
    }

    impl RepositoryBehavior for NestedOrderBehavior {
        fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
            vec!["lines.product".to_owned()]
        }
    }

    #[derive(Clone)]
    struct SqliteSyncExecutor {
        inner: SqliteMutationExecutor,
    }

    impl SqliteSyncExecutor {
        fn new(inner: SqliteMutationExecutor) -> Self {
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

    #[tokio::test]
    async fn sqlite_executor_runs_crud_flow() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let executor = SqliteMutationExecutor::new(pool.clone());
        let dialect = SqliteDialect;
        let entity = entity();
        executor.ensure_schema(&dialect, &[&entity]).await.unwrap();

        let insert = dialect
            .compile_insert(
                &entity,
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "first"),
            )
            .unwrap();
        assert_eq!(executor.execute(&insert).await.unwrap(), 1);

        let select = dialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order")
                    .project("id")
                    .project("version")
                    .project("name")
                    .filter(Expr::eq("id", 1_u64)),
            )
            .unwrap();
        let rows = executor.fetch_all(&select).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("version"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("name"), Some(&Value::Text("first".to_owned())));

        let update = dialect
            .compile_update(
                &entity,
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(1)
                    .value("name", "second"),
            )
            .unwrap();
        assert_eq!(executor.execute(&update).await.unwrap(), 1);

        let after_update = executor.fetch_all(&select).await.unwrap();
        assert_eq!(after_update[0].get("version"), Some(&Value::I64(2)));
        assert_eq!(
            after_update[0].get("name"),
            Some(&Value::Text("second".to_owned()))
        );

        let delete = dialect
            .compile_delete(
                &entity,
                &DeleteCommand::new("Order", 1_u64).expected_version(2),
            )
            .unwrap();
        assert_eq!(executor.execute(&delete).await.unwrap(), 1);

        let after_delete = executor.fetch_all(&select).await.unwrap();
        assert_eq!(after_delete[0].get("version"), Some(&Value::I64(-3)));

        let recover = dialect
            .compile_recover(&entity, &RecoverCommand::new("Order", 1_u64, -3))
            .unwrap();
        assert_eq!(executor.execute(&recover).await.unwrap(), 1);

        let after_recover = executor.fetch_all(&select).await.unwrap();
        assert_eq!(after_recover[0].get("version"), Some(&Value::I64(4)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_executor_enhances_relations() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let mutation_executor = SqliteMutationExecutor::new(pool.clone());
        let order = entity();
        let line = line_entity();
        mutation_executor
            .ensure_schema(&SqliteDialect, &[&order, &line])
            .await
            .unwrap();

        sqlx::query("INSERT INTO orders (id, version, name) VALUES (1, 1, 'o1'), (2, 1, 'o2')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO orderline (id, order_id, product_id, name) VALUES
                (101, 1, 1001, 'l1'),
                (102, 1, 1002, 'l2'),
                (201, 2, 1003, 'l3')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteSyncExecutor::new(mutation_executor);
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(order)
                    .with_entity(line)
                    .with_entity(product_entity()),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new().with_behavior("Order", OrderBehavior),
            );
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let mut parents = repo
            .fetch_all(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name")
                    .order_by(teaql_core::OrderBy::asc("id")),
            )
            .unwrap();

        repo.enhance_relations(&mut parents).unwrap();

        assert_eq!(parents.len(), 2);
        match parents[0].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 2),
            other => panic!("unexpected first lines payload: {other:?}"),
        }
        match parents[1].get("lines") {
            Some(Value::List(lines)) => assert_eq!(lines.len(), 1),
            other => panic!("unexpected second lines payload: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_executor_enhances_nested_relations() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let mutation_executor = SqliteMutationExecutor::new(pool.clone());
        let order = entity();
        let line = line_entity();
        let product = product_entity();
        mutation_executor
            .ensure_schema(&SqliteDialect, &[&order, &line, &product])
            .await
            .unwrap();

        sqlx::query("INSERT INTO orders (id, version, name) VALUES (1, 1, 'o1')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO orderline (id, order_id, product_id, name) VALUES
                (101, 1, 1001, 'l1'),
                (102, 1, 1002, 'l2')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO product (id, name) VALUES
                (1001, 'p1'),
                (1002, 'p2')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteSyncExecutor::new(mutation_executor);
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(order)
                    .with_entity(line)
                    .with_entity(product),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new()
                    .with_behavior("Order", NestedOrderBehavior),
            );
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let mut parents = repo
            .fetch_all(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name"),
            )
            .unwrap();

        repo.enhance_relations(&mut parents).unwrap();

        match parents[0].get("lines") {
            Some(Value::List(lines)) => {
                assert_eq!(lines.len(), 2);
                for line in lines {
                    match line {
                        Value::Object(line_record) => match line_record.get("product") {
                            Some(Value::Object(product)) => {
                                assert!(product.get("name").is_some());
                            }
                            other => panic!("unexpected product payload: {other:?}"),
                        },
                        other => panic!("unexpected line payload: {other:?}"),
                    }
                }
            }
            other => panic!("unexpected nested lines payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sqlite_executor_ensure_schema_adds_missing_columns() {
        use sqlx::Row;
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::query(
            "CREATE TABLE orders (
                id INTEGER PRIMARY KEY
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteMutationExecutor::new(pool.clone());
        let dialect = SqliteDialect;
        let entity = entity();

        executor.ensure_schema(&dialect, &[&entity]).await.unwrap();

        let columns = sqlx::query("PRAGMA table_info(\"orders\")")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String, _>("name").unwrap())
            .collect::<Vec<_>>();

        assert!(columns.contains(&"id".to_owned()));
        assert!(columns.contains(&"version".to_owned()));
        assert!(columns.contains(&"name".to_owned()));
    }

    #[tokio::test]
    async fn user_context_can_ensure_sqlite_schema_from_runtime_module() {
        use sqlx::Row;
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let module = super::RuntimeModule::new()
            .descriptor(entity())
            .descriptor(line_entity())
            .descriptor(product_entity());
        let mut ctx = super::UserContext::new().with_module(module);
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(SqliteMutationExecutor::new(pool.clone()));

        ctx.ensure_sqlite_schema().await.unwrap();

        let tables = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('orders', 'orderline', 'product') ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String, _>("name").unwrap())
        .collect::<Vec<_>>();

        assert_eq!(
            tables,
            vec![
                "orderline".to_owned(),
                "orders".to_owned(),
                "product".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn sqlite_executor_roundtrips_json_date_and_timestamp() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let executor = SqliteMutationExecutor::new(pool.clone());
        let dialect = SqliteDialect;
        let entity = typed_entity();
        executor.ensure_schema(&dialect, &[&entity]).await.unwrap();

        let birthday = NaiveDate::from_ymd_opt(2024, 2, 3).unwrap();
        let happened_at = Utc.with_ymd_and_hms(2024, 2, 3, 4, 5, 6).unwrap();
        let payload = serde_json::json!({"name": "teaql", "count": 2});

        let insert = dialect
            .compile_insert(
                &entity,
                &InsertCommand::new("TypedValue")
                    .value("id", 1_u64)
                    .value("payload", payload.clone())
                    .value("birthday", birthday)
                    .value("happened_at", happened_at),
            )
            .unwrap();
        assert_eq!(executor.execute(&insert).await.unwrap(), 1);

        let select = dialect
            .compile_select(
                &entity,
                &SelectQuery::new("TypedValue")
                    .project("id")
                    .project("payload")
                    .project("birthday")
                    .project("happened_at")
                    .filter(Expr::eq("id", 1_u64)),
            )
            .unwrap();
        let rows = executor.fetch_all(&select).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("payload"), Some(&Value::Json(payload)));
        assert_eq!(rows[0].get("birthday"), Some(&Value::Date(birthday)));
        assert_eq!(
            rows[0].get("happened_at"),
            Some(&Value::Timestamp(happened_at))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_fetches_enhanced_typed_entities() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let mutation_executor = SqliteMutationExecutor::new(pool.clone());
        let order = entity();
        let line = line_entity();
        let product = product_entity();
        mutation_executor
            .ensure_schema(&SqliteDialect, &[&order, &line, &product])
            .await
            .unwrap();

        sqlx::query("INSERT INTO orders (id, version, name) VALUES (1, 1, 'o1')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO orderline (id, order_id, product_id, name) VALUES
                (101, 1, 1001, 'l1'),
                (102, 1, 1002, 'l2')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO product (id, name) VALUES
                (1001, 'p1'),
                (1002, 'p2')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteSyncExecutor::new(mutation_executor);
        let mut ctx = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(order)
                    .with_entity(line)
                    .with_entity(product),
            )
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"))
            .with_repository_behavior_registry(
                InMemoryRepositoryBehaviorRegistry::new()
                    .with_behavior("Order", NestedOrderBehavior),
            );
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_enhanced_entities::<OrderAggregateRow>(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name"),
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        let order = rows.first().unwrap();
        assert_eq!(order.id, 1);
        assert_eq!(order.lines.len(), 2);
        assert_eq!(order.lines.data[0].product.as_ref().unwrap().name, "p1");
        assert_eq!(order.lines.data[1].product.as_ref().unwrap().name, "p2");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_fetches_typed_smart_list_entities() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let mutation_executor = SqliteMutationExecutor::new(pool.clone());
        let order = entity();
        mutation_executor
            .ensure_schema(&SqliteDialect, &[&order])
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO orders (id, version, name) VALUES
                (1, 1, 'o1'),
                (2, 3, 'o2')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteSyncExecutor::new(mutation_executor);
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(order))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_entities::<OrderAggregateRow>(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name"),
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows.ids(), vec![Value::U64(1), Value::U64(2)]);
        assert_eq!(rows.versions(), vec![1, 3]);
        assert!(rows.data[0].lines.is_empty());
        assert!(rows.data[1].lines.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_fetches_smart_list_of_order_entities() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let mutation_executor = SqliteMutationExecutor::new(pool.clone());
        let order = entity();
        mutation_executor
            .ensure_schema(&SqliteDialect, &[&order])
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO orders (id, version, name) VALUES
                (1, 1, 'o1'),
                (2, 3, 'o2')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let executor = SqliteSyncExecutor::new(mutation_executor);
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(order))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let rows = repo
            .fetch_entities::<Order>(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name"),
            )
            .unwrap();

        assert_eq!(
            rows.data,
            vec![
                Order {
                    id: 1,
                    version: 1,
                    name: "o1".to_owned(),
                },
                Order {
                    id: 2,
                    version: 3,
                    name: "o2".to_owned(),
                }
            ]
        );
        assert_eq!(rows.ids(), vec![Value::U64(1), Value::U64(2)]);
        assert_eq!(rows.versions(), vec![1, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_insert_generates_missing_id() {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        let mutation_executor = SqliteMutationExecutor::new(pool.clone());
        let order = entity();
        mutation_executor
            .ensure_schema(&SqliteDialect, &[&order])
            .await
            .unwrap();

        let executor = SqliteSyncExecutor::new(mutation_executor);
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(order))
            .with_repository_registry(InMemoryRepositoryRegistry::new().with_entity("Order"));
        ctx.insert_resource(SqliteDialect);
        ctx.insert_resource(executor);

        let repo = ctx
            .resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")
            .unwrap();
        let affected = repo
            .insert(
                &repo
                    .insert_command()
                    .value("version", 1_i64)
                    .value("name", "generated"),
            )
            .unwrap();
        assert_eq!(affected, 1);

        let rows = repo
            .fetch_entities::<Order>(
                &repo
                    .select()
                    .project("id")
                    .project("version")
                    .project("name")
                    .filter(Expr::eq("name", "generated")),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        let order = rows.first().unwrap();
        assert!(order.id > 0);
        assert_eq!(order.version, 1);
        assert_eq!(order.name, "generated");
    }

    #[tokio::test]
    async fn postgres_executor_ensure_schema_roundtrip_when_database_is_available() {
        use sqlx::{PgPool, Row};

        let Some(database_url) = std::env::var("TEAQL_TEST_PG_URL").ok() else {
            return;
        };

        let pool = PgPool::connect(&database_url).await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS orderline")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS orders")
            .execute(&pool)
            .await
            .unwrap();

        let executor = PgMutationExecutor::new(pool.clone());
        let dialect = PostgresDialect;
        let order = entity();
        let line = line_entity();

        executor
            .ensure_schema(&dialect, &[&order, &line])
            .await
            .unwrap();

        let tables = sqlx::query(
            "SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name IN ('orders', 'orderline')
             ORDER BY table_name",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String, _>("table_name").unwrap())
        .collect::<Vec<_>>();
        assert_eq!(tables, vec!["orderline".to_owned(), "orders".to_owned()]);

        sqlx::query("DROP TABLE orderline")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE orderline (id BIGINT PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();

        executor.ensure_schema(&dialect, &[&line]).await.unwrap();

        let columns = sqlx::query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = 'orderline'
             ORDER BY column_name",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String, _>("column_name").unwrap())
        .collect::<Vec<_>>();
        assert!(columns.contains(&"id".to_owned()));
        assert!(columns.contains(&"order_id".to_owned()));
        assert!(columns.contains(&"product_id".to_owned()));
        assert!(columns.contains(&"name".to_owned()));

        sqlx::query("DROP TABLE IF EXISTS orderline")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS orders")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn user_context_can_ensure_postgres_schema_when_database_is_available() {
        use sqlx::{PgPool, Row};

        let Some(database_url) = std::env::var("TEAQL_TEST_PG_URL").ok() else {
            return;
        };

        let pool = PgPool::connect(&database_url).await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS product_ctx")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS orderline_ctx")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS orders_ctx")
            .execute(&pool)
            .await
            .unwrap();

        let order = entity().table_name("orders_ctx");
        let line = line_entity().table_name("orderline_ctx");
        let product = product_entity().table_name("product_ctx");
        let module = super::RuntimeModule::new()
            .descriptor(order)
            .descriptor(line)
            .descriptor(product);
        let mut ctx = super::UserContext::new().with_module(module);
        ctx.insert_resource(PostgresDialect);
        ctx.insert_resource(PgMutationExecutor::new(pool.clone()));

        ctx.ensure_postgres_schema().await.unwrap();

        let tables = sqlx::query(
            "SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name IN ('orders_ctx', 'orderline_ctx', 'product_ctx')
             ORDER BY table_name",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String, _>("table_name").unwrap())
        .collect::<Vec<_>>();

        assert_eq!(
            tables,
            vec![
                "orderline_ctx".to_owned(),
                "orders_ctx".to_owned(),
                "product_ctx".to_owned()
            ]
        );

        sqlx::query("DROP TABLE IF EXISTS product_ctx")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS orderline_ctx")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS orders_ctx")
            .execute(&pool)
            .await
            .unwrap();
    }
}
