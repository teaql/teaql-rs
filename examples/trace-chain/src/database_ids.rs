//! Generated APIs allocate IDs at construction, before the save transaction.
//! The native ledger probe separately verifies late allocation inside that
//! transaction. Do not confuse these two execution paths.
use super::{
    AuditCapture, ExpectedItem, Observation, Observed, Outcome, assert_audit_graph,
    assert_execution_lineage,
};
use std::sync::{Arc, Mutex};
use teaql_provider_sqlite::{SqliteIdSpaceGenerator, SqliteMutationExecutor};
use teaql_runtime::{InternalIdGenerator, RawAuditEventKind, RuntimeError};
use trace_chain_service_core::teaql_core::Entity as _;
use trace_chain_service_core::{
    AuditedSave as _, E, LedgerEntity as _, Q, ServiceRuntimeConfig, ServiceRuntimeExecutor,
    service_runtime,
};

#[derive(Clone)]
struct DatabaseIdWitness {
    generator: SqliteIdSpaceGenerator,
    transport: SqliteMutationExecutor,
    calls: Arc<Mutex<Vec<(String, u64, bool)>>>,
}

impl InternalIdGenerator for DatabaseIdWitness {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        let autocommit = self.transport.connection().lock().unwrap().is_autocommit();
        let id = self.generator.generate_id(entity)?;
        self.calls
            .lock()
            .unwrap()
            .push((entity.into(), id, autocommit));
        Ok(id)
    }

    fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), RuntimeError> {
        InternalIdGenerator::ensure_floor(&self.generator, entity, floor)
    }
}

pub async fn generated_database_ids(database_url: String) -> Outcome<()> {
    let capture = AuditCapture::default();
    let observation = Observation::default();
    let mut context = service_runtime(ServiceRuntimeConfig { database_url })
        .await?
        .with_custom_event_sink(capture.clone());
    context.ensure_schema().await?;
    let transport = context
        .require_resource::<SqliteMutationExecutor>()?
        .clone();
    let ids = DatabaseIdWitness {
        generator: SqliteIdSpaceGenerator::from_executor(transport.clone()),
        transport,
        calls: Default::default(),
    };
    context.set_internal_id_generator(ids.clone());
    let executor = context
        .require_resource::<ServiceRuntimeExecutor>()?
        .clone()
        .with_query_metadata_observer(observation.query_metadata_observer());
    context.insert_resource(executor.clone());
    context.register_executor(Observed::new(executor, observation.clone()));
    let platform = Q::platforms()
        .limit(1)
        .comment("what: load the seeded platform for database ID verification")
        .purpose("why: create a generated graph with the real SQLite ID allocator")
        .execute_for_one(&context)
        .await?
        .ok_or("seeded platform must exist")?;

    let mut order = Q::customer_orders()
        .comment("what: construct a database-identified order")
        .purpose("why: verify generated early ID allocation")
        .new_entity(&context);
    order.update_platform_id(platform.id());
    order.update_order_number("TRACE-DATABASE-ID");
    order.update_description("Database ID graph");
    let order_id = order.id();
    let mut item = Q::order_items()
        .comment("what: construct a database-identified order item")
        .purpose("why: verify typed child identity and audited composition")
        .new_entity(&context);
    item.update_customer_order_id(order_id);
    item.update_name("Database ID item");
    let item_id = item.id();
    let item = item
        .audit_as("authorize database-identified item")
        .into_entity();
    order.include_pending_mutations_from(&item)?;
    assert_eq!(
        *ids.calls.lock().unwrap(),
        [
            ("Customer Order".into(), order_id, true),
            ("Order Item".into(), item_id, true),
        ],
        "generated new_entity IDs are allocated outside the save transaction"
    );
    assert!(order_id > 0 && item_id > 0);
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    let saved = order
        .audit_as("save database-identified graph")
        .save(&context)
        .await?;
    assert_eq!(saved.id(), order_id);
    assert_eq!(
        ids.calls.lock().unwrap().len(),
        2,
        "save must not allocate a second identity"
    );
    let root_reason = ("CustomerOrder", order_id, "save database-identified graph");
    let expected = [
        ExpectedItem {
            entity: "CustomerOrder",
            id: order_id,
            kind: RawAuditEventKind::Created,
            reasons: vec![root_reason],
        },
        ExpectedItem {
            entity: "OrderItem",
            id: item_id,
            kind: RawAuditEventKind::Created,
            reasons: vec![
                root_reason,
                ("OrderItem", item_id, "authorize database-identified item"),
            ],
        },
    ];
    assert_execution_lineage(&observation, &expected);
    assert_audit_graph(&capture.events(), &expected);
    let reloaded = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .select_order_item_list_with(Q::order_items().order_by_id_asc().limit(10))
        .comment("what: reload the database-identified graph")
        .purpose("why: verify generated Q/E persisted identities and relationship")
        .execute_for_one(&context)
        .await?
        .ok_or("database-identified root missing")?;
    assert_eq!(E::customer_order(&reloaded).get_id().eval(), Some(order_id));
    assert_eq!(E::customer_order(&reloaded).get_version().eval(), Some(1));
    assert_eq!(
        E::customer_order(&reloaded)
            .get_order_item_list()
            .size()
            .eval(),
        Some(1)
    );
    assert_eq!(
        E::customer_order(&reloaded)
            .get_order_item_list()
            .first()
            .get_id()
            .eval(),
        Some(item_id)
    );
    let child = Q::order_items()
        .with_id_is(item_id)
        .limit(1)
        .comment("what: verify the database-identified child")
        .purpose("why: assert generated E foreign key, version and business value")
        .execute_for_one(&context)
        .await?
        .ok_or("database-identified child missing")?;
    assert_eq!(
        E::order_item(&child).get_customer_order_id().eval(),
        Some(order_id)
    );
    assert_eq!(E::order_item(&child).get_version().eval(), Some(1));
    assert_eq!(
        E::order_item(&child).get_name().eval().as_deref(),
        Some("Database ID item")
    );
    println!(
        "TC-MUT-07 GENERATED DATABASE IDS PASSED order={order_id} item={item_id}; construction before BEGIN, command/SQL/audit and generated Q/E verified"
    );
    Ok(())
}
