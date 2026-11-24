use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use chrono::NaiveDate;
use rusqlite::Connection;
use teaql_core::business_id::{
    BusinessIdAllocator, BusinessIdDefinition, BusinessIdErrorCode, BusinessIdGenerationRequest,
    BusinessIdProfile, BusinessIdSlot, BusinessIdValue,
};
use teaql_provider_sqlite::{
    SqliteBusinessIdAllocator, SqliteMutationExecutor, SqliteProviderExt as _,
};
use teaql_runtime::{
    BusinessDate, BusinessIdEncodingKey, BusinessIdService, DailySequenceBusinessIdProfile,
    InMemoryBusinessIdAllocator, PermutedDailyBusinessIdProfile, StaticBusinessIdKeyProvider,
    UserContext,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderNumber(String);

impl OrderNumber {
    fn new(value: String) -> Self {
        assert!(value.starts_with("CO-"));
        Self(value)
    }
}

#[derive(Debug, Default)]
struct OrderX {
    order_number: Option<OrderNumber>,
    new_aggregate: bool,
    mutation_ledger: Vec<(String, String)>,
}

impl OrderX {
    fn new() -> Self {
        Self {
            new_aggregate: true,
            ..Self::default()
        }
    }
}

impl BusinessIdSlot for OrderX {
    fn current_business_id(&self) -> Option<&str> {
        self.order_number.as_ref().map(|value| value.0.as_str())
    }

    fn is_new_aggregate(&self) -> bool {
        self.new_aggregate
    }

    fn assign_business_id(&mut self, value: BusinessIdValue) {
        let typed = OrderNumber::new(value.0);
        self.mutation_ledger
            .push(("order_number".to_owned(), typed.0.clone()));
        self.order_number = Some(typed);
    }
}

fn definition() -> BusinessIdDefinition {
    BusinessIdDefinition::daily_sequence("order_number", "CO", "commerce_order")
}

fn test_key_provider() -> StaticBusinessIdKeyProvider {
    StaticBusinessIdKeyProvider::new(
        BusinessIdEncodingKey::new(
            1,
            [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            ],
        )
        .expect("valid test key"),
    )
}

#[test]
fn memory_business_id_is_typed_scoped_and_retry_stable() {
    let mut context = UserContext::default();
    context.set_business_id_service(
        BusinessIdService::new(InMemoryBusinessIdAllocator::default())
            .with_key_provider(test_key_provider()),
    );
    context.insert_resource(BusinessDate(
        NaiveDate::from_ymd_opt(2026, 9, 20).expect("valid fixture date"),
    ));

    let mut first = OrderX::new();
    let initial = context
        .business_ids()
        .expect("Business ID service in Context")
        .ensure(
            &context,
            &definition(),
            "tenant-a",
            "commerce_order",
            &mut first,
        )
        .expect("allocate first business ID");
    assert_eq!(initial.0, "CO-20260920-KSHQ82");
    assert_eq!(first.order_number, Some(OrderNumber(initial.0.clone())));
    assert_eq!(first.mutation_ledger.len(), 1);

    // A failed provider save retries the same aggregate and must not burn another sequence.
    let retry = context
        .business_ids()
        .expect("Business ID service in Context")
        .ensure(
            &context,
            &definition(),
            "tenant-a",
            "commerce_order",
            &mut first,
        )
        .expect("reuse assigned business ID");
    assert_eq!(retry, initial);
    assert_eq!(first.mutation_ledger.len(), 1);

    let mut second = OrderX::new();
    assert_eq!(
        context
            .business_ids()
            .expect("Business ID service in Context")
            .ensure(
                &context,
                &definition(),
                "tenant-a",
                "commerce_order",
                &mut second
            )
            .expect("allocate second business ID")
            .0,
        "CO-20260920-B8S7XN"
    );

    let mut other_tenant = OrderX::new();
    assert_eq!(
        context
            .business_ids()
            .expect("Business ID service in Context")
            .ensure(
                &context,
                &definition(),
                "tenant-b",
                "commerce_order",
                &mut other_tenant,
            )
            .expect("tenant-scoped sequence")
            .0,
        "CO-20260920-Y5Z052"
    );

    let mut next_day_context = UserContext::default();
    next_day_context.set_business_id_service(
        context
            .business_ids()
            .expect("Business ID service in Context")
            .clone(),
    );
    next_day_context.insert_resource(BusinessDate(
        NaiveDate::from_ymd_opt(2026, 9, 21).expect("valid next-day fixture date"),
    ));
    let mut next_day = OrderX::new();
    assert_eq!(
        next_day_context
            .business_ids()
            .expect("Business ID service in Context")
            .ensure(
                &next_day_context,
                &definition(),
                "tenant-a",
                "commerce_order",
                &mut next_day,
            )
            .expect("date-scoped sequence")
            .0,
        "CO-20260921-XAUWT0"
    );

    let mut persisted = OrderX::default();
    let error = context
        .business_ids()
        .expect("Business ID service in Context")
        .ensure(
            &context,
            &definition(),
            "tenant-a",
            "commerce_order",
            &mut persisted,
        )
        .expect_err("persisted blank business ID must not be repaired silently");
    assert!(format!("{error}").contains("Immutable"));

    let mut invalid_explicit = OrderX::new();
    invalid_explicit.order_number = Some(OrderNumber("CO-not-a-date-ABC123".to_owned()));
    let error = context
        .business_ids()
        .expect("Business ID service in Context")
        .ensure(
            &context,
            &definition(),
            "tenant-a",
            "commerce_order",
            &mut invalid_explicit,
        )
        .expect_err("explicit Business ID must pass profile validation");
    assert!(format!("{error}").contains("InvalidFormat"));

    let mut valid_explicit = OrderX::new();
    valid_explicit.order_number = Some(OrderNumber("CO-20260920-ABC123".to_owned()));
    assert_eq!(
        context
            .business_ids()
            .expect("Business ID service in Context")
            .ensure(
                &context,
                &definition(),
                "tenant-a",
                "commerce_order",
                &mut valid_explicit,
            )
            .expect("valid explicit Business ID")
            .0,
        "CO-20260920-ABC123"
    );
}

#[test]
fn default_profile_fails_closed_without_key_provider() {
    let mut context = UserContext::default();
    context.set_business_id_service(BusinessIdService::new(
        InMemoryBusinessIdAllocator::default(),
    ));
    context.insert_resource(BusinessDate(
        NaiveDate::from_ymd_opt(2026, 9, 20).expect("valid fixture date"),
    ));

    let mut order = OrderX::new();
    let error = context
        .business_ids()
        .expect("Business ID service in Context")
        .ensure(
            &context,
            &definition(),
            "domain-root-a",
            "commerce_order",
            &mut order,
        )
        .expect_err("the default profile must not run without a key provider");
    assert_eq!(error.code, BusinessIdErrorCode::KeyNotFound);
    assert!(order.order_number.is_none());
    assert!(order.mutation_ledger.is_empty());
}

#[test]
fn explicit_legacy_profile_retains_one_based_decimal_sequence() {
    let mut context = UserContext::default();
    context.set_business_id_service(BusinessIdService::new(
        InMemoryBusinessIdAllocator::default(),
    ));
    context.insert_resource(BusinessDate(
        NaiveDate::from_ymd_opt(2026, 9, 20).expect("valid fixture date"),
    ));
    let legacy = BusinessIdDefinition::legacy_daily_sequence(
        "legacy_order_number",
        "CO",
        "legacy_order_number",
    );

    let mut first = OrderX::new();
    let first_value = context
        .business_ids()
        .expect("Business ID service in Context")
        .ensure(
            &context,
            &legacy,
            "domain-root-a",
            "commerce_order",
            &mut first,
        )
        .expect("allocate first legacy Business ID");
    assert_eq!(first_value.0, "CO-20260920-00000001");

    let mut second = OrderX::new();
    let second_value = context
        .business_ids()
        .expect("Business ID service in Context")
        .ensure(
            &context,
            &legacy,
            "domain-root-a",
            "commerce_order",
            &mut second,
        )
        .expect("allocate second legacy Business ID");
    assert_eq!(second_value.0, "CO-20260920-00000002");
}

#[tokio::test]
async fn sqlite_allocator_is_explicit_concurrent_and_restart_safe() {
    let path = unique_database_path();
    let executor =
        SqliteMutationExecutor::from_connection(Connection::open(&path).expect("open first DB"));
    let first = SqliteBusinessIdAllocator::from_executor(executor.clone());
    let mut context = UserContext::default();
    context.use_sqlite_provider(executor);
    context.set_business_id_infrastructure(first);
    context
        .set_business_id_key_provider(test_key_provider())
        .expect("attach Business ID V1 key provider");
    assert!(context.business_ids().is_ok());
    assert!(!business_id_table_exists(&path));
    context
        .ensure_schema()
        .await
        .expect("explicit context schema lifecycle installs allocator table");
    assert!(business_id_table_exists(&path));

    let plan = PermutedDailyBusinessIdProfile::new(
        BusinessIdEncodingKey::new(
            1,
            [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            ],
        )
        .expect("valid test key"),
    )
    .plan(&BusinessIdGenerationRequest {
        definition: &definition(),
        domain_root_key: "tenant-a",
        aggregate_type: "commerce_order",
        business_date: NaiveDate::from_ymd_opt(2026, 9, 20).expect("valid fixture date"),
    })
    .expect("build allocation plan");

    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let path = path.clone();
        let plan = plan.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let connection = Connection::open(path).expect("open worker DB");
            connection
                .busy_timeout(std::time::Duration::from_secs(5))
                .expect("set busy timeout");
            let allocator = SqliteBusinessIdAllocator::new(connection);
            barrier.wait();
            (0..25)
                .map(|_| {
                    allocator
                        .allocate(&plan)
                        .expect("allocate concurrently")
                        .sequence
                })
                .collect::<Vec<_>>()
        }));
    }
    let allocated = workers
        .into_iter()
        .flat_map(|worker| worker.join().expect("join allocator worker"))
        .collect::<Vec<_>>();
    assert_eq!(allocated.len(), 50);
    assert_eq!(allocated.iter().copied().collect::<HashSet<_>>().len(), 50);

    let restarted = SqliteBusinessIdAllocator::new(Connection::open(&path).expect("reopen DB"));
    let restart_allocation = restarted.allocate(&plan).expect("allocate after restart");
    assert_eq!(restart_allocation.sequence, 50);
    assert_eq!(
        PermutedDailyBusinessIdProfile::new(
            BusinessIdEncodingKey::new(
                1,
                [
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                    0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
                    0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
                ],
            )
            .expect("valid test key"),
        )
        .format(&plan, &restart_allocation)
        .expect("format persistent allocation")
        .0,
        "CO-20260920-SVTYLW"
    );

    let legacy_definition = BusinessIdDefinition::legacy_daily_sequence(
        "legacy_order_number",
        "CO",
        "legacy_order_number",
    );
    let legacy_plan = DailySequenceBusinessIdProfile
        .plan(&BusinessIdGenerationRequest {
            definition: &legacy_definition,
            domain_root_key: "domain-root-a",
            aggregate_type: "commerce_order",
            business_date: NaiveDate::from_ymd_opt(2026, 9, 20).expect("valid fixture date"),
        })
        .expect("build legacy allocation plan");
    assert_eq!(
        restarted
            .allocate(&legacy_plan)
            .expect("allocate first persistent legacy sequence")
            .sequence,
        1
    );
    assert_eq!(
        restarted
            .allocate(&legacy_plan)
            .expect("allocate second persistent legacy sequence")
            .sequence,
        2
    );
    std::fs::remove_file(path).expect("remove fixture DB");
}

fn business_id_table_exists(path: &PathBuf) -> bool {
    let connection = Connection::open(path).expect("inspect fixture DB");
    connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='teaql_business_id_space'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("inspect Business ID table")
        == 1
}

fn unique_database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "teaql-business-id-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ))
}
