use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use chrono::NaiveDate;
use rusqlite::Connection;
use teaql_core::business_id::{
    BusinessIdAllocator, BusinessIdDefinition, BusinessIdGenerationRequest, BusinessIdProfile,
    BusinessIdSlot, BusinessIdValue,
};
use teaql_provider_sqlite::SqliteBusinessIdAllocator;
use teaql_runtime::{
    BusinessDate, BusinessIdService, DailySequenceBusinessIdProfile, InMemoryBusinessIdAllocator,
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

#[test]
fn memory_business_id_is_typed_scoped_and_retry_stable() {
    let service = BusinessIdService::new(InMemoryBusinessIdAllocator::default());
    let mut context = UserContext::default();
    context.insert_resource(BusinessDate(
        NaiveDate::from_ymd_opt(2026, 9, 20).expect("valid fixture date"),
    ));

    let mut first = OrderX::new();
    let initial = service
        .ensure(
            &context,
            &definition(),
            "tenant-a",
            "commerce_order",
            &mut first,
        )
        .expect("allocate first business ID");
    assert_eq!(initial.0, "CO-20260920-00000001");
    assert_eq!(first.order_number, Some(OrderNumber(initial.0.clone())));
    assert_eq!(first.mutation_ledger.len(), 1);

    // A failed provider save retries the same aggregate and must not burn another sequence.
    let retry = service
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
        service
            .ensure(
                &context,
                &definition(),
                "tenant-a",
                "commerce_order",
                &mut second
            )
            .expect("allocate second business ID")
            .0,
        "CO-20260920-00000002"
    );

    let mut other_tenant = OrderX::new();
    assert_eq!(
        service
            .ensure(
                &context,
                &definition(),
                "tenant-b",
                "commerce_order",
                &mut other_tenant,
            )
            .expect("tenant-scoped sequence")
            .0,
        "CO-20260920-00000001"
    );

    let mut next_day_context = UserContext::default();
    next_day_context.insert_resource(BusinessDate(
        NaiveDate::from_ymd_opt(2026, 9, 21).expect("valid next-day fixture date"),
    ));
    let mut next_day = OrderX::new();
    assert_eq!(
        service
            .ensure(
                &next_day_context,
                &definition(),
                "tenant-a",
                "commerce_order",
                &mut next_day,
            )
            .expect("date-scoped sequence")
            .0,
        "CO-20260921-00000001"
    );

    let mut persisted = OrderX::default();
    let error = service
        .ensure(
            &context,
            &definition(),
            "tenant-a",
            "commerce_order",
            &mut persisted,
        )
        .expect_err("persisted blank business ID must not be repaired silently");
    assert!(format!("{error}").contains("Immutable"));
}

#[test]
fn sqlite_allocator_is_explicit_concurrent_and_restart_safe() {
    let path = unique_database_path();
    let first = SqliteBusinessIdAllocator::new(Connection::open(&path).expect("open first DB"));
    first
        .ensure_schema(&UserContext::default())
        .expect("explicitly install allocator table");

    let plan = DailySequenceBusinessIdProfile
        .plan(&BusinessIdGenerationRequest {
            definition: &definition(),
            tenant: "tenant-a",
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
    assert_eq!(restart_allocation.sequence, 51);
    assert_eq!(
        DailySequenceBusinessIdProfile
            .format(&plan, &restart_allocation)
            .expect("format persistent allocation")
            .0,
        "CO-20260920-00000051"
    );
    std::fs::remove_file(path).expect("remove fixture DB");
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
