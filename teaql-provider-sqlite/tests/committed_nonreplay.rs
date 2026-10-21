//! CMR-09: public-API positive controls and zero business effects on target apply.
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{
    DataType, DeleteCommand, EntityDescriptor, InsertCommand, PropertyDescriptor, RecoverCommand,
    UpdateCommand, Value,
};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, InboxOutcome, OutboxRecord};
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    BusinessClock, CheckResults, Checker, EntityDataServiceBehavior,
    EntityDataServiceBehaviorRegistry, EntityValues, HttpCall, HttpFailure, HttpRequestService,
    HttpResponse, InMemoryCheckerRegistry, InMemoryMetadataStore, InternalIdGenerator,
    ObjectLocation, RuntimeError, UserContext,
};

const SOURCE: &str = "nonreplay-source";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const ID: i64 = 9_007_199_254_740_993;
const NAMES: [&str; 8] = [
    "checker",
    "fix",
    "behavior",
    "clock",
    "id",
    "http",
    "llm",
    "notifications",
];

#[derive(Clone, Default)]
struct Effects(Arc<[AtomicUsize; 8]>);
impl Effects {
    fn hit(&self, index: usize) {
        self.0[index].fetch_add(1, Ordering::SeqCst);
    }
    fn counts(&self) -> [usize; 8] {
        std::array::from_fn(|i| self.0[i].load(Ordering::SeqCst))
    }
    fn clear(&self) {
        for counter in self.0.iter() {
            counter.store(0, Ordering::SeqCst);
        }
    }
    // Application extension points, not claims of built-in LLM/notification SDKs.
    fn llm(&self) {
        self.hit(6);
    }
    fn notify(&self) {
        self.hit(7);
    }
}
impl Checker for Effects {
    fn entity(&self) -> &str {
        "order"
    }
    fn check_and_fix(
        &self,
        _: &UserContext,
        values: &mut EntityValues,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.hit(0);
        self.hit(1);
        values.insert("name".into(), Value::Text("REPLAYED FIX".into()));
    }
}
impl InternalIdGenerator for Effects {
    fn generate_id(&self, _: &str) -> Result<u64, RuntimeError> {
        self.hit(4);
        Ok(1)
    }
}
impl BusinessClock for Effects {
    fn business_time(&self) -> DateTime<Utc> {
        self.hit(3);
        DateTime::from_timestamp(0, 0).unwrap()
    }
}
#[async_trait]
impl HttpRequestService for Effects {
    async fn execute(&self, _: &UserContext, _: &HttpCall) -> Result<HttpResponse, HttpFailure> {
        self.hit(5);
        HttpResponse::new(200, Vec::new(), 1)
    }
}
impl Effects {
    fn before_mutation(&self, context: &UserContext) -> Result<(), RuntimeError> {
        self.hit(2);
        let app = context
            .get_resource::<Effects>()
            .expect("application services installed");
        app.llm();
        app.notify();
        Ok(())
    }
}
impl EntityDataServiceBehavior for Effects {
    fn before_insert(
        &self,
        context: &UserContext,
        _: &mut InsertCommand,
    ) -> Result<(), RuntimeError> {
        self.before_mutation(context)
    }
    fn before_update(
        &self,
        context: &UserContext,
        _: &mut UpdateCommand,
    ) -> Result<(), RuntimeError> {
        self.before_mutation(context)
    }
    fn before_delete(
        &self,
        context: &UserContext,
        _: &mut DeleteCommand,
    ) -> Result<(), RuntimeError> {
        self.before_mutation(context)
    }
    fn before_recover(
        &self,
        context: &UserContext,
        _: &mut RecoverCommand,
    ) -> Result<(), RuntimeError> {
        self.before_mutation(context)
    }
}
struct Behaviors(Effects);
impl EntityDataServiceBehaviorRegistry for Behaviors {
    fn behavior(&self, entity: &str) -> Option<Arc<dyn EntityDataServiceBehavior>> {
        (entity == "order").then(|| Arc::new(self.0.clone()) as Arc<dyn EntityDataServiceBehavior>)
    }
}
fn record(
    sequence: u64,
    operation: &str,
    base: Option<i64>,
    version: i64,
    fields: Json,
) -> OutboxRecord {
    let change = format!("nonreplay-{sequence}");
    let body = json!({"protocol_version":1,"change_set_id":change,"source_runtime_id":SOURCE,
        "source_sequence":sequence.to_string(),"source_model_hash":MODEL,"source_transaction_id":format!("tx-{sequence}"),
        "committed_at":"2026-10-16T00:00:00Z","audit":{"actor_ref":"fixture:operator","comment":"Apply source facts",
        "purpose":"Verify no target business replay","trace_id":"nonreplay-trace"},"mutations":[{
        "entity_type":"order","entity_id":ID.to_string(),"operation":operation,"base_version":base.map(|v|v.to_string()),
        "committed_version":version.to_string(),"fields":fields,"trace_chain":[]}]}).to_string();
    OutboxRecord {
        source_runtime_id: SOURCE.into(),
        source_sequence: sequence,
        change_set_id: change,
        model_hash: MODEL.into(),
        payload_sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
        payload_json: body,
    }
}

#[test]
fn committed_target_does_not_replay_positively_wired_business_effects() {
    futures_executor::block_on(async {
        let entity = EntityDescriptor::new("order")
            .table_name("nonreplay_order")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).not_null())
            .property(PropertyDescriptor::new("occurred_at", DataType::I64).not_null());
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let mut context =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(entity));
        context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL).unwrap());
        context.use_sqlite_provider(executor.clone());
        context.ensure_schema().await.unwrap();
        let effects = Effects::default();
        context.set_checker_registry(InMemoryCheckerRegistry::new().with_checker(effects.clone()));
        context.set_entity_data_service_behavior_registry(Behaviors(effects.clone()));
        context.set_business_clock(effects.clone());
        context.set_internal_id_generator(effects.clone());
        context.set_http_request_service(Arc::new(effects.clone()));
        context.insert_resource(effects.clone());

        // Positive controls through the installed Context services. Do not merely
        // assert zeros on unwired mocks. Check/Fix deliberately changes a value.
        let mut values = EntityValues::from([
            ("name".into(), Value::Text("control".into())),
            ("occurred_at".into(), Value::I64(1)),
        ]);
        context.check_and_fix_values("order", &mut values).unwrap();
        assert_eq!(values["name"], Value::Text("REPLAYED FIX".into()));
        context
            .entity_data_service_behavior("order")
            .unwrap()
            .before_insert(&context, &mut InsertCommand::new("order"))
            .unwrap();
        assert_eq!(context.business_time().timestamp(), 0);
        assert_eq!(context.next_id("order").unwrap(), 1);
        context
            .http_requests()
            .unwrap()
            .execute(
                &context,
                &HttpCall::get(
                    "orders",
                    "orders/1",
                    "probe installed service",
                    "verify wiring",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            effects.counts(),
            [1; 8],
            "positive control counts: {NAMES:?}"
        );
        effects.clear();

        let facts = [
            record(
                1,
                "create",
                None,
                1,
                json!({"name":"SOURCE","occurred_at":"-315619199877"}),
            ),
            record(2, "update", Some(1), 2, json!({"name":"UPDATED SOURCE"})),
            record(3, "delete", Some(2), -3, json!({})),
            record(
                4,
                "recover",
                Some(-3),
                4,
                json!({"name":"RECOVERED SOURCE"}),
            ),
        ];
        for (i, event) in facts.iter().enumerate() {
            let checkpoint = (i + 1) as u64;
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, event)
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint }
            );
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, event)
                    .await
                    .unwrap(),
                InboxOutcome::Duplicate { checkpoint }
            );
            assert_eq!(effects.counts(), [0; 8], "target effects: {NAMES:?}");
        }
        let connection = executor.connection();
        let db = connection.lock().unwrap();
        let row: (i64, i64, String, i64) = db
            .query_row(
                "SELECT id,version,name,occurred_at FROM nonreplay_order",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(row, (ID, 4, "RECOVERED SOURCE".into(), -315619199877));
        for table in ["teaql_event_inbox", "teaql_event_applied_payload"] {
            assert_eq!(
                db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                4
            );
        }
        assert_eq!(
            db.query_row(
                "SELECT source_sequence FROM teaql_event_checkpoint WHERE source_runtime_id=?1",
                [SOURCE],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            4
        );
        println!(
            "CMR_NONREPLAY_PASS language=rust controls=8 business_calls=0 operations=4 duplicates=4"
        );
    });
}
