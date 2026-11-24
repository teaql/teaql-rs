//! CMR-05/06: mixed create/update/delete, failed final create, exact retry.
//! Uses only public APIs so the same test can consume Registry packages.
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor};
use teaql_provider_sqlite::committed_events::{
    CommittedEventError, CommittedEventTarget, InboxOutcome, OutboxRecord,
};
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, UserContext};

const SOURCE: &str = "sqlite-order-source";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const ORDER: i64 = 9_007_199_254_740_993;
const OLD: i64 = ORDER + 1;
const NEW: i64 = ORDER + 2;

async fn fixture() -> (SqliteMutationExecutor, UserContext) {
    let executor = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
    let order = EntityDescriptor::new("order")
        .table_name("mixed_order")
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("order_number", DataType::Text).not_null())
        .property(PropertyDescriptor::new("status", DataType::Text).not_null());
    let item = EntityDescriptor::new("order_item")
        .table_name("mixed_item")
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(
            PropertyDescriptor::new("order", DataType::U64)
                .column_name("order_id")
                .not_null(),
        )
        .property(PropertyDescriptor::new("quantity", DataType::I64).not_null())
        .relation(RelationDescriptor::new("order_ref", "order").local_key("order"));
    let metadata = InMemoryMetadataStore::new()
        .with_entity(order)
        .with_entity(item);
    let mut context = UserContext::new().with_metadata(metadata);
    context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL).unwrap());
    context.use_sqlite_provider(executor.clone());
    context.ensure_schema().await.unwrap();
    (executor, context)
}

fn record(sequence: u64, mutations: Value) -> OutboxRecord {
    let change = format!("mixed-{sequence}");
    let body = json!({"protocol_version":1,"change_set_id":change,"source_runtime_id":SOURCE,
        "source_sequence":sequence.to_string(),"source_model_hash":MODEL,
        "source_transaction_id":format!("tx-{sequence}"),"committed_at":"2026-10-16T00:00:00Z",
        "audit":{"actor_ref":"fixture:operator","comment":"Apply mixed target graph",
        "purpose":"Verify atomic target application","trace_id":"mixed-fixture-trace"},"mutations":mutations}).to_string();
    OutboxRecord {
        source_runtime_id: SOURCE.into(),
        source_sequence: sequence,
        change_set_id: change,
        model_hash: MODEL.into(),
        payload_sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
        payload_json: body,
    }
}

fn item_create(id: i64, quantity: i64) -> Value {
    json!({"entity_type":"order_item","entity_id":id.to_string(),"operation":"create",
        "base_version":null,"committed_version":"1","trace_chain":[],
        "fields":{"order":{"entity_type":"order","entity_id":ORDER.to_string()},"quantity":quantity.to_string()}})
}

#[test]
fn mixed_create_update_delete_rolls_back_then_retries_and_deduplicates() {
    futures_executor::block_on(async {
        let (executor, context) = fixture().await;
        let first = record(
            1,
            json!([
            {"entity_type":"order","entity_id":ORDER.to_string(),"operation":"create",
             "base_version":null,"committed_version":"1","trace_chain":[],
             "fields":{"order_number":"ORD-0001","status":"DRAFT"}}, item_create(OLD, 2)]),
        );
        assert_eq!(
            executor
                .apply_model_checked_committed_set(&context, &first)
                .await
                .unwrap(),
            InboxOutcome::Applied { checkpoint: 1 }
        );
        let mixed = record(
            2,
            json!([
            {"entity_type":"order","entity_id":ORDER.to_string(),"operation":"update",
             "base_version":"1","committed_version":"2","trace_chain":[],"fields":{"status":"SUBMITTED"}},
            {"entity_type":"order_item","entity_id":OLD.to_string(),"operation":"delete",
             "base_version":"1","committed_version":"-2","trace_chain":[],"fields":{}}, item_create(NEW, 3)]),
        );
        executor
            .connection()
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_mixed_create BEFORE INSERT ON mixed_item
             WHEN NEW.id = 9007199254740995
             AND (SELECT version FROM mixed_order WHERE id = 9007199254740993) = 2
             AND (SELECT version FROM mixed_item WHERE id = 9007199254740994) = -2
             BEGIN SELECT RAISE(ABORT, 'fixture rejects final create'); END",
            )
            .unwrap();
        // The trigger can fire only after the preceding update and delete.
        assert!(matches!(
            executor
                .apply_model_checked_committed_set(&context, &mixed)
                .await,
            Err(CommittedEventError::Storage(_))
        ));
        {
            let connection = executor.connection();
            let db = connection.lock().unwrap();
            let order: (i64, String) = db
                .query_row(
                    "SELECT version,status FROM mixed_order WHERE id=?1",
                    params![ORDER],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(order, (1, "DRAFT".into()));
            assert_eq!(
                db.query_row(
                    "SELECT version FROM mixed_item WHERE id=?1",
                    params![OLD],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                1
            );
            assert_eq!(
                db.query_row(
                    "SELECT COUNT(*) FROM mixed_item WHERE id=?1",
                    params![NEW],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                0
            );
            for table in ["teaql_event_inbox", "teaql_event_applied_payload"] {
                assert_eq!(
                    db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                        .get::<_, i64>(0))
                        .unwrap(),
                    1
                );
            }
            assert_eq!(
                db.query_row(
                    "SELECT source_sequence FROM teaql_event_checkpoint WHERE source_runtime_id=?1",
                    params![SOURCE],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                1
            );
            db.execute_batch("DROP TRIGGER reject_mixed_create")
                .unwrap(); // disarm only; keep data
        }
        assert_eq!(
            executor
                .apply_model_checked_committed_set(&context, &mixed)
                .await
                .unwrap(),
            InboxOutcome::Applied { checkpoint: 2 }
        );
        assert_eq!(
            executor
                .apply_model_checked_committed_set(&context, &mixed)
                .await
                .unwrap(),
            InboxOutcome::Duplicate { checkpoint: 2 }
        );
        {
            let connection = executor.connection();
            let db = connection.lock().unwrap();
            let order: (i64, String) = db
                .query_row(
                    "SELECT version,status FROM mixed_order WHERE id=?1",
                    params![ORDER],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(order, (2, "SUBMITTED".into()));
            assert_eq!(
                db.query_row(
                    "SELECT version FROM mixed_item WHERE id=?1",
                    params![OLD],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                -2
            );
            let added: (i64, i64, i64) = db
                .query_row(
                    "SELECT version,order_id,quantity FROM mixed_item WHERE id=?1",
                    params![NEW],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            assert_eq!(added, (1, ORDER, 3));
            assert_eq!(
                db.query_row("SELECT COUNT(*) FROM mixed_order", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                1
            );
            for table in [
                "mixed_item",
                "teaql_event_inbox",
                "teaql_event_applied_payload",
            ] {
                assert_eq!(
                    db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                        .get::<_, i64>(0))
                        .unwrap(),
                    2
                );
            }
            assert_eq!(
                db.query_row(
                    "SELECT source_sequence FROM teaql_event_checkpoint WHERE source_runtime_id=?1",
                    params![SOURCE],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                2
            );
            let payload: String = db
                .query_row(
                    "SELECT payload_json FROM teaql_event_applied_payload WHERE source_sequence=2",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&payload).unwrap()["mutations"]
                    .as_array()
                    .unwrap()
                    .len(),
                3
            );
        }
        println!("MIXED_TARGET_ATOMIC_PASS language=rust mixed=3 rollback=1 retry=1 duplicate=1");
    });
}
