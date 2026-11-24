//! CMR-13 public authenticated receiver: positive controls and audited rejection.
use rusqlite::Connection;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, InboxOutcome, OutboxRecord};
use teaql_provider_sqlite::committed_security::{SignedDelivery, SqliteCommittedReceiver};
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, UserContext};
use teaql_sql::{SqlTransaction, SqlTransactionTransport};

const SOURCE: &str = "receiver-source";
const TARGET: &str = "receiver-target";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const CANARY: &str = "BUSINESS_SECRET_must_not_appear_in_audit";
fn digest(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}
fn event(sequence: u64, change: &str, source: &str, update: bool) -> OutboxRecord {
    let body=json!({"protocol_version":1,"change_set_id":change,"source_runtime_id":source,
      "source_sequence":sequence.to_string(),"source_model_hash":MODEL,"source_transaction_id":"tx",
      "committed_at":"2026-10-22T00:00:00Z","audit":{"comment":"receive school","purpose":"verify signed delivery"},
      "mutations":[{"entity_type":"school","entity_id":"7","operation":if update{"update"}else{"create"},
      "base_version":if update{Value::String("1".into())}else{Value::Null},"committed_version":if update{"2"}else{"1"},
      "fields":{"name":if update{"updated"}else{CANARY}},"trace_chain":[]}]}).to_string();
    OutboxRecord {
        source_runtime_id: source.into(),
        source_sequence: sequence,
        change_set_id: change.into(),
        model_hash: MODEL.into(),
        payload_sha256: digest(&body),
        payload_json: body,
    }
}
fn sign(record: OutboxRecord) -> SignedDelivery {
    SqliteCommittedReceiver::sign("current", TARGET, KEY, record).unwrap()
}
struct Fixture {
    executor: SqliteMutationExecutor,
    context: UserContext,
    receiver: SqliteCommittedReceiver,
    owner: tempfile::TempDir,
}
async fn fixture() -> Fixture {
    let owner = tempfile::tempdir().unwrap();
    let executor = SqliteMutationExecutor::from_connection(
        Connection::open(owner.path().join("target.sqlite")).unwrap(),
    );
    let metadata = InMemoryMetadataStore::new().with_entity(
        EntityDescriptor::new("school")
            .table_name("school_data")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).not_null()),
    );
    let mut context = UserContext::new().with_metadata(metadata);
    context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL).unwrap());
    context.use_sqlite_provider(executor.clone());
    context.ensure_schema().await.unwrap();
    let receiver = SqliteCommittedReceiver::new(
        executor.clone(),
        SOURCE,
        TARGET,
        MODEL,
        HashMap::from([("current".to_string(), KEY.to_vec())]),
    )
    .unwrap();
    receiver.ensure_schema(&context).unwrap();
    Fixture {
        executor,
        context,
        receiver,
        owner,
    }
}
fn count(f: &Fixture, table: &str) -> i64 {
    f.executor
        .connection()
        .lock()
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}
fn unchanged(f: &Fixture, rows: i64, inbox: i64, audits: i64) {
    assert_eq!(count(f, "school_data"), rows);
    assert_eq!(count(f, "teaql_event_inbox"), inbox);
    assert_eq!(count(f, "teaql_event_rejection"), audits);
}
async fn rejected(f: &Fixture, code: &str, delivery: &SignedDelivery) {
    let err = f.receiver.receive(&f.context, delivery).await.unwrap_err();
    assert_eq!(err.code(), code);
    assert!(!format!("{err:?} {err}").contains(CANARY));
}

#[test]
fn valid_apply_update_and_retained_duplicate() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let a = sign(event(1, "a", SOURCE, false));
        let b = sign(event(2, "b", SOURCE, true));
        assert!(matches!(
            f.receiver.receive(&f.context, &a).await.unwrap(),
            InboxOutcome::Applied { checkpoint: 1 }
        ));
        assert!(matches!(
            f.receiver.receive(&f.context, &b).await.unwrap(),
            InboxOutcome::Applied { checkpoint: 2 }
        ));
        assert!(matches!(
            f.receiver.receive(&f.context, &a).await.unwrap(),
            InboxOutcome::Duplicate { checkpoint: 1 }
        ));
        assert!(matches!(
            f.receiver.receive(&f.context, &b).await.unwrap(),
            InboxOutcome::Duplicate { checkpoint: 2 }
        ));
        unchanged(&f, 1, 2, 0);
        assert_eq!(
            f.executor
                .connection()
                .lock()
                .unwrap()
                .query_row("SELECT name FROM school_data", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            "updated"
        );
        println!("CMR13_SOURCE_PASS language=rust valid=2 duplicates=2");
    });
}
#[test]
fn changed_payload_even_with_recomputed_digest_is_not_authentic() {
    futures_executor::block_on(async {
        let f = fixture().await;
        for recompute in [false, true] {
            let mut d = sign(event(1, "a", SOURCE, false));
            d.event.payload_json = d.event.payload_json.replace(CANARY, "attacker-value");
            if recompute {
                d.event.payload_sha256 = digest(&d.event.payload_json);
            }
            rejected(&f, "INVALID_SIGNATURE", &d).await;
        }
        unchanged(&f, 0, 0, 2);
    });
}
#[test]
fn routing_and_key_changes_are_rejected_and_audited() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let d = sign(event(1, "a", SOURCE, false));
        rejected(
            &f,
            "UNAUTHORIZED_SOURCE",
            &sign(event(1, "a", "other-source", false)),
        )
        .await;
        let mut x = d.clone();
        x.target_id = "other-target".into();
        rejected(&f, "UNAUTHORIZED_TARGET", &x).await;
        let mut x = d.clone();
        x.key_id = "retired".into();
        rejected(&f, "UNKNOWN_KEY", &x).await;
        let mut x = d.clone();
        x.signature = vec![0; 32];
        rejected(&f, "INVALID_SIGNATURE", &x).await;
        let mut x = d.clone();
        x.signature.clear();
        rejected(&f, "INVALID_ENVELOPE", &x).await;
        let mut x = d.clone();
        x.event.source_sequence = 2;
        rejected(&f, "INVALID_SIGNATURE", &x).await;
        let mut x = d.clone();
        x.event.change_set_id = "b".into();
        rejected(&f, "INVALID_SIGNATURE", &x).await;
        let mut x = d.clone();
        x.event.model_hash = format!("sha256:{}", "1".repeat(64));
        rejected(&f, "MODEL_MISMATCH", &x).await;
        unchanged(&f, 0, 0, 8);
    });
}
#[test]
fn signed_replay_gap_and_conflicting_duplicate_preserve_state() {
    futures_executor::block_on(async {
        let f = fixture().await;
        f.receiver
            .receive(&f.context, &sign(event(1, "a", SOURCE, false)))
            .await
            .unwrap();
        f.receiver
            .receive(&f.context, &sign(event(2, "b", SOURCE, true)))
            .await
            .unwrap();
        rejected(
            &f,
            "SEQUENCE_GAP",
            &sign(event(1, "unseen-old", SOURCE, false)),
        )
        .await;
        rejected(&f, "SEQUENCE_GAP", &sign(event(4, "gap", SOURCE, true))).await;
        let mut e = event(1, "a", SOURCE, false);
        e.payload_json = e.payload_json.replace(CANARY, "changed");
        e.payload_sha256 = digest(&e.payload_json);
        rejected(&f, "DUPLICATE_CONFLICT", &sign(e)).await;
        unchanged(&f, 1, 2, 3);
        assert_eq!(
            f.executor
                .connection()
                .lock()
                .unwrap()
                .query_row(
                    "SELECT source_sequence FROM teaql_event_checkpoint",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            2
        );
    });
}
#[test]
fn audit_survives_reopen_and_never_retains_payload_or_key() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let mut d = sign(event(1, CANARY, SOURCE, false));
        d.signature = vec![0; 32];
        rejected(&f, "INVALID_SIGNATURE", &d).await;
        let reopened = Connection::open(f.owner.path().join("target.sqlite")).unwrap();
        let row:String=reopened.query_row("SELECT reason_code||source_fingerprint||change_fingerprint||target_fingerprint||payload_fingerprint FROM teaql_event_rejection",[],|r|r.get(0)).unwrap();
        assert!(!row.contains(CANARY));
        assert!(!row.contains(std::str::from_utf8(KEY).unwrap()));
        assert!(row.starts_with("INVALID_SIGNATURE"));
        f.executor.connection().lock().unwrap().execute_batch(&format!("CREATE TRIGGER reject_audit BEFORE INSERT ON teaql_event_rejection BEGIN SELECT RAISE(ABORT,'{CANARY}'); END")).unwrap();
        rejected(&f, "AUDIT_UNAVAILABLE", &d).await;
        unchanged(&f, 0, 0, 1);
    });
}
#[test]
fn rotation_and_debug_redaction() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let receiver = SqliteCommittedReceiver::new(
            f.executor.clone(),
            SOURCE,
            TARGET,
            MODEL,
            HashMap::from([
                ("current".to_string(), KEY.to_vec()),
                ("previous".to_string(), KEY.to_vec()),
            ]),
        )
        .unwrap();
        let d =
            SqliteCommittedReceiver::sign("previous", TARGET, KEY, event(1, "a", SOURCE, false))
                .unwrap();
        receiver.receive(&f.context, &d).await.unwrap();
        rejected(&f, "UNKNOWN_KEY", &d).await;
        assert!(!format!("{d:?} {receiver:?}").contains(CANARY));
        assert!(!format!("{receiver:?}").contains(std::str::from_utf8(KEY).unwrap()));
        unchanged(&f, 1, 1, 1);
    });
}
#[test]
fn bounds_configuration_and_ambient_transaction_fail_closed() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let d = sign(event(1, "a", SOURCE, false));
        let mut x = d.clone();
        x.event.source_sequence = 0;
        rejected(&f, "INVALID_ENVELOPE", &x).await;
        let mut x = d.clone();
        x.event.source_sequence = u64::MAX;
        rejected(&f, "INVALID_ENVELOPE", &x).await;
        let mut x = d.clone();
        x.event.payload_json = "x".repeat(1024 * 1024 + 1);
        rejected(&f, "INVALID_ENVELOPE", &x).await;
        let mut x = d.clone();
        x.key_id = "x".repeat(257);
        rejected(&f, "INVALID_ENVELOPE", &x).await;
        unchanged(&f, 0, 0, 4);
        let tx = f.executor.begin_sql().await.unwrap();
        rejected(&f, "AMBIENT_TRANSACTION", &d).await;
        tx.rollback_sql().await.unwrap();
        unchanged(&f, 0, 0, 4);
        assert!(
            SqliteCommittedReceiver::new(
                f.executor.clone(),
                SOURCE,
                TARGET,
                MODEL,
                HashMap::from([("weak".to_string(), vec![0; 8])])
            )
            .is_err()
        );
    });
}
#[test]
fn independent_cross_language_golden_tag() {
    let e = OutboxRecord {
        source_runtime_id: "source".into(),
        source_sequence: 42,
        change_set_id: "change".into(),
        model_hash: MODEL.into(),
        payload_sha256: "a".repeat(64),
        payload_json: "{\"x\":\"雪\"}".into(),
    };
    let d = SqliteCommittedReceiver::sign("key", "target", KEY, e).unwrap();
    let hex = d
        .signature
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect::<String>();
    assert_eq!(
        hex,
        "f1405877cd5999425693e559a147aa27294240456f88e307208b7162ff0bd8ac"
    );
}
