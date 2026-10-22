//! CMR-13 authenticated snapshots: separate type domain and durable safe rejection.
use rusqlite::Connection;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_provider_sqlite::committed_events::snapshot::SnapshotImportMode;
use teaql_provider_sqlite::committed_events::snapshot::SnapshotRecord;
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, OutboxRecord};
use teaql_provider_sqlite::committed_security::{
    SignedDelivery, SignedSnapshotDelivery, SqliteCommittedReceiver,
};
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, UserContext};
use teaql_sql::{SqlTransaction, SqlTransactionTransport};
const SOURCE: &str = "snapshot-source";
const TARGET: &str = "snapshot-target";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const CANARY: &str = "SNAPSHOT_SECRET_never_in_rejection_audit";
#[test]
fn trusted_startup_replacement_cannot_reset_active_stream() {
    futures_executor::block_on(async {
        let f = fixture().await;
        // Existing pre-stream fixture, not production seed SQL or a wire option.
        f.executor
            .connection()
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO school_data(id,version,name) VALUES(99,1,'pre-stream fixture')",
                [],
            )
            .unwrap();
        let original = signed(snapshot(0, "baseline"));
        reject(&f, "SNAPSHOT_CONFLICT", &original).await;
        assert_eq!(
            f.executor
                .connection()
                .lock()
                .unwrap()
                .query_row("SELECT id FROM school_data", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            99
        );
        let configured = SqliteCommittedReceiver::new(
            f.executor.clone(),
            SOURCE,
            TARGET,
            MODEL,
            HashMap::from([("current".into(), KEY.to_vec())]),
        )
        .unwrap()
        .with_snapshot_import_mode(SnapshotImportMode::ReplaceUninitialized);
        assert!(
            !configured
                .receive_snapshot(&f.context, &original)
                .await
                .unwrap()
                .duplicate
        );
        configured
            .receive(
                &f.context,
                &SqliteCommittedReceiver::sign("current", TARGET, KEY, tail(1)).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            configured
                .receive_snapshot(&f.context, &signed(snapshot(0, "different")))
                .await
                .unwrap_err()
                .code(),
            "SNAPSHOT_CONFLICT"
        );
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
        assert_eq!(count(&f, "school_data"), 1);
        assert_eq!(count(&f, "teaql_event_inbox"), 1);
        assert_eq!(count(&f, "teaql_event_rejection"), 2);
        println!(
            "CMR13_STARTUP_POLICY_PASS language=rust default_reject=1 explicit_replace=1 active_stream_fenced=1"
        );
    });
}
fn digest(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}
fn snapshot(n: u64, id: &str) -> SnapshotRecord {
    let body=json!({"snapshot_version":1,"snapshot_id":id,"source_runtime_id":SOURCE,"source_sequence":n.to_string(),"source_model_hash":MODEL,
      "audit":{"comment":"load baseline","purpose":"verify authenticated recovery"},"entity_types":["school"],
      "rows":[{"entity_type":"school","entity_id":"7","version":"1","fields":{"name":CANARY}}]}).to_string();
    SnapshotRecord {
        source_runtime_id: SOURCE.into(),
        source_sequence: n,
        snapshot_id: id.into(),
        model_hash: MODEL.into(),
        payload_sha256: digest(&body),
        payload_json: body,
    }
}
fn tail(n: u64) -> OutboxRecord {
    let body=json!({"protocol_version":1,"change_set_id":"tail","source_runtime_id":SOURCE,"source_sequence":n.to_string(),"source_model_hash":MODEL,"source_transaction_id":"tx","committed_at":"2026-10-22T00:00:00Z",
      "audit":{"comment":"apply tail","purpose":"verify recovery"},"mutations":[{"entity_type":"school","entity_id":"7","operation":"update","base_version":"1","committed_version":"2","fields":{"name":"updated"},"trace_chain":[]}]}).to_string();
    OutboxRecord {
        source_runtime_id: SOURCE.into(),
        source_sequence: n,
        change_set_id: "tail".into(),
        model_hash: MODEL.into(),
        payload_sha256: digest(&body),
        payload_json: body,
    }
}
fn signed(s: SnapshotRecord) -> SignedSnapshotDelivery {
    SqliteCommittedReceiver::sign_snapshot("current", TARGET, KEY, s).unwrap()
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
        HashMap::from([("current".into(), KEY.to_vec())]),
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
fn count(f: &Fixture, t: &str) -> i64 {
    f.executor
        .connection()
        .lock()
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))
        .unwrap()
}
fn empty(f: &Fixture, audits: i64) {
    for t in [
        "school_data",
        "teaql_event_snapshot",
        "teaql_event_checkpoint",
        "teaql_event_inbox",
    ] {
        assert_eq!(count(f, t), 0, "{t}");
    }
    assert_eq!(count(f, "teaql_event_rejection"), audits);
}
async fn reject(f: &Fixture, code: &str, d: &SignedSnapshotDelivery) {
    let e = f
        .receiver
        .receive_snapshot(&f.context, d)
        .await
        .unwrap_err();
    assert_eq!(e.code(), code);
    assert!(!format!("{e:?} {e}").contains(CANARY));
}

#[test]
fn zero_nonzero_baseline_tail_and_duplicate_never_rewind() {
    futures_executor::block_on(async {
        for n in [0, 5] {
            let f = fixture().await;
            let d = signed(snapshot(n, "baseline"));
            let ack = f.receiver.receive_snapshot(&f.context, &d).await.unwrap();
            assert!(!ack.duplicate);
            assert_eq!(ack.source_sequence, n);
            assert_eq!(ack.target_id, TARGET);
            assert_eq!(ack.payload_sha256, d.snapshot.payload_sha256);
            let event = SqliteCommittedReceiver::sign("current", TARGET, KEY, tail(n + 1)).unwrap();
            f.receiver.receive(&f.context, &event).await.unwrap();
            let duplicate = f.receiver.receive_snapshot(&f.context, &d).await.unwrap();
            assert!(duplicate.duplicate);
            assert_eq!(duplicate.source_sequence, n);
            let conn = f.executor.connection();
            let db = conn.lock().unwrap();
            assert_eq!(
                db.query_row("SELECT name FROM school_data", [], |r| r
                    .get::<_, String>(0))
                    .unwrap(),
                "updated"
            );
            assert_eq!(
                db.query_row(
                    "SELECT source_sequence FROM teaql_event_checkpoint",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                (n + 1) as i64
            );
            drop(db);
            assert_eq!(count(&f, "teaql_event_rejection"), 0);
            println!(
                "CMR13_SNAPSHOT_SOURCE_PASS language=rust baseline={n} tail={} duplicate_no_rewind=1",
                n + 1
            );
        }
    });
}
#[test]
fn identical_fields_cannot_cross_event_snapshot_signature_domains() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let s = snapshot(1, "same");
        let e = OutboxRecord {
            source_runtime_id: s.source_runtime_id.clone(),
            source_sequence: 1,
            change_set_id: s.snapshot_id.clone(),
            model_hash: s.model_hash.clone(),
            payload_sha256: s.payload_sha256.clone(),
            payload_json: s.payload_json.clone(),
        };
        let event = SqliteCommittedReceiver::sign("current", TARGET, KEY, e.clone()).unwrap();
        let snapshot = signed(s);
        let mut wrong = snapshot.clone();
        wrong.signature = event.signature;
        reject(&f, "INVALID_SIGNATURE", &wrong).await;
        let event = SignedDelivery {
            event: e,
            key_id: "current".into(),
            target_id: TARGET.into(),
            signature: snapshot.signature,
        };
        assert_eq!(
            f.receiver
                .receive(&f.context, &event)
                .await
                .unwrap_err()
                .code(),
            "INVALID_SIGNATURE"
        );
        empty(&f, 2);
    });
}
#[test]
fn snapshot_tamper_authority_and_every_identity_reject() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let d = signed(snapshot(0, "baseline"));
        for recompute in [false, true] {
            let mut x = d.clone();
            x.snapshot.payload_json = x.snapshot.payload_json.replace(CANARY, "attacker");
            if recompute {
                x.snapshot.payload_sha256 = digest(&x.snapshot.payload_json);
            }
            reject(&f, "INVALID_SIGNATURE", &x).await;
        }
        let mut x = d.clone();
        x.snapshot.source_sequence = 1;
        reject(&f, "INVALID_SIGNATURE", &x).await;
        let mut x = d.clone();
        x.snapshot.snapshot_id = "other".into();
        reject(&f, "INVALID_SIGNATURE", &x).await;
        let mut x = d.clone();
        x.snapshot.source_runtime_id = "other".into();
        reject(&f, "UNAUTHORIZED_SOURCE", &x).await;
        let mut x = d.clone();
        x.target_id = "other".into();
        reject(&f, "UNAUTHORIZED_TARGET", &x).await;
        let mut x = d.clone();
        x.key_id = "retired".into();
        reject(&f, "UNKNOWN_KEY", &x).await;
        let mut x = d.clone();
        x.snapshot.model_hash = format!("sha256:{}", "1".repeat(64));
        reject(&f, "MODEL_MISMATCH", &x).await;
        empty(&f, 8);
    });
}
#[test]
fn signed_malformed_snapshot_and_conflicting_baseline_are_audited() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let mut s = snapshot(0, "baseline");
        s.payload_json = s.payload_json.replace("\"school\"", "\"unknown\"");
        s.payload_sha256 = digest(&s.payload_json);
        reject(&f, "INVALID_SNAPSHOT", &signed(s)).await;
        empty(&f, 1);
        f.receiver
            .receive_snapshot(&f.context, &signed(snapshot(0, "baseline")))
            .await
            .unwrap();
        reject(&f, "SNAPSHOT_CONFLICT", &signed(snapshot(1, "different"))).await;
        assert_eq!(count(&f, "school_data"), 1);
        assert_eq!(count(&f, "teaql_event_rejection"), 2);
        assert_eq!(
            f.executor
                .connection()
                .lock()
                .unwrap()
                .query_row("SELECT name FROM school_data", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            CANARY
        );
    });
}
#[test]
fn snapshot_audit_reopens_and_audit_failure_never_acknowledges() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let mut d = signed(snapshot(0, CANARY));
        d.signature = vec![0; 32];
        reject(&f, "INVALID_SIGNATURE", &d).await;
        empty(&f, 1);
        let reopened = Connection::open(f.owner.path().join("target.sqlite")).unwrap();
        let text:String=reopened.query_row("SELECT reason_code||source_fingerprint||change_fingerprint||target_fingerprint||payload_fingerprint FROM teaql_event_rejection",[],|r|r.get(0)).unwrap();
        assert!(!text.contains(CANARY));
        assert!(!text.contains(std::str::from_utf8(KEY).unwrap()));
        f.executor.connection().lock().unwrap().execute_batch(&format!("CREATE TRIGGER reject_audit BEFORE INSERT ON teaql_event_rejection BEGIN SELECT RAISE(ABORT,'{CANARY}'); END")).unwrap();
        reject(&f, "AUDIT_UNAVAILABLE", &d).await;
        empty(&f, 1);
    });
}
#[test]
fn receiver_limits_and_ambient_transactions_fail_closed() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let d = signed(snapshot(0, "baseline"));
        let strict = SqliteCommittedReceiver::new(
            f.executor.clone(),
            SOURCE,
            TARGET,
            MODEL,
            HashMap::from([("current".into(), KEY.to_vec())]),
        )
        .unwrap()
        .with_snapshot_limits(1, 16)
        .unwrap();
        assert_eq!(
            strict
                .receive_snapshot(&f.context, &d)
                .await
                .unwrap_err()
                .code(),
            "INVALID_ENVELOPE"
        );
        empty(&f, 1);
        let mut x = d.clone();
        x.snapshot.source_sequence = u64::MAX;
        reject(&f, "INVALID_ENVELOPE", &x).await;
        let mut x = d.clone();
        x.signature.clear();
        reject(&f, "INVALID_ENVELOPE", &x).await;
        empty(&f, 3);
        let tx = f.executor.begin_sql().await.unwrap();
        reject(&f, "AMBIENT_TRANSACTION", &d).await;
        tx.rollback_sql().await.unwrap();
        empty(&f, 3);
    });
}
#[test]
fn known_key_alias_destination_and_length_framing_are_bound() {
    futures_executor::block_on(async {
        let f = fixture().await;
        let d = signed(snapshot(0, "baseline"));
        let aliases = SqliteCommittedReceiver::new(
            f.executor.clone(),
            SOURCE,
            TARGET,
            MODEL,
            HashMap::from([
                ("current".into(), KEY.to_vec()),
                ("alias".into(), KEY.to_vec()),
            ]),
        )
        .unwrap();
        let mut x = d.clone();
        x.key_id = "alias".into();
        assert_eq!(
            aliases
                .receive_snapshot(&f.context, &x)
                .await
                .unwrap_err()
                .code(),
            "INVALID_SIGNATURE"
        );
        let destination = SqliteCommittedReceiver::new(
            f.executor.clone(),
            SOURCE,
            "other",
            MODEL,
            HashMap::from([("current".into(), KEY.to_vec())]),
        )
        .unwrap();
        let mut x = d.clone();
        x.target_id = "other".into();
        assert_eq!(
            destination
                .receive_snapshot(&f.context, &x)
                .await
                .unwrap_err()
                .code(),
            "INVALID_SIGNATURE"
        );
        assert_ne!(
            SqliteCommittedReceiver::sign_snapshot("a", "bc", KEY, d.snapshot.clone())
                .unwrap()
                .signature,
            SqliteCommittedReceiver::sign_snapshot("ab", "c", KEY, d.snapshot)
                .unwrap()
                .signature
        );
        empty(&f, 2);
    });
}
#[test]
fn independent_snapshot_golden_and_debug_redaction() {
    let s = SnapshotRecord {
        source_runtime_id: "source".into(),
        source_sequence: 0,
        snapshot_id: "baseline".into(),
        model_hash: MODEL.into(),
        payload_sha256: "a".repeat(64),
        payload_json: "{\"x\":\"雪\"}".into(),
    };
    let d = SqliteCommittedReceiver::sign_snapshot("key", "target", KEY, s).unwrap();
    let hex = d
        .signature
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    assert_eq!(
        hex,
        "21dd4226dec5f6513b30b82b9bc4b8a340421a5663e4635658c54291226fe8d9"
    );
    assert!(!format!("{d:?}").contains(&d.snapshot.payload_json));
}
