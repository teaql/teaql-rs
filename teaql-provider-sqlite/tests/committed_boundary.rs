//! Signed canonical-frame checks. Infrastructure unit fixture, not application saves.
use futures_executor::block_on;
use rusqlite::Connection;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, InboxOutcome, OutboxRecord};
use teaql_provider_sqlite::committed_security::SqliteCommittedReceiver;
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, UserContext};
const SOURCE: &str = "boundary-source";
const TARGET: &str = "boundary-target";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const SQL: &str = "INSERT INTO school_data(id,version,name) VALUES(99,1,'SQL-CANARY')";
fn body() -> Value {
    json!({"protocol_version":1,"change_set_id":"fact","source_runtime_id":SOURCE,"source_sequence":"1","source_model_hash":MODEL,"source_transaction_id":"tx","committed_at":"2026-10-22T00:00:00Z","audit":{"comment":"receive school","purpose":"check protocol boundary"},"mutations":[{"entity_type":"school","entity_id":"7","operation":"create","base_version":null,"committed_version":"1","fields":{"name":"original"},"trace_chain":[]}]})
}
fn snapshot_body() -> Value {
    json!({"snapshot_version":1,"snapshot_id":"baseline","source_runtime_id":SOURCE,"source_sequence":"0","source_model_hash":MODEL,"audit":{"comment":"receive baseline","purpose":"check snapshot boundary"},"entity_types":["school"],"rows":[{"entity_type":"school","entity_id":"7","version":"1","fields":{"name":"original"}}]})
}
struct Fixture {
    executor: SqliteMutationExecutor,
    context: UserContext,
    receiver: SqliteCommittedReceiver,
    _owner: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
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
        Self {
            executor,
            context,
            receiver,
            _owner: owner,
        }
    }
    fn count(&self, table: &str) -> i64 {
        self.executor
            .connection()
            .lock()
            .unwrap()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
    async fn receive(
        &self,
        body: Value,
    ) -> Result<InboxOutcome, teaql_provider_sqlite::committed_security::ReceiverError> {
        let text = body.to_string();
        let e = OutboxRecord {
            source_runtime_id: SOURCE.into(),
            source_sequence: 1,
            change_set_id: "fact".into(),
            model_hash: MODEL.into(),
            payload_sha256: format!("{:x}", Sha256::digest(text.as_bytes())),
            payload_json: text,
        };
        self.receiver
            .receive(
                &self.context,
                &SqliteCommittedReceiver::sign("current", TARGET, KEY, e).unwrap(),
            )
            .await
    }
    async fn reject(&self, b: Value) {
        let e = self
            .receive(b)
            .await
            .expect_err("noncanonical controls must fail");
        assert_eq!(e.code(), "INVALID_ENVELOPE");
        assert!(!e.to_string().contains(SQL));
        assert_eq!(
            (
                self.count("school_data"),
                self.count("teaql_event_inbox"),
                self.count("teaql_event_checkpoint"),
                self.count("teaql_event_rejection")
            ),
            (0, 0, 0, 1)
        );
        assert!(matches!(
            self.receive(body()).await.unwrap(),
            InboxOutcome::Applied { checkpoint: 1 }
        ));
        assert_eq!(self.count("school_data"), 1);
    }
    async fn receive_snapshot(
        &self,
        body: Value,
    ) -> Result<(), teaql_provider_sqlite::committed_security::ReceiverError> {
        let text = body.to_string();
        let record = teaql_provider_sqlite::committed_events::snapshot::SnapshotRecord {
            source_runtime_id: SOURCE.into(),
            source_sequence: 0,
            snapshot_id: "baseline".into(),
            model_hash: MODEL.into(),
            payload_sha256: format!("{:x}", Sha256::digest(text.as_bytes())),
            payload_json: text,
        };
        self.receiver
            .receive_snapshot(
                &self.context,
                &SqliteCommittedReceiver::sign_snapshot("current", TARGET, KEY, record).unwrap(),
            )
            .await
            .map(|_| ())
    }
    async fn reject_snapshot(&self, body: Value) {
        let e = self
            .receive_snapshot(body)
            .await
            .expect_err("noncanonical snapshot controls must fail");
        assert_eq!(e.code(), "INVALID_SNAPSHOT");
        assert!(!e.to_string().contains(SQL));
        for table in [
            "school_data",
            "teaql_event_snapshot",
            "teaql_event_inbox",
            "teaql_event_checkpoint",
        ] {
            assert_eq!(self.count(table), 0, "{table}");
        }
        assert_eq!(self.count("teaql_event_rejection"), 1);
        self.receive_snapshot(snapshot_body()).await.unwrap();
        assert_eq!(self.count("school_data"), 1);
        assert_eq!(self.count("teaql_event_snapshot"), 1);
    }
}

#[test]
fn snapshot_container_controls_stay_strict() {
    block_on(async {
        for key in ["sql", "rawSql", "statement", "provider", "replace"] {
            let f = Fixture::new().await;
            let mut b = snapshot_body();
            b[key] = json!(SQL);
            f.reject_snapshot(b).await;
        }
    });
}
#[test]
fn snapshot_row_controls_stay_strict() {
    block_on(async {
        for key in ["sql", "statement", "table", "command", "operation"] {
            let f = Fixture::new().await;
            let mut b = snapshot_body();
            b["rows"][0][key] = json!(SQL);
            f.reject_snapshot(b).await;
        }
    });
}
#[test]
fn snapshot_audit_controls_cannot_smuggle_sql() {
    block_on(async {
        for key in [
            "sql",
            "rawSql",
            "statement",
            "provider",
            "class",
            "command",
            "tenantId",
        ] {
            let f = Fixture::new().await;
            let mut b = snapshot_body();
            b["audit"][key] = json!(SQL);
            f.reject_snapshot(b).await;
        }
        println!("CMR15_SNAPSHOT_CONTROLS_PASS language=rust container=5 row=5 audit=7");
    });
}
#[test]
fn snapshot_sql_looking_business_and_audit_text_remain_bound_data() {
    block_on(async {
        let f = Fixture::new().await;
        let mut b = snapshot_body();
        b["rows"][0]["fields"]["name"] = json!(SQL);
        b["audit"]["comment"] = json!(SQL);
        b["audit"]["actor_ref"] = json!("user:7");
        b["audit"]["trace_id"] = json!("trace:1");
        f.receive_snapshot(b).await.unwrap();
        assert_eq!(f.count("school_data"), 1);
        assert_eq!(f.count("teaql_event_rejection"), 0);
        assert_eq!(
            f.executor
                .connection()
                .lock()
                .unwrap()
                .query_row("SELECT name FROM school_data WHERE id=7", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            SQL
        );
    });
}
#[test]
fn envelope_controls_cannot_smuggle_sql() {
    block_on(async {
        for key in [
            "sql",
            "rawSql",
            "raw_sql",
            "statement",
            "statements",
            "provider",
            "mutation_plan",
            "replace",
        ] {
            let f = Fixture::new().await;
            let mut b = body();
            b[key] = json!(SQL);
            f.reject(b).await;
        }
        println!("CMR15_CONTROL_FRAMES_PASS language=rust scope=envelope rejected=8");
    });
}
#[test]
fn mutation_controls_cannot_smuggle_sql() {
    block_on(async {
        for key in [
            "sql",
            "rawSql",
            "raw_sql",
            "statement",
            "table",
            "provider",
            "physical_table",
            "command",
        ] {
            let f = Fixture::new().await;
            let mut b = body();
            b["mutations"][0][key] = json!(SQL);
            f.reject(b).await;
        }
    });
}
#[test]
fn audit_controls_cannot_smuggle_sql() {
    block_on(async {
        for key in [
            "sql",
            "rawSql",
            "statement",
            "provider",
            "class",
            "command",
            "tenantId",
        ] {
            let f = Fixture::new().await;
            let mut b = body();
            b["audit"][key] = json!(SQL);
            f.reject(b).await;
        }
    });
}
#[test]
fn trace_controls_stay_strict() {
    block_on(async {
        let f = Fixture::new().await;
        let mut b = body();
        b["mutations"][0]["trace_chain"] = json!([{"kind":"SQL","name":"diagnostic","entity_id":null,"comment":"trace metadata","statement":SQL}]);
        f.reject(b).await;
    });
}
#[test]
fn sql_looking_business_and_audit_text_remain_bound_data() {
    block_on(async {
        let f = Fixture::new().await;
        let mut b = body();
        b["mutations"][0]["fields"]["name"] = json!(SQL);
        b["audit"]["comment"] = json!(SQL);
        b["audit"]["actor_ref"] = json!("user:7");
        b["audit"]["trace_id"] = json!("trace:1");
        f.receive(b).await.unwrap();
        assert_eq!(f.count("school_data"), 1);
        let name: String = f
            .executor
            .connection()
            .lock()
            .unwrap()
            .query_row("SELECT name FROM school_data WHERE id=7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(name, SQL);
        assert_eq!(f.count("teaql_event_rejection"), 0);
    });
}
#[test]
fn ordinary_requests_are_not_committed_envelopes() {
    block_on(async {
        for b in [
            json!({"sql":"SELECT 1"}),
            json!({"entity":"school","limitValue":1,"commentText":"read","purposeText":"inspect"}),
            json!({"entity":"school","action":"Create","payload":{"name":"business"},"comment":"create school"}),
        ] {
            Fixture::new().await.reject(b).await;
        }
    });
}
