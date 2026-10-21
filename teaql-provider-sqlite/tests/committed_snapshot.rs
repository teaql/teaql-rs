//! CMR-11: authoritative legacy snapshot plus the ordered N+1 tail.
//! Public provider APIs only: reusable by an isolated Registry consumer.
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use futures_executor::block_on;
use rusqlite::{Connection, types::Value as SqlValue};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor};
use teaql_provider_sqlite::SqliteMutationExecutor;
use teaql_provider_sqlite::SqliteProviderExt;
use teaql_provider_sqlite::committed_events::bootstrap::SnapshotReceipt;
use teaql_provider_sqlite::committed_events::snapshot::{
    SnapshotImportMode, SnapshotLimits, SnapshotRecord,
};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, InboxOutcome, OutboxRecord};
use teaql_runtime::{CommittedEventSource, InMemoryMetadataStore, MetadataStore, UserContext};
use teaql_sql::{SqlTransaction, SqlTransactionTransport};

const SOURCE: &str = "snapshot-source";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const ORDER: i64 = 9_007_199_254_740_993;
const TABLES: [&str; 5] = [
    "platform_data",
    "order_status_data",
    "customer_order_data",
    "order_item_data",
    "empty_probe_data",
];

fn entity(name: &str, field: &str, data_type: DataType) -> EntityDescriptor {
    EntityDescriptor::new(name)
        .table_name(format!("{name}_data"))
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new(field, data_type))
}
fn reference(entity: EntityDescriptor, field: &str, target: &str) -> EntityDescriptor {
    entity
        .property(PropertyDescriptor::new(field, DataType::U64))
        .relation(RelationDescriptor::new(format!("{field}_ref"), target).local_key(field))
}
struct BarrierMetadata {
    inner: InMemoryMetadataStore,
    writer: Mutex<Option<PathBuf>>,
}
impl MetadataStore for BarrierMetadata {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.inner.entity(name)
    }
    fn all_entities(&self) -> Vec<&EntityDescriptor> {
        if let Some(path) = self.writer.lock().unwrap().take() {
            // A real second WAL connection commits only after the export read
            // transaction acquired its N view. No nested async executor.
            std::thread::spawn(move || block_on(commit_tail(&path, 1)))
                .join()
                .unwrap();
        }
        self.inner.all_entities()
    }
}
fn connection(path: &Path) -> Connection {
    let db = Connection::open(path).unwrap();
    db.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;")
        .unwrap();
    db
}
struct Fixture {
    // Connections are dropped before TempDir removes this task's owned DBs.
    context: UserContext,
    executor: SqliteMutationExecutor,
    path: PathBuf,
    _directory: tempfile::TempDir,
}
impl Fixture {
    fn new(legacy: bool, concurrent_writer: bool) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("teaql-cmr11-")
            .tempdir()
            .unwrap();
        let path = directory.path().join("snapshot.sqlite");
        let db = connection(&path);
        db.execute_batch("CREATE TABLE platform_data(Id INTEGER PRIMARY KEY,Version INTEGER NOT NULL,name TEXT,favorite_order INTEGER REFERENCES customer_order_data(id));
            CREATE TABLE order_status_data(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,code TEXT,platform INTEGER REFERENCES platform_data(Id));
            CREATE TABLE customer_order_data(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,name TEXT,amount TEXT,platform INTEGER REFERENCES platform_data(Id),status INTEGER REFERENCES order_status_data(id));
            CREATE TABLE order_item_data(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,quantity INTEGER,customer_order INTEGER REFERENCES customer_order_data(id),occurred_at INTEGER,planned_date TEXT,active INTEGER,ratio REAL,note TEXT);
            CREATE TABLE empty_probe_data(id INTEGER PRIMARY KEY,version INTEGER,name TEXT);").unwrap();
        if legacy {
            // Deliberate legacy fixture, not an invented mutation history.
            db.execute_batch(&format!("BEGIN; PRAGMA defer_foreign_keys=ON;
                INSERT INTO platform_data VALUES(1,3,'SOURCE ROOT',{ORDER});
                INSERT INTO order_status_data VALUES(1001,2,'OPEN',1);
                INSERT INTO customer_order_data VALUES({ORDER},7,'LEGACY','123.4500000',1,1001);
                INSERT INTO order_item_data VALUES({},-4,2,{ORDER},-315619199877,'2026-10-16',1,0.125,NULL); COMMIT;", ORDER + 1)).unwrap();
        }
        let executor = SqliteMutationExecutor::from_connection(db);
        executor.ensure_committed_event_schema().unwrap();
        let root = reference(
            entity("platform", "name", DataType::Text),
            "favorite_order",
            "customer_order",
        );
        let status = reference(
            entity("order_status", "code", DataType::Text),
            "platform",
            "platform",
        );
        let order = reference(
            reference(
                entity("customer_order", "name", DataType::Text).property(
                    PropertyDescriptor::new("amount", DataType::Decimal)
                        .numeric_precision(19)
                        .numeric_scale(7),
                ),
                "platform",
                "platform",
            ),
            "status",
            "order_status",
        );
        let item = reference(
            entity("order_item", "quantity", DataType::I64),
            "customer_order",
            "customer_order",
        )
        .property(PropertyDescriptor::new("occurred_at", DataType::Timestamp))
        .property(PropertyDescriptor::new("planned_date", DataType::Date))
        .property(PropertyDescriptor::new("active", DataType::Bool))
        .property(PropertyDescriptor::new("ratio", DataType::F64))
        .property(PropertyDescriptor::new("note", DataType::Text));
        let metadata = InMemoryMetadataStore::new()
            .with_entity(root)
            .with_entity(status)
            .with_entity(order)
            .with_entity(item)
            .with_entity(entity("empty_probe", "name", DataType::Text));
        let mut context = UserContext::new()
            .with_metadata(BarrierMetadata {
                inner: metadata,
                writer: Mutex::new(concurrent_writer.then(|| path.clone())),
            })
            .with_committed_event_source(CommittedEventSource::new(SOURCE, MODEL).unwrap());
        context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL).unwrap());
        context.use_sqlite_provider(executor.clone());
        Self {
            context,
            executor,
            path,
            _directory: directory,
        }
    }
    async fn snapshot(&self) -> SnapshotRecord {
        self.executor
            .export_model_checked_snapshot(
                &self.context,
                "snapshot-1",
                "copy legacy state",
                "verify consistent bootstrap",
                SnapshotLimits::default(),
            )
            .await
            .unwrap()
    }
    async fn import(
        &self,
        snapshot: &SnapshotRecord,
        mode: SnapshotImportMode,
    ) -> Result<InboxOutcome, teaql_provider_sqlite::committed_events::CommittedEventError> {
        self.executor
            .initialize_model_checked_snapshot(
                &self.context,
                snapshot,
                mode,
                SnapshotLimits::default(),
            )
            .await
    }
    fn scalar(&self, sql: &str) -> i64 {
        self.executor
            .connection()
            .lock()
            .unwrap()
            .query_row(sql, [], |r| r.get(0))
            .unwrap()
    }
    fn execute(&self, sql: &str) {
        self.executor
            .connection()
            .lock()
            .unwrap()
            .execute_batch(sql)
            .unwrap();
    }
    fn rows(&self, table: &str) -> Vec<Vec<SqlValue>> {
        let shared = self.executor.connection();
        let db = shared.lock().unwrap();
        let mut statement = db
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
            .unwrap();
        let count = statement.column_count();
        statement
            .query_map([], |row| (0..count).map(|i| row.get(i)).collect())
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
}
fn same(source: &Fixture, target: &Fixture) {
    for table in TABLES {
        assert_eq!(source.rows(table), target.rows(table), "{table}");
    }
}
fn hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}
fn payload(sequence: u64) -> String {
    let (base, next, name) = if sequence == 1 {
        (7, 8, "AFTER N")
    } else {
        (8, 9, "AT TWO")
    };
    let mut mutations = vec![
        json!({"entity_type":"customer_order","entity_id":ORDER.to_string(),"operation":"update","base_version":base.to_string(),"committed_version":next.to_string(),"trace_chain":[],"fields":{"name":name,"amount":"125.0000000"}}),
    ];
    if sequence == 1 {
        mutations.push(json!({"entity_type":"order_item","entity_id":(ORDER+1).to_string(),"operation":"recover","base_version":"-4","committed_version":"5","trace_chain":[],"fields":{"quantity":"3"}}));
    }
    json!({"protocol_version":1,"change_set_id":format!("tail-{sequence}"),"source_runtime_id":SOURCE,"source_sequence":sequence.to_string(),"source_model_hash":MODEL,"source_transaction_id":format!("tx-{sequence}"),"committed_at":"2026-10-22T00:00:00Z","audit":{"comment":"advance source","purpose":"verify snapshot tail"},"mutations":mutations}).to_string()
}
async fn commit_tail(path: &Path, sequence: u64) {
    let executor = SqliteMutationExecutor::from_connection(connection(path));
    let transaction = executor.begin_sql().await.unwrap();
    let (version, name) = if sequence == 1 {
        (8, "AFTER N")
    } else {
        (9, "AT TWO")
    };
    executor.connection().lock().unwrap().execute_batch(&format!("UPDATE customer_order_data SET version={version},name='{name}',amount='125.0000000' WHERE id={ORDER};")).unwrap();
    if sequence == 1 {
        executor
            .connection()
            .lock()
            .unwrap()
            .execute_batch("UPDATE order_item_data SET version=5,quantity=3")
            .unwrap();
    }
    transaction
        .append_committed_event(SOURCE, &format!("tail-{sequence}"), MODEL, |n| {
            assert_eq!(n, sequence);
            Ok(payload(n))
        })
        .unwrap();
    transaction.commit_sql().await.unwrap();
}
fn changed(original: &SnapshotRecord, edit: impl FnOnce(&mut Value)) -> SnapshotRecord {
    let mut body: Value = serde_json::from_str(&original.payload_json).unwrap();
    edit(&mut body);
    let text = body.to_string();
    SnapshotRecord {
        payload_sha256: hash(&text),
        payload_json: text,
        ..original.clone()
    }
}

#[test]
fn durable_bootstrap_keeps_original_bytes_and_resumes_after_lost_ack() {
    block_on(async {
        use teaql_provider_sqlite::committed_events::CommittedEventError;
        let source = Fixture::new(true, false);
        let target = Fixture::new(false, false);
        commit_tail(&source.path, 1).await;
        let prepared = source
            .executor
            .prepare_committed_snapshot(
                &source.context,
                "analytics",
                "bootstrap",
                "copy",
                "bootstrap",
                SnapshotLimits::default(),
            )
            .await
            .unwrap();
        assert_eq!(prepared.snapshot.source_sequence, 1);
        assert!(!prepared.acknowledged);
        commit_tail(&source.path, 2).await;
        assert_eq!(
            prepared,
            source
                .executor
                .prepare_committed_snapshot(
                    &source.context,
                    "analytics",
                    "bootstrap",
                    "retry",
                    "retry",
                    SnapshotLimits::default()
                )
                .await
                .unwrap()
        );
        assert!(
            source
                .executor
                .publish_committed_batch(SOURCE, "analytics", MODEL, 10, |_| async {
                    panic!("pending bootstrap must block tail")
                })
                .await
                .is_err()
        );
        assert!(
            source
                .executor
                .publish_prepared_snapshot(
                    &source.context,
                    "analytics",
                    "bootstrap",
                    SnapshotLimits::default(),
                    |record| {
                        let replica = &target;
                        let original = &source;
                        async move {
                            assert!(
                                original
                                    .executor
                                    .connection()
                                    .lock()
                                    .unwrap()
                                    .is_autocommit()
                            );
                            let ack = replica
                                .executor
                                .initialize_snapshot_and_acknowledge(
                                    &replica.context,
                                    &record,
                                    "analytics",
                                    SnapshotImportMode::RequireEmpty,
                                    SnapshotLimits::default(),
                                )
                                .await?;
                            assert!(!ack.duplicate);
                            Err(CommittedEventError::TransportUnavailable)
                        }
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(
            source.scalar("SELECT count(*) FROM teaql_event_delivery_cursor"),
            0
        );
        let acknowledged = source
            .executor
            .publish_prepared_snapshot(
                &source.context,
                "analytics",
                "bootstrap",
                SnapshotLimits::default(),
                |record| {
                    let replica = &target;
                    async move {
                        let ack = replica
                            .executor
                            .initialize_snapshot_and_acknowledge(
                                &replica.context,
                                &record,
                                "analytics",
                                SnapshotImportMode::RequireEmpty,
                                SnapshotLimits::default(),
                            )
                            .await?;
                        assert!(ack.duplicate);
                        Ok(ack)
                    }
                },
            )
            .await
            .unwrap();
        assert!(acknowledged.acknowledged);
        assert_eq!(
            source
                .executor
                .committed_delivery_status(SOURCE, "analytics", MODEL)
                .unwrap()
                .acked_sequence,
            1
        );
        let batch = source
            .executor
            .publish_committed_batch(SOURCE, "analytics", MODEL, 10, |record| {
                let replica = &target;
                async move {
                    assert_eq!(record.source_sequence, 2);
                    replica
                        .executor
                        .apply_model_checked_committed_set(&replica.context, &record)
                        .await
                }
            })
            .await
            .unwrap();
        assert_eq!(batch.acknowledged, 1);
        assert_eq!(batch.status.acked_sequence, 2);
        same(&source, &target);
        assert!(
            source
                .executor
                .publish_prepared_snapshot(
                    &source.context,
                    "analytics",
                    "bootstrap",
                    SnapshotLimits::default(),
                    |_| async { panic!("known ACK must not redeliver") }
                )
                .await
                .unwrap()
                .acknowledged
        );
        assert_eq!(
            source
                .executor
                .committed_delivery_status(SOURCE, "analytics", MODEL)
                .unwrap()
                .acked_sequence,
            2
        );
        assert!(
            source
                .executor
                .prepare_committed_snapshot(
                    &source.context,
                    "analytics",
                    "different",
                    "copy",
                    "bootstrap",
                    SnapshotLimits::default()
                )
                .await
                .is_err()
        );
        println!(
            "CMR12_BOOTSTRAP_PASS language=rust baseline=1 tail=2 retained_bytes=1 lost_ack=1 pending_gate=1 no_rewind=1"
        );
    });
}

#[test]
fn bootstrap_ack_is_bound_and_cursor_publication_commit_is_atomic() {
    block_on(async {
        let source = Fixture::new(true, false);
        let target = Fixture::new(false, false);
        commit_tail(&source.path, 1).await;
        let prepared = source
            .executor
            .prepare_committed_snapshot(
                &source.context,
                "analytics",
                "bootstrap",
                "copy",
                "bootstrap",
                SnapshotLimits::default(),
            )
            .await
            .unwrap();
        let ack = target
            .executor
            .initialize_snapshot_and_acknowledge(
                &target.context,
                &prepared.snapshot,
                "analytics",
                SnapshotImportMode::RequireEmpty,
                SnapshotLimits::default(),
            )
            .await
            .unwrap();
        for bad in [
            SnapshotReceipt {
                source_runtime_id: "other".into(),
                ..ack.clone()
            },
            SnapshotReceipt {
                target_id: "other".into(),
                ..ack.clone()
            },
            SnapshotReceipt {
                snapshot_id: "other".into(),
                ..ack.clone()
            },
            SnapshotReceipt {
                source_sequence: 2,
                ..ack.clone()
            },
            SnapshotReceipt {
                model_hash: format!("sha256:{}", "1".repeat(64)),
                ..ack.clone()
            },
            SnapshotReceipt {
                payload_sha256: "bad".into(),
                ..ack.clone()
            },
        ] {
            assert!(
                source
                    .executor
                    .publish_prepared_snapshot(
                        &source.context,
                        "analytics",
                        "bootstrap",
                        SnapshotLimits::default(),
                        |_| async move { Ok(bad) }
                    )
                    .await
                    .is_err()
            );
        }
        source.execute("CREATE TRIGGER reject_bootstrap_ack BEFORE UPDATE ON teaql_event_snapshot_publication WHEN NEW.acknowledged=1 BEGIN SELECT RAISE(ABORT,'fixture rejects ACK after cursor insert'); END");
        assert!(
            source
                .executor
                .publish_prepared_snapshot(
                    &source.context,
                    "analytics",
                    "bootstrap",
                    SnapshotLimits::default(),
                    |_| async { Ok(ack.clone()) }
                )
                .await
                .is_err()
        );
        assert_eq!(
            source.scalar("SELECT count(*) FROM teaql_event_delivery_cursor"),
            0
        );
        assert_eq!(
            source.scalar("SELECT acknowledged FROM teaql_event_snapshot_publication"),
            0
        );
        source.execute("DROP TRIGGER reject_bootstrap_ack");
        assert!(
            source
                .executor
                .publish_prepared_snapshot(
                    &source.context,
                    "analytics",
                    "bootstrap",
                    SnapshotLimits::default(),
                    |_| async { Ok(ack.clone()) }
                )
                .await
                .unwrap()
                .acknowledged
        );
        source.execute("DELETE FROM teaql_event_delivery_cursor");
        assert!(
            source
                .executor
                .publish_committed_batch(SOURCE, "analytics", MODEL, 10, |_| async {
                    panic!("corrupt cursor must fail before callback")
                })
                .await
                .is_err()
        );
        println!(
            "CMR12_ACK_ATOMIC_PASS language=rust bound_fields=6 rollback=1 retry=1 corrupt_cursor=1"
        );
    });
}

#[test]
fn bootstrap_mode_is_exclusive_and_cannot_reset_source_publication_state() {
    block_on(async {
        let source = Fixture::new(true, false);
        let target = Fixture::new(false, false);
        assert!(
            source
                .executor
                .publish_committed_batch(SOURCE, "incremental", MODEL, 0, |_| async { panic!() })
                .await
                .is_err()
        );
        assert_eq!(
            source.scalar("SELECT count(*) FROM teaql_event_delivery_cursor"),
            0
        );
        assert_eq!(
            source
                .executor
                .publish_committed_batch(SOURCE, "incremental", MODEL, 1, |_| async { panic!() })
                .await
                .unwrap()
                .acknowledged,
            0
        );
        assert!(
            source
                .executor
                .prepare_committed_snapshot(
                    &source.context,
                    "incremental",
                    "bootstrap",
                    "copy",
                    "bootstrap",
                    SnapshotLimits::default()
                )
                .await
                .is_err()
        );
        assert_eq!(
            source.scalar("SELECT count(*) FROM teaql_event_snapshot_publication"),
            0
        );
        let tx = source.executor.begin_sql().await.unwrap();
        assert!(
            source
                .executor
                .prepare_committed_snapshot(
                    &source.context,
                    "new-target",
                    "bootstrap",
                    "copy",
                    "bootstrap",
                    SnapshotLimits::default()
                )
                .await
                .is_err()
        );
        tx.rollback_sql().await.unwrap();
        target
            .executor
            .prepare_committed_snapshot(
                &target.context,
                "another-target",
                "bootstrap",
                "copy",
                "bootstrap",
                SnapshotLimits::default(),
            )
            .await
            .unwrap();
        assert!(
            target
                .import(
                    &source.snapshot().await,
                    SnapshotImportMode::ReplaceUninitialized
                )
                .await
                .is_err()
        );
        assert_eq!(
            target.scalar("SELECT count(*) FROM teaql_event_snapshot_publication"),
            1
        );
    });
}

#[test]
fn legacy_snapshot_concurrent_tail_matches_source_without_rewind() {
    block_on(async {
        let source = Fixture::new(true, true);
        let target = Fixture::new(false, false);
        let snapshot = source.snapshot().await;
        assert_eq!(snapshot.source_sequence, 0);
        let body: Value = serde_json::from_str(&snapshot.payload_json).unwrap();
        assert_eq!(body["entity_types"].as_array().unwrap().len(), 5);
        assert_eq!(body["rows"].as_array().unwrap().len(), 4);
        assert!(snapshot.payload_json.contains("LEGACY"));
        assert!(!snapshot.payload_json.contains("AFTER N"));
        assert!(snapshot.payload_json.contains("\"version\":\"-4\""));
        assert_eq!(
            target
                .import(&snapshot, SnapshotImportMode::RequireEmpty)
                .await
                .unwrap(),
            InboxOutcome::Applied { checkpoint: 0 }
        );
        let events = source
            .executor
            .committed_outbox_after(SOURCE, 0, 10)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            target
                .executor
                .apply_model_checked_committed_set(&target.context, &events[0])
                .await
                .unwrap(),
            InboxOutcome::Applied { checkpoint: 1 }
        );
        same(&source, &target);
        assert_eq!(
            target
                .import(&snapshot, SnapshotImportMode::RequireEmpty)
                .await
                .unwrap(),
            InboxOutcome::Duplicate { checkpoint: 0 }
        );
        assert_eq!(
            target.scalar("SELECT source_sequence FROM teaql_event_checkpoint"),
            1
        );
        same(&source, &target);
        assert_eq!(
            target
                .executor
                .apply_model_checked_committed_set(&target.context, &events[0])
                .await
                .unwrap(),
            InboxOutcome::Duplicate { checkpoint: 1 }
        );
        assert!(
            target
                .import(
                    &source.snapshot().await,
                    SnapshotImportMode::ReplaceUninitialized
                )
                .await
                .is_err()
        );
        println!(
            "CMR11_SNAPSHOT_PASS language=rust snapshot=0 tail=1 rows=4 catalog=5 concurrent_writer=1 duplicate_after_tail=1"
        );
    });
}

#[test]
fn nonzero_snapshot_continues_at_n_plus_one_without_inventing_history() {
    block_on(async {
        let source = Fixture::new(true, false);
        let target = Fixture::new(false, false);
        commit_tail(&source.path, 1).await;
        let snapshot = source.snapshot().await;
        assert_eq!(snapshot.source_sequence, 1);
        target
            .import(&snapshot, SnapshotImportMode::RequireEmpty)
            .await
            .unwrap();
        same(&source, &target);
        let old = source
            .executor
            .committed_outbox_after(SOURCE, 0, 10)
            .unwrap();
        assert!(
            target
                .executor
                .apply_model_checked_committed_set(&target.context, &old[0])
                .await
                .is_err()
        );
        commit_tail(&source.path, 2).await;
        let next = source
            .executor
            .committed_outbox_after(SOURCE, 1, 10)
            .unwrap();
        assert_eq!(next.len(), 1);
        target
            .executor
            .apply_model_checked_committed_set(&target.context, &next[0])
            .await
            .unwrap();
        same(&source, &target);
        assert_eq!(
            target
                .import(&snapshot, SnapshotImportMode::RequireEmpty)
                .await
                .unwrap(),
            InboxOutcome::Duplicate { checkpoint: 1 }
        );
        assert_eq!(
            target.scalar("SELECT source_sequence FROM teaql_event_checkpoint"),
            2
        );
        println!("CMR11_NONZERO_PASS language=rust snapshot=1 tail=2 legacy_history_absent=1");
    });
}

#[test]
fn bootstrap_replacement_is_explicit_and_late_failure_is_atomic() {
    block_on(async {
        let source = Fixture::new(true, false);
        let target = Fixture::new(true, false);
        let snapshot = source.snapshot().await;
        target.execute("UPDATE platform_data SET name='BOOTSTRAP'");
        assert!(
            target
                .import(&snapshot, SnapshotImportMode::RequireEmpty)
                .await
                .is_err()
        );
        let before = TABLES.map(|t| target.rows(t));
        target.execute("CREATE TRIGGER reject_snapshot BEFORE INSERT ON order_item_data BEGIN SELECT RAISE(ABORT,'snapshot fault'); END");
        assert!(
            target
                .import(&snapshot, SnapshotImportMode::ReplaceUninitialized)
                .await
                .is_err()
        );
        assert_eq!(before, TABLES.map(|t| target.rows(t)));
        assert_eq!(
            target.scalar("SELECT count(*) FROM teaql_event_snapshot"),
            0
        );
        assert_eq!(
            target.scalar("SELECT count(*) FROM teaql_event_checkpoint"),
            0
        );
        target.execute("DROP TRIGGER reject_snapshot");
        target
            .import(&snapshot, SnapshotImportMode::ReplaceUninitialized)
            .await
            .unwrap();
        same(&source, &target);
    });
}

#[test]
fn malformed_records_budgets_and_dangling_references_leave_target_empty() {
    block_on(async {
        let source = Fixture::new(true, false);
        let target = Fixture::new(false, false);
        let snapshot = source.snapshot().await;
        let mut invalid = vec![
            changed(&snapshot, |b| {
                b["entity_types"].as_array_mut().unwrap().remove(0);
            }),
            changed(&snapshot, |b| {
                b["rows"][0]["fields"]["unknown"] = json!("bad");
            }),
            changed(&snapshot, |b| {
                b["rows"][0]["version"] = json!("0");
            }),
            changed(&snapshot, |b| {
                b["snapshot_version"] = json!(4294967297u64);
            }),
            changed(&snapshot, |b| {
                let row = b["rows"][0].clone();
                b["rows"].as_array_mut().unwrap().push(row);
            }),
            changed(&snapshot, |b| {
                b["rows"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|r| r["entity_type"] == "customer_order")
                    .unwrap()["fields"]["platform"]["entity_id"] = json!("999");
            }),
            SnapshotRecord {
                source_runtime_id: "other-source".into(),
                ..snapshot.clone()
            },
            SnapshotRecord {
                model_hash: format!("sha256:{}", "1".repeat(64)),
                ..snapshot.clone()
            },
            SnapshotRecord {
                payload_sha256: "bad".into(),
                ..snapshot.clone()
            },
        ];
        for text in [
            snapshot.payload_json.replacen(
                "\"snapshot_version\":1",
                "\"snapshot_version\":1,\"snapshot_version\":1",
                1,
            ),
            format!("{} {{}}", snapshot.payload_json),
        ] {
            invalid.push(SnapshotRecord {
                payload_sha256: hash(&text),
                payload_json: text,
                ..snapshot.clone()
            });
        }
        for record in invalid {
            assert!(
                target
                    .import(&record, SnapshotImportMode::RequireEmpty)
                    .await
                    .is_err(),
                "must reject malformed snapshot"
            );
        }
        for limits in [
            SnapshotLimits::new(3, 100_000).unwrap(),
            SnapshotLimits::new(10, 1).unwrap(),
        ] {
            assert!(
                target
                    .executor
                    .initialize_model_checked_snapshot(
                        &target.context,
                        &snapshot,
                        SnapshotImportMode::RequireEmpty,
                        limits
                    )
                    .await
                    .is_err()
            );
            assert!(
                source
                    .executor
                    .export_model_checked_snapshot(
                        &source.context,
                        "small",
                        "copy",
                        "budget",
                        limits
                    )
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            target.scalar("SELECT count(*) FROM teaql_event_snapshot"),
            0
        );
        assert_eq!(
            target.scalar("SELECT count(*) FROM teaql_event_checkpoint"),
            0
        );
        for table in TABLES {
            assert!(target.rows(table).is_empty());
        }
        assert!(!format!("{snapshot:?}").contains("SOURCE ROOT"));
    });
}

#[test]
fn snapshot_does_not_bypass_context_or_caller_transaction_or_normal_version_rules() {
    block_on(async {
        let source = Fixture::new(true, false);
        let target = Fixture::new(false, false);
        assert!(
            source
                .executor
                .export_model_checked_snapshot(
                    &source.context,
                    "x",
                    " ",
                    "purpose",
                    SnapshotLimits::default()
                )
                .await
                .is_err()
        );
        assert!(
            source
                .executor
                .export_model_checked_snapshot(
                    &target.context,
                    "x",
                    "copy",
                    "purpose",
                    SnapshotLimits::default()
                )
                .await
                .is_err()
        );
        let transaction = source.executor.begin_sql().await.unwrap();
        assert!(
            source
                .executor
                .export_model_checked_snapshot(
                    &source.context,
                    "x",
                    "copy",
                    "purpose",
                    SnapshotLimits::default()
                )
                .await
                .is_err()
        );
        transaction.rollback_sql().await.unwrap();
        let snapshot = source.snapshot().await;
        target
            .import(&snapshot, SnapshotImportMode::RequireEmpty)
            .await
            .unwrap();
        let conflict = changed(&snapshot, |b| {
            b["audit"]["comment"] = json!("conflicting bytes");
        });
        assert!(
            target
                .import(&conflict, SnapshotImportMode::RequireEmpty)
                .await
                .is_err()
        );
        let empty = Fixture::new(false, false);
        let text = payload(1);
        let mut body: Value = serde_json::from_str(&text).unwrap();
        body["mutations"][0]["operation"] = json!("create");
        body["mutations"][0]["base_version"] = Value::Null;
        let text = body.to_string();
        let event = OutboxRecord {
            source_runtime_id: SOURCE.into(),
            source_sequence: 1,
            change_set_id: "tail-1".into(),
            model_hash: MODEL.into(),
            payload_sha256: hash(&text),
            payload_json: text,
        };
        assert!(
            empty
                .executor
                .apply_model_checked_committed_set(&empty.context, &event)
                .await
                .is_err()
        );
        assert!(empty.rows("customer_order_data").is_empty());
    });
}

#[test]
fn source_json_null_and_ambiguous_metadata_fail_closed_without_poisoning_the_connection() {
    async fn export(
        executor: &SqliteMutationExecutor,
        context: &UserContext,
    ) -> Result<SnapshotRecord, teaql_provider_sqlite::committed_events::CommittedEventError> {
        executor
            .export_model_checked_snapshot(
                context,
                "json-probe",
                "preserve JSON semantics",
                "verify lossless export",
                SnapshotLimits::default(),
            )
            .await
    }
    block_on(async {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor.ensure_committed_event_schema().unwrap();
        executor
            .connection()
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE json_probe_data(id INTEGER PRIMARY KEY,version INTEGER,payload TEXT);
             INSERT INTO json_probe_data VALUES(1,3,'null');",
            )
            .unwrap();
        let descriptor = entity("json_probe", "payload", DataType::Json);
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()))
            .with_committed_event_source(CommittedEventSource::new(SOURCE, MODEL).unwrap());
        context.use_sqlite_provider(executor.clone());
        // JSON-text null must not be silently restored as SQL NULL.
        assert!(export(&executor, &context).await.is_err());
        executor
            .connection()
            .lock()
            .unwrap()
            .execute("UPDATE json_probe_data SET payload=NULL", [])
            .unwrap();
        let snapshot = export(&executor, &context).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&snapshot.payload_json).unwrap()["rows"][0]["fields"]["payload"],
            Value::Null
        );
        executor
            .connection()
            .lock()
            .unwrap()
            .execute("UPDATE json_probe_data SET payload=?1", [" {\"a\" : 1} "])
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&export(&executor, &context).await.unwrap().payload_json)
                .unwrap()["rows"][0]["fields"]["payload"],
            json!({"a":1})
        );
        let ambiguous = descriptor.clone().property(
            PropertyDescriptor::new("payload", DataType::Text).column_name("other_payload"),
        );
        context.set_metadata(InMemoryMetadataStore::new().with_entity(ambiguous));
        assert!(export(&executor, &context).await.is_err());
        context.set_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
        assert!(export(&executor, &context).await.is_ok());
    });
}
