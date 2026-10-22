//! Focused infrastructure fixture, not an audited application example.
use futures_executor::block_on;
use rusqlite::{Connection, params};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_provider_sqlite::committed_events::snapshot::SnapshotLimits;
use teaql_provider_sqlite::committed_events::{CommittedEventError, InboxOutcome};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, OutboxRecord};
use teaql_provider_sqlite::committed_security::{SignedDelivery, SqliteCommittedReceiver};
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{CommittedEventSource, InMemoryMetadataStore, UserContext};
const SOURCE: &str = "source-CANARY";
const TARGET: &str = "target-CANARY";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
fn fixture() -> (SqliteMutationExecutor, UserContext, tempfile::TempDir) {
    let owner = tempfile::tempdir().unwrap();
    let executor = SqliteMutationExecutor::from_connection(
        Connection::open(owner.path().join("source.sqlite")).unwrap(),
    );
    executor.ensure_committed_event_schema().unwrap();
    let mut context = UserContext::new();
    context.use_sqlite_provider(executor.clone());
    {
        let c = executor.connection();
        let db = c.lock().unwrap();
        db.execute("INSERT INTO teaql_event_sequence VALUES(?1,2)", [SOURCE])
            .unwrap();
        for n in 1..=2 {
            db.execute(
                "INSERT INTO teaql_event_outbox VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    SOURCE,
                    n,
                    format!("fact-CANARY-{n}"),
                    MODEL,
                    "a".repeat(64),
                    "payload-CANARY"
                ],
            )
            .unwrap();
        }
    }
    (executor, context, owner)
}
#[test]
fn retry_has_durable_attempts_without_losing_original_facts() {
    block_on(async {
        let (executor, ctx, _owner) = fixture();
        assert!(
            executor
                .publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                    Err(CommittedEventError::TransportUnavailable)
                })
                .await
                .is_err()
        );
        let a = executor
            .observe_committed_source(&ctx, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!(
            (
                a.source_high_watermark,
                a.acked_sequence,
                a.sequence_lag,
                a.backlog
            ),
            (2, 0, 2, 2)
        );
        assert_eq!(
            (a.attempts, a.failures, a.retries, a.dead_letters),
            (1, 1, 0, 0)
        );
        executor
            .publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                Ok(InboxOutcome::Duplicate { checkpoint: 1 })
            })
            .await
            .unwrap();
        let reopened = SqliteMutationExecutor::from_connection(
            Connection::open(_owner.path().join("source.sqlite")).unwrap(),
        );
        let mut reopened_context = UserContext::new();
        reopened_context.use_sqlite_provider(reopened.clone());
        let b = reopened
            .observe_committed_source(&reopened_context, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!(
            (
                b.acked_sequence,
                b.backlog,
                b.attempts,
                b.retries,
                b.failures
            ),
            (1, 1, 2, 1, 1)
        );
        let rows: i64 = executor
            .connection()
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM teaql_event_outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 2);
        println!("CMR14_SOURCE_PASS language=rust retry=1 backlog=1");
    });
}
#[test]
fn backlog_counts_rows_not_sequence_distance() {
    block_on(async {
        let (e, c, _o) = fixture();
        e.connection()
            .lock()
            .unwrap()
            .execute("DELETE FROM teaql_event_outbox WHERE source_sequence=1", [])
            .unwrap();
        let s = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!((s.sequence_lag, s.backlog), (2, 1));
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 2, |r| async move {
                Ok(InboxOutcome::Applied {
                    checkpoint: r.source_sequence,
                })
            })
            .await
            .is_err()
        );
        assert_eq!(
            e.observe_committed_source(&c, SOURCE, TARGET, MODEL)
                .await
                .unwrap()
                .attempts,
            0
        );
    });
}
#[test]
fn bad_ack_leaves_cursor_and_observable_failure() {
    block_on(async {
        let (e, c, _o) = fixture();
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                Ok(InboxOutcome::Applied { checkpoint: 2 })
            })
            .await
            .is_err()
        );
        let s = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!((s.acked_sequence, s.failures, s.dead_letters), (0, 1, 0));
    });
}
#[test]
fn journal_ack_failure_cannot_advance_cursor() {
    block_on(async {
        let (e, c, _o) = fixture();
        e.connection().lock().unwrap().execute_batch("CREATE TRIGGER refuse_ack BEFORE UPDATE ON teaql_event_delivery_journal WHEN NEW.last_outcome='ACKNOWLEDGED' BEGIN SELECT RAISE(ABORT,'journal-CANARY'); END").unwrap();
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                Ok(InboxOutcome::Applied { checkpoint: 1 })
            })
            .await
            .is_err()
        );
        let s = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!(
            (s.acked_sequence, s.unresolved_attempts, s.failures),
            (0, 1, 0)
        );
    });
}

#[test]
fn permanent_rejection_is_inspectable_and_success_preserves_history() {
    block_on(async {
        let (e, c, _o) = fixture();
        let target = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let mut target_context = UserContext::new();
        target_context.use_sqlite_provider(target.clone());
        target_context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL).unwrap());
        let receiver = SqliteCommittedReceiver::new(
            target.clone(),
            SOURCE,
            TARGET,
            MODEL,
            std::collections::HashMap::from([("current".into(), vec![0; 32])]),
        )
        .unwrap();
        receiver.ensure_schema(&target_context).unwrap();
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |event| async {
                receiver
                    .receive(
                        &target_context,
                        &SignedDelivery {
                            event,
                            key_id: "current".into(),
                            target_id: TARGET.into(),
                            signature: vec![0; 32],
                        },
                    )
                    .await
                    .map_err(CommittedEventError::from)
            })
            .await
            .is_err()
        );
        let s = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!((s.rejections, s.dead_letters, s.acked_sequence), (1, 1, 0));
        let failures = e
            .committed_dead_letters(&c, SOURCE, TARGET, MODEL, 10)
            .await
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].reason, "INVALID_SIGNATURE");
        assert!(!format!("{failures:?}").contains("CANARY"));
        for limit in [0, 1001] {
            assert!(
                e.committed_dead_letters(&c, SOURCE, TARGET, MODEL, limit)
                    .await
                    .is_err()
            );
        }
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                Err(CommittedEventError::TransportUnavailable)
            })
            .await
            .is_err()
        );
        assert_eq!(
            e.committed_dead_letters(&c, SOURCE, TARGET, MODEL, 10)
                .await
                .unwrap()[0]
                .reason,
            "INVALID_SIGNATURE"
        );
        // Deliberately fake callback ACK is not target-application evidence.
        e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
            Ok(InboxOutcome::Applied { checkpoint: 1 })
        })
        .await
        .unwrap();
        let a = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!((a.dead_letters, a.rejections, a.retries), (0, 1, 2));
        let t = target
            .observe_committed_target(&target_context, SOURCE, TARGET, MODEL, Some(2))
            .await
            .unwrap();
        assert_eq!(
            (t.checkpoint, t.sequence_lag, t.rejections),
            (0, Some(2), 1)
        );
    });
}
#[test]
fn unauthorized_source_counts_and_absent_lag_is_not_zero() {
    block_on(async {
        let (e, mut c, _o) = fixture();
        c.insert_resource(CommittedEventTarget::new(SOURCE, MODEL).unwrap());
        let receiver = SqliteCommittedReceiver::new(
            e.clone(),
            SOURCE,
            TARGET,
            MODEL,
            std::collections::HashMap::from([("current".into(), vec![0; 32])]),
        )
        .unwrap();
        receiver.ensure_schema(&c).unwrap();
        let event = OutboxRecord {
            source_runtime_id: "intruder".into(),
            source_sequence: 1,
            change_set_id: "x".into(),
            model_hash: MODEL.into(),
            payload_sha256: "a".repeat(64),
            payload_json: "payload-CANARY".into(),
        };
        let error = receiver
            .receive(
                &c,
                &SignedDelivery {
                    event,
                    key_id: "current".into(),
                    target_id: TARGET.into(),
                    signature: vec![0; 32],
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "UNAUTHORIZED_SOURCE");
        let t = e
            .observe_committed_target(&c, SOURCE, TARGET, MODEL, None)
            .await
            .unwrap();
        assert_eq!((t.rejections, t.sequence_lag), (1, None));
        e.connection()
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO teaql_event_checkpoint VALUES(?1,1,?2)",
                params![SOURCE, MODEL],
            )
            .unwrap();
        assert!(
            e.observe_committed_target(&c, SOURCE, TARGET, MODEL, Some(0))
                .await
                .is_err()
        );
    });
}
#[test]
fn dropped_callback_is_unresolved_not_confirmed_failure() {
    use std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
    };
    struct Nop;
    impl Wake for Nop {
        fn wake(self: Arc<Self>) {}
    }
    let (e, c, _o) = fixture();
    let mut future = Box::pin(e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| {
        std::future::pending::<Result<InboxOutcome, CommittedEventError>>()
    }));
    let waker = Waker::from(Arc::new(Nop));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    drop(future);
    block_on(async {
        let s = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!(
            (s.acked_sequence, s.unresolved_attempts, s.failures),
            (0, 1, 0)
        );
        e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
            Ok(InboxOutcome::Applied { checkpoint: 1 })
        })
        .await
        .unwrap();
        assert_eq!(
            e.observe_committed_source(&c, SOURCE, TARGET, MODEL)
                .await
                .unwrap()
                .retries,
            1
        );
    });
}
#[test]
fn stale_attempt_cannot_replace_newer_success() {
    block_on(async {
        let (e, c, _o) = fixture();
        let inner = e.clone();
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                inner
                    .publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                        Ok(InboxOutcome::Applied { checkpoint: 1 })
                    })
                    .await
                    .unwrap();
                Ok(InboxOutcome::Applied { checkpoint: 1 })
            })
            .await
            .is_err()
        );
        let s = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!(
            (
                s.acked_sequence,
                s.attempts,
                s.failures,
                s.unresolved_attempts
            ),
            (1, 2, 0, 0)
        );
    });
}
#[test]
fn snapshot_zero_has_durable_independent_attempts() {
    block_on(async {
        let (e, mut c, _o) = fixture();
        {
            let conn = e.connection();
            let db = conn.lock().unwrap();
            db.execute_batch("DELETE FROM teaql_event_outbox;DELETE FROM teaql_event_sequence;CREATE TABLE school_data(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,name TEXT);").unwrap();
        }
        c = c
            .with_metadata(
                InMemoryMetadataStore::new().with_entity(
                    EntityDescriptor::new("school")
                        .table_name("school_data")
                        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                        .property(
                            PropertyDescriptor::new("version", DataType::I64)
                                .version()
                                .not_null(),
                        )
                        .property(PropertyDescriptor::new("name", DataType::Text)),
                ),
            )
            .with_committed_event_source(CommittedEventSource::new(SOURCE, MODEL).unwrap());
        e.prepare_committed_snapshot(
            &c,
            TARGET,
            "baseline",
            "capture empty baseline",
            "verify observations",
            SnapshotLimits::default(),
        )
        .await
        .unwrap();
        assert!(
            e.publish_prepared_snapshot(
                &c,
                TARGET,
                "baseline",
                SnapshotLimits::default(),
                |_| async { Err(CommittedEventError::TransportUnavailable) }
            )
            .await
            .is_err()
        );
        let before = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!(
            (before.pending_snapshots, before.attempts, before.failures),
            (1, 1, 1)
        );
        e.publish_prepared_snapshot(
            &c,
            TARGET,
            "baseline",
            SnapshotLimits::default(),
            |s| async move {
                Ok(
                    teaql_provider_sqlite::committed_events::bootstrap::SnapshotReceipt {
                        source_runtime_id: s.source_runtime_id,
                        target_id: TARGET.into(),
                        snapshot_id: s.snapshot_id,
                        source_sequence: s.source_sequence,
                        model_hash: s.model_hash,
                        payload_sha256: s.payload_sha256,
                        duplicate: false,
                    },
                )
            },
        )
        .await
        .unwrap();
        let after = e
            .observe_committed_source(&c, SOURCE, TARGET, MODEL)
            .await
            .unwrap();
        assert_eq!(
            (
                after.pending_snapshots,
                after.source_high_watermark,
                after.acked_sequence,
                after.attempts,
                after.retries
            ),
            (0, 0, 0, 2, 1)
        );
        e.publish_prepared_snapshot(
            &c,
            TARGET,
            "baseline",
            SnapshotLimits::default(),
            |_| async { panic!("already acked") },
        )
        .await
        .unwrap();
        assert_eq!(
            e.observe_committed_source(&c, SOURCE, TARGET, MODEL)
                .await
                .unwrap()
                .attempts,
            2
        );
    });
}

#[test]
fn counters_and_wrong_models_fail_closed() {
    block_on(async {
        let (e, c, _o) = fixture();
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                Err(CommittedEventError::TransportUnavailable)
            })
            .await
            .is_err()
        );
        e.connection()
            .lock()
            .unwrap()
            .execute(
                "UPDATE teaql_event_delivery_journal SET attempts=?1",
                [i64::MAX],
            )
            .unwrap();
        assert!(
            e.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                panic!("overflow must not invoke transport")
            })
            .await
            .is_err()
        );
        assert_eq!(
            e.observe_committed_source(&c, SOURCE, TARGET, MODEL)
                .await
                .unwrap()
                .attempts,
            i64::MAX as u64
        );
        e.connection()
            .lock()
            .unwrap()
            .execute(
                "UPDATE teaql_event_delivery_journal SET model_hash=?1",
                [format!("sha256:{}", "1".repeat(64))],
            )
            .unwrap();
        assert!(
            e.observe_committed_source(&c, SOURCE, TARGET, MODEL)
                .await
                .is_err()
        );
    });
}
#[test]
fn telemetry_is_safe_and_exporter_failures_are_fail_open() {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    use teaql_runtime::{
        RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry, RuntimeTelemetryScope,
    };
    struct Telemetry {
        events: Arc<Mutex<Vec<RuntimeOperation>>>,
        broken: bool,
    }
    impl RuntimeTelemetry for Telemetry {
        fn start(&self, op: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
            assert!(!self.broken, "exporter unavailable");
            self.events.lock().unwrap().push(op);
            Box::new(Scope)
        }
    }
    struct Scope;
    impl RuntimeTelemetryScope for Scope {
        fn success(&mut self, _: BTreeMap<String, RuntimeAttributeValue>) {
            panic!("exporter unavailable");
        }
        fn failure(&mut self, _: &str) {
            panic!("unexpected failed operation");
        }
    }
    block_on(async {
        let (e, c, _o) = fixture();
        let events = Arc::new(Mutex::new(vec![]));
        let c = c.with_runtime_telemetry(Arc::new(Telemetry {
            events: events.clone(),
            broken: false,
        }));
        assert_eq!(
            e.observe_committed_source(&c, SOURCE, TARGET, MODEL)
                .await
                .unwrap()
                .backlog,
            2
        );
        let recorded = events.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert!(
            recorded[0]
                .attributes
                .contains_key("teaql.replication.retries")
        );
        assert!(!format!("{recorded:?}").contains("CANARY"));
        drop(recorded);
        let c = c.with_runtime_telemetry(Arc::new(Telemetry {
            events,
            broken: true,
        }));
        assert_eq!(
            e.observe_committed_source(&c, SOURCE, TARGET, MODEL)
                .await
                .unwrap()
                .backlog,
            2
        );
    });
}
