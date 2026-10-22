//! Focused infrastructure fixture, not an audited application example.
use futures_executor::block_on;
use rusqlite::{Connection, params};
use teaql_provider_sqlite::committed_events::{CommittedEventError, InboxOutcome};
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::UserContext;
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
        let b = executor
            .observe_committed_source(&ctx, SOURCE, TARGET, MODEL)
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
