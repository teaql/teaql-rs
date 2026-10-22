//! Audited source saves, authenticated snapshot/tail and durable safe rejection.
//! Local infrastructure example, not a network server or a process-crash claim.
use futures_executor::block_on;
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};
use teaql_core::{CompactRow, Entity, TeaqlEntity as _, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::committed_events::snapshot::SnapshotLimits;
use teaql_provider_sqlite::committed_events::{
    CommittedEventError, CommittedEventTarget, InboxOutcome,
};
use teaql_provider_sqlite::committed_security::SqliteCommittedReceiver;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    CommittedEventSource, EntityKey, InMemoryMetadataStore, LedgerEntity, UserContext,
    save_audited_ledger_entity,
};
use teaql_sql::SqlDataServiceExecutor;

type Failure = Box<dyn std::error::Error + Send + Sync>;
type ProbeResult<T> = std::result::Result<T, Failure>;
const SOURCE: &str = "cmr13-example-source";
const TARGET: &str = "analytics";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const ID: u64 = 9_007_199_254_740_993;
const CANARY: &str = "private-business-fact";
// Public test material only. Deployments provision their own independent secret.
const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

#[teaql_entity]
#[derive(Debug, TeaqlEntity)]
#[teaql(entity = "CustomerOrder", table = "customer_order_data")]
struct CustomerOrder {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    memo: String,
}
struct Fixture {
    executor: SqliteMutationExecutor,
    context: UserContext,
}
impl Fixture {
    async fn new(file: &Path, source: bool) -> ProbeResult<Self> {
        let executor = SqliteMutationExecutor::from_connection(Connection::open(file)?);
        let metadata = InMemoryMetadataStore::new().with_entity(CustomerOrder::entity_descriptor());
        let service =
            SqlDataServiceExecutor::new(SqliteDialect, executor.clone(), metadata.clone());
        let mut context = UserContext::new().with_metadata(metadata);
        if source {
            context =
                context.with_committed_event_source(CommittedEventSource::new(SOURCE, MODEL)?);
        } else {
            context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL)?);
        }
        context.use_sqlite_provider(executor.clone());
        context.register_executor(service);
        context.ensure_schema().await?;
        Ok(Self { executor, context })
    }
    fn receiver(&self) -> ProbeResult<SqliteCommittedReceiver> {
        let receiver = SqliteCommittedReceiver::new(
            self.executor.clone(),
            SOURCE,
            TARGET,
            MODEL,
            HashMap::from([("current".into(), KEY.to_vec())]),
        )?;
        receiver.ensure_schema(&self.context)?;
        Ok(receiver)
    }
    fn scalar(&self, query: &str) -> ProbeResult<i64> {
        Ok(self
            .executor
            .connection()
            .lock()
            .unwrap()
            .query_row(query, [], |r| r.get(0))?)
    }
    fn domain(&self) -> ProbeResult<Json> {
        assert_eq!(self.scalar("SELECT count(*) FROM customer_order_data")?, 1);
        Ok(self.executor.connection().lock().unwrap().query_row(
            "SELECT id,version,name,memo FROM customer_order_data",
            [],
            |r| {
                Ok(json!({
                "id":r.get::<_,i64>(0)?,"version":r.get::<_,i64>(1)?,
                "name":r.get::<_,String>(2)?,"memo":r.get::<_,String>(3)?}))
            },
        )?)
    }
    fn position(&self, checkpoint: i64, inbox: i64, audits: i64, name: &str) -> ProbeResult<()> {
        assert_eq!(
            self.scalar("SELECT source_sequence FROM teaql_event_checkpoint")?,
            checkpoint
        );
        assert_eq!(
            self.scalar("SELECT count(*) FROM teaql_event_inbox")?,
            inbox
        );
        assert_eq!(
            self.scalar("SELECT count(*) FROM teaql_event_rejection")?,
            audits
        );
        assert_eq!(self.domain()?["name"], name);
        Ok(())
    }
}
fn new_order() -> ProbeResult<CustomerOrder> {
    let mut order = CustomerOrder::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(ID)),
        ("version".into(), Value::I64(0)),
        ("name".into(), Value::Text("draft".into())),
        ("memo".into(), Value::Text(CANARY.into())),
    ])))?;
    order.mark_as_new();
    let state = order.entity_runtime_state().expect("new order ledger");
    state.set(EntityKey::new("CustomerOrder", ID), "name", "draft");
    state.set(EntityKey::new("CustomerOrder", ID), "memo", CANARY);
    Ok(order)
}
async fn business_call(
    directory: &Path,
    order: CustomerOrder,
    source: &Fixture,
    comment: &str,
) -> ProbeResult<CustomerOrder> {
    writeln!(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join("business-calls.txt"))?,
        "{comment}"
    )?;
    Ok(save_audited_ledger_entity(order.audit_as(comment), &source.context).await?)
}
fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
async fn scenario(directory: &Path) -> ProbeResult<()> {
    let source = Fixture::new(&directory.join("source.sqlite"), true).await?;
    let target = Fixture::new(&directory.join("target.sqlite"), false).await?;
    let receiver = target.receiver()?;
    let limits = SnapshotLimits::new(8, 65536)?;
    let mut order = business_call(
        directory,
        new_order()?,
        &source,
        "create authenticated replication example",
    )
    .await?;
    let first = source
        .executor
        .committed_outbox_after(SOURCE, 0, 8)?
        .remove(0);
    assert_eq!(first.source_sequence, 1);
    let prepared = source
        .executor
        .prepare_committed_snapshot(
            &source.context,
            TARGET,
            "baseline",
            "capture accepted order",
            "bootstrap analytics",
            limits,
        )
        .await?;
    assert_eq!(prepared.snapshot.source_sequence, 1);
    assert!(!prepared.acknowledged);
    let signed =
        SqliteCommittedReceiver::sign_snapshot("current", TARGET, KEY, prepared.snapshot.clone())?;
    let mut invalid = signed.clone();
    invalid.signature[0] ^= 1;
    let failed = source
        .executor
        .publish_prepared_snapshot(&source.context, TARGET, "baseline", limits, |_| async {
            let error = receiver
                .receive_snapshot(&target.context, &invalid)
                .await
                .unwrap_err();
            assert_eq!(error.code(), "INVALID_SIGNATURE");
            assert!(!error.to_string().contains(CANARY));
            Err(CommittedEventError::TransportUnavailable)
        })
        .await;
    assert!(matches!(
        failed,
        Err(CommittedEventError::TransportUnavailable)
    ));
    assert_eq!(
        source.scalar("SELECT count(*) FROM teaql_event_delivery_cursor")?,
        0
    );
    assert_eq!(
        source.scalar("SELECT acknowledged FROM teaql_event_snapshot_publication")?,
        0
    );
    assert_eq!(
        target.scalar("SELECT count(*) FROM customer_order_data")?,
        0
    );
    assert_eq!(
        target.scalar("SELECT count(*) FROM teaql_event_rejection")?,
        1
    );

    let lost = source
        .executor
        .publish_prepared_snapshot(
            &source.context,
            TARGET,
            "baseline",
            limits,
            |record| async {
                let message =
                    SqliteCommittedReceiver::sign_snapshot("current", TARGET, KEY, record).unwrap();
                receiver
                    .receive_snapshot(&target.context, &message)
                    .await
                    .unwrap();
                Err(CommittedEventError::TransportUnavailable) // Target committed; source never gets ACK.
            },
        )
        .await;
    assert!(matches!(
        lost,
        Err(CommittedEventError::TransportUnavailable)
    ));
    assert_eq!(
        source.scalar("SELECT count(*) FROM teaql_event_delivery_cursor")?,
        0
    );
    target.position(1, 0, 1, "draft")?;
    let recovered = source
        .executor
        .publish_prepared_snapshot(
            &source.context,
            TARGET,
            "baseline",
            limits,
            |record| async {
                let message =
                    SqliteCommittedReceiver::sign_snapshot("current", TARGET, KEY, record).unwrap();
                let receipt = receiver
                    .receive_snapshot(&target.context, &message)
                    .await
                    .unwrap();
                assert!(receipt.duplicate);
                Ok(receipt)
            },
        )
        .await?;
    assert!(recovered.acknowledged);
    assert_eq!(recovered.snapshot, prepared.snapshot);
    assert_eq!(
        source.scalar("SELECT acked_sequence FROM teaql_event_delivery_cursor")?,
        1
    );

    order.name = "submitted".into();
    order.entity_runtime_state().expect("loaded ledger").set(
        EntityKey::new("CustomerOrder", ID),
        "name",
        "submitted",
    );
    business_call(
        directory,
        order,
        &source,
        "submit authenticated replication example",
    )
    .await?;
    let tail = source
        .executor
        .committed_outbox_after(SOURCE, 1, 8)?
        .remove(0);
    assert_eq!(tail.source_sequence, 2);
    let original = SqliteCommittedReceiver::sign("current", TARGET, KEY, tail.clone())?;
    let mut forged = original.clone();
    forged.event.payload_json = tail.payload_json.replace("submitted", "forged");
    assert_ne!(forged.event.payload_json, tail.payload_json);
    forged.event.payload_sha256 = digest(&forged.event.payload_json);
    let failed = source
        .executor
        .publish_committed_batch(SOURCE, TARGET, MODEL, 8, |_| async {
            assert_eq!(
                receiver
                    .receive(&target.context, &forged)
                    .await
                    .unwrap_err()
                    .code(),
                "INVALID_SIGNATURE"
            );
            Err(CommittedEventError::TransportUnavailable)
        })
        .await;
    assert!(matches!(
        failed,
        Err(CommittedEventError::TransportUnavailable)
    ));
    target.position(1, 0, 2, "draft")?;
    assert_eq!(
        source
            .executor
            .committed_delivery_status(SOURCE, TARGET, MODEL)?
            .acked_sequence,
        1
    );
    let mut stranger = tail.clone();
    stranger.source_runtime_id = "untrusted-source".into();
    let stranger = SqliteCommittedReceiver::sign("current", TARGET, KEY, stranger)?;
    assert_eq!(
        receiver
            .receive(&target.context, &stranger)
            .await
            .unwrap_err()
            .code(),
        "UNAUTHORIZED_SOURCE"
    );
    target.position(1, 0, 3, "draft")?;
    let delivered = source
        .executor
        .publish_committed_batch(SOURCE, TARGET, MODEL, 8, |record| async {
            let message = SqliteCommittedReceiver::sign("current", TARGET, KEY, record).unwrap();
            receiver
                .receive(&target.context, &message)
                .await
                .map_err(|_| CommittedEventError::TransportUnavailable)
        })
        .await?;
    assert_eq!(delivered.acknowledged, 1);
    assert_eq!(delivered.status.acked_sequence, 2);
    assert_eq!(delivered.status.backlog, 0);
    assert!(matches!(
        receiver.receive(&target.context, &original).await?,
        InboxOutcome::Duplicate { .. }
    ));
    let past = SqliteCommittedReceiver::sign("current", TARGET, KEY, first)?;
    assert_eq!(
        receiver
            .receive(&target.context, &past)
            .await
            .unwrap_err()
            .code(),
        "SEQUENCE_GAP"
    );
    assert!(
        receiver
            .receive_snapshot(&target.context, &signed)
            .await?
            .duplicate
    );
    target.position(2, 1, 4, "submitted")?;
    let newer = source
        .executor
        .export_model_checked_snapshot(
            &source.context,
            "replacement",
            "capture a newer baseline",
            "prove active stream cannot reset",
            limits,
        )
        .await?;
    let newer = SqliteCommittedReceiver::sign_snapshot("current", TARGET, KEY, newer)?;
    assert_eq!(
        receiver
            .receive_snapshot(&target.context, &newer)
            .await
            .unwrap_err()
            .code(),
        "SNAPSHOT_CONFLICT"
    );
    target.position(2, 1, 5, "submitted")?;
    drop(receiver);
    drop(target);

    let reopened = Fixture::new(&directory.join("target.sqlite"), false).await?;
    let receiver = reopened.receiver()?;
    assert!(matches!(
        receiver.receive(&reopened.context, &original).await?,
        InboxOutcome::Duplicate { .. }
    ));
    assert!(
        receiver
            .receive_snapshot(&reopened.context, &signed)
            .await?
            .duplicate
    );
    reopened.position(2, 1, 5, "submitted")?;
    assert_eq!(source.domain()?, reopened.domain()?);
    let db = Connection::open_with_flags(
        directory.join("target.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut query = db.prepare("SELECT audit_id,reason_code,source_sequence,source_fingerprint,change_fingerprint,target_fingerprint,payload_fingerprint FROM teaql_event_rejection ORDER BY audit_id")?;
    let audits = query.query_map([], |r| Ok(json!({"audit_id":r.get::<_,i64>(0)?,"reason_code":r.get::<_,String>(1)?,
        "source_sequence":r.get::<_,i64>(2)?,"source_fingerprint":r.get::<_,String>(3)?,
        "change_fingerprint":r.get::<_,String>(4)?,"target_fingerprint":r.get::<_,String>(5)?,"payload_fingerprint":r.get::<_,String>(6)?})))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    assert_eq!(
        audits
            .iter()
            .map(|a| a["reason_code"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "SNAPSHOT_INVALID_SIGNATURE",
            "INVALID_SIGNATURE",
            "UNAUTHORIZED_SOURCE",
            "SEQUENCE_GAP",
            "SNAPSHOT_CONFLICT"
        ]
    );
    let audit_text = serde_json::to_string(&audits)?;
    for private in [CANARY, "submitted", std::str::from_utf8(KEY)?] {
        assert!(!audit_text.contains(private));
    }
    for row in &audits {
        for field in [
            "source_fingerprint",
            "change_fingerprint",
            "target_fingerprint",
            "payload_fingerprint",
        ] {
            let hash = row[field].as_str().unwrap();
            assert_eq!(hash.len(), 64);
            assert!(
                hash.bytes()
                    .all(|x| x.is_ascii_digit() || (b'a'..=b'f').contains(&x))
            );
        }
    }
    let calls = fs::read_to_string(directory.join("business-calls.txt"))?
        .lines()
        .count();
    assert_eq!(calls, 2);
    let result = json!({"schema":"teaql.cmr13-example/v1","language":"rust","source":source.domain()?,"target":reopened.domain()?,
        "businessCalls":calls,"sourceCursor":source.executor.committed_delivery_status(SOURCE,TARGET,MODEL)?.acked_sequence,
        "targetCheckpoint":reopened.scalar("SELECT source_sequence FROM teaql_event_checkpoint")?,
        "snapshotSequence":prepared.snapshot.source_sequence,"snapshotHash":prepared.snapshot.payload_sha256,"rejections":audits,
        "outbox":source.executor.committed_outbox_after(SOURCE,0,8)?.len(),"inbox":reopened.scalar("SELECT count(*) FROM teaql_event_inbox")?});
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join("result.json"))?
        .write_all(&serde_json::to_vec_pretty(&result)?)?;
    println!(
        "CMR13_SECURITY_EXAMPLE_PASS language=rust snapshot=1 tail=2 audited_saves=2 rejected=5 lost_ack=1 duplicate_no_rewind=1 reopen=1"
    );
    Ok(())
}
fn main() -> ProbeResult<()> {
    let retained = std::env::var_os("TEAQL_CMR13_EVIDENCE_DIR");
    let owner = if retained.is_none() {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let directory = retained
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| owner.as_ref().unwrap().path().to_owned());
    if owner.is_none() {
        assert!(directory.is_absolute());
        fs::create_dir(&directory)?;
    }
    let result = block_on(scenario(&directory));
    if result.is_err() {
        if let Some(owner) = owner {
            let _ = owner.keep();
        }
        eprintln!("CMR13 failed compact diagnostic: {}", directory.display());
    }
    result
}
