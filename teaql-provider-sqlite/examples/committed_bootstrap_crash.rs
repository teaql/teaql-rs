//! CMR-12: owned Linux process death at bootstrap/tail transaction boundaries.
//! All barriers are test fixtures; no production callback or remote restore API.
use futures_executor::block_on;
use rusqlite::{Connection, OpenFlags, OptionalExtension, functions::FunctionFlags};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use teaql_core::{CompactRow, Entity, EntityDescriptor, TeaqlEntity as _, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::committed_events::snapshot::{SnapshotImportMode, SnapshotLimits};
use teaql_provider_sqlite::committed_events::{
    CommittedEventError, CommittedEventTarget, InboxOutcome,
};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    CommittedEventSource, EntityKey, InMemoryMetadataStore, LedgerEntity, MetadataStore,
    UserContext, save_audited_ledger_entity,
};
use teaql_sql::SqlDataServiceExecutor;

type Failure = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Failure>;
const SOURCE: &str = "cmr12-rust-source";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const LEGACY: i64 = 9_007_199_254_740_993;
const PHASES: [&str; 13] = [
    "seed",
    "capture",
    "retain",
    "prepare",
    "tail",
    "target_apply",
    "snapshot_receipt_lost",
    "source_ack",
    "ack",
    "tail_apply",
    "tail_receipt_lost",
    "tail_ack",
    "verify",
];
const BUDGET: Duration = Duration::from_secs(30);
const MAX_LOG: u64 = 256 * 1024;
fn slot(round: u64) -> String {
    format!("analytics-{round}")
}
fn identity(round: u64) -> String {
    format!("bootstrap-{round}")
}
fn baseline(round: u64) -> u64 {
    2 * round - 1
}
fn limits() -> SnapshotLimits {
    SnapshotLimits::new(32, 65536).unwrap()
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn write_new(file: &Path, bytes: &[u8]) -> Result<()> {
    let mut output = OpenOptions::new().create_new(true).write(true).open(file)?;
    output.write_all(bytes)?;
    output.sync_all()?;
    Ok(())
}
fn pause(directory: &Path, phase: &str, round: u64, visible_rows: i64) -> ! {
    // Flush before publishing readiness: the parent can kill immediately after rename.
    println!("CMR12_SQL_BARRIER phase={phase} round={round} visible_rows={visible_rows}");
    std::io::stdout()
        .flush()
        .expect("flush barrier diagnostics");
    let temporary = directory.join(format!("{phase}-{round}.ready.tmp"));
    write_new(
        &temporary,
        &serde_json::to_vec(&json!({"pid":std::process::id(),"phase":phase,
        "round":round,"visibleRows":visible_rows}))
        .unwrap(),
    )
    .expect("create PID barrier");
    fs::rename(
        temporary,
        directory.join(format!("{phase}-{round}.ready.json")),
    )
    .expect("publish barrier");
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[teaql_entity]
#[derive(Debug, TeaqlEntity)]
#[teaql(entity = "Order", table = "order_data")]
struct BootstrapOrder {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    memo: Option<String>,
}
struct BarrierMetadata {
    inner: InMemoryMetadataStore,
    armed: Arc<AtomicBool>,
    directory: PathBuf,
    round: u64,
}
impl MetadataStore for BarrierMetadata {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.inner.entity(name)
    }
    fn all_entities(&self) -> Vec<&EntityDescriptor> {
        // export_model_checked_snapshot enumerates metadata after its N SELECT.
        if self.armed.load(Ordering::SeqCst) {
            pause(&self.directory, "capture", self.round, -1);
        }
        self.inner.all_entities()
    }
}
struct Fixture {
    executor: SqliteMutationExecutor,
    context: UserContext,
    armed: Arc<AtomicBool>,
}
impl Fixture {
    fn new(directory: &Path, phase: &str, round: u64, source: bool) -> Result<Self> {
        let file = directory.join(if source {
            "source.sqlite".into()
        } else {
            format!("target-{round}.sqlite")
        });
        let db = Connection::open(file)?;
        db.busy_timeout(Duration::from_secs(5))?;
        let armed = Arc::new(AtomicBool::new(false));
        let signal = armed.clone();
        let dir = directory.to_path_buf();
        let current = phase.to_owned();
        db.create_scalar_function(
            "cmr12_test_barrier",
            2,
            FunctionFlags::SQLITE_UTF8,
            move |args| {
                let wanted: String = args.get(0)?;
                let rows: i64 = args.get(1)?;
                if signal.load(Ordering::SeqCst) && current == wanted {
                    pause(&dir, &current, round, rows);
                }
                Ok(0i64)
            },
        )?;
        let executor = SqliteMutationExecutor::from_connection(db);
        let metadata =
            InMemoryMetadataStore::new().with_entity(BootstrapOrder::entity_descriptor());
        let service =
            SqlDataServiceExecutor::new(SqliteDialect, executor.clone(), metadata.clone());
        let capture = Arc::new(AtomicBool::new(false));
        // Only the capture worker arms the metadata barrier, after schema setup.
        let capture_metadata = if phase == "capture" {
            armed.clone()
        } else {
            capture
        };
        let mut context = UserContext::new().with_metadata(BarrierMetadata {
            inner: metadata,
            armed: capture_metadata,
            directory: directory.to_path_buf(),
            round,
        });
        if source {
            context =
                context.with_committed_event_source(CommittedEventSource::new(SOURCE, MODEL)?);
        }
        context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL)?);
        context.use_sqlite_provider(executor.clone());
        context.register_executor(service);
        Ok(Self {
            executor,
            context,
            armed,
        })
    }
    async fn schema(&self) -> Result<()> {
        self.context.ensure_schema().await?;
        self.executor.ensure_committed_event_schema()?;
        Ok(())
    }
    fn trigger(&self, sql: &str) -> Result<()> {
        self.executor
            .connection()
            .lock()
            .unwrap()
            .execute_batch(sql)?;
        Ok(())
    }
}
async fn save(source: &Fixture, directory: &Path, round: u64, tail: bool) -> Result<()> {
    let name = format!("{}-{round}", if tail { "tail" } else { "seed" });
    let mut calls = OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join("business-calls.txt"))?;
    writeln!(calls, "{name}")?;
    calls.sync_all()?;
    let id = if tail { 200 + round } else { 100 + round };
    let mut order = BootstrapOrder::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(id)),
        ("version".into(), Value::I64(0)),
        ("name".into(), Value::Text(name.clone())),
        ("memo".into(), Value::Text("业务事实".into())),
    ])))?;
    order.mark_as_new();
    let state = order.entity_runtime_state().expect("new mutation ledger");
    state.set(EntityKey::new("Order", id), "name", name);
    state.set(EntityKey::new("Order", id), "memo", "业务事实");
    save_audited_ledger_entity(
        order.audit_as("persist source facts for controlled bootstrap recovery"),
        &source.context,
    )
    .await?;
    Ok(())
}
fn readonly(file: &Path) -> Result<Connection> {
    Ok(Connection::open_with_flags(
        file,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?)
}
fn count(db: &Connection, table: &str) -> Result<i64> {
    Ok(db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
}
fn domain(db: &Connection) -> Result<Vec<Json>> {
    let mut query = db.prepare("SELECT id,version,name,memo FROM order_data ORDER BY id")?;
    Ok(query.query_map([], |r| Ok(json!({"id":r.get::<_,i64>(0)?.to_string(),
        "version":r.get::<_,i64>(1)?.to_string(),"name":r.get::<_,String>(2)?,"memo":r.get::<_,Option<String>>(3)?})))?
        .collect::<std::result::Result<_,_>>()?)
}
fn observed(directory: &Path, round: u64) -> Result<Json> {
    let db = readonly(&directory.join("source.sqlite"))?;
    let job: Option<(i64,i64,String)> = db.query_row("SELECT source_sequence,acknowledged,payload_sha256 FROM teaql_event_snapshot_publication WHERE target_id=?1", [slot(round)], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let cursor: Option<i64> = db
        .query_row(
            "SELECT acked_sequence FROM teaql_event_delivery_cursor WHERE target_id=?1",
            [slot(round)],
            |r| r.get(0),
        )
        .optional()?;
    let high: i64 = db.query_row(
        "SELECT current_sequence FROM teaql_event_sequence WHERE source_runtime_id=?1",
        [SOURCE],
        |r| r.get(0),
    )?;
    let mut result = json!({"source":{"high":high,"domainRows":count(&db,"order_data")?,
        "businessCalls":fs::read_to_string(directory.join("business-calls.txt"))?.lines().count(),
        "snapshotSequence":job.as_ref().map(|x|x.0).unwrap_or(-1),"snapshotAcknowledged":job.as_ref().map(|x|x.1).unwrap_or(-1),
        "snapshotHash":job.map(|x|x.2).unwrap_or_default(),"cursor":cursor.unwrap_or(-1)},"target":{"present":false}});
    let target = directory.join(format!("target-{round}.sqlite"));
    if target.exists() {
        let db = readonly(&target)?;
        let checkpoint: Option<i64> = db
            .query_row(
                "SELECT source_sequence FROM teaql_event_checkpoint",
                [],
                |r| r.get(0),
            )
            .optional()?;
        result["target"] = json!({"present":true,"domainRows":count(&db,"order_data")?,"snapshotRows":count(&db,"teaql_event_snapshot")?,
            "checkpoint":checkpoint.unwrap_or(0),"inbox":count(&db,"teaql_event_inbox")?,"applied":count(&db,"teaql_event_applied_payload")?});
    }
    Ok(result)
}
fn retained(source: &Fixture, directory: &Path, round: u64) -> Result<()> {
    let shared = source.executor.connection();
    let db = shared.lock().unwrap();
    let (n, payload, digest): (i64,String,String) = db.query_row("SELECT source_sequence,payload_json,payload_sha256 FROM teaql_event_snapshot_publication WHERE target_id=?1", [slot(round)], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    assert_eq!(n as u64, baseline(round));
    assert_eq!(
        payload,
        fs::read_to_string(directory.join(format!("snapshot-{round}.json")))?
    );
    assert_eq!(digest, hash(payload.as_bytes()));
    assert_eq!(
        digest,
        fs::read_to_string(directory.join(format!("snapshot-{round}.sha256")))?
    );
    Ok(())
}
async fn worker(phase: &str, directory: &Path, round: u64) -> Result<()> {
    if phase == "premature" {
        return Ok(());
    }
    let source = Fixture::new(directory, phase, round, true)?;
    match phase {
        "seed" => {
            source.schema().await?;
            if round == 1 {
                source.executor.connection().lock().unwrap().execute(
                    "INSERT INTO order_data(id,version,name,memo) VALUES(?1,-4,'旧记录',NULL)",
                    [LEGACY],
                )?;
            }
            save(&source, directory, round, false).await?;
        }
        "tail" => save(&source, directory, round, true).await?,
        "capture" | "retain" | "prepare" => {
            if phase == "retain" {
                source.trigger(&format!("CREATE TRIGGER retain_barrier_{round} AFTER INSERT ON teaql_event_snapshot_publication WHEN NEW.target_id='{}' BEGIN SELECT cmr12_test_barrier('retain',(SELECT count(*) FROM teaql_event_snapshot_publication WHERE target_id=NEW.target_id)); END",slot(round)))?;
            }
            source.armed.store(true, Ordering::SeqCst);
            let prepared = source
                .executor
                .prepare_committed_snapshot(
                    &source.context,
                    &slot(round),
                    &identity(round),
                    "capture bootstrap facts",
                    "recover analytics without replay",
                    limits(),
                )
                .await?;
            assert!(!prepared.acknowledged);
            assert_eq!(prepared.snapshot.source_sequence, baseline(round));
            write_new(
                &directory.join(format!("snapshot-{round}.json")),
                prepared.snapshot.payload_json.as_bytes(),
            )?;
            write_new(
                &directory.join(format!("snapshot-{round}.sha256")),
                prepared.snapshot.payload_sha256.as_bytes(),
            )?;
        }
        _ => {
            retained(&source, directory, round)?;
            let target = Fixture::new(directory, phase, round, false)?;
            if phase == "target_apply" {
                target.schema().await?;
                target.trigger("CREATE TRIGGER snapshot_barrier AFTER INSERT ON order_data WHEN NEW.id=101 BEGIN SELECT cmr12_test_barrier('target_apply',(SELECT count(*) FROM order_data)); END")?;
                target.armed.store(true, Ordering::SeqCst);
            }
            if phase == "source_ack" {
                source.trigger(&format!("CREATE TRIGGER ack_barrier_{round} AFTER INSERT ON teaql_event_delivery_cursor WHEN NEW.target_id='{}' BEGIN SELECT cmr12_test_barrier('source_ack',(SELECT count(*) FROM teaql_event_delivery_cursor WHERE target_id=NEW.target_id)); END", slot(round)))?;
                source.armed.store(true, Ordering::SeqCst);
            }
            if matches!(
                phase,
                "target_apply" | "snapshot_receipt_lost" | "source_ack" | "ack"
            ) {
                let prepared = source
                    .executor
                    .publish_prepared_snapshot(
                        &source.context,
                        &slot(round),
                        &identity(round),
                        limits(),
                        |record| {
                            let target = &target;
                            async move {
                                let receipt = target
                                    .executor
                                    .initialize_snapshot_and_acknowledge(
                                        &target.context,
                                        &record,
                                        &slot(round),
                                        SnapshotImportMode::RequireEmpty,
                                        limits(),
                                    )
                                    .await?;
                                assert_eq!(
                                    receipt.duplicate,
                                    !matches!(phase, "snapshot_receipt_lost" | "target_apply")
                                );
                                if phase == "snapshot_receipt_lost" {
                                    let rows = count(
                                        &target.executor.connection().lock().unwrap(),
                                        "order_data",
                                    )
                                    .unwrap();
                                    pause(directory, phase, round, rows);
                                }
                                Ok(receipt)
                            }
                        },
                    )
                    .await?;
                assert!(prepared.acknowledged);
            } else if matches!(
                phase,
                "tail_apply" | "tail_receipt_lost" | "tail_ack" | "catchup"
            ) {
                if phase == "tail_apply" {
                    target.trigger(&format!("CREATE TRIGGER tail_barrier AFTER INSERT ON order_data WHEN NEW.id={} BEGIN SELECT cmr12_test_barrier('tail_apply',(SELECT count(*) FROM order_data)); END", 200+round))?;
                    target.armed.store(true, Ordering::SeqCst);
                }
                let batch = source
                    .executor
                    .publish_committed_batch(
                        SOURCE,
                        &slot(round),
                        MODEL,
                        if phase == "catchup" { 10 } else { 1 },
                        |record| {
                            let target = &target;
                            async move {
                                if phase != "catchup" {
                                    assert_eq!(record.source_sequence, baseline(round) + 1);
                                }
                                let outcome = target
                                    .executor
                                    .apply_model_checked_committed_set(&target.context, &record)
                                    .await?;
                                assert_eq!(
                                    matches!(outcome, InboxOutcome::Duplicate { .. }),
                                    phase == "tail_ack"
                                );
                                if phase == "tail_receipt_lost" {
                                    let rows = count(
                                        &target.executor.connection().lock().unwrap(),
                                        "order_data",
                                    )
                                    .unwrap();
                                    pause(directory, phase, round, rows);
                                }
                                Ok(outcome)
                            }
                        },
                    )
                    .await?;
                assert_eq!(batch.acknowledged, if phase == "catchup" { 2 } else { 1 });
                if phase == "catchup" {
                    assert_eq!(batch.status.acked_sequence, 4);
                }
            } else if phase != "verify" {
                return Err("unknown worker phase".into());
            }
            if phase == "verify" || phase == "catchup" {
                let prepared = source
                    .executor
                    .publish_prepared_snapshot(
                        &source.context,
                        &slot(round),
                        &identity(round),
                        limits(),
                        |_| async {
                            panic!("known snapshot ACK must not redeliver");
                            #[allow(unreachable_code)]
                            Err(CommittedEventError::DuplicateConflict)
                        },
                    )
                    .await?;
                assert!(prepared.acknowledged);
                assert_eq!(prepared.snapshot.source_sequence, baseline(round));
                let batch = source
                    .executor
                    .publish_committed_batch(SOURCE, &slot(round), MODEL, 1, |_| async {
                        panic!("known tail ACK must not redeliver");
                        #[allow(unreachable_code)]
                        Err(CommittedEventError::DuplicateConflict)
                    })
                    .await?;
                assert_eq!(batch.acknowledged, 0);
                assert_eq!(
                    domain(&readonly(&directory.join("source.sqlite"))?)?,
                    domain(&readonly(
                        &directory.join(format!("target-{round}.sqlite"))
                    )?)?
                );
            }
        }
    }
    println!(
        "CMR12_WORKER_PASS phase={phase} round={round} pid={}",
        std::process::id()
    );
    Ok(())
}

struct OwnedChild {
    child: Child,
    log: PathBuf,
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}
fn start(phase: &str, directory: &Path, round: u64) -> Result<OwnedChild> {
    let log = directory.join(format!("{phase}-{round}.log"));
    let output = OpenOptions::new().create_new(true).write(true).open(&log)?;
    let child = Command::new(std::env::current_exe()?)
        .args(["--worker", phase])
        .arg(directory)
        .arg(round.to_string())
        .stdin(Stdio::null())
        .stdout(output.try_clone()?)
        .stderr(output)
        .spawn()?;
    Ok(OwnedChild { child, log })
}
fn check(child: &OwnedChild, deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err("worker deadline".into());
    }
    if fs::metadata(&child.log)?.len() > MAX_LOG {
        return Err("worker log budget".into());
    }
    Ok(())
}
fn await_barrier(child: &mut OwnedChild, directory: &Path, phase: &str, round: u64) -> Result<()> {
    let ready = directory.join(format!("{phase}-{round}.ready.json"));
    let deadline = Instant::now() + BUDGET;
    while !ready.exists() {
        if child.child.try_wait()?.is_some() {
            return Err("worker exited before SQL/commit barrier".into());
        }
        check(child, deadline)?;
        std::thread::sleep(Duration::from_millis(20));
    }
    let barrier: Json = serde_json::from_slice(&fs::read(ready)?)?;
    assert_eq!(barrier["pid"], json!(child.child.id()));
    assert_eq!(barrier["phase"], phase);
    assert_eq!(barrier["round"], round);
    assert!(child.child.try_wait()?.is_none());
    if matches!(phase, "retain" | "source_ack" | "target_apply") {
        assert_eq!(barrier["visibleRows"], 1);
    }
    if phase == "capture" {
        assert_eq!(barrier["visibleRows"], -1);
    }
    if phase == "tail_apply" {
        assert_eq!(barrier["visibleRows"], baseline(round) + 2);
    }
    Ok(())
}
fn execute(phase: &str, directory: &Path, round: u64, crash: bool) -> Result<Json> {
    use std::os::unix::process::ExitStatusExt;
    let mut child = start(phase, directory, round)?;
    let pid = child.child.id();
    let status = if crash {
        await_barrier(&mut child, directory, phase, round)?;
        child.child.kill()?;
        let status = child.child.wait()?;
        assert_eq!(status.signal(), Some(9));
        assert_eq!(status.code(), None);
        status
    } else {
        let deadline = Instant::now() + BUDGET;
        loop {
            check(&child, deadline)?;
            if let Some(status) = child.child.try_wait()? {
                if !status.success() {
                    return Err(format!("worker failed: {}", child.log.display()).into());
                }
                break status;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let bytes = fs::read(&child.log)?;
    assert!(bytes.len() as u64 <= MAX_LOG);
    if !crash {
        assert!(String::from_utf8_lossy(&bytes).contains(&format!(
            "CMR12_WORKER_PASS phase={phase} round={round} pid={pid}"
        )));
    }
    Ok(
        json!({"phase":phase,"round":round,"pid":pid,"exit":status.code(),"signal":status.signal(),"killed":crash,
        "logSha256":hash(&bytes),"observed":observed(directory,round)?}),
    )
}
fn negative(directory: &Path) -> Result<()> {
    let dir = directory.join("negative");
    fs::create_dir(&dir)?;
    let mut child = start("premature", &dir, 1)?;
    assert!(
        await_barrier(&mut child, &dir, "premature", 1)
            .unwrap_err()
            .to_string()
            .contains("exited before SQL/commit barrier")
    );
    let exit = child.child.wait()?;
    assert!(exit.success());
    assert!(!dir.join("source.sqlite").exists());
    assert!(!dir.join("premature-1.ready.json").exists());
    write_new(
        &dir.join("result.json"),
        &serde_json::to_vec(
            &json!({"passed":true,"pid":child.child.id(),"exit":exit.code(),"barrierAbsent":true,"databaseAbsent":true}),
        )?,
    )?;
    println!("CMR12_NEGATIVE_CONTROL_PASS language=rust");
    Ok(())
}
fn verify_observed(state: &Json, phase: &str, round: u64) -> Result<()> {
    let n = baseline(round) as i64;
    let before_tail = matches!(phase, "seed" | "capture" | "retain" | "prepare");
    let high = if phase == "catchup" {
        4
    } else {
        n + if before_tail { 0 } else { 1 }
    };
    let prepared = !matches!(phase, "seed" | "capture" | "retain");
    let acked = matches!(
        phase,
        "ack" | "tail_apply" | "tail_receipt_lost" | "tail_ack" | "verify" | "catchup"
    );
    let delivered = matches!(phase, "tail_ack" | "verify" | "catchup");
    assert_eq!(state["source"]["high"], high);
    assert_eq!(state["source"]["domainRows"], high + 1);
    assert_eq!(state["source"]["businessCalls"], high);
    assert_eq!(
        state["source"]["snapshotSequence"],
        if prepared { n } else { -1 }
    );
    assert_eq!(
        state["source"]["snapshotAcknowledged"],
        if prepared {
            if acked { 1 } else { 0 }
        } else {
            -1
        }
    );
    assert_eq!(
        state["source"]["cursor"],
        if acked {
            if delivered { high } else { n }
        } else {
            -1
        }
    );
    let absent = matches!(phase, "seed" | "capture" | "retain" | "prepare" | "tail");
    assert_eq!(state["target"]["present"], !absent);
    if !absent {
        let installed = phase != "target_apply";
        let applied = matches!(
            phase,
            "tail_receipt_lost" | "tail_ack" | "verify" | "catchup"
        );
        let checkpoint = if phase == "catchup" {
            4
        } else if installed {
            n + if applied { 1 } else { 0 }
        } else {
            0
        };
        assert_eq!(state["target"]["checkpoint"], checkpoint);
        assert_eq!(
            state["target"]["domainRows"],
            if installed { checkpoint + 1 } else { 0 }
        );
        assert_eq!(
            state["target"]["snapshotRows"],
            if installed { 1 } else { 0 }
        );
        assert_eq!(
            state["target"]["inbox"],
            if phase == "catchup" {
                3
            } else if applied {
                1
            } else {
                0
            }
        );
        assert_eq!(state["target"]["applied"], state["target"]["inbox"]);
    }
    Ok(())
}
fn parent(directory: &Path) -> Result<()> {
    assert_eq!(
        std::env::consts::OS,
        "linux",
        "controlled Linux SIGKILL gate"
    );
    fs::create_dir_all(directory)?;
    assert!(
        !directory.join("source.sqlite").exists(),
        "never reset earlier DBs"
    );
    write_new(
        &directory.join("owner.json"),
        &serde_json::to_vec(
            &json!({"pid":std::process::id(),"scope":"cmr12-rust-process-example"}),
        )?,
    )?;
    negative(directory)?;
    let mut workers = Vec::new();
    for round in 1..=2 {
        for phase in PHASES {
            let crash = matches!(
                phase,
                "capture"
                    | "retain"
                    | "target_apply"
                    | "snapshot_receipt_lost"
                    | "source_ack"
                    | "tail_apply"
                    | "tail_receipt_lost"
            );
            let worker = execute(phase, directory, round, crash)?;
            verify_observed(&worker["observed"], phase, round)?;
            workers.push(worker);
        }
        println!(
            "CMR12_PROCESS_PASS language=rust round={round} baseline={} tail={} killed=7 same_source_database=1 original_bytes=1",
            baseline(round),
            baseline(round) + 1
        );
    }
    let catchup = execute("catchup", directory, 1, false)?;
    verify_observed(&catchup["observed"], "catchup", 1)?;
    workers.push(catchup);
    assert_eq!(
        workers
            .iter()
            .map(|x| x["pid"].as_u64().unwrap())
            .collect::<HashSet<_>>()
            .len(),
        27
    );
    let source = readonly(&directory.join("source.sqlite"))?;
    assert_eq!(count(&source, "teaql_event_snapshot_publication")?, 2);
    for round in 1..=2 {
        let target = readonly(&directory.join(format!("target-{round}.sqlite")))?;
        assert_eq!(domain(&source)?, domain(&target)?);
        let cursor: i64 = source.query_row(
            "SELECT acked_sequence FROM teaql_event_delivery_cursor WHERE target_id=?1",
            [slot(round)],
            |r| r.get(0),
        )?;
        assert_eq!(cursor, 4);
    }
    write_new(
        &directory.join("result.json"),
        &serde_json::to_vec(
            &json!({"passed":true,"workers":workers,"rounds":2,"kills":14,
        "sameSourceDatabase":true,"sourceHighWatermark":4,"businessCalls":4,"targetCheckpoints":[4,4],"platform":"linux",
        "scope":"controlled Linux SQLite process crash; not power loss, installed artifacts or full CMR-12 acceptance"}),
        )?,
    )?;
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() == 2 && args[1] == "--build-receipt" {
        let binary = std::env::current_exe()?;
        println!(
            "{}",
            json!({"binary":binary,"sha256":hash(&fs::read(&binary)?),
            "manifest":env!("CARGO_MANIFEST_DIR"),"platform":std::env::consts::OS})
        );
        return Ok(());
    }
    if args.len() == 5 && args[1] == "--worker" {
        return block_on(worker(
            args[2].to_str().ok_or("phase encoding")?,
            Path::new(&args[3]),
            args[4].to_str().ok_or("round encoding")?.parse()?,
        ));
    }
    if args.len() != 1 {
        return Err("usage: committed_bootstrap_crash (worker flags are test-internal)".into());
    }
    let owned = if std::env::var_os("TEAQL_CMR12_EVIDENCE_DIR").is_none() {
        Some(
            tempfile::Builder::new()
                .prefix("teaql-cmr12-rust-")
                .tempdir()?,
        )
    } else {
        None
    };
    let directory = std::env::var_os("TEAQL_CMR12_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| owned.as_ref().unwrap().path().to_path_buf());
    let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parent(&directory)))
    {
        Ok(result) => result,
        Err(panic) => {
            if let Some(owned) = owned {
                let _ = owned.keep();
            }
            println!(
                "CMR12_PROCESS_EVIDENCE language=rust path={}",
                directory.display()
            );
            std::panic::resume_unwind(panic);
        }
    };
    if result.is_err() {
        if let Some(owned) = owned {
            let _ = owned.keep();
        }
    }
    if std::env::var_os("TEAQL_CMR12_EVIDENCE_DIR").is_some() || result.is_err() {
        println!(
            "CMR12_PROCESS_EVIDENCE language=rust path={}",
            directory.display()
        );
    }
    result
}
