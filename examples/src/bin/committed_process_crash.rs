//! CMR-03: kill a real source process after an audited SQLite save, before
//! publication; recover and verify in different fresh processes. No networking.
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde_json::json;
use teaql_core::{CompactRow, Entity, TeaqlEntity as _, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, InboxOutcome};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    CommittedEventSource, EntityKey, InMemoryMetadataStore, LedgerEntity, UserContext,
    save_audited_ledger_entity,
};
use teaql_sql::SqlDataServiceExecutor;

type Failure = Box<dyn std::error::Error + Send + Sync>;
const SOURCE: &str = "cmr03-rust-source";
const TARGET: &str = "cmr03-rust-target";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const BUDGET: Duration = Duration::from_secs(30);
const MAX_LOG: u64 = 256 * 1024;

#[teaql_entity]
#[derive(Debug, TeaqlEntity)]
#[teaql(entity = "CrashOrder", table = "process_crash_order")]
struct CrashOrder {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}

fn new_order(round: u64) -> CrashOrder {
    let id = 100 + round;
    let name = format!("persisted-{round}");
    let mut order = CrashOrder::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(id)),
        ("version".into(), Value::I64(0)),
        ("name".into(), Value::Text(name.clone())),
    ])))
    .expect("decode crash example order");
    order.mark_as_new();
    order.entity_runtime_state().expect("new ledger").set(
        EntityKey::new("CrashOrder", id),
        "name",
        name,
    );
    order
}

async fn fixture(
    path: &Path,
    capture: bool,
) -> Result<(SqliteMutationExecutor, UserContext), Failure> {
    let executor = SqliteMutationExecutor::from_connection(Connection::open(path)?);
    let metadata = InMemoryMetadataStore::new().with_entity(CrashOrder::entity_descriptor());
    let service = SqlDataServiceExecutor::new(SqliteDialect, executor.clone(), metadata.clone());
    let mut context = UserContext::new().with_metadata(metadata);
    if capture {
        context = context.with_committed_event_source(CommittedEventSource::new(SOURCE, MODEL)?);
    } else {
        context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL)?);
    }
    context.use_sqlite_provider(executor.clone());
    context.register_executor(service);
    context.ensure_schema().await?;
    Ok((executor, context))
}

fn count(executor: &SqliteMutationExecutor, trusted_table: &str) -> Result<u64, Failure> {
    let count: i64 = executor.connection().lock().unwrap().query_row(
        &format!("SELECT COUNT(*) FROM {trusted_table}"),
        [],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(count)?)
}

fn source_state(directory: &Path, round: u64, ack: u64) -> Result<(), Failure> {
    let source =
        SqliteMutationExecutor::from_connection(Connection::open(directory.join("source.sqlite"))?);
    assert_eq!(count(&source, "process_crash_order")?, round);
    assert_eq!(
        source.committed_outbox_after(SOURCE, 0, 10)?.len() as u64,
        round
    );
    let status = source.committed_delivery_status(SOURCE, TARGET, MODEL)?;
    assert_eq!(status.source_high_watermark, round);
    assert_eq!(status.acked_sequence, ack);
    assert_eq!(status.backlog, round - ack);
    assert_eq!(
        fs::read_to_string(directory.join("business-invocations.txt"))?
            .lines()
            .count() as u64,
        round
    );
    Ok(())
}

fn target_state(target: &SqliteMutationExecutor, round: u64) -> Result<(), Failure> {
    assert_eq!(count(target, "process_crash_order")?, round);
    assert_eq!(count(target, "teaql_event_inbox")?, round);
    for r in 1..=round {
        let (version, name): (i64, String) = target.connection().lock().unwrap().query_row(
            "SELECT version, name FROM process_crash_order WHERE id = ?1",
            [i64::try_from(100 + r)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(version, 1);
        assert_eq!(name, format!("persisted-{r}"));
    }
    Ok(())
}

async fn worker(phase: &str, directory: &Path, round: u64) -> Result<(), Failure> {
    if phase == "premature" {
        return Ok(());
    } // negative control: no commit/barrier
    if phase == "seed" {
        let (_source, context) = fixture(&directory.join("source.sqlite"), true).await?;
        let mut calls = OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join("business-invocations.txt"))?;
        writeln!(calls, "seed-{round}")?;
        calls.sync_all()?;
        let _order = save_audited_ledger_entity(
            new_order(round).audit_as("commit order before controlled source process crash"),
            &context,
        )
        .await?;
        source_state(directory, round, round - 1)?;
        let temporary = directory.join(format!("ready-{round}.tmp"));
        let mut signal = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        signal.write_all(serde_json::to_string(&json!({"phase":"committed_before_publish", "pid":std::process::id(), "round":round}))?.as_bytes())?;
        signal.sync_all()?;
        fs::rename(temporary, directory.join(format!("ready-{round}.json")))?;
        println!("CMR03_BARRIER pid={} round={round}", std::process::id());
        std::io::stdout().flush()?;
        loop {
            std::thread::sleep(Duration::from_secs(1));
        } // parent must kill us; no normal destructor path
    }
    if phase != "recover" && phase != "verify" {
        return Err("unknown worker phase".into());
    }
    let source =
        SqliteMutationExecutor::from_connection(Connection::open(directory.join("source.sqlite"))?);
    let (target, context) = fixture(&directory.join("target.sqlite"), false).await?;
    let record = source
        .committed_outbox_after(SOURCE, round - 1, 1)?
        .remove(0);
    assert_eq!(record.source_sequence, round);
    if phase == "recover" {
        source_state(directory, round, round - 1)?;
        assert_eq!(count(&target, "process_crash_order")?, round - 1);
        let applied = target
            .apply_model_checked_committed_set(&context, &record)
            .await?;
        assert!(matches!(applied, InboxOutcome::Applied { checkpoint } if checkpoint == round));
        assert_eq!(
            source
                .committed_delivery_status(SOURCE, TARGET, MODEL)?
                .acked_sequence,
            round - 1
        );
        // The first target acknowledgement is lost. The source publisher must
        // accept a durable duplicate, not re-execute an application method.
        let batch = source.publish_committed_batch(SOURCE, TARGET, MODEL, 1, |item| {
            let target = &target; let context = &context;
            async move {
                let duplicate = target.apply_model_checked_committed_set(context, &item).await?;
                assert!(matches!(duplicate, InboxOutcome::Duplicate { checkpoint } if checkpoint == round));
                Ok(duplicate)
            }
        }).await?;
        assert_eq!(batch.acknowledged, 1);
        assert_eq!(batch.status.acked_sequence, round);
        assert_eq!(batch.status.backlog, 0);
    } else {
        assert!(
            matches!(target.apply_model_checked_committed_set(&context, &record).await?, InboxOutcome::Duplicate { checkpoint } if checkpoint == round)
        );
        let batch = source
            .publish_committed_batch(SOURCE, TARGET, MODEL, 1, |_| async {
                panic!("acked events must not be sent again");
                #[allow(unreachable_code)]
                Ok(InboxOutcome::Duplicate { checkpoint: 0 })
            })
            .await?;
        assert_eq!(batch.acknowledged, 0);
    }
    source_state(directory, round, round)?;
    target_state(&target, round)?;
    println!(
        "CMR03_WORKER_PASS phase={phase} pid={} round={round} ack={round} inbox={round} business_invocations={round}",
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
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn start(phase: &str, directory: &Path, round: u64) -> Result<OwnedChild, Failure> {
    let log = directory.join(format!("{phase}-{round}.log"));
    let stdout = OpenOptions::new().create_new(true).write(true).open(&log)?;
    let child = Command::new(std::env::current_exe()?)
        .args(["--worker", phase])
        .arg(directory)
        .arg(round.to_string())
        .stdin(Stdio::null())
        .stdout(stdout.try_clone()?)
        .stderr(stdout)
        .spawn()?;
    Ok(OwnedChild { child, log })
}
fn check_log(child: &OwnedChild) -> Result<(), Failure> {
    if fs::metadata(&child.log)?.len() > MAX_LOG {
        return Err("worker log exceeds bound".into());
    }
    Ok(())
}
fn barrier(child: &mut OwnedChild, directory: &Path, round: u64) -> Result<(), Failure> {
    let deadline = Instant::now() + BUDGET;
    let ready = directory.join(format!("ready-{round}.json"));
    while !ready.exists() {
        if child.child.try_wait()?.is_some() {
            return Err("writer exited before durable barrier".into());
        }
        if Instant::now() >= deadline {
            return Err("writer barrier deadline".into());
        }
        check_log(child)?;
        std::thread::sleep(Duration::from_millis(20));
    }
    let observed: serde_json::Value = serde_json::from_slice(&fs::read(ready)?)?;
    assert_eq!(observed["pid"].as_u64(), Some(u64::from(child.child.id())));
    assert_eq!(observed["round"].as_u64(), Some(round));
    assert_eq!(observed["phase"], "committed_before_publish");
    assert!(
        child.child.try_wait()?.is_none(),
        "writer must still wait for parent kill"
    );
    Ok(())
}
fn finished(child: &mut OwnedChild) -> Result<ExitStatus, Failure> {
    let deadline = Instant::now() + BUDGET;
    loop {
        check_log(child)?;
        if let Some(status) = child.child.try_wait()? {
            if !status.success() {
                return Err(format!("worker failed; inspect {}", child.log.display()).into());
            }
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("worker deadline".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn parent() -> Result<(), Failure> {
    let directory = match std::env::var_os("TEAQL_CMR03_EVIDENCE_DIR") {
        Some(path) => PathBuf::from(path),
        None => std::env::temp_dir().join(format!(
            "teaql-cmr03-rust-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        )),
    };
    fs::create_dir_all(&directory)?;
    assert!(
        !directory.join("source.sqlite").exists(),
        "never erase an earlier database"
    );
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join("owner.json"))?
        .write_all(
            serde_json::to_string(&json!({"pid":std::process::id(),"kind":"cmr03-example"}))?
                .as_bytes(),
        )?;
    println!("CMR03_EVIDENCE language=rust path={}", directory.display());
    let negative = directory.join("negative");
    fs::create_dir(&negative)?;
    let mut control = start("premature", &negative, 1)?;
    assert!(
        barrier(&mut control, &negative, 1)
            .unwrap_err()
            .to_string()
            .contains("exited before durable barrier")
    );
    drop(control); // reap before proceeding; no database or fake ready marker
    assert!(!negative.join("source.sqlite").exists());
    println!("CMR03_NEGATIVE_CONTROL_PASS language=rust");
    let mut rounds = Vec::new();
    for round in 1..=2 {
        let mut writer = start("seed", &directory, round)?;
        barrier(&mut writer, &directory, round)?;
        let writer_pid = writer.child.id();
        writer.child.kill()?;
        let death = writer.child.wait()?;
        assert!(
            !death.success(),
            "actual forced source termination required"
        );
        #[cfg(unix)]
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            let signal = death.signal();
            assert_eq!(signal, Some(9));
            signal
        };
        #[cfg(not(unix))]
        let signal: Option<i32> = None;
        source_state(&directory, round, round - 1)?; // separate connection after killed process is reaped
        let mut recovered = start("recover", &directory, round)?;
        finished(&mut recovered)?;
        let mut verified = start("verify", &directory, round)?;
        finished(&mut verified)?;
        let recovery_pid = recovered.child.id();
        let verify_pid = verified.child.id();
        assert_eq!(
            HashSet::from([writer_pid, recovery_pid, verify_pid]).len(),
            3
        );
        rounds.push(json!({"round":round,"writerPid":writer_pid,"writerExit":death.code(),"writerSignal":signal,
            "recoveryPid":recovery_pid,"verificationPid":verify_pid,"ack":round,"domainRows":round,
            "outboxRows":round,"inboxRows":round,"businessInvocations":round}));
        println!(
            "CMR03_PROCESS_CRASH_PASS language=rust round={round} killed_pid={writer_pid} killed_signal={} recovery_pid={recovery_pid} verify_pid={verify_pid}",
            signal.unwrap_or(-1)
        );
    }
    fs::write(
        directory.join("result.json"),
        serde_json::to_vec_pretty(&json!({"passed":true,"rounds":rounds,"sameDatabases":true,
        "scope":"controlled SQLite source-process crash, not power failure or snapshot crash"}))?,
    )?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() == 5 && args[1] == "--worker" {
        return worker(
            args[2].to_str().ok_or("phase encoding")?,
            Path::new(&args[3]),
            args[4].to_str().ok_or("round encoding")?.parse()?,
        )
        .await;
    }
    if args.len() != 1 {
        return Err("usage: committed_process_crash (worker flags are test-internal)".into());
    }
    parent()
}
