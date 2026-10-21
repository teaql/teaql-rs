//! Trusted startup bootstrap. The direct writes below only simulate legacy
//! rows and a captured infrastructure tail, not a public business mutation API.
use rusqlite::Connection;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, path::Path};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_provider_sqlite::committed_events::snapshot::{
    SnapshotImportMode, SnapshotLimits, SnapshotRecord,
};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, InboxOutcome, OutboxRecord};
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{CommittedEventSource, InMemoryMetadataStore, UserContext};
use teaql_sql::{SqlTransaction, SqlTransactionTransport};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const SOURCE: &str = "snapshot-example";
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const ID: i64 = 9_007_199_254_740_993;
struct Fixture {
    context: UserContext,
    executor: SqliteMutationExecutor,
}
fn entity(name: &str, order: bool) -> EntityDescriptor {
    let entity = EntityDescriptor::new(name)
        .table_name(format!("{name}_data"))
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("name", DataType::Text));
    if order {
        entity
            .property(PropertyDescriptor::new("occurred_at", DataType::Timestamp))
            .property(PropertyDescriptor::new("active", DataType::Bool))
            .property(PropertyDescriptor::new("note", DataType::Text))
    } else {
        entity
    }
}
impl Fixture {
    async fn new(legacy: bool) -> Result<Self> {
        let executor = SqliteMutationExecutor::from_connection(Connection::open_in_memory()?);
        let metadata = InMemoryMetadataStore::new()
            .with_entity(entity("customer_order", true))
            .with_entity(entity("empty_probe", false));
        let mut context = UserContext::new()
            .with_metadata(metadata)
            .with_committed_event_source(CommittedEventSource::new(SOURCE, MODEL)?);
        context.insert_resource(CommittedEventTarget::new(SOURCE, MODEL)?);
        context.use_sqlite_provider(executor.clone());
        context.ensure_schema().await?;
        if legacy {
            // Existing database fixture: no captured history is fabricated.
            executor.connection().lock().unwrap().execute_batch(&format!(
                "INSERT INTO customer_order_data(id,version,name,occurred_at,active,note) VALUES({ID},7,'LEGACY',-315619199877,1,NULL);
                 INSERT INTO customer_order_data(id,version,name,occurred_at,active,note) VALUES({},-4,'已删除',0,0,'retained tombstone');", ID+1))?;
        }
        Ok(Self { context, executor })
    }
    async fn import(&self, snapshot: &SnapshotRecord) -> Result<InboxOutcome> {
        Ok(self
            .executor
            .initialize_model_checked_snapshot(
                &self.context,
                snapshot,
                SnapshotImportMode::RequireEmpty,
                limits(),
            )
            .await?)
    }
    fn state(&self) -> Result<Vec<(i64, i64, String, i64, i64, Option<String>)>> {
        let shared = self.executor.connection();
        let connection = shared.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT id,version,name,occurred_at,active,note FROM customer_order_data ORDER BY id",
        )?;
        Ok(statement
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            })?
            .collect::<std::result::Result<_, _>>()?)
    }
}
fn limits() -> SnapshotLimits {
    SnapshotLimits::new(10, 64 * 1024).unwrap()
}
fn hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}
fn tail(sequence: u64) -> String {
    json!({"protocol_version":1,"change_set_id":"snapshot-example-tail","source_runtime_id":SOURCE,
        "source_sequence":sequence.to_string(),"source_model_hash":MODEL,"source_transaction_id":"snapshot-example-tx",
        "committed_at":"2026-10-22T00:00:00Z","audit":{"comment":"advance legacy order","purpose":"verify snapshot tail"},
        "mutations":[{"entity_type":"customer_order","entity_id":ID.to_string(),"operation":"update",
        "base_version":"7","committed_version":"8","fields":{"name":"AFTER SNAPSHOT"},"trace_chain":[]}]}).to_string()
}
async fn commit_fixture_tail(source: &Fixture) -> Result<OutboxRecord> {
    // Normal audited application saves are shown by the existing committed_events example.
    let transaction = source.executor.begin_sql().await?;
    let changed = source.executor.connection().lock().unwrap().execute(
        "UPDATE customer_order_data SET version=8,name='AFTER SNAPSHOT' WHERE id=?1 AND version=7",
        [ID],
    )?;
    assert_eq!(changed, 1);
    let event =
        transaction
            .append_committed_event(SOURCE, "snapshot-example-tail", MODEL, |n| Ok(tail(n)))?;
    transaction.commit_sql().await?;
    Ok(event)
}
fn write(directory: &Path, name: &str, text: &str) -> Result<()> {
    use std::io::Write;
    fs::create_dir_all(directory)?;
    for (extension, bytes) in [("json", text.to_owned()), ("sha256", hash(text))] {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{name}.{extension}")))?
            .write_all(bytes.as_bytes())?;
    }
    Ok(())
}
fn read(directory: &Path, name: &str) -> Result<String> {
    let text = fs::read_to_string(directory.join(format!("{name}.json")))?;
    assert_eq!(
        fs::read_to_string(directory.join(format!("{name}.sha256")))?.trim(),
        hash(&text),
        "exchange digest"
    );
    Ok(text)
}
async fn run() -> Result<()> {
    let input = std::env::var_os("TEAQL_SNAPSHOT_IMPORT_DIR");
    let output = std::env::var_os("TEAQL_SNAPSHOT_EXPORT_DIR");
    assert!(
        !(input.is_some() && output.is_some()),
        "separate producer/consumer roles"
    );
    let source = Fixture::new(true).await?;
    let target = Fixture::new(false).await?;
    let (snapshot, event) = if let Some(ref input) = input {
        let text = read(Path::new(input), "snapshot")?;
        let body: Value = serde_json::from_str(&text)?;
        let snapshot = SnapshotRecord {
            source_runtime_id: SOURCE.into(),
            source_sequence: body["source_sequence"].as_str().unwrap().parse()?,
            snapshot_id: body["snapshot_id"].as_str().unwrap().into(),
            model_hash: MODEL.into(),
            payload_sha256: hash(&text),
            payload_json: text,
        };
        let text = read(Path::new(input), "tail")?;
        let body: Value = serde_json::from_str(&text)?;
        let event = OutboxRecord {
            source_runtime_id: SOURCE.into(),
            source_sequence: body["source_sequence"].as_str().unwrap().parse()?,
            change_set_id: body["change_set_id"].as_str().unwrap().into(),
            model_hash: MODEL.into(),
            payload_sha256: hash(&text),
            payload_json: text,
        };
        (snapshot, event)
    } else {
        let snapshot = source
            .executor
            .export_model_checked_snapshot(
                &source.context,
                "example-snapshot",
                "copy legacy facts",
                "initialize analytics",
                limits(),
            )
            .await?;
        assert_eq!(snapshot.source_sequence, 0);
        let event = commit_fixture_tail(&source).await?;
        if let Some(output) = output {
            write(Path::new(&output), "snapshot", &snapshot.payload_json)?;
            write(Path::new(&output), "tail", &event.payload_json)?;
        }
        (snapshot, event)
    };
    assert_eq!(
        target.import(&snapshot).await?,
        InboxOutcome::Applied { checkpoint: 0 }
    );
    assert_eq!(
        target
            .executor
            .apply_model_checked_committed_set(&target.context, &event)
            .await?,
        InboxOutcome::Applied { checkpoint: 1 }
    );
    assert_eq!(
        target
            .executor
            .apply_model_checked_committed_set(&target.context, &event)
            .await?,
        InboxOutcome::Duplicate { checkpoint: 1 }
    );
    assert_eq!(
        target.import(&snapshot).await?,
        InboxOutcome::Duplicate { checkpoint: 0 }
    );
    assert_eq!(
        target.state()?,
        vec![
            (ID, 8, "AFTER SNAPSHOT".into(), -315619199877, 1, None),
            (
                ID + 1,
                -4,
                "已删除".into(),
                0,
                0,
                Some("retained tombstone".into())
            )
        ]
    );
    {
        let shared = target.executor.connection();
        let db = shared.lock().unwrap();
        assert_eq!(
            db.query_row(
                "SELECT source_sequence FROM teaql_event_checkpoint",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            1
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM empty_probe_data", [], |r| r
                .get::<_, i64>(0))?,
            0
        );
    }
    if input.is_none() {
        assert_eq!(source.state()?, target.state()?);
    }
    println!(
        "CMR11_EXAMPLE_PASS language=rust mode={} snapshot=0 tail=1 rows=2 catalog=2 duplicate_after_tail=1",
        if input.is_some() { "exchange" } else { "local" }
    );
    Ok(())
}
fn main() -> Result<()> {
    futures_executor::block_on(run())
}
