use deadpool_postgres::{Config, Pool};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use teaql_blob_reference::*;
use teaql_core::{BlobFieldSpec, BlobRef, DataType, EntityDescriptor, PropertyDescriptor, Value};
use teaql_data_service::infrastructure_sql::{InfrastructureSql, InfrastructureStatement};
use teaql_provider_postgres::{PgMutationExecutor, PostgresDialect};
use teaql_runtime::InMemoryMetadataStore;
use teaql_sql::SqlDataServiceExecutor;
pub mod proxy;
static NEXT: AtomicUsize = AtomicUsize::new(1);
static MANIFEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
pub type Business =
    SqlDataServiceExecutor<PostgresDialect, PgMutationExecutor, InMemoryMetadataStore>;

pub fn config(port: u16) -> Config {
    let mut config = Config::new();
    config.host = Some("127.0.0.1".into());
    config.port = Some(port);
    config.user = Some("teaql_reference_test".into());
    config.dbname = Some("teaql_reference_test".into());
    config.password = Some(
        std::env::var("TEAQL_BLOB_RUST_PG_PASSWORD")
            .expect("explicit live PostgreSQL password required"),
    );
    config
}
pub fn pool(config: Config) -> Pool {
    config
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .unwrap()
}
pub struct Fixture {
    pub root: Arc<PgMutationExecutor>,
    pub catalog: PostgresBlobCatalog,
    pub schema: String,
    pub admin: Pool,
}
impl Fixture {
    pub async fn new(migrate: bool) -> Self {
        Self::with_port(migrate, 55432, 6).await
    }
    pub async fn with_port(migrate: bool, port: u16, maximum: usize) -> Self {
        let label = std::env::var("TEAQL_BLOB_RUST_RUN_LABEL").expect("live gate label required");
        let hash = format!("{:x}", Sha256::digest(label.as_bytes()));
        let schema = format!(
            "r5_blob_{}_{}_{}",
            &hash[..8],
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        assert!(
            schema
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        let manifest = std::env::var("TEAQL_BLOB_RUST_SCHEMA_MANIFEST")
            .expect("owned schema cleanup manifest required");
        {
            let _guard = MANIFEST_LOCK.lock().unwrap();
            let mut manifest = std::fs::OpenOptions::new()
                .append(true)
                .open(manifest)
                .unwrap();
            manifest
                .write_all(format!("{schema}\n").as_bytes())
                .unwrap();
        }
        let admin = pool(config(55432));
        admin
            .get()
            .await
            .unwrap()
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .await
            .unwrap();
        let mut scoped = config(port);
        scoped.options = Some(format!("-c search_path={schema}"));
        scoped.application_name = Some(schema.clone());
        scoped.pool = Some(deadpool_postgres::PoolConfig::new(maximum));
        let root = Arc::new(PgMutationExecutor::new(pool(scoped)));
        let catalog = PostgresBlobCatalog::new(root.clone()).unwrap();
        execute(root.as_ref(),"CREATE TABLE r5_assets (id BIGINT PRIMARY KEY,version BIGINT NOT NULL,document VARCHAR(128),preview VARCHAR(128))",vec![]).await;
        if migrate {
            catalog.migrate().await.unwrap();
        }
        Self {
            root,
            catalog,
            schema,
            admin,
        }
    }
    pub fn scope(&self) -> BlobScope {
        BlobScope::new("fixture", "tenant-a").unwrap()
    }
    pub fn target(&self, id: u64, field: &str) -> BlobFieldTarget {
        BlobFieldTarget::new("Asset", Some(id), field).unwrap()
    }
    pub fn business(&self) -> Business {
        let descriptor = EntityDescriptor::new("Asset")
            .table_name("r5_assets")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("document", DataType::BlobRef))
            .property(PropertyDescriptor::new("preview", DataType::BlobRef));
        SqlDataServiceExecutor::new(
            PostgresDialect,
            (*self.root).clone(),
            InMemoryMetadataStore::new().with_entity(descriptor),
        )
    }
    pub fn draft(
        &self,
        label: &str,
        target: BlobFieldTarget,
        policy: BlobPolicySnapshot,
    ) -> UploadDraft {
        let reference = BlobRef::new(format!("blob_{}_{label}", self.schema)).unwrap();
        UploadDraft {
            scope: self.scope(),
            reference,
            upload_id: format!("upload_{}_{label}", self.schema),
            request_key: format!("request_{label}"),
            request_digest: format!("{:x}", Sha256::digest(label.as_bytes())),
            original_name: "sample.pdf".into(),
            extension: "pdf".into(),
            storage_location: format!("owned/{}/{label}", self.schema),
            target,
            policy,
        }
    }
    pub async fn upload(
        &self,
        label: &str,
        target: BlobFieldTarget,
        policy: BlobPolicySnapshot,
        now: i64,
    ) -> BlobRef {
        let draft = self.draft(label, target, policy);
        let reference = draft.reference.clone();
        self.catalog.begin_upload(draft, now).await.unwrap();
        let bytes = std::fs::read(
            std::env::var("TEAQL_BLOB_RUST_PDF").expect("actual fixture path required"),
        )
        .unwrap();
        // Trusted verified-content boundary only; real storage and the format verifier are separate gates.
        let content = VerifiedContent {
            content_type: "application/pdf".into(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            content_version: format!("version_{label}"),
        };
        self.catalog
            .complete_verified(&self.scope(), &reference, content, now)
            .await
            .unwrap();
        reference
    }
    pub async fn bind(
        &self,
        target: BlobFieldTarget,
        previous: Option<BlobRef>,
        next: Option<BlobRef>,
        policy: BlobPolicySnapshot,
        now: i64,
    ) {
        let transaction = self.root.begin().await.unwrap();
        let prepared = self
            .catalog
            .prepare_bindings(
                transaction.as_ref(),
                &self.scope(),
                vec![change(target.clone(), previous, next.clone(), policy)],
                now,
            )
            .await
            .unwrap();
        self.write(transaction.as_ref(), &target, next.as_ref())
            .await;
        self.catalog
            .apply_bindings(transaction.as_ref(), prepared, now)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }
    pub async fn write(
        &self,
        sql: &dyn InfrastructureSql,
        target: &BlobFieldTarget,
        key: Option<&BlobRef>,
    ) {
        assert!(matches!(target.field(), "document" | "preview"));
        execute(sql,&format!("INSERT INTO r5_assets(id,version,{0}) VALUES ($1,1,$2) ON CONFLICT (id) DO UPDATE SET {0}=EXCLUDED.{0},version=r5_assets.version+1",target.field()),vec![Value::U64(target.id().unwrap()),key.map_or(Value::Null,|key|Value::Text(key.as_str().into()))]).await;
    }
    pub async fn count(&self, table: &str) -> i64 {
        assert!(
            [
                "r5_assets",
                "teaql_blob_binding",
                "teaql_blob_object",
                "teaql_blob_schema"
            ]
            .contains(&table)
        );
        self.admin
            .get()
            .await
            .unwrap()
            .query_one(
                &format!("SELECT count(*) FROM \"{}\".{table}", self.schema),
                &[],
            )
            .await
            .unwrap()
            .get(0)
    }
    pub async fn wait_for_lock(&self) {
        tokio::time::timeout(Duration::from_secs(10),async {
            loop { let row=self.admin.get().await.unwrap().query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'",&[&self.schema]).await.unwrap();
                if row.get::<_,i64>(0)>0 { break; } tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.expect("actual PostgreSQL lock waiter required");
    }
    pub async fn wait_backend_gone(&self, pid: i64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row = self
                    .admin
                    .get()
                    .await
                    .unwrap()
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE pid=$1::bigint",
                        &[&pid],
                    )
                    .await
                    .unwrap();
                if row.get::<_, i64>(0) == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled physical backend must close");
    }
    pub async fn finish(self) {
        self.root.pool().close();
        self.admin
            .get()
            .await
            .unwrap()
            .batch_execute(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .await
            .unwrap();
        self.admin.close();
    }
}
pub fn policy(
    field: &str,
    minimum: u64,
    unreferenced: u64,
    sharing: Sharing,
) -> BlobPolicySnapshot {
    let mut profile = BlobPolicyProfile::named("fixture");
    profile.overrides.minimum_seconds = Some(minimum);
    profile.overrides.unreferenced_seconds = Some(unreferenced);
    profile.overrides.sharing = Some(sharing);
    BlobPolicies::new(
        PolicyLimits::default(),
        BlobPolicyProfile::named("default"),
        BTreeMap::from([("fixture".into(), profile)]),
        BTreeMap::new(),
    )
    .unwrap()
    .resolve(&BlobFieldSpec::new("Asset", field, ["pdf"], true, "fixture").unwrap())
    .unwrap()
}
pub fn change(
    target: BlobFieldTarget,
    previous: Option<BlobRef>,
    next: Option<BlobRef>,
    policy: BlobPolicySnapshot,
) -> BindingChange {
    BindingChange {
        target,
        previous,
        next,
        expected_binding_version: None,
        policy,
    }
}
pub async fn execute(sql: &dyn InfrastructureSql, query: &str, params: Vec<Value>) -> u64 {
    sql.execute(&InfrastructureStatement::new(query, params).unwrap())
        .await
        .unwrap()
}
pub async fn scalar(sql: &dyn InfrastructureSql, query: &str) -> i64 {
    let rows = sql
        .fetch(&InfrastructureStatement::new(query, vec![]).unwrap())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    rows[0].get("value").and_then(Value::try_i64).unwrap()
}
