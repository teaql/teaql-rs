#![cfg(feature = "live-postgres")]

use deadpool_postgres::Config;
use std::sync::Arc;
use teaql_blob_reference::{
    BindingChange, BindingChangeKind, BlobErrorCode, BlobFieldTarget, BlobPolicies, BlobScope,
    BlobState, PostgresBlobCatalog, UploadDraft, VerifiedContent,
};
use teaql_core::{BlobFieldSpec, BlobRef, Value};
use teaql_data_service::infrastructure_sql::{InfrastructureSql, InfrastructureStatement};
use teaql_provider_postgres::PgMutationExecutor;

/// The conformance runner calls this once before Java and once after it, in separate databases.
#[tokio::test]
async fn current_catalog_can_hand_off_to_or_from_java() {
    let phase = match std::env::var("TEAQL_R7_HANDOFF_PHASE") {
        Ok(value) if value.starts_with("rust-") => value,
        _ => return,
    };
    assert!(phase == "rust-first" || phase == "rust-second");
    let mut config = Config::new();
    config.host = Some("127.0.0.1".into());
    config.port = Some(std::env::var("TEAQL_R7_PG_PORT").unwrap().parse().unwrap());
    config.user = Some("teaql_r7_test".into());
    config.dbname = Some(std::env::var("TEAQL_R7_DB_NAME").unwrap());
    config.password = Some(std::env::var("TEAQL_BLOB_RUST_PG_PASSWORD").unwrap());
    let pool = config
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .unwrap();
    let root = Arc::new(PgMutationExecutor::new(pool.clone()));
    let catalog = PostgresBlobCatalog::new(root.clone()).unwrap();
    let client = pool.get().await.unwrap();
    if phase == "rust-first" {
        client.batch_execute("CREATE TABLE asset_data(id BIGINT PRIMARY KEY,version BIGINT NOT NULL,document VARCHAR(128));
            INSERT INTO asset_data VALUES (1,1,'legacy-invoice.pdf'),(2,1,'https://legacy.example.test/files/2.pdf')")
            .await.unwrap();
        assert_eq!(
            catalog.validate_schema().await.unwrap_err().code,
            BlobErrorCode::SchemaMissing
        );
    }
    catalog.migrate().await.unwrap();
    catalog.validate_schema().await.unwrap();
    let old = client
        .query("SELECT document FROM asset_data ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(
        old.iter()
            .map(|row| row.get::<_, &str>(0))
            .collect::<Vec<_>>(),
        vec![
            "legacy-invoice.pdf",
            "https://legacy.example.test/files/2.pdf"
        ]
    );
    let binding_count: i64 = client
        .query_one("SELECT count(*) FROM teaql_blob_binding", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(binding_count, 0);
    let scope = BlobScope::new("test", "tenant-a").unwrap();
    if phase == "rust-second" {
        let foreign = catalog
            .find(&scope, &BlobRef::new("blob_r7_java").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(foreign.state(), BlobState::UploadedUnbound);
        assert_eq!(foreign.original_name(), "sample.pdf");
        assert_eq!(foreign.size_bytes(), Some(1));
        assert_eq!(foreign.actual_mime(), Some("application/pdf"));
        let target = BlobFieldTarget::new("Asset", Some(4), "document").unwrap();
        let tx = root.begin().await.unwrap();
        let prepared = catalog
            .prepare_bindings(
                tx.as_ref(),
                &scope,
                vec![BindingChange {
                    target,
                    previous: None,
                    next: Some(foreign.reference().clone()),
                    expected_binding_version: Some(0),
                    policy: foreign.policy().clone(),
                    kind: BindingChangeKind::FieldWrite,
                }],
                1_000_002,
            )
            .await
            .unwrap();
        tx.execute(
            &InfrastructureStatement::new(
                "INSERT INTO asset_data(id,version,document) VALUES (4,1,$1)",
                vec![Value::Text(foreign.reference().as_str().into())],
            )
            .unwrap(),
        )
        .await
        .unwrap();
        catalog
            .apply_bindings(tx.as_ref(), prepared, 1_000_002)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let field = BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap();
    let policy = BlobPolicies::default().resolve(&field).unwrap();
    let reference = BlobRef::new("blob_r7_rust").unwrap();
    let draft = UploadDraft {
        scope: scope.clone(),
        reference: reference.clone(),
        upload_id: "upload_r7_rust".into(),
        request_key: "request_r7_rust".into(),
        request_digest: "c".repeat(64),
        original_name: "sample.pdf".into(),
        extension: "pdf".into(),
        storage_location: "owned/rust".into(),
        target: BlobFieldTarget::new("Asset", None, "document").unwrap(),
        policy,
    };
    assert_eq!(
        catalog
            .begin_upload(draft, 1_000_000)
            .await
            .unwrap()
            .state(),
        BlobState::Initiated
    );
    let complete = catalog
        .complete_verified(
            &scope,
            &reference,
            VerifiedContent {
                content_type: "application/pdf".into(),
                size_bytes: 1,
                sha256: "d".repeat(64),
                content_version: "version-rust".into(),
            },
            1_000_001,
        )
        .await
        .unwrap();
    assert_eq!(complete.state(), BlobState::UploadedUnbound);
    assert_eq!(
        catalog
            .find(&scope, &reference)
            .await
            .unwrap()
            .unwrap()
            .content_version(),
        Some("version-rust")
    );
    pool.close();
}
