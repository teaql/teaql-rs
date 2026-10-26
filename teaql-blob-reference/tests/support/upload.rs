//! Actual SQL lease/journal tests. Storage/verifier integration is a later layer.
use super::*;
use sha2::{Digest, Sha256};
use teaql_core::BlobRef;

async fn initiated(f: &Fixture, label: &str) -> BlobCatalogRecord {
    f.catalog
        .begin_upload(
            f.draft(
                label,
                f.target(42, "document"),
                policy("document", 0, 1, Sharing::AuthorizedSameScope),
            ),
            0,
        )
        .await
        .unwrap()
}
fn written(blob: &BlobCatalogRecord, version: &str) -> StoredBlob {
    let bytes = std::fs::read(std::env::var("TEAQL_BLOB_RUST_PDF").unwrap()).unwrap();
    StoredBlob::new(
        blob.storage_location(),
        version,
        bytes.len() as u64,
        format!("{:x}", Sha256::digest(&bytes)),
    )
    .unwrap()
}
fn verified(content: &StoredBlob) -> VerifiedContent {
    VerifiedContent {
        content_type: "application/pdf".into(),
        size_bytes: content.size_bytes(),
        sha256: content.sha256().into(),
        content_version: content.content_version().into(),
    }
}
async fn backend(f: &Fixture) -> (i64, String) {
    let rows=f.admin.get().await.unwrap().query("SELECT pid::bigint,backend_xid::text FROM pg_stat_activity WHERE application_name=$1 AND state='idle in transaction'",&[&f.schema]).await.unwrap();
    assert_eq!(rows.len(), 1, "exactly one owned lease backend");
    (rows[0].get(0), rows[0].get(1))
}
async fn oracle(
    f: &Fixture,
    key: &BlobRef,
) -> (
    String,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
) {
    let row=f.admin.get().await.unwrap().query_one(&format!("SELECT state,size_bytes,sha256,content_version,actual_mime,xmin::text FROM \"{}\".teaql_blob_object WHERE blob_key=$1",f.schema),&[&key.as_str()]).await.unwrap();
    (
        row.get(0),
        row.get(1),
        row.get(2),
        row.get(3),
        row.get(4),
        row.get(5),
    )
}
fn error<T>(result: BlobResult<T>, expected: BlobErrorCode) {
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("unexpected acceptance"),
    };
    assert_eq!(error.code, expected);
    assert_eq!(
        error.completion,
        Some(PhysicalTransactionOutcome::RolledBack)
    );
}

#[tokio::test]
async fn owned_upload_lookup_is_scoped_and_request_digest_is_authoritative() {
    let f = Fixture::new(true).await;
    let draft = f.draft(
        "lookup",
        f.target(42, "document"),
        policy("document", 0, 1, Sharing::AuthorizedSameScope),
    );
    let request = draft.request_key.clone();
    let digest = draft.request_digest.clone();
    let blob = f.catalog.begin_upload(draft, 0).await.unwrap();
    let found = f
        .catalog
        .find_upload(&f.scope(), blob.upload_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.reference(), blob.reference());
    assert_eq!(
        f.catalog
            .find_upload_request(&f.scope(), &request, &digest)
            .await
            .unwrap()
            .unwrap()
            .reference(),
        blob.reference()
    );
    let other = BlobScope::new("fixture", "tenant-b").unwrap();
    assert!(
        f.catalog
            .find_upload(&other, blob.upload_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.catalog
            .find_upload_request(&other, &request, &"a".repeat(64))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.catalog
            .find_upload_request(&f.scope(), &request, &"a".repeat(64))
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::IdempotencyConflict
    );
    assert_eq!(
        f.catalog
            .find_upload_request(&f.scope(), &request, "invalid")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::ReferenceInvalid
    );
    error(
        f.catalog.lock_upload(&other, blob.reference()).await,
        BlobErrorCode::BindDenied,
    );
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_write_journal_has_one_physical_xid_and_does_not_promote_bytes() {
    let f = Fixture::new(true).await;
    let blob = initiated(&f, "journal").await;
    let content = written(&blob, "generation-v1");
    let before = oracle(&f, blob.reference()).await;
    let lease = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap();
    assert_eq!(format!("{lease:?}"), "BlobUploadLease([redacted])");
    let (pid, xid) = backend(&f).await;
    let lease = lease.record_write(&content, 100).await.unwrap();
    assert_eq!(lease.record().state(), BlobState::Initiated);
    assert_eq!(lease.record().size_bytes(), Some(content.size_bytes()));
    assert_eq!(lease.record().actual_mime(), None);
    assert_eq!(lease.record().completed_at(), None);
    assert_eq!(backend(&f).await, (pid, xid.clone()));
    assert_eq!(
        oracle(&f, blob.reference()).await,
        before,
        "independent connection sees no uncommitted journal"
    );
    let committed = lease.commit().await.unwrap();
    let after = oracle(&f, blob.reference()).await;
    assert_eq!(after.0, "initiated");
    assert_eq!(after.1, Some(content.size_bytes() as i64));
    assert_eq!(after.2.as_deref(), Some(content.sha256()));
    assert_eq!(after.3.as_deref(), Some(content.content_version()));
    assert_eq!(after.4, None);
    assert_eq!(after.5, xid);
    assert_eq!(committed.completed_at(), None);
    let tx = f.root.begin().await.unwrap();
    assert_eq!(
        f.catalog
            .prepare_bindings(
                tx.as_ref(),
                &f.scope(),
                vec![change(
                    f.target(42, "document"),
                    None,
                    Some(blob.reference().clone()),
                    blob.policy().clone()
                )],
                100
            )
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::UploadIncomplete
    );
    assert!(tx.commit().await.is_err());
    assert_eq!(f.count("teaql_blob_binding").await, 0);
    let completed = f
        .catalog
        .complete_verified(&f.scope(), blob.reference(), verified(&content), 101)
        .await
        .unwrap();
    assert_eq!(completed.state(), BlobState::UploadedUnbound);
    assert_eq!(completed.content_version(), Some(content.content_version()));
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_write_replay_and_conflicts_keep_original_facts_and_rollback() {
    let f = Fixture::new(true).await;
    let blob = initiated(&f, "replay-write").await;
    let content = written(&blob, "v1");
    f.catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap()
        .record_write(&content, 100)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let again = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap()
        .record_write(&content, 101)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert_eq!(again.state(), BlobState::Initiated);
    let before = oracle(&f, blob.reference()).await;
    for (bad, expected) in [
        (
            StoredBlob::new(
                "other/location",
                "v1",
                content.size_bytes(),
                content.sha256(),
            )
            .unwrap(),
            BlobErrorCode::ReferenceInvalid,
        ),
        (written(&blob, "v2"), BlobErrorCode::IdempotencyConflict),
        (
            StoredBlob::new(
                content.location(),
                "v1",
                content.size_bytes() + 1,
                content.sha256(),
            )
            .unwrap(),
            BlobErrorCode::IdempotencyConflict,
        ),
        (
            StoredBlob::new(
                content.location(),
                "v1",
                content.size_bytes(),
                "a".repeat(64),
            )
            .unwrap(),
            BlobErrorCode::IdempotencyConflict,
        ),
        (
            StoredBlob::new(
                content.location(),
                "v1",
                blob.policy().max_size_bytes() + 1,
                content.sha256(),
            )
            .unwrap(),
            BlobErrorCode::SizeExceeded,
        ),
    ] {
        error(
            f.catalog
                .lock_upload(&f.scope(), blob.reference())
                .await
                .unwrap()
                .record_write(&bad, 102)
                .await,
            expected,
        );
        assert_eq!(oracle(&f, blob.reference()).await, before);
    }
    error(
        f.catalog
            .lock_upload(&f.scope(), blob.reference())
            .await
            .unwrap()
            .record_write(&content, blob.upload_expires_at())
            .await,
        BlobErrorCode::UploadExpired,
    );
    assert_eq!(oracle(&f, blob.reference()).await, before);
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_completion_compares_the_recorded_generation_size_hash_and_mime() {
    let f = Fixture::new(true).await;
    let blob = initiated(&f, "verify-conflict").await;
    let content = written(&blob, "v1");
    f.catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap()
        .record_write(&content, 100)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let before = oracle(&f, blob.reference()).await;
    for index in 0..4 {
        let mut bad = verified(&content);
        match index {
            0 => bad.content_version = "v2".into(),
            1 => bad.size_bytes += 1,
            2 => bad.sha256 = "a".repeat(64),
            _ => bad.content_type = "image/png".into(),
        }
        error(
            f.catalog
                .complete_verified(&f.scope(), blob.reference(), bad, 101)
                .await,
            if index == 3 {
                BlobErrorCode::FormatNotAllowed
            } else {
                BlobErrorCode::IdempotencyConflict
            },
        );
        assert_eq!(oracle(&f, blob.reference()).await, before);
    }
    let lease = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap()
        .complete_verified(verified(&content), 101)
        .await
        .unwrap();
    assert_eq!(oracle(&f, blob.reference()).await, before);
    lease.rollback().await.unwrap();
    assert_eq!(oracle(&f, blob.reference()).await, before);
    let lease = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap()
        .complete_verified(verified(&content), 102)
        .await
        .unwrap();
    let (_, xid) = backend(&f).await;
    lease.commit().await.unwrap();
    assert_eq!(oracle(&f, blob.reference()).await.5, xid);
    let again = f
        .catalog
        .complete_verified(
            &f.scope(),
            blob.reference(),
            verified(&content),
            blob.upload_expires_at() + 1,
        )
        .await
        .unwrap();
    assert_eq!(again.completed_at(), Some(102));
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_cancellation_discards_backend_rolls_back_journal_and_releases_gc_lock() {
    let f = Fixture::new(true).await;
    let blob = initiated(&f, "cancel").await;
    let content = written(&blob, "v1");
    let lease = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap()
        .record_write(&content, 100)
        .await
        .unwrap();
    let (pid, _) = backend(&f).await;
    let (send, ready) = tokio::sync::oneshot::channel();
    let work = tokio::spawn(async move {
        let _lease = lease;
        send.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    let catalog = f.catalog.clone();
    let scope = f.scope();
    let key = blob.reference().clone();
    let expires = blob.upload_expires_at();
    let gc = tokio::spawn(async move { catalog.claim_deletion(&scope, &key, expires).await });
    f.wait_for_lock().await;
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    f.wait_backend_gone(pid).await;
    let claimed = tokio::time::timeout(std::time::Duration::from_secs(10), gc)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(claimed.state(), BlobState::Deleting);
    assert_eq!(claimed.content_version(), None);
    assert_eq!(claimed.sha256(), None);
    assert_eq!(oracle(&f, blob.reference()).await.3, None);
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_cancelling_the_consuming_update_future_cannot_leave_a_committable_handle() {
    let f = Fixture::new(true).await;
    let blob = initiated(&f, "cancel-update").await;
    let content = written(&blob, "v1");
    // The trigger blocks the actual UPDATE after the lease has been consumed.
    f.admin.get().await.unwrap().batch_execute(&format!("CREATE FUNCTION \"{0}\".hold_upload_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(73402110319407); RETURN NEW; END $$; CREATE TRIGGER hold_upload_write BEFORE UPDATE ON \"{0}\".teaql_blob_object FOR EACH ROW EXECUTE FUNCTION \"{0}\".hold_upload_write()",f.schema)).await.unwrap();
    let lease = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap();
    let (pid, _) = backend(&f).await;
    let blocker = f.root.begin().await.unwrap();
    scalar(
        blocker.as_ref(),
        "SELECT 1::bigint AS value FROM pg_advisory_xact_lock(73402110319407)",
    )
    .await;
    let work = tokio::spawn(async move { lease.record_write(&content, 100).await });
    f.wait_for_lock().await;
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    f.wait_backend_gone(pid).await;
    blocker.rollback().await.unwrap();
    let after = oracle(&f, blob.reference()).await;
    assert_eq!(after.0, "initiated");
    assert_eq!(after.1, None);
    assert_eq!(after.3, None);
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_concurrent_confirmation_waits_and_rejects_a_stale_generation() {
    let f = Fixture::new(true).await;
    let blob = initiated(&f, "confirm-race").await;
    let content = written(&blob, "v1");
    let lease = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap();
    let catalog = f.catalog.clone();
    let scope = f.scope();
    let key = blob.reference().clone();
    let stale = verified(&written(&blob, "v2"));
    let confirm =
        tokio::spawn(async move { catalog.complete_verified(&scope, &key, stale, 101).await });
    f.wait_for_lock().await;
    lease
        .record_write(&content, 100)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    error(
        tokio::time::timeout(std::time::Duration::from_secs(10), confirm)
            .await
            .unwrap()
            .unwrap(),
        BlobErrorCode::IdempotencyConflict,
    );
    let after = oracle(&f, blob.reference()).await;
    assert_eq!(after.0, "initiated");
    assert_eq!(after.3.as_deref(), Some("v1"));
    assert_eq!(after.4, None);
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_commit_ack_loss_keeps_unknown_while_sql_oracle_proves_recorded_bytes() {
    let mut proxy = support::proxy::ProtocolFault::new().await;
    let f = Fixture::with_port(true, proxy.port, 1).await;
    let blob = initiated(&f, "write-ackloss").await;
    let content = written(&blob, "v1");
    let lease = f
        .catalog
        .lock_upload(&f.scope(), blob.reference())
        .await
        .unwrap()
        .record_write(&content, 100)
        .await
        .unwrap();
    proxy.drop_commit_ack();
    let result = lease.commit().await;
    proxy.intercepted().await;
    let error = result.unwrap_err();
    assert_eq!(error.code, BlobErrorCode::TransactionRequired);
    assert_eq!(error.completion, Some(PhysicalTransactionOutcome::Unknown));
    let after = oracle(&f, blob.reference()).await;
    assert_eq!(after.0, "initiated");
    assert_eq!(after.3.as_deref(), Some("v1"));
    assert_eq!(after.4, None);
    f.finish().await;
    drop(proxy);
}

#[tokio::test]
async fn owned_upload_readiness_is_read_only_and_disconnect_setting_is_transaction_local() {
    let f = Fixture::with_port(true, 55432, 1).await;
    let blob = initiated(&f, "local-setting").await;
    let pid = scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
    let setting = InfrastructureStatement::new(
        "SELECT current_setting('client_connection_check_interval') AS value",
        vec![],
    )
    .unwrap();
    let before = f.root.fetch(&setting).await.unwrap();
    let before = before[0].get("value").cloned().unwrap();
    f.catalog.validate_upload_ready().await.unwrap();
    for rollback in [false, true] {
        let lease = f
            .catalog
            .lock_upload(&f.scope(), blob.reference())
            .await
            .unwrap()
            .record_write(&written(&blob, "v1"), 100)
            .await
            .unwrap();
        if rollback {
            lease.rollback().await.unwrap();
        } else {
            lease.commit().await.unwrap();
        }
        assert_eq!(
            scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await,
            pid
        );
        let after = f.root.fetch(&setting).await.unwrap();
        assert_eq!(after[0].get("value"), Some(&before));
    }
    assert_eq!(f.count("teaql_blob_object").await, 1);
    f.finish().await;
}

#[tokio::test]
async fn owned_upload_corrupt_unverified_journals_are_not_repaired_or_promoted() {
    let f = Fixture::new(true).await;
    for (index, assignment) in [
        "content_version=NULL",
        "sha256='invalid'",
        "size_bytes=NULL",
        "actual_mime='application/pdf'",
    ]
    .into_iter()
    .enumerate()
    {
        let blob = initiated(&f, &format!("corrupt-write-{index}")).await;
        let content = written(&blob, "v1");
        f.catalog
            .lock_upload(&f.scope(), blob.reference())
            .await
            .unwrap()
            .record_write(&content, 100)
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        execute(
            f.root.as_ref(),
            &format!("UPDATE teaql_blob_object SET {assignment} WHERE blob_key=$1"),
            vec![blob.reference().as_str().into()],
        )
        .await;
        assert_eq!(
            f.catalog
                .find_upload(&f.scope(), blob.upload_id())
                .await
                .unwrap_err()
                .code,
            BlobErrorCode::CatalogFailed
        );
        error(
            f.catalog.lock_upload(&f.scope(), blob.reference()).await,
            BlobErrorCode::CatalogFailed,
        );
        error(
            f.catalog
                .complete_verified(&f.scope(), blob.reference(), verified(&content), 101)
                .await,
            BlobErrorCode::CatalogFailed,
        );
        assert_eq!(oracle(&f, blob.reference()).await.0, "initiated");
    }
    f.finish().await;
}
