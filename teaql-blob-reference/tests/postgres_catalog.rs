#![cfg(feature = "live-postgres")]
#[path = "support/completion.rs"]
mod completion;
#[cfg(feature = "runtime-integration")]
#[path = "support/dynamic_fields.rs"]
mod dynamic_fields;
#[cfg(feature = "runtime-integration")]
#[path = "support/runtime_journal.rs"]
mod runtime_journal;
#[cfg(feature = "runtime-integration")]
#[path = "support/saved_hydration.rs"]
mod saved_hydration;
#[cfg(feature = "runtime-integration")]
#[path = "support/service.rs"]
mod service;
mod support;
#[path = "support/upload.rs"]
mod upload;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use support::*;
use teaql_blob_reference::*;
use teaql_core::InsertCommand;
use teaql_data_service::infrastructure_sql::*;
use teaql_data_service::{DataServiceExecutor, MutationCommand, MutationExecutor, MutationRequest};

struct Counted {
    inner: Arc<dyn InfrastructureSql>,
    calls: Arc<AtomicUsize>,
}

#[tokio::test]
async fn corrupt_completed_version_digest_or_retention_never_enters_a_binding_or_gc_claim() {
    for (index, assignment) in [
        "content_version=NULL",
        "sha256='invalid'",
        "effective_unref_seconds=0",
    ]
    .into_iter()
    .enumerate()
    {
        let f = Fixture::new(true).await;
        let p = policy("document", 0, 2, Sharing::AuthorizedSameScope);
        let target = f.target(42, "document");
        let key = f
            .upload(&format!("corrupt{index}"), target.clone(), p.clone(), 0)
            .await;
        execute(
            f.root.as_ref(),
            &format!("UPDATE teaql_blob_object SET {assignment}"),
            vec![],
        )
        .await;
        assert_eq!(
            f.catalog.find(&f.scope(), &key).await.unwrap_err().code,
            BlobErrorCode::CatalogFailed
        );
        let tx = f.root.begin().await.unwrap();
        assert_eq!(
            f.catalog
                .prepare_bindings(
                    tx.as_ref(),
                    &f.scope(),
                    vec![change(target.clone(), None, Some(key.clone()), p)],
                    100
                )
                .await
                .unwrap_err()
                .code,
            BlobErrorCode::CatalogFailed
        );
        f.write(tx.as_ref(), &target, None).await;
        assert!(tx.commit().await.is_err());
        assert_eq!(f.count("r5_assets").await, 0);
        assert_eq!(
            f.catalog
                .claim_deletion(&f.scope(), &key, i64::MAX)
                .await
                .unwrap_err()
                .code,
            BlobErrorCode::CatalogFailed
        );
        f.finish().await;
    }
}
impl InfrastructureSql for Counted {
    fn database_kind(&self) -> &'static str {
        self.inner.database_kind()
    }
    fn resource_identity(&self) -> SqlResourceIdentity {
        self.inner.resource_identity()
    }
    fn transaction_identity(&self) -> Option<PhysicalTransactionIdentity> {
        self.inner.transaction_identity()
    }
    fn mark_rollback_only(&self) -> Result<(), InfrastructureSqlError> {
        self.inner.mark_rollback_only()
    }
    fn fetch<'a>(
        &'a self,
        s: &'a InfrastructureStatement,
    ) -> InfrastructureSqlFuture<'a, Vec<teaql_core::CompactRow>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.fetch(s)
    }
    fn execute<'a>(&'a self, s: &'a InfrastructureStatement) -> InfrastructureSqlFuture<'a, u64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(s)
    }
    fn begin(&self) -> InfrastructureSqlFuture<'_, Box<dyn InfrastructureTransaction>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.begin()
    }
}

#[tokio::test]
async fn construction_is_passive_and_fixed_migration_is_explicit_idempotent_and_fail_closed() {
    let f = Fixture::new(false).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let catalog = PostgresBlobCatalog::new(Arc::new(Counted {
        inner: f.root.clone(),
        calls: calls.clone(),
    }))
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        catalog.validate_schema().await.unwrap_err().code,
        BlobErrorCode::SchemaMissing
    );
    catalog.migrate().await.unwrap();
    catalog.migrate().await.unwrap();
    assert_eq!(f.count("teaql_blob_schema").await, 1);
    execute(
        f.root.as_ref(),
        "UPDATE teaql_blob_schema SET schema_digest='invalid'",
        vec![],
    )
    .await;
    assert_eq!(
        catalog.migrate().await.unwrap_err().code,
        BlobErrorCode::SchemaMissing
    );
    f.finish().await;
}

#[tokio::test]
async fn missing_required_binding_constraint_is_not_repaired_by_readiness_or_migrate() {
    let f = Fixture::new(true).await;
    execute(
        f.root.as_ref(),
        "ALTER TABLE teaql_blob_binding DROP CONSTRAINT teaql_blob_binding_fk",
        vec![],
    )
    .await;
    assert_eq!(
        f.catalog.validate_schema().await.unwrap_err().code,
        BlobErrorCode::SchemaMissing
    );
    assert_eq!(
        f.catalog.migrate().await.unwrap_err().code,
        BlobErrorCode::SchemaMissing
    );
    f.finish().await;
}

#[tokio::test]
async fn native_sql_wrapper_and_blob_binding_commit_with_identical_physical_xids() {
    let f = Fixture::new(true).await;
    let p = policy("document", 0, 2, Sharing::AuthorizedSameScope);
    let key = f
        .upload("native", f.target(42, "document"), p.clone(), 0)
        .await;
    let business = f.business();
    let tx = teaql_data_service::TransactionExecutor::begin(&business)
        .await
        .unwrap();
    let sql = tx.infrastructure_sql().unwrap();
    let identity = sql.transaction_identity().unwrap();
    assert_eq!(identity.resource(), f.catalog.resource_identity());
    let xid = scalar(sql, "SELECT txid_current() AS value").await;
    let prepared = f
        .catalog
        .prepare_bindings(
            sql,
            &f.scope(),
            vec![change(f.target(42, "document"), None, Some(key.clone()), p)],
            100,
        )
        .await
        .unwrap();
    let request = MutationRequest::new(
        MutationCommand::Insert(
            InsertCommand::new("Asset")
                .value("id", 42_u64)
                .value("version", 1_i64)
                .value("document", key.clone()),
        ),
        "what: insert native Blob key; why: verify one physical transaction",
    )
    .unwrap();
    tx.mutate(request).await.unwrap();
    f.catalog.apply_bindings(sql, prepared, 100).await.unwrap();
    assert_eq!(scalar(sql, "SELECT txid_current() AS value").await, xid);
    assert_eq!(f.count("r5_assets").await, 0);
    assert_eq!(f.count("teaql_blob_binding").await, 0);
    teaql_data_service::Transaction::commit(tx).await.unwrap();
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
    let statement = format!(
        "SELECT a.document,b.blob_key,a.xmin::text::bigint AS native_xid,b.xmin::text::bigint AS binding_xid,o.xmin::text::bigint AS catalog_xid FROM \"{0}\".r5_assets a JOIN \"{0}\".teaql_blob_binding b ON b.entity_id=a.id JOIN \"{0}\".teaql_blob_object o ON o.blob_key=b.blob_key",
        f.schema
    );
    let rows = f
        .admin
        .get()
        .await
        .unwrap()
        .query(&statement, &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("document"), key.as_str());
    assert_eq!(rows[0].get::<_, String>("blob_key"), key.as_str());
    for column in ["native_xid", "binding_xid", "catalog_xid"] {
        assert_eq!(rows[0].get::<_, i64>(column), xid);
    }
    println!("R5_NATIVE_CATALOG_SAME_PHYSICAL_XID_PASS xid={xid}");
    f.finish().await;
}

#[tokio::test]
async fn rollback_preserves_original_business_key_binding_and_upload_state() {
    let f = Fixture::new(true).await;
    let p = policy("document", 0, 2, Sharing::AuthorizedSameScope);
    let target = f.target(42, "document");
    let first = f.upload("old", target.clone(), p.clone(), 0).await;
    let next = f.upload("new", target.clone(), p.clone(), 0).await;
    f.bind(target.clone(), None, Some(first.clone()), p.clone(), 100)
        .await;
    let tx = f.root.begin().await.unwrap();
    let token = tx.transaction_identity().unwrap();
    let prepared = f
        .catalog
        .prepare_bindings(
            tx.as_ref(),
            &f.scope(),
            vec![change(
                target.clone(),
                Some(first.clone()),
                Some(next.clone()),
                p,
            )],
            200,
        )
        .await
        .unwrap();
    f.write(tx.as_ref(), &target, Some(&next)).await;
    f.catalog
        .apply_bindings(tx.as_ref(), prepared, 200)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(token.outcome(), PhysicalTransactionOutcome::RolledBack);
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(first)
    );
    assert_eq!(
        f.catalog
            .find(&f.scope(), &next)
            .await
            .unwrap()
            .unwrap()
            .state(),
        BlobState::UploadedUnbound
    );
    let row = f
        .admin
        .get()
        .await
        .unwrap()
        .query_one(
            &format!("SELECT version FROM \"{}\".r5_assets WHERE id=42", f.schema),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    f.finish().await;
}

#[tokio::test]
async fn prepare_rejects_nontransaction_wrong_resource_and_different_physical_owner_before_io() {
    let f = Fixture::new(true).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Counted {
        inner: f.root.clone(),
        calls: calls.clone(),
    };
    assert_eq!(
        f.catalog
            .prepare_bindings(&counted, &f.scope(), vec![], 0)
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::TransactionRequired
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let wrong = teaql_provider_postgres::PgMutationExecutor::new(f.root.pool());
    let wrong = wrong.begin().await.unwrap();
    let wrong: Arc<dyn InfrastructureTransaction> = Arc::from(wrong);
    let inner: Arc<dyn InfrastructureSql> = wrong;
    let counted = Counted {
        inner,
        calls: calls.clone(),
    };
    assert_eq!(
        f.catalog
            .prepare_bindings(&counted, &f.scope(), vec![], 0)
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::TransactionRequired
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(counted);
    let first = f.root.begin().await.unwrap();
    let prepared = f
        .catalog
        .prepare_bindings(first.as_ref(), &f.scope(), vec![], 0)
        .await
        .unwrap();
    let second = f.root.begin().await.unwrap();
    let second: Arc<dyn InfrastructureTransaction> = Arc::from(second);
    let inner: Arc<dyn InfrastructureSql> = second;
    let counted = Counted {
        inner,
        calls: calls.clone(),
    };
    assert_eq!(
        f.catalog
            .apply_bindings(&counted, prepared, 0)
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::TransactionRequired
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(counted);
    first.rollback().await.unwrap();
    f.finish().await;
}

#[tokio::test]
async fn expired_unbound_and_logical_binding_errors_make_the_borrowed_transaction_rollback_only() {
    let f = Fixture::new(true).await;
    let p = policy("document", 0, 0, Sharing::AuthorizedSameScope);
    let target = f.target(42, "document");
    let key = f.upload("expired", target.clone(), p.clone(), 0).await;
    let tx = f.root.begin().await.unwrap();
    let token = tx.transaction_identity().unwrap();
    let error = f
        .catalog
        .prepare_bindings(
            tx.as_ref(),
            &f.scope(),
            vec![change(target.clone(), None, Some(key), p)],
            86_400_000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, BlobErrorCode::ReferenceExpired);
    f.write(tx.as_ref(), &target, None).await;
    assert!(tx.commit().await.is_err());
    assert_eq!(token.outcome(), PhysicalTransactionOutcome::RolledBack);
    assert_eq!(f.count("r5_assets").await, 0);
    assert_eq!(f.count("teaql_blob_binding").await, 0);
    f.finish().await;
}

#[tokio::test]
async fn a_caught_postgres_statement_failure_cannot_turn_aborted_commit_into_success() {
    let f = Fixture::new(true).await;
    let tx = f.root.begin().await.unwrap();
    let token = tx.transaction_identity().unwrap();
    execute(
        tx.as_ref(),
        "INSERT INTO r5_assets(id,version) VALUES (42,1)",
        vec![],
    )
    .await;
    let statement = InfrastructureStatement::new("SELECT 1/0 AS invalid", vec![]).unwrap();
    assert!(tx.fetch(&statement).await.is_err());
    assert_eq!(
        tx.commit().await.unwrap_err(),
        InfrastructureSqlError::StatementFailed
    );
    assert_eq!(token.outcome(), PhysicalTransactionOutcome::RolledBack);
    assert_eq!(f.count("r5_assets").await, 0);
    f.finish().await;
}

#[tokio::test]
async fn replace_clear_tombstones_and_delayed_apply_keep_full_minimum_retention() {
    let f = Fixture::new(true).await;
    let p = policy("document", 10, 0, Sharing::AuthorizedSameScope);
    let target = f.target(42, "document");
    let key = f.upload("delayed", target.clone(), p.clone(), 0).await;
    let tx = f.root.begin().await.unwrap();
    let prepared = f
        .catalog
        .prepare_bindings(
            tx.as_ref(),
            &f.scope(),
            vec![change(target.clone(), None, Some(key.clone()), p.clone())],
            100,
        )
        .await
        .unwrap();
    f.write(tx.as_ref(), &target, Some(&key)).await;
    f.catalog
        .apply_bindings(tx.as_ref(), prepared, 5000)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let binding = f
        .catalog
        .binding(&f.scope(), &target)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.bound_at, Some(5000));
    assert_eq!(binding.retain_until, Some(15000));
    f.bind(target.clone(), Some(key.clone()), None, p, 6000)
        .await;
    let binding = f
        .catalog
        .binding(&f.scope(), &target)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.version, 2);
    assert_eq!(binding.retired_reference, Some(key.clone()));
    assert!(binding.reference.is_none());
    assert!(
        f.catalog
            .claim_deletion(&f.scope(), &key, 14999)
            .await
            .unwrap()
            .is_none()
    );
    let claimed = f
        .catalog
        .claim_deletion(&f.scope(), &key, 15000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.state(), BlobState::Deleting);
    assert_eq!(
        f.catalog
            .acknowledge_deleted(&f.scope(), &key, Some("wrong".into()), 15000)
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::IdempotencyConflict
    );
    f.catalog
        .acknowledge_deleted(
            &f.scope(),
            &key,
            claimed.content_version().map(str::to_owned),
            15000,
        )
        .await
        .unwrap();
    f.catalog
        .acknowledge_deleted(
            &f.scope(),
            &key,
            claimed.content_version().map(str::to_owned),
            15000,
        )
        .await
        .unwrap();
    f.finish().await;
}

#[tokio::test]
async fn shared_fields_keep_the_longest_accepted_retention_after_profile_reduction() {
    let f = Fixture::new(true).await;
    let short = policy("document", 5, 2, Sharing::AuthorizedSameScope);
    let longer = policy("preview", 10, 30, Sharing::AuthorizedSameScope);
    let target = f.target(42, "document");
    let preview = f.target(42, "preview");
    let key = f.upload("shared", target.clone(), short.clone(), 0).await;
    f.bind(target.clone(), None, Some(key.clone()), short.clone(), 1000)
        .await;
    f.bind(preview.clone(), None, Some(key.clone()), longer, 2000)
        .await;
    assert!(
        f.catalog
            .claim_deletion(&f.scope(), &key, i64::MAX)
            .await
            .unwrap()
            .is_none()
    );
    f.bind(target, Some(key.clone()), None, short, 3000).await;
    f.bind(
        preview,
        Some(key.clone()),
        None,
        policy("preview", 0, 0, Sharing::AuthorizedSameScope),
        4000,
    )
    .await;
    let blob = f.catalog.find(&f.scope(), &key).await.unwrap().unwrap();
    assert_eq!(blob.effective_unreferenced_seconds(), 30);
    assert_eq!(blob.retention_floor_at(), Some(12000));
    assert_eq!(blob.unreferenced_at(), Some(4000));
    assert!(
        f.catalog
            .claim_deletion(&f.scope(), &key, 33999)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.catalog
            .claim_deletion(&f.scope(), &key, 34000)
            .await
            .unwrap()
            .is_some()
    );
    f.finish().await;
}

#[tokio::test]
async fn single_binding_net_swaps_succeed_but_net_sharing_is_rejected() {
    let f = Fixture::new(true).await;
    let p = policy("document", 0, 2, Sharing::SingleBinding);
    let a = f.target(42, "document");
    let b = f.target(43, "document");
    let first = f.upload("singlea", a.clone(), p.clone(), 0).await;
    let second = f.upload("singleb", b.clone(), p.clone(), 0).await;
    f.bind(a.clone(), None, Some(first.clone()), p.clone(), 100)
        .await;
    f.bind(b.clone(), None, Some(second.clone()), p.clone(), 100)
        .await;
    let tx = f.root.begin().await.unwrap();
    let prepared = f
        .catalog
        .prepare_bindings(
            tx.as_ref(),
            &f.scope(),
            vec![
                change(
                    a.clone(),
                    Some(first.clone()),
                    Some(second.clone()),
                    p.clone(),
                ),
                change(
                    b.clone(),
                    Some(second.clone()),
                    Some(first.clone()),
                    p.clone(),
                ),
            ],
            200,
        )
        .await
        .unwrap();
    f.write(tx.as_ref(), &a, Some(&second)).await;
    f.write(tx.as_ref(), &b, Some(&first)).await;
    f.catalog
        .apply_bindings(tx.as_ref(), prepared, 200)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = f.root.begin().await.unwrap();
    assert_eq!(
        f.catalog
            .prepare_bindings(
                tx.as_ref(),
                &f.scope(),
                vec![change(
                    f.target(44, "document"),
                    None,
                    Some(first.clone()),
                    p
                )],
                300
            )
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::BindDenied
    );
    assert!(tx.commit().await.is_err());
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &a)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(second)
    );
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &b)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(first)
    );
    f.finish().await;
}

#[tokio::test]
async fn binding_lock_wins_against_a_real_waiting_collection_and_reverse_claim_denies_rebind() {
    let f = Fixture::new(true).await;
    let p = policy("document", 0, 1, Sharing::AuthorizedSameScope);
    let target = f.target(42, "document");
    let key = f.upload("race", target.clone(), p.clone(), 0).await;
    f.bind(target.clone(), None, Some(key.clone()), p.clone(), 100)
        .await;
    f.bind(target.clone(), Some(key.clone()), None, p.clone(), 200)
        .await;
    // Rebind was authorized just before the deadline; collection observes the later clock.
    let tx = f.root.begin().await.unwrap();
    let prepared = f
        .catalog
        .prepare_bindings(
            tx.as_ref(),
            &f.scope(),
            vec![change(target.clone(), None, Some(key.clone()), p.clone())],
            1199,
        )
        .await
        .unwrap();
    let catalog = f.catalog.clone();
    let scope = f.scope();
    let candidate = key.clone();
    let collector =
        tokio::spawn(async move { catalog.claim_deletion(&scope, &candidate, 1200).await });
    f.wait_for_lock().await;
    f.write(tx.as_ref(), &target, Some(&key)).await;
    f.catalog
        .apply_bindings(tx.as_ref(), prepared, 1199)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(collector.await.unwrap().unwrap().is_none());
    f.bind(target.clone(), Some(key.clone()), None, p.clone(), 1300)
        .await;
    assert!(
        f.catalog
            .claim_deletion(&f.scope(), &key, 2300)
            .await
            .unwrap()
            .is_some()
    );
    let tx = f.root.begin().await.unwrap();
    assert_eq!(
        f.catalog
            .prepare_bindings(
                tx.as_ref(),
                &f.scope(),
                vec![change(target, None, Some(key), p)],
                2299
            )
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::ReferenceExpired
    );
    tx.rollback().await.unwrap();
    println!("R5_ACTUAL_BIND_GC_LOCK_WAITER_PASS");
    f.finish().await;
}

#[tokio::test]
async fn upload_replay_snapshot_scope_and_completion_remain_authoritative() {
    let f = Fixture::new(true).await;
    let p = policy("document", 0, 1, Sharing::AuthorizedSameScope);
    let target = f.target(42, "document");
    let original = f
        .catalog
        .begin_upload(f.draft("replay", target.clone(), p.clone()), 0)
        .await
        .unwrap();
    let again = f
        .catalog
        .begin_upload(f.draft("replay", target.clone(), p.clone()), 1000)
        .await
        .unwrap();
    assert_eq!(original.upload_expires_at(), again.upload_expires_at());
    let mut conflict = f.draft("replay", target.clone(), p.clone());
    conflict.request_digest = "a".repeat(64);
    assert_eq!(
        f.catalog.begin_upload(conflict, 0).await.unwrap_err().code,
        BlobErrorCode::IdempotencyConflict
    );
    let key = f.upload("valid", target.clone(), p.clone(), 0).await;
    assert!(
        f.catalog
            .find(&BlobScope::new("fixture", "tenant-b").unwrap(), &key)
            .await
            .unwrap()
            .is_none()
    );
    let mut change = change(target, None, Some(key), p);
    change.expected_binding_version = Some(3);
    let tx = f.root.begin().await.unwrap();
    assert_eq!(
        f.catalog
            .prepare_bindings(tx.as_ref(), &f.scope(), vec![change], 100)
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::IdempotencyConflict
    );
    tx.rollback().await.unwrap();
    f.finish().await;
}

#[tokio::test]
async fn dropping_an_active_transaction_discards_its_backend_and_rolls_back_all_rows() {
    let f = Fixture::with_port(true, 55432, 1).await;
    let tx = f.root.begin().await.unwrap();
    let token = tx.transaction_identity().unwrap();
    let pid = scalar(tx.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
    execute(
        tx.as_ref(),
        "INSERT INTO r5_assets(id,version) VALUES (42,1)",
        vec![],
    )
    .await;
    drop(tx);
    assert_eq!(token.outcome(), PhysicalTransactionOutcome::Unknown);
    let next = scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
    assert_ne!(next, pid);
    f.wait_backend_gone(pid).await;
    assert_eq!(f.count("r5_assets").await, 0);
    f.finish().await;
}

#[tokio::test]
async fn cancellation_after_server_begin_before_ack_cannot_return_a_dirty_connection_to_pool() {
    let mut proxy = support::proxy::ProtocolFault::new().await;
    let f = Fixture::with_port(true, proxy.port, 1).await;
    let before = scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
    proxy.hold_begin_ack();
    let root = f.root.clone();
    let task = tokio::spawn(async move { root.begin().await });
    proxy.intercepted().await;
    task.abort();
    assert!(matches!(task.await,Err(error) if error.is_cancelled()));
    let next = scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
    assert_ne!(before, next);
    f.wait_backend_gone(before).await;
    assert_eq!(f.count("r5_assets").await, 0);
    f.finish().await;
    drop(proxy);
}

#[tokio::test]
async fn actual_commit_ack_loss_is_unknown_while_independent_sql_proves_atomic_commit() {
    let mut proxy = support::proxy::ProtocolFault::new().await;
    let f = Fixture::with_port(true, proxy.port, 1).await;
    let p = policy("document", 0, 2, Sharing::AuthorizedSameScope);
    let target = f.target(42, "document");
    let key = f.upload("ackloss", target.clone(), p.clone(), 0).await;
    let tx = f.root.begin().await.unwrap();
    let token = tx.transaction_identity().unwrap();
    let prepared = f
        .catalog
        .prepare_bindings(
            tx.as_ref(),
            &f.scope(),
            vec![change(target.clone(), None, Some(key.clone()), p)],
            100,
        )
        .await
        .unwrap();
    f.write(tx.as_ref(), &target, Some(&key)).await;
    f.catalog
        .apply_bindings(tx.as_ref(), prepared, 100)
        .await
        .unwrap();
    proxy.drop_commit_ack();
    assert_eq!(
        tx.commit().await.unwrap_err(),
        InfrastructureSqlError::TransactionUnknown
    );
    proxy.intercepted().await;
    assert_eq!(token.outcome(), PhysicalTransactionOutcome::Unknown);
    assert_eq!(f.count("r5_assets").await, 1);
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(key)
    );
    println!("R5_REAL_COMMIT_ACK_LOSS_ATOMIC_SQL_PASS");
    f.finish().await;
    drop(proxy);
}
