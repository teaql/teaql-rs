//! Framework journal primitive with real native SQL. Automatic save enlistment is separate.
use super::{Fixture, scalar};
use std::sync::Arc;
use teaql_core::{BlobRef, EntitySnapshot, InsertCommand, UpdateCommand, Value};
use teaql_data_service::infrastructure_sql::*;
use teaql_data_service::{
    DataServiceExecutor, MutationCommand, MutationExecutor, MutationRequest, Transaction,
    TransactionExecutor,
};
use teaql_runtime::{EntityKey, EntityRuntimeState};
fn key() -> EntityKey {
    EntityKey::new_static("Asset", 42_u64)
}
fn blob(value: &str) -> BlobRef {
    BlobRef::new(format!("blob_journal_{value}")).unwrap()
}
fn edit(root: &EntityRuntimeState, value: &str) {
    root.set(key(), "document", Value::BlobRef(blob(value)));
}
async fn readback(sql: &dyn InfrastructureSql) -> EntitySnapshot {
    let rows = sql
        .fetch(
            &InfrastructureStatement::new(
                "SELECT id,version,document FROM r5_assets WHERE id=42",
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let mut values: EntitySnapshot = rows.into_iter().next().unwrap().into_map().into();
    if let Some(Value::Text(value)) = values.get("document").cloned() {
        values.insert(
            "document".into(),
            Value::BlobRef(BlobRef::new(value).unwrap()),
        );
    }
    values
}
struct AfterCommitFailure {
    root: EntityRuntimeState,
}
impl PhysicalTransactionParticipant for AfterCommitFailure {
    fn after_completion(
        &self,
        _: PhysicalTransactionOutcome,
    ) -> Result<(), InfrastructureSqlError> {
        Ok(())
    }
    fn after_commit(&self) -> Result<(), InfrastructureSqlError> {
        assert!(!self.root.has_pending_submission());
        assert_eq!(
            self.root.get(&key(), "document"),
            Some(Value::BlobRef(blob("late")))
        );
        Err(InfrastructureSqlError::CompletionFailed)
    }
}

#[tokio::test]
async fn ownerless_review_draft_attaches_to_real_sql_owner_after_begin_without_late_edit_leak() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let root = EntityRuntimeState::default();
    root.set_original_version(key(), 1);
    edit(&root, "preflight");
    let mut draft = root.reserve_submission(None).unwrap();
    draft
        .execution_state()
        .set(key(), "document", Value::BlobRef(blob("reviewed")));
    draft.seal_reviewed_intent().unwrap();
    edit(&root, "late");
    let business = f.business();
    let tx = TransactionExecutor::begin(&business).await.unwrap();
    let identity = Transaction::physical_identity(&tx).unwrap();
    let sql = tx.infrastructure_sql().unwrap();
    let submission = draft.attach(&identity).unwrap();
    let reviewed = submission
        .execution_state()
        .get(&key(), "document")
        .unwrap();
    let Value::BlobRef(reviewed) = reviewed else {
        panic!("reviewed BlobRef required")
    };
    assert_eq!(
        sql.execute(
            &InfrastructureStatement::new(
                "UPDATE r5_assets SET document=$1,version=2 WHERE id=42 AND version=1",
                vec![Value::Text(reviewed.as_str().into())],
            )
            .unwrap()
        )
        .await
        .unwrap(),
        1
    );
    submission
        .acknowledge([(key(), readback(sql).await)])
        .unwrap();
    assert_eq!(
        f.root
            .fetch(
                &InfrastructureStatement::new(
                    "SELECT version,document FROM r5_assets WHERE id=42",
                    vec![],
                )
                .unwrap()
            )
            .await
            .unwrap()[0]
            .get("version"),
        Some(&Value::I64(1))
    );
    Transaction::commit(tx).await.unwrap();
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
    assert_eq!(root.get_original_version(&key()), Some(2));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("late")))
    );
    let actual = f
        .admin
        .get()
        .await
        .unwrap()
        .query_one(
            &format!(
                "SELECT version,document FROM \"{}\".r5_assets WHERE id=42",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(actual.get::<_, i64>("version"), 2);
    assert_eq!(
        actual.get::<_, String>("document"),
        blob("reviewed").as_str()
    );
    println!("R5_REVIEW_DRAFT_REAL_OWNER_REVIEWED_ROW_LATE_EDIT_PASS");
    f.finish().await;
}

#[tokio::test]
async fn journal_actual_native_repeated_flush_preserves_late_edits_after_commit_notification_failure()
 {
    let f = Fixture::new(true).await;
    let business = f.business();
    let tx = TransactionExecutor::begin(&business).await.unwrap();
    let sql = tx.infrastructure_sql().unwrap();
    let identity = sql.transaction_identity().unwrap();
    let root = EntityRuntimeState::default();
    let alias = root.clone();
    root.mark_as_new(key());
    edit(&root, "first");
    let first = root.begin_submission(&identity).unwrap();
    assert!(first.execution_state().is_new(&key()));
    tx.mutate(
        MutationRequest::new(
            MutationCommand::Insert(
                InsertCommand::new("Asset")
                    .value("id", 42_u64)
                    .value("version", 1_i64)
                    .value(
                        "document",
                        first.execution_state().get(&key(), "document").unwrap(),
                    ),
            ),
            "what: native journal insert; why: physical completion",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    first.acknowledge([(key(), readback(sql).await)]).unwrap();
    edit(&root, "second");
    let second = root.begin_submission(&identity).unwrap();
    assert!(!second.execution_state().is_new(&key()));
    assert_eq!(
        second.execution_state().get_original_version(&key()),
        Some(1)
    );
    tx.mutate(
        MutationRequest::new(
            MutationCommand::Update(
                UpdateCommand::new("Asset", 42_u64)
                    .expected_version(1)
                    .value("version", 2_i64)
                    .value(
                        "document",
                        second.execution_state().get(&key(), "document").unwrap(),
                    ),
            ),
            "what: second native journal flush; why: original version advances",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    second.acknowledge([(key(), readback(sql).await)]).unwrap();
    edit(&alias, "late");
    identity
        .enlist(Arc::new(AfterCommitFailure {
            root: alias.clone(),
        }))
        .unwrap();
    assert_eq!(f.count("r5_assets").await, 0);
    assert!(
        Transaction::commit(tx)
            .await
            .unwrap_err()
            .to_string()
            .contains("POSTGRES_TRANSACTION_AFTER_COMMIT_FAILED")
    );
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
    assert!(!root.has_pending_submission());
    assert!(!root.requires_reload_after_completion());
    assert_eq!(root.get_original_version(&key()), Some(2));
    assert_eq!(
        root.current_change_set().get(&key(), "document"),
        Some(&Value::BlobRef(blob("late")))
    );
    let row = f
        .admin
        .get()
        .await
        .unwrap()
        .query_one(
            &format!(
                "SELECT version,document FROM \"{}\".r5_assets WHERE id=42",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>("version"), 2);
    assert_eq!(row.get::<_, String>("document"), blob("second").as_str());
    println!("R5_RUNTIME_JOURNAL_NATIVE_SQL_REPEAT_LATE_EDIT_KNOWN_COMMIT_PASS");
    f.finish().await;
}

#[tokio::test]
async fn journal_actual_rollback_restores_intent_and_original_version_after_two_flushed_updates() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let root = EntityRuntimeState::default();
    root.set_original_version(key(), 1);
    let tx = f.root.begin().await.unwrap();
    let identity = tx.transaction_identity().unwrap();
    for (index, value) in ["first", "second"].into_iter().enumerate() {
        edit(&root, value);
        let batch = root.begin_submission(&identity).unwrap();
        let version = batch
            .execution_state()
            .get_original_version(&key())
            .unwrap();
        assert_eq!(version, index as i64 + 1);
        let affected = tx
            .execute(
                &InfrastructureStatement::new(
                    "UPDATE r5_assets SET document=$1,version=version+1 WHERE id=42 AND version=$2",
                    vec![
                        Value::Text(blob(value).as_str().into()),
                        Value::I64(version),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(affected, 1);
        batch
            .acknowledge([(key(), readback(tx.as_ref()).await)])
            .unwrap();
    }
    edit(&root, "late");
    tx.rollback().await.unwrap();
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::RolledBack);
    assert_eq!(root.get_original_version(&key()), Some(1));
    assert_eq!(
        root.current_change_set().get(&key(), "document"),
        Some(&Value::BlobRef(blob("late")))
    );
    assert!(!root.requires_reload_after_completion());
    let row = f
        .admin
        .get()
        .await
        .unwrap()
        .query_one(
            &format!(
                "SELECT version,document FROM \"{}\".r5_assets WHERE id=42",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>("version"), 1);
    assert_eq!(row.get::<_, String>("document"), blob("original").as_str());
    println!("R5_RUNTIME_JOURNAL_REAL_ROLLBACK_ORIGINAL_VERSION_AND_LATE_INTENT_PASS");
    f.finish().await;
}

#[tokio::test]
async fn journal_real_commit_ack_loss_retains_uncertain_intent_and_fences_aliases() {
    let mut proxy = super::support::proxy::ProtocolFault::new().await;
    let f = Fixture::with_port(true, proxy.port, 1).await;
    let root = EntityRuntimeState::default();
    let alias = root.clone();
    root.mark_as_new(key());
    edit(&root, "submitted");
    let tx = f.root.begin().await.unwrap();
    let identity = tx.transaction_identity().unwrap();
    let batch = root.begin_submission(&identity).unwrap();
    f.write(
        tx.as_ref(),
        &f.target(42, "document"),
        Some(&blob("submitted")),
    )
    .await;
    batch
        .acknowledge([(key(), readback(tx.as_ref()).await)])
        .unwrap();
    proxy.drop_commit_ack();
    assert_eq!(
        tx.commit().await.unwrap_err(),
        InfrastructureSqlError::TransactionUnknown
    );
    proxy.intercepted().await;
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Unknown);
    assert!(root.requires_reload_after_completion());
    assert!(alias.requires_reload_after_completion());
    root.clear_committed();
    assert!(root.is_new(&key()));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("submitted")))
    );
    let next = f.root.begin().await.unwrap();
    let next_identity = next.transaction_identity().unwrap();
    assert!(root.require_submission_owner(&next_identity).is_err());
    assert!(root.begin_submission(&next_identity).is_err());
    let independent = EntityRuntimeState::default();
    assert!(independent.require_submission_owner(&next_identity).is_ok());
    assert_eq!(
        scalar(next.as_ref(), "SELECT count(*) AS value FROM r5_assets").await,
        1
    );
    next.rollback().await.unwrap();
    assert_eq!(f.count("r5_assets").await, 1);
    println!("R5_RUNTIME_JOURNAL_REAL_ACK_LOSS_UNKNOWN_FENCE_SQL_COMMITTED_PASS");
    f.finish().await;
    drop(proxy);
}
