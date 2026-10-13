//! Real request -> SQLite transaction -> Context log checks (teaql-rs #239).
//! Tests never insert expected trace nodes into requests or provider metadata.
use std::sync::Arc;

use teaql_core::{
    DataType, DeleteCommand, EntityDescriptor, InsertCommand, PropertyDescriptor, RecoverCommand,
    SelectQuery, TraceKind, UpdateCommand, Value,
};
use teaql_data_service::{
    DataServiceOperation, ExecutionMetadata, MutationCommand, MutationRequest, QueryRequest,
    SchemaProvider,
};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, SqlLogOperation, UserContext};
use teaql_sql::SqlDataServiceExecutor;

#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);

impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (self.0.name == name).then(|| self.0.clone())
    }
}

type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

#[derive(Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "BatchProbe")]
struct PaginationProbe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}

#[test]
fn compound_aggregates_emit_one_physical_statement() {
    futures_executor::block_on(async {
        use teaql_runtime::PurposedSelectQuery;
        for logging in [false, true] {
            let mut context = batch_context().await;
            if !logging {
                context.disable_sql_log();
            }
            context.clear_sql_logs();
            let query = PurposedSelectQuery::new(
                SelectQuery::new("BatchProbe")
                    .count("probeCount")
                    .sum("id", "idTotal")
                    .comment("calculate probe totals"),
                "render totals",
            );
            let rows = context
                .entity_data_service::<Executor>("BatchProbe")
                .unwrap()
                .fetch_all(&query)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("probeCount").and_then(Value::try_i64), Some(2));
            assert_eq!(rows[0].get("idTotal").and_then(Value::try_i64), Some(19));
            let logs = context.sql_logs();
            assert_eq!(logs.len(), usize::from(logging));
            if logging {
                assert!(logs[0].sql.to_uppercase().contains("COUNT("));
                assert!(logs[0].sql.to_uppercase().contains("SUM("));
                assert_eq!(
                    logs[0]
                        .trace_chain
                        .iter()
                        .map(|node| node.comment.as_str())
                        .collect::<Vec<_>>(),
                    ["render totals", "calculate probe totals"]
                );
            }
            println!("RTC_COM_07_AGGREGATE_PASS logging={logging} expressions=2 statements=1");
        }
    });
}

#[test]
fn id_set_collection_owns_a_helper_comment_and_cache_hits_do_not_invent_sql() {
    futures_executor::block_on(async {
        use teaql_runtime::{InMemoryIdSetStore, PurposedSelectQuery};
        for logging in [true, false] {
            let mut context = batch_context().await;
            context.set_id_set_store(Arc::new(InMemoryIdSetStore::default()));
            if !logging {
                context.disable_sql_log();
            }
            let query = PurposedSelectQuery::new(
                SelectQuery::new("BatchProbe")
                    .projects(vec!["id", "version", "name"])
                    .order_desc("id")
                    .page(1, 1)
                    .optimize_pagination_with_id_set_config("request-tree-probes", 60, 10)
                    .comment("load batch probes"),
                "render probe page",
            );
            let original = query.clone();
            let repository = context
                .entity_data_service::<Executor>("BatchProbe")
                .unwrap();
            context.clear_sql_logs();
            let rows = repository
                .fetch_enhanced_entities::<PaginationProbe>(&query)
                .await
                .unwrap();
            assert_eq!(
                rows.data.iter().map(|row| row.id).collect::<Vec<_>>(),
                vec![9]
            );
            assert_eq!(rows.total_count, Some(2));
            assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_BUILD"));
            let logs = context.sql_logs();
            assert_eq!(logs.len(), if logging { 2 } else { 0 });
            if logging {
                assert_eq!(
                    logs[0]
                        .trace_chain
                        .iter()
                        .map(|node| node.comment.as_str())
                        .collect::<Vec<_>>(),
                    vec![
                        "render probe page",
                        "load batch probes",
                        "collect BatchProbe IDs for pagination"
                    ]
                );
                assert_eq!(
                    logs[1]
                        .trace_chain
                        .iter()
                        .map(|node| node.comment.as_str())
                        .collect::<Vec<_>>(),
                    vec!["render probe page", "load batch probes"]
                );
                assert!(logs.iter().all(|entry| {
                    !entry
                        .trace_path
                        .iter()
                        .any(|node| node.kind == TraceKind::Relation)
                }));
            }
            context.clear_sql_logs();
            let next =
                PurposedSelectQuery::new(query.as_query().clone().page(0, 1), "render probe page");
            let rows = repository
                .fetch_enhanced_entities::<PaginationProbe>(&next)
                .await
                .unwrap();
            assert_eq!(
                rows.data.iter().map(|row| row.id).collect::<Vec<_>>(),
                vec![10]
            );
            assert_eq!(rows.total_count, Some(2));
            assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_HIT"));
            assert_eq!(
                context.sql_logs().len(),
                usize::from(logging),
                "cache hit must not manufacture ID collection or count SQL"
            );
            if logging {
                assert_eq!(
                    context.sql_logs()[0]
                        .trace_chain
                        .iter()
                        .map(|node| node.comment.as_str())
                        .collect::<Vec<_>>(),
                    vec!["render probe page", "load batch probes"]
                );
            }
            assert_eq!(
                query.as_query(),
                original.as_query(),
                "helper binding mutated caller input"
            );
            if logging {
                assert!(!context.sql_logs()[0].sql.to_uppercase().contains("COUNT("));
            }
            println!("RTC_COM_07_CACHE_PASS logging={logging} accuracy=EXACT hit_statements=1");
        }
    });
}

#[test]
fn derived_count_retains_removed_relation_privacy_without_executing_it() {
    futures_executor::block_on(async {
        use teaql_core::Expr;
        use teaql_runtime::PurposedSelectQuery;
        let context = batch_context().await;
        let secret = "COUNT-PRIVATE-DESCENDANT";
        let source = PurposedSelectQuery::new(
            SelectQuery::new("BatchProbe")
                .filter(Expr::eq("status", "ACTIVE"))
                .relation_query(
                    "unused_relation",
                    SelectQuery::new("BatchProbe").filter(Expr::eq("name", secret)),
                )
                .limit(1)
                .comment(format!("count ACTIVE including {secret}")),
            format!("page ACTIVE with {secret}"),
        );
        let count = source.clone().for_exact_count("count");
        assert!(
            !format!("{count:?}").contains("unused_relation"),
            "removed source is not exposed by Debug"
        );
        assert!(count.as_query().relations.is_empty());
        assert!(count.as_query().slice.is_none());
        let repository = context
            .entity_data_service::<Executor>("BatchProbe")
            .unwrap();
        for owned in [false, true] {
            context.clear_sql_logs();
            let rows = if owned {
                repository.fetch_all_owned(count.clone()).await.unwrap()
            } else {
                repository.fetch_all(&count).await.unwrap()
            };
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("count").and_then(Value::try_u64), Some(2));
            let logs = context.sql_logs();
            assert_eq!(
                logs.len(),
                1,
                "classify removed relation without executing SQL"
            );
            assert!(
                !format!("{logs:?}").contains(secret),
                "COUNT must redact descendant value in intent"
            );
            assert!(
                logs[0].comment.as_deref().unwrap().contains("ACTIVE"),
                "ordinary bindings remain visible"
            );
            assert!(logs[0].purpose.as_deref().unwrap().contains("ACTIVE"));
            assert_eq!(logs[0].params, vec![Value::from("ACTIVE")]);
        }
        context.clear_sql_logs();
        repository
            .fetch_all(&PurposedSelectQuery::new(
                SelectQuery::new("BatchProbe").limit(1).comment(secret),
                "independent query",
            ))
            .await
            .unwrap();
        assert_eq!(
            context.sql_logs()[0].comment.as_deref(),
            Some(secret),
            "no request-local classification leaks to the next query"
        );

        // Reproduce the old generated transform: discarding source provenance
        // really does leak this canary despite the physical COUNT being correct.
        context.clear_sql_logs();
        let legacy = PurposedSelectQuery::new(count.clone().into_query(), "legacy probe");
        repository.fetch_all(&legacy).await.unwrap();
        assert!(
            context.sql_logs()[0]
                .comment
                .as_deref()
                .unwrap()
                .contains(secret)
        );
    });
}

#[test]
fn sqlite_stream_retains_native_count_scope_without_a_fake_record_for_each_chunk() {
    futures_executor::block_on(async {
        use futures_util::StreamExt;
        use teaql_core::{QueryOperation, QueryOperationKind};
        use teaql_runtime::PurposedSelectQuery;
        for logging in [true, false] {
            let mut context = batch_context().await;
            if !logging {
                context.disable_sql_log();
            }
            let root = PurposedSelectQuery::new(
                SelectQuery::new("BatchProbe")
                    .stream(1)
                    .comment("load probe totals"),
                "render streamed totals",
            );
            let counted = root.for_exact_count_with_operation(
                "totalProbes",
                QueryOperation::new(QueryOperationKind::NamedCount, "count_as")
                    .with_alias("totalProbes"),
            );
            let repository = context
                .entity_data_service::<Executor>("BatchProbe")
                .unwrap();
            context.clear_sql_logs();
            let mut stream = repository.fetch_stream(&counted).await.unwrap();
            let mut values = Vec::new();
            while let Some(chunk) = stream.next().await {
                values.extend(chunk.unwrap().rows);
            }
            drop(stream);
            assert_eq!(values.len(), 1);
            assert_eq!(
                values[0].get("totalProbes").and_then(Value::try_u64),
                Some(2)
            );
            let entries = context.sql_logs();
            assert_eq!(entries.len(), usize::from(logging));
            if logging {
                assert_eq!(
                    entries[0]
                        .trace_chain
                        .iter()
                        .map(|node| node.comment.as_str())
                        .collect::<Vec<_>>(),
                    vec![
                        "render streamed totals",
                        "load probe totals",
                        "count_as(\"totalProbes\")"
                    ]
                );
                assert!(
                    !entries[0]
                        .trace_path
                        .iter()
                        .any(|node| node.kind == TraceKind::Relation)
                );
                assert_eq!(
                    entries[0].log_context.execution_outcome,
                    Some(teaql_data_service::SqlExecutionOutcome::Success)
                );
            }
        }
    });
}

#[test]
fn failed_count_retains_removed_relation_privacy_at_the_error_sink() {
    futures_executor::block_on(async {
        use teaql_core::Expr;
        use teaql_runtime::PurposedSelectQuery;
        let context = batch_context().await;
        // Provider-owned in-memory fixture only: cause a real SELECT failure.
        context
            .require_resource::<Executor>()
            .unwrap()
            .transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch("DROP TABLE batch_probe")
            .unwrap();
        let secret = "FAILED-COUNT-PRIVATE";
        let count = PurposedSelectQuery::new(
            SelectQuery::new("BatchProbe")
                .relation_query(
                    "unused",
                    SelectQuery::new("BatchProbe").filter(Expr::eq("password", secret)),
                )
                .comment(format!("count {secret}")),
            format!("failure {secret}"),
        )
        .for_exact_count("count");
        assert!(
            context
                .entity_data_service::<Executor>("BatchProbe")
                .unwrap()
                .fetch_all(&count)
                .await
                .is_err()
        );
        let logs = context.sql_logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].log_context.execution_outcome,
            Some(teaql_data_service::SqlExecutionOutcome::Failure)
        );
        assert!(!format!("{logs:?}").contains(secret));
    });
}

#[test]
fn count_logging_off_does_not_disable_required_intent() {
    futures_executor::block_on(async {
        use teaql_runtime::PurposedSelectQuery;
        let mut context = batch_context().await;
        context.disable_sql_log();
        let query =
            PurposedSelectQuery::new(SelectQuery::new("BatchProbe"), "missing comment probe")
                .for_exact_count("count");
        assert!(
            context
                .entity_data_service::<Executor>("BatchProbe")
                .unwrap()
                .fetch_all(&query)
                .await
                .is_err()
        );
        assert!(context.sql_logs().is_empty());
        let query = PurposedSelectQuery::new(
            SelectQuery::new("BatchProbe").comment("count with logging off"),
            "prove unchanged query semantics",
        )
        .for_exact_count("count");
        let rows = context
            .entity_data_service::<Executor>("BatchProbe")
            .unwrap()
            .fetch_all(&query)
            .await
            .unwrap();
        assert_eq!(rows[0].get("count").and_then(Value::try_u64), Some(2));
        assert!(context.sql_logs().is_empty());
    });
}

#[test]
fn native_recover_request_preserves_operation_and_owned_reason_at_safe_log_sink() {
    futures_executor::block_on(async {
        let descriptor = EntityDescriptor::new("RecoveryProbe")
            .table_name("recovery_probe")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let transport = SqliteMutationExecutor::from_connection(
            rusqlite::Connection::open_in_memory().unwrap(),
        );
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
        context.use_sqlite_provider(transport.clone());
        context.ensure_schema().await.unwrap();
        context.insert_resource(Executor::new(
            SqliteDialect,
            transport,
            Schema(Arc::new(descriptor)),
        ));

        let result = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    transaction
                        .mutate(
                            MutationCommand::Insert(
                                InsertCommand::new("RecoveryProbe")
                                    .value("id", 7_i64)
                                    .value("version", 1_i64)
                                    .value("name", "private recovery fixture"),
                            )
                            .request("seed recovery fixture")?,
                        )
                        .await?;
                    transaction
                        .mutate(
                            MutationCommand::Delete(
                                DeleteCommand::new("RecoveryProbe", 7_i64).expected_version(1),
                            )
                            .request("archive recovery fixture")?,
                        )
                        .await?;
                    transaction
                        .mutate(
                            MutationCommand::Recover(RecoverCommand::new(
                                "RecoveryProbe",
                                7_i64,
                                -2,
                            ))
                            .request("restore archived object")?,
                        )
                        .await
                })
            })
            .await
            .unwrap();

        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.metadata.operation, DataServiceOperation::Recover);
        let persisted = result.persisted_snapshot.unwrap();
        assert_eq!(
            persisted.get("version").and_then(|value| value.try_i64()),
            Some(3)
        );
        let logs = context.sql_logs();
        let recovery = logs
            .iter()
            .find(|entry| entry.operation == SqlLogOperation::Recover)
            .expect("Recover must not be relabeled Update in the Context log");
        assert_eq!(
            recovery.audit_reason.as_deref(),
            Some("restore archived object")
        );
        assert_eq!(
            recovery.trace_path.first().unwrap().entity_type,
            "RecoveryProbe"
        );
        assert_eq!(recovery.trace_path.last().unwrap().entity_type, "recover");
        assert!(recovery.trace_path.iter().all(|node| !matches!(
            node.kind,
            TraceKind::Comment | TraceKind::Purpose | TraceKind::AuditReason
        )));
        assert!(!format!("{logs:?}").contains("private recovery fixture"));

        let queried = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    transaction
                        .query(QueryRequest::from_query(
                            SelectQuery::new("RecoveryProbe")
                                .limit(1)
                                .comment("load restored object")
                                .purpose("verify recovered version"),
                        )?)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(queried.rows.len(), 1);
        let query_logs = context.sql_logs();
        let query = query_logs
            .iter()
            .find(|entry| entry.purpose.as_deref() == Some("verify recovered version"))
            .expect("native Query Request must retain its owned purpose");
        assert_eq!(query.comment.as_deref(), Some("load restored object"));
        assert_eq!(
            query.trace_path.first().unwrap().entity_type,
            "RecoveryProbe"
        );
        assert_eq!(query.trace_path.last().unwrap().entity_type, "select");
    });
}

async fn batch_context() -> UserContext {
    let descriptor = EntityDescriptor::new("BatchProbe")
        .table_name("batch_probe")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .property(PropertyDescriptor::new("name", DataType::Text))
        .property(PropertyDescriptor::new("password", DataType::Text))
        .property(PropertyDescriptor::new("status", DataType::Text))
        .audit_mask_fields(vec!["name".into()]);
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
    context.use_sqlite_provider(transport.clone());
    context.ensure_schema().await.unwrap();
    context.insert_resource(Executor::new(
        SqliteDialect,
        transport,
        Schema(Arc::new(descriptor)),
    ));
    context
        .execute_in_transaction::<Executor, _, _>(|transaction| {
            Box::pin(async move {
                for id in [9_i64, 10] {
                    transaction
                        .mutate(
                            MutationCommand::Insert(
                                InsertCommand::new("BatchProbe")
                                    .value("id", id)
                                    .value("version", 1_i64)
                                    .value("name", "before")
                                    .value("password", "BEFORE-CREDENTIAL")
                                    .value("status", "ACTIVE"),
                            )
                            .request("seed native batch probes")?,
                        )
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    context.clear_sql_logs();
    context
}

// TC-REQ-13 native request input, not an injected expected graph lineage.
// Direct TransactionScope::mutate does not emit a graph safe-audit event;
// committed graph-audit assertions remain in ledger_allocated_trace.rs.
async fn assert_blank_typed_tail_at_sqlite(logging: bool) {
    let mut context = batch_context().await;
    if !logging {
        context.disable_sql_log();
    }
    for (index, kind) in [TraceKind::Entity, TraceKind::Provider, TraceKind::Sql]
        .into_iter()
        .enumerate()
    {
        context.clear_sql_logs();
        let id = 100_i64 + index as i64;
        let tail = teaql_core::TraceNode::typed(kind, "BatchProbe", Some(id as u64), "");
        let mut command = InsertCommand::new("BatchProbe")
            .value("id", id)
            .value("version", 1_i64)
            .value("name", "native blank tail")
            .value("status", "ACTIVE");
        command.trace_chain.push(tail.clone());
        let request = MutationCommand::Insert(command)
            .request(" explicit root comment ")
            .unwrap();
        assert_eq!(request.trace_chain().last(), Some(&tail));
        let original = request.clone();
        let result = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move { transaction.mutate(request).await })
            })
            .await
            .unwrap();
        assert_eq!(original.comment(), " explicit root comment ");
        assert_eq!(original.trace_chain().last(), Some(&tail));
        assert_eq!(result.affected_rows, 1);
        assert_eq!(
            result
                .persisted_snapshot
                .as_ref()
                .unwrap()
                .get("id")
                .and_then(Value::try_i64),
            Some(id)
        );
        let physical = leaf_metadata(&result.metadata);
        assert_eq!(
            physical.len(),
            2,
            "real insert and authoritative readback required"
        );
        assert_eq!(physical[0].operation, DataServiceOperation::Insert);
        assert_eq!(physical[1].operation, DataServiceOperation::Query);
        for statement in physical {
            assert_eq!(statement.comment.as_deref(), Some(original.comment()));
            assert_eq!(
                statement.sql_log.execution_outcome,
                Some(teaql_data_service::SqlExecutionOutcome::Success)
            );
            assert!(
                statement
                    .parameterized_query
                    .as_ref()
                    .is_some_and(|sql| !sql.is_empty())
            );
        }
        let logs = context.sql_logs();
        assert_eq!(logs.len(), if logging { 2 } else { 0 });
        for entry in logs {
            assert_eq!(entry.audit_reason.as_deref(), Some(original.comment()));
            assert_eq!(entry.comment.as_deref(), Some(original.comment()));
        }
        // A new transaction must see the committed row, not a pre-commit echo.
        let rows = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    transaction
                        .query(QueryRequest::from_query(
                            SelectQuery::new("BatchProbe")
                                .filter(teaql_core::Expr::eq("id", id))
                                .limit(1)
                                .comment("load committed blank-tail probe")
                                .purpose("verify request-owned intent survives SQLite execution"),
                        )?)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("id").and_then(Value::try_i64), Some(id));
    }
}

#[test]
fn blank_typed_route_tails_keep_owned_intent_at_sqlite_logging_on() {
    futures_executor::block_on(assert_blank_typed_tail_at_sqlite(true));
}

#[test]
fn blank_typed_route_tails_keep_owned_intent_at_sqlite_logging_off() {
    futures_executor::block_on(assert_blank_typed_tail_at_sqlite(false));
}

fn batch_with_sibling_secret(fail_second_write: bool) -> MutationRequest {
    let second = if fail_second_write {
        MutationCommand::Insert(
            InsertCommand::new("BatchProbe")
                .value("id", 10_i64)
                .value("version", 1_i64)
                .value("name", "Riverside")
                .value("password", "PASSWORD-CANARY")
                .value("status", "ACTIVE"),
        )
    } else {
        MutationCommand::Update(
            UpdateCommand::new("BatchProbe", 10_i64)
                .expected_version(1)
                .value("name", "Riverside")
                .value("password", "PASSWORD-CANARY")
                .value("status", "ACTIVE"),
        )
    }
    .request("update live probe")
    .unwrap();
    MutationCommand::Batch(vec![
        MutationCommand::Delete(DeleteCommand::new("BatchProbe", 9_i64).expected_version(1))
            .request("remove stale probe")
            .unwrap(),
        MutationCommand::Batch(vec![second])
            .request("update current probes")
            .unwrap(),
    ])
    .request("what: synchronize Riverside PASSWORD-CANARY while ACTIVE")
    .unwrap()
}

fn leaf_metadata(metadata: &ExecutionMetadata) -> Vec<&ExecutionMetadata> {
    if metadata.statements.is_empty() {
        vec![metadata]
    } else {
        metadata.statements.iter().flat_map(leaf_metadata).collect()
    }
}

#[test]
fn native_nested_batch_inherits_root_intent_and_preserves_local_lineage() {
    futures_executor::block_on(async {
        let context = batch_context().await;
        let request = batch_with_sibling_secret(false);
        let original = request.clone();
        let result = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move { transaction.mutate(request).await })
            })
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 2);
        assert_eq!(result.metadata.comment.as_deref(), Some(original.comment()));
        let leaves = leaf_metadata(&result.metadata);
        assert_eq!(leaves.len(), 4, "two writes and their actual readbacks");
        let writes: Vec<_> = leaves
            .iter()
            .filter(|statement| statement.operation != DataServiceOperation::Query)
            .collect();
        assert_eq!(writes.len(), 2);
        for (pair, expected) in leaves.as_chunks::<2>().0.iter().zip([
            vec![original.comment(), "remove stale probe"],
            vec![
                original.comment(),
                "update current probes",
                "update live probe",
            ],
        ]) {
            assert_ne!(pair[0].operation, DataServiceOperation::Query);
            assert_eq!(pair[1].operation, DataServiceOperation::Query);
            assert_eq!(pair[1].result_count, Some(1));
            for statement in pair {
                assert_eq!(statement.comment.as_deref(), Some(original.comment()));
                let lineage = statement
                    .trace_chain
                    .iter()
                    .filter(|node| node.kind == TraceKind::AuditReason)
                    .map(|node| node.comment.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(lineage, expected);
            }
        }
        assert!(writes[1].params.contains(&Value::from("Riverside")));
        assert!(writes[1].params.contains(&Value::from("PASSWORD-CANARY")));
        let MutationCommand::Batch(children) = &original.command else {
            panic!("retained original is a batch");
        };
        assert_eq!(children[0].comment(), "remove stale probe");
        assert_eq!(children[1].comment(), "update current probes");
        let logs = context.sql_logs();
        assert_eq!(logs.len(), 4, "each physical statement logged once");
        assert_eq!(
            logs.iter()
                .filter(|log| log.operation == SqlLogOperation::Select)
                .count(),
            2
        );
        for log in &logs {
            assert!(log.comment.as_deref().unwrap().contains("ACTIVE"));
            assert!(!format!("{log:?}").contains("Riverside"));
            assert!(!format!("{log:?}").contains("PASSWORD-CANARY"));
            assert_eq!(log.trace_path.last().unwrap().kind, TraceKind::Sql);
            assert!(log.trace_path.iter().all(|node| !matches!(
                node.kind,
                TraceKind::Comment | TraceKind::Purpose | TraceKind::AuditReason
            )));
        }
        let rows = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    transaction
                        .query(QueryRequest::from_query(
                            SelectQuery::new("BatchProbe")
                                .limit(10)
                                .comment("load Riverside after batch")
                                .purpose("verify unchanged bindings and request isolation"),
                        )?)
                        .await
                })
            })
            .await
            .unwrap();
        let live = rows
            .rows
            .iter()
            .find(|row| row.get("id").unwrap().try_i64() == Some(10))
            .unwrap();
        assert_eq!(live.get("name"), Some(&Value::from("Riverside")));
        assert_eq!(live.get("password"), Some(&Value::from("PASSWORD-CANARY")));
        assert_eq!(live.get("version").and_then(Value::try_i64), Some(2));
        assert_eq!(
            context.sql_logs().last().unwrap().comment.as_deref(),
            Some("load Riverside after batch")
        );
    });
}

#[test]
fn failed_native_batch_keeps_root_intent_and_masks_future_sibling_on_prior_statement() {
    futures_executor::block_on(async {
        let context = batch_context().await;
        let request = batch_with_sibling_secret(true);
        let result = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move { transaction.mutate(request).await })
            })
            .await;
        assert!(
            result.is_err(),
            "second INSERT must hit the existing primary key"
        );
        let logs = context.sql_logs();
        assert!(
            logs.iter()
                .any(|entry| entry.operation == SqlLogOperation::Delete)
        );
        assert!(logs.iter().any(|entry| {
            entry.operation == SqlLogOperation::Insert
                && entry.log_context.execution_outcome
                    == Some(teaql_data_service::SqlExecutionOutcome::Failure)
        }));
        for entry in &logs {
            assert!(
                entry
                    .comment
                    .as_deref()
                    .unwrap()
                    .starts_with("what: synchronize")
            );
            assert!(!format!("{entry:?}").contains("Riverside"));
            assert!(!format!("{entry:?}").contains("PASSWORD-CANARY"));
            if entry.operation == SqlLogOperation::Select {
                assert_eq!(
                    entry.purpose.as_deref(),
                    Some("verify the persisted mutation result")
                );
            }
        }
        let rows = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    transaction
                        .query(QueryRequest::from_query(
                            SelectQuery::new("BatchProbe")
                                .limit(10)
                                .comment("inspect rollback")
                                .purpose("verify native batch rollback retained the prior version"),
                        )?)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert!(
            rows.rows
                .iter()
                .all(|row| row.get("version").and_then(Value::try_i64) == Some(1))
        );
    });
}

#[test]
fn independent_native_batches_on_one_context_keep_operation_local_execution_intent() {
    let context = Arc::new(futures_executor::block_on(batch_context()));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let results = std::thread::scope(|threads| {
        let handles = [
            ("A", 20_i64, "ALPHA-PRIVATE"),
            ("B", 30_i64, "BETA-PRIVATE"),
        ]
        .into_iter()
        .map(|(label, id, name)| {
            let context = context.clone();
            let barrier = barrier.clone();
            threads.spawn(move || {
                let reason = format!("save native batch {label} {name}");
                let request = MutationCommand::Batch(
                    [id, id + 1]
                        .into_iter()
                        .map(|id| {
                            MutationCommand::Insert(
                                InsertCommand::new("BatchProbe")
                                    .value("id", id)
                                    .value("version", 1_i64)
                                    .value("name", name)
                                    .value("status", "ACTIVE"),
                            )
                            .request(format!("create {label} probe"))
                            .unwrap()
                        })
                        .collect(),
                )
                .request(reason.clone())
                .unwrap();
                barrier.wait();
                let result =
                    futures_executor::block_on(context.execute_in_transaction::<Executor, _, _>(
                        |transaction| Box::pin(async move { transaction.mutate(request).await }),
                    ))
                    .unwrap();
                assert_eq!(result.affected_rows, 2);
                for leaf in leaf_metadata(&result.metadata) {
                    assert_eq!(leaf.comment.as_deref(), Some(reason.as_str()));
                    let lineage = leaf
                        .trace_chain
                        .iter()
                        .filter(|node| node.kind == TraceKind::AuditReason)
                        .map(|node| node.comment.as_str())
                        .collect::<Vec<_>>();
                    assert_eq!(lineage, [reason.as_str(), &format!("create {label} probe")]);
                }
                label
            })
        })
        .collect::<Vec<_>>();
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results, ["A", "B"]);
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        8,
        "four writes and four independently traced readbacks"
    );
    assert_eq!(
        logs.iter()
            .filter(|log| log.operation == SqlLogOperation::Select)
            .count(),
        4
    );
    assert_eq!(
        logs.iter()
            .filter(|log| log
                .comment
                .as_deref()
                .unwrap()
                .starts_with("save native batch A"))
            .count(),
        4
    );
    assert_eq!(
        logs.iter()
            .filter(|log| log
                .comment
                .as_deref()
                .unwrap()
                .starts_with("save native batch B"))
            .count(),
        4
    );
    assert!(!format!("{logs:?}").contains("ALPHA-PRIVATE"));
    assert!(!format!("{logs:?}").contains("BETA-PRIVATE"));
    let rows = futures_executor::block_on(context.execute_in_transaction::<Executor, _, _>(
        |transaction| {
            Box::pin(async move {
                transaction
                    .query(QueryRequest::from_query(
                        SelectQuery::new("BatchProbe")
                            .limit(10)
                            .comment("inspect native batches")
                            .purpose("verify both independent transactions committed"),
                    )?)
                    .await
            })
        },
    ))
    .unwrap();
    assert_eq!(rows.rows.len(), 6);
}
