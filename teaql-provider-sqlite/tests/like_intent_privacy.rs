//! Typed LIKE operand provenance at real SQLite and safe Context log boundaries.
use std::sync::{Arc, Mutex};
use teaql_core::{
    BinaryOp, CompactRow, DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor,
    RelationDescriptor, SelectQuery, TraceKind, Value,
};
use teaql_data_service::{MutationCommand, SchemaProvider, SqlParameterLogPolicy};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    InMemoryMetadataStore, PurposedSelectQuery, RequestPolicy, RuntimeError, UserContext,
};
use teaql_sql::{CompiledQuery, SqlDataServiceExecutor, SqlTransport};

#[derive(Clone)]
struct Schema(Vec<Arc<EntityDescriptor>>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.iter().find(|entity| entity.name == name).cloned()
    }
}
#[derive(Clone)]
struct Probe {
    inner: SqliteMutationExecutor,
    reads: Arc<Mutex<Vec<CompiledQuery>>>,
}
impl SqlTransport for Probe {
    type Error = <SqliteMutationExecutor as SqlTransport>::Error;
    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        self.reads.lock().unwrap().push(query.clone());
        self.inner.fetch_all_compact_sql(query).await
    }
    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.inner.execute_sql(query).await
    }
}
#[derive(Clone, Default)]
struct Policy(Arc<Mutex<Vec<ObservedIntent>>>);

type ObservedIntent = (Option<String>, Option<String>);
impl RequestPolicy for Policy {
    fn enforce_select(&self, _: &UserContext, query: &mut SelectQuery) -> Result<(), RuntimeError> {
        self.0
            .lock()
            .unwrap()
            .push((query.comment.clone(), query.purpose.clone()));
        Ok(())
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, Probe, Schema>;
type SeedExecutor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

#[derive(Clone, Copy, Debug)]
enum Kind {
    Contain,
    NotContain,
    Begin,
    NotBegin,
    End,
    NotEnd,
}
impl Kind {
    fn expr(self, field: &str, operand: &str) -> Expr {
        match self {
            Self::Contain => Expr::contain(field, operand),
            Self::NotContain => Expr::not_contain(field, operand),
            Self::Begin => Expr::begin_with(field, operand),
            Self::NotBegin => Expr::not_begin_with(field, operand),
            Self::End => Expr::end_with(field, operand),
            Self::NotEnd => Expr::not_end_with(field, operand),
        }
    }
    fn pattern(self, operand: &str) -> String {
        match self {
            Self::Contain | Self::NotContain => format!("%{operand}%"),
            Self::Begin | Self::NotBegin => format!("{operand}%"),
            Self::End | Self::NotEnd => format!("%{operand}"),
        }
    }
    fn negative(self) -> bool {
        matches!(self, Self::NotContain | Self::NotBegin | Self::NotEnd)
    }
}

async fn setup(operand: &str) -> (UserContext, Probe, Policy) {
    let parent = EntityDescriptor::new("Owner")
        .table_name("like_owner")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .audit_mask_fields(vec![])
        .relation(
            RelationDescriptor::new("children", "SecretRow")
                .many()
                .local_key("id")
                .foreign_key("owner_id"),
        );
    let child = EntityDescriptor::new("SecretRow")
        .table_name("like_child")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .property(PropertyDescriptor::new("owner_id", DataType::I64))
        .property(PropertyDescriptor::new("name", DataType::Text))
        .property(PropertyDescriptor::new("visible", DataType::Text))
        .audit_mask_fields(vec!["name".into()]);
    let schema = Schema(vec![Arc::new(parent.clone()), Arc::new(child.clone())]);
    let mut context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new()
            .with_entity(parent)
            .with_entity(child),
    );
    let inner =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    context.use_sqlite_provider(inner.clone());
    context.ensure_schema().await.unwrap();
    context.insert_resource(SeedExecutor::new(
        SqliteDialect,
        inner.clone(),
        schema.clone(),
    ));
    context
        .execute_in_transaction::<SeedExecutor, _, _>(|tx| {
            Box::pin(async move {
                for command in [
                    InsertCommand::new("Owner").value("id", 1_i64),
                    InsertCommand::new("SecretRow")
                        .value("id", 1_i64)
                        .value("owner_id", 1_i64)
                        .value("name", operand)
                        .value("visible", operand),
                    InsertCommand::new("SecretRow")
                        .value("id", 2_i64)
                        .value("owner_id", 1_i64)
                        .value("name", "UNRELATED")
                        .value("visible", "UNRELATED"),
                ] {
                    tx.mutate(
                        MutationCommand::Insert(command.value("version", 1_i64))
                            .request("seed LIKE fixture")?,
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    let probe = Probe {
        inner,
        reads: Arc::new(Mutex::new(Vec::new())),
    };
    context.insert_resource(Executor::new(SqliteDialect, probe.clone(), schema));
    let policy = Policy::default();
    context.set_request_policy(policy.clone());
    context.clear_sql_logs();
    (context, probe, policy)
}

async fn matrix(future: bool) {
    let mut leaks = Vec::new();
    let mut scenarios = 0;
    for operand in ["RUST-LIKE-PRIVATE", "%RUST_LIKE\\_%"] {
        for kind in [
            Kind::Contain,
            Kind::NotContain,
            Kind::Begin,
            Kind::NotBegin,
            Kind::End,
            Kind::NotEnd,
        ] {
            for masked in [false, true] {
                for logging in [false, true] {
                    let (mut context, probe, policy) = setup(operand).await;
                    if !logging {
                        context.disable_sql_log();
                    }
                    let child = SelectQuery::new("SecretRow")
                        .filter(kind.expr(if masked { "name" } else { "visible" }, operand))
                        .limit(10);
                    let root = if future { "Owner" } else { "SecretRow" };
                    let query = if future {
                        SelectQuery::new("Owner")
                            .relation_query("children", child)
                            .limit(1)
                    } else {
                        child
                    };
                    let comment = format!("load {operand} with PLAIN-CONTROL");
                    let purpose = format!("purpose {operand} with PLAIN-CONTROL");
                    let request =
                        PurposedSelectQuery::new(query.comment(comment.clone()), purpose.clone());
                    let repository = context.entity_data_service::<Executor>(root).unwrap();
                    let rows = repository.fetch_all(&request).await.unwrap();
                    assert_eq!(rows.len(), 1);
                    let expected_id = if kind.negative() { 2 } else { 1 };
                    if future {
                        let Value::List(children) = rows[0].get("children").unwrap() else {
                            panic!("loaded children");
                        };
                        assert_eq!(children.len(), 1);
                        let Value::Object(child) = &children[0] else {
                            panic!("child object");
                        };
                        assert_eq!(child.get("id").and_then(Value::try_i64), Some(expected_id));
                    } else {
                        assert_eq!(
                            rows[0].get("id").and_then(Value::try_i64),
                            Some(expected_id)
                        );
                    }
                    {
                        let reads = probe.reads.lock().unwrap();
                        assert_eq!(reads.len(), if future { 2 } else { 1 });
                        if future {
                            assert!(
                                reads[0]
                                    .params
                                    .iter()
                                    .all(|value| value != &Value::from(operand))
                            );
                        }
                        let physical = reads.last().unwrap();
                        let pattern = Value::from(kind.pattern(operand));
                        let index = physical
                            .params
                            .iter()
                            .position(|value| value == &pattern)
                            .expect("unchanged decorated SQL bind");
                        assert_eq!(
                            physical.log_context.parameter_policies[index],
                            if masked {
                                SqlParameterLogPolicy::Masked
                            } else {
                                SqlParameterLogPolicy::Plain
                            }
                        );
                        assert!(physical.sql.contains(if kind.negative() {
                            "NOT LIKE"
                        } else {
                            "LIKE"
                        }));
                    }
                    assert_eq!(
                        request.as_query().comment.as_deref(),
                        Some(comment.as_str())
                    );
                    assert_eq!(
                        request.as_query().purpose.as_deref(),
                        Some(purpose.as_str())
                    );
                    assert!(!policy.0.lock().unwrap().is_empty());
                    assert!(
                        policy
                            .0
                            .lock()
                            .unwrap()
                            .iter()
                            .all(|(seen_comment, seen_purpose)| seen_comment.as_deref()
                                == Some(comment.as_str())
                                && seen_purpose.as_deref() == Some(purpose.as_str())),
                        "policy must receive original intent"
                    );
                    let logs = context.sql_logs();
                    assert_eq!(
                        logs.len(),
                        if logging {
                            if future { 2 } else { 1 }
                        } else {
                            0
                        }
                    );
                    for (depth, log) in logs.iter().enumerate() {
                        assert_eq!(log.trace_path[0].entity_type, root);
                        assert_eq!(
                            log.trace_path
                                .iter()
                                .filter(|node| node.kind == TraceKind::Relation)
                                .count(),
                            depth
                        );
                        assert!(log.comment.as_deref().unwrap().contains("PLAIN-CONTROL"));
                        assert!(log.purpose.as_deref().unwrap().contains("PLAIN-CONTROL"));
                        if masked {
                            if format!("{log:?}").contains(operand) {
                                leaks.push(format!(
                                    "{kind:?} operand={operand:?} future={future} depth={depth}"
                                ));
                            }
                        } else {
                            assert_eq!(log.comment.as_deref(), Some(comment.as_str()));
                            assert_eq!(log.purpose.as_deref(), Some(purpose.as_str()));
                        }
                    }
                    context.clear_sql_logs();
                    repository
                        .fetch_all(&PurposedSelectQuery::new(
                            SelectQuery::new(root).limit(1).comment(comment.clone()),
                            purpose.clone(),
                        ))
                        .await
                        .unwrap();
                    if logging {
                        let independent = context.sql_logs();
                        assert_eq!(independent.len(), 1);
                        assert_eq!(independent[0].comment.as_deref(), Some(comment.as_str()));
                        assert_eq!(independent[0].purpose.as_deref(), Some(purpose.as_str()));
                    }
                    scenarios += 1;
                }
            }
        }
    }
    println!(
        "LIKE_INTENT scenarios={scenarios} future={future} privacy_failures={}",
        leaks.len()
    );
    assert!(
        leaks.is_empty(),
        "safe LIKE intent leaked original operands: {leaks:?}"
    );
}

#[test]
fn root_typed_like_original_operand_privacy() {
    futures_executor::block_on(matrix(false));
}
#[test]
fn future_child_typed_like_original_operand_privacy() {
    futures_executor::block_on(matrix(true));
}

#[test]
fn raw_like_does_not_invent_an_original_operand_by_stripping_wildcards() {
    futures_executor::block_on(async {
        let bare = "RAW_LITERAL\\";
        let (context, probe, _) = setup(bare).await;
        let pattern = format!("%{bare}%");
        let comment = format!("bare {bare}; exact pattern {pattern}");
        context
            .entity_data_service::<Executor>("SecretRow")
            .unwrap()
            .fetch_all(&PurposedSelectQuery::new(
                SelectQuery::new("SecretRow")
                    .filter(Expr::like("name", &pattern))
                    .limit(1)
                    .comment(comment),
                "raw LIKE control",
            ))
            .await
            .unwrap();
        assert_eq!(
            probe.reads.lock().unwrap()[0].params,
            vec![Value::from(pattern.clone())]
        );
        let logs = context.sql_logs();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].comment.as_deref().unwrap().contains(bare));
        assert!(!logs[0].comment.as_deref().unwrap().contains(&pattern));
    });
}

#[test]
fn cached_like_plan_uses_only_current_operand_provenance() {
    futures_executor::block_on(async {
        let first = "LIKE-FIRST-PRIVATE";
        let second = "LIKE-SECOND-PRIVATE";
        let (context, probe, _) = setup(first).await;
        let repository = context
            .entity_data_service::<Executor>("SecretRow")
            .unwrap();
        for (current, ordinary) in [(first, second), (second, first)] {
            context.clear_sql_logs();
            repository
                .fetch_all(&PurposedSelectQuery::new(
                    SelectQuery::new("SecretRow")
                        .filter(Expr::begin_with("name", current))
                        .limit(1)
                        .comment(format!("private {current}; ordinary {ordinary}")),
                    format!("purpose private {current}; ordinary {ordinary}"),
                ))
                .await
                .unwrap();
            let logs = context.sql_logs();
            assert_eq!(logs.len(), 1);
            assert!(!format!("{:?}", logs[0]).contains(current));
            assert!(logs[0].comment.as_deref().unwrap().contains(ordinary));
            assert!(logs[0].purpose.as_deref().unwrap().contains(ordinary));
        }
        let reads = probe.reads.lock().unwrap();
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].sql, reads[1].sql);
        assert_eq!(reads[0].params, vec![Value::from(format!("{first}%"))]);
        assert_eq!(reads[1].params, vec![Value::from(format!("{second}%"))]);
    });
}

#[test]
fn rewritten_like_ast_does_not_retain_stale_original_provenance() {
    futures_executor::block_on(async {
        let original = "DISCARDED-LIKE-ORIGINAL";
        for logging in [false, true] {
            for rewrite_pattern in [false, true] {
                let (mut context, probe, _) = setup(original).await;
                if !logging {
                    context.disable_sql_log();
                }
                let mut expr = Expr::begin_with("name", original);
                let Expr::Binary { op, right, .. } = &mut expr else {
                    panic!()
                };
                if rewrite_pattern {
                    let Expr::LikePattern { pattern, .. } = right.as_mut() else {
                        panic!()
                    };
                    *pattern = "UNRELATED%".into();
                } else {
                    *op = BinaryOp::Eq;
                }
                let comment = format!("ordinary {original}");
                let request = PurposedSelectQuery::new(
                    SelectQuery::new("SecretRow")
                        .filter(expr)
                        .limit(10)
                        .comment(comment.clone()),
                    "rewritten AST control",
                );
                let rows = context
                    .entity_data_service::<Executor>("SecretRow")
                    .unwrap()
                    .fetch_all(&request)
                    .await
                    .unwrap();
                assert_eq!(rows.len(), usize::from(rewrite_pattern));
                assert_eq!(
                    probe.reads.lock().unwrap()[0].params,
                    [Value::from(if rewrite_pattern {
                        "UNRELATED%".into()
                    } else {
                        format!("{original}%")
                    })]
                );
                let logs = context.sql_logs();
                assert_eq!(logs.len(), usize::from(logging));
                if logging {
                    assert_eq!(logs[0].comment.as_deref(), Some(comment.as_str()));
                }
            }
        }
    });
}
