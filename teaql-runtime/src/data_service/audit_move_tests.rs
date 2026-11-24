//! Audit ownership transfer must not change provider inputs or retained events.
use super::EntityDataService;
use crate::{RawAuditEvent, RawAuditEventKind, RawAuditEventSink, RuntimeError, UserContext};
use std::sync::{Arc, Mutex};
use teaql_core::{
    BatchInsertCommand, BatchUpdateCommand, GeneratedValues, InsertCommand, MutationIntent, Record,
    TraceNode, UpdateCommand, Value,
};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, MutationCommand,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

#[derive(Default)]
struct Provider {
    commands: Mutex<Vec<MutationCommand>>,
    fail_at: Option<usize>,
    conflict: bool,
}
impl DataServiceExecutor for Provider {
    type Error = std::io::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        Default::default()
    }
}
impl QueryExecutor for Provider {
    async fn query(&self, _: QueryRequest) -> Result<QueryResult, Self::Error> {
        panic!("audit must not fetch old values from the database")
    }
}
impl MutationExecutor for Provider {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        let mut commands = self.commands.lock().unwrap();
        commands.push(request.command);
        if self.fail_at == Some(commands.len()) {
            return Err(std::io::Error::other("provider failed"));
        }
        Ok(MutationResult {
            affected_rows: u64::from(!self.conflict),
            generated_values: GeneratedValues::new(),
            persisted_snapshot: None,
            metadata: ExecutionMetadata::unrecorded_query(0),
        })
    }
}
struct Sink(Arc<Mutex<Vec<RawAuditEvent>>>);
impl RawAuditEventSink for Sink {
    fn on_event(&self, _: &UserContext, event: &RawAuditEvent) -> Result<(), RuntimeError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
}
fn trace(i: usize) -> Vec<TraceNode> {
    vec![TraceNode::new("Fixture", None, format!("row {i}"))]
}
fn values(name: &str) -> Record {
    Record::from([("name".into(), Value::from(name))])
}
fn batch() -> BatchUpdateCommand {
    let mut cmd = BatchUpdateCommand::new("Fixture", vec!["name".into()]);
    cmd.batch_ids = (0..3).map(|i| Value::from(format!("row-{i}"))).collect();
    cmd.batch_values = (0..3).map(|i| values(&format!("new-{i}")).into()).collect();
    cmd.batch_expected_versions = vec![Some(7)];
    let mut old = values("old-0");
    old.insert("untouched".into(), Value::from("keep me"));
    old.insert("version".into(), Value::I64(7));
    cmd.batch_old_values = vec![Some(old.into()), None];
    cmd.trace_chains = vec![trace(0), vec![]];
    cmd
}
fn context(events: Arc<Mutex<Vec<RawAuditEvent>>>) -> UserContext {
    let mut ctx = UserContext::new().with_event_sink(Sink(events));
    ctx.disable_sql_log();
    ctx
}

#[tokio::test]
async fn batch_update_keeps_provider_inputs_and_all_audit_snapshots() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let ctx = context(events.clone());
    let provider = Provider::default();
    let service = EntityDataService::for_executor(&ctx, "Fixture", &provider)
        .with_mutation_intent(MutationIntent::new("audit ownership regression").unwrap());
    let original = batch();
    assert_eq!(
        service
            .execute_prepared_batch_update(original.clone())
            .await
            .unwrap(),
        3
    );
    let commands = provider.commands.lock().unwrap();
    for (i, command) in commands.iter().enumerate() {
        let MutationCommand::Update(command) = command else {
            panic!("expected update")
        };
        assert_eq!(command.id, original.batch_ids[i]);
        assert_eq!(command.values, original.batch_values[i]);
        assert_eq!(
            command.old_values,
            original.batch_old_values.get(i).cloned().flatten()
        );
        assert_eq!(
            command.expected_version,
            original.batch_expected_versions.get(i).copied().flatten()
        );
        if i == 0 {
            assert!(command.trace_chain.starts_with(&trace(0)));
        }
    }
    let mut events = events.lock().unwrap();
    assert_eq!(events.len(), 3);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.kind, RawAuditEventKind::Updated);
        assert_eq!(event.entity, "Fixture");
        assert_eq!(event.updated_fields, vec!["name"]);
        assert_eq!(event.values.get("id"), Some(&original.batch_ids[i]));
        assert_eq!(
            event.values.get("name"),
            Some(&Value::from(format!("new-{i}")))
        );
        assert_eq!(
            event.old_values,
            original
                .batch_old_values
                .get(i)
                .cloned()
                .flatten()
                .map(Into::into)
        );
        assert_eq!(
            event.trace_chain,
            original.trace_chains.get(i).cloned().unwrap_or_default()
        );
        assert_eq!(event.changes.len(), 1);
        assert_eq!(
            event.changes[0].old_value,
            if i == 0 {
                Some(Value::from("old-0"))
            } else {
                None
            }
        );
        assert_eq!(
            event.changes[0].new_value,
            Some(Value::from(format!("new-{i}")))
        );
        assert_eq!(
            event.new_values.as_ref().unwrap().get("version"),
            if i == 0 { Some(&Value::I64(8)) } else { None }
        );
    }
    assert_eq!(
        events[0].new_values.as_ref().unwrap().get("untouched"),
        Some(&Value::from("keep me"))
    );
    events[0]
        .new_values
        .as_mut()
        .unwrap()
        .insert("name".into(), Value::from("changed later"));
    assert_eq!(
        events[0].old_values.as_ref().unwrap().get("name"),
        Some(&Value::from("old-0"))
    );
    assert_eq!(events[0].values.get("name"), Some(&Value::from("new-0")));
    assert_eq!(
        events[1].new_values.as_ref().unwrap().get("name"),
        Some(&Value::from("new-1"))
    );
}

#[tokio::test]
async fn batch_failure_conflict_and_empty_batch_emit_no_events() {
    for (fail_at, conflict) in [(Some(2), false), (None, true)] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = context(events.clone());
        let provider = Provider {
            fail_at,
            conflict,
            ..Default::default()
        };
        let service = EntityDataService::for_executor(&ctx, "Fixture", &provider)
            .with_mutation_intent(MutationIntent::new("failed audit regression").unwrap());
        assert!(
            service
                .execute_prepared_batch_update(batch())
                .await
                .is_err()
        );
        assert!(events.lock().unwrap().is_empty());
        let count = provider.commands.lock().unwrap().len();
        assert_eq!(
            service
                .execute_prepared_batch_update(BatchUpdateCommand::new("Fixture", vec![]))
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            service
                .execute_prepared_batch_insert(BatchInsertCommand::new("Fixture"))
                .await
                .unwrap(),
            0
        );
        assert_eq!(provider.commands.lock().unwrap().len(), count);
    }
}

#[tokio::test]
async fn inserts_and_single_updates_keep_owned_trace_and_old_values() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let ctx = context(events.clone());
    let provider = Provider::default();
    let service = EntityDataService::for_executor(&ctx, "Fixture", &provider)
        .with_mutation_intent(MutationIntent::new("single and batch audit regression").unwrap());
    let mut insert = InsertCommand::new("Fixture");
    insert.values = values("insert").into();
    service
        .execute_prepared_insert_with_comment(insert, trace(9))
        .await
        .unwrap();
    let mut inserts = BatchInsertCommand::new("Fixture");
    inserts.batch_values = vec![values("a").into(), values("b").into()];
    inserts.trace_chains = vec![trace(10)];
    service
        .execute_prepared_batch_insert(inserts)
        .await
        .unwrap();
    for complete in [true, false] {
        let mut update = UpdateCommand::new("Fixture", Value::from("text-id"));
        update.values = values("new").into();
        update.expected_version = Some(4);
        update.old_values = Some(if complete {
            values("old").into()
        } else {
            Default::default()
        });
        let old = update.old_values.clone();
        service
            .execute_prepared_update_with_comment(update, trace(11))
            .await
            .unwrap();
        let commands = provider.commands.lock().unwrap();
        let MutationCommand::Update(command) = commands.last().unwrap() else {
            panic!("update")
        };
        assert_eq!(
            command.old_values, old,
            "provider must receive the original snapshot"
        );
        assert!(command.trace_chain.starts_with(&trace(11)));
    }
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 5);
    assert_eq!(events[0].trace_chain, trace(9));
    assert_eq!(events[1].trace_chain, trace(10));
    assert!(events[2].trace_chain.is_empty());
    assert_eq!(events[1].entity, "Fixture");
    assert_eq!(events[2].entity, "Fixture");
    assert_eq!(events[1].values.get("name"), Some(&Value::from("a")));
    assert_eq!(events[2].values.get("name"), Some(&Value::from("b")));
    assert_eq!(events[3].old_values, Some(values("old")));
    assert!(
        events[4].old_values.is_none(),
        "incomplete old snapshot still follows existing fallback"
    );
    for event in &events[3..] {
        assert_eq!(event.trace_chain, trace(11));
        assert_eq!(event.values.get("id"), Some(&Value::from("text-id")));
        assert_eq!(
            event.new_values.as_ref().unwrap().get("version"),
            Some(&Value::I64(5))
        );
        assert_eq!(event.updated_fields, vec!["name"]);
    }
}
