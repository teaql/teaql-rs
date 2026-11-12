use checker_natural_message_service_core::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering::SeqCst},
};
use teaql_core::{Entity, time::Timestamp};
use teaql_runtime::checker_messages::{
    CheckerMessageOutput, CheckerMessageRenderError, CheckerMessageRenderer, CheckerMessageRequest,
    StandardCheckerMessageRenderer,
};
use teaql_runtime::{
    FixedBusinessClock, JsonFieldNamingProfile, Language, RawAuditEventKind, RuntimeError,
    SafeAuditEvent, SafeAuditEventSink, UserContext,
};

fn private_name() -> String {
    format!("密钥🔑{}", "x".repeat(98))
}

struct Audit(Arc<AtomicUsize>);
impl SafeAuditEventSink for Audit {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        if matches!(
            event.kind,
            RawAuditEventKind::Created | RawAuditEventKind::Updated | RawAuditEventKind::Deleted
        ) {
            self.0.fetch_add(1, SeqCst);
        }
        Ok(())
    }
}

struct PrivateRenderer(Arc<AtomicUsize>);
impl CheckerMessageRenderer for PrivateRenderer {
    fn render(
        &self,
        request: &CheckerMessageRequest,
    ) -> Result<CheckerMessageOutput, CheckerMessageRenderError> {
        self.0.fetch_add(1, SeqCst);
        assert_eq!(
            request.safe_arguments["input"],
            format!("密钥{}xx", "*".repeat(97))
        );
        assert_eq!(request.safe_arguments["input_len"], "101");
        Ok(CheckerMessageOutput {
            message: format!("Private {}", private_name()),
            warnings: Default::default(),
        })
    }
}

async fn reject(
    parent: &Catalog,
    context: &UserContext,
    statements: &AtomicUsize,
    audits: &AtomicUsize,
    audit_before: usize,
    expected: &str,
) {
    let sql_before = statements.load(SeqCst);
    let error = parent
        .clone()
        .audit_as("Reject the second child before SQL or committed audit")
        .save(context)
        .await
        .unwrap_err();
    assert_eq!(
        statements.load(SeqCst),
        sql_before,
        "validation must precede SQL, including BEGIN"
    );
    assert_eq!(audits.load(SeqCst), audit_before);
    let RuntimeError::Check(results) = error else {
        panic!("expected a located Checker failure: {error:?}")
    };
    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert_eq!(result.entity_type.as_deref(), Some("Entry"));
    assert_eq!(result.location.model_path(), "entry_list[1].name");
    assert_eq!(result.location.instance_path(), "/entryList/1/name");
    assert_eq!(result.message.as_deref(), Some(expected));
    let response = serde_json::json!({"violations": results.iter()
        .map(|result| result.to_wire(JsonFieldNamingProfile::CamelCase)).collect::<Vec<_>>()});
    assert!(!response.to_string().contains(&private_name()));
    assert!(!format!("{results:?}").contains(&private_name()));
    println!("RUST_NATURAL_CHECKER_RESPONSE={response}");
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var("TEAQL_NATURAL_MESSAGES_DB")?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(database)?));
    let statements = Arc::new(AtomicUsize::new(0));
    let observed = statements.clone();
    pool.lock().unwrap().trace_v2(
        rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
        Some(move |_: rusqlite::trace::TraceEvent<'_>| {
            observed.fetch_add(1, SeqCst);
        }),
    )?;
    // The generated factory installs its own original Checker and passive model metadata.
    let mut context = service_runtime_from_pool(pool.clone()).await?;
    let a =
        chrono::DateTime::parse_from_rfc3339("2026-11-12T12:00:00Z")?.with_timezone(&chrono::Utc);
    let b = a + chrono::Duration::hours(1);
    context.set_business_clock(FixedBusinessClock::new(a));
    let audits = Arc::new(AtomicUsize::new(0));
    context.set_custom_event_sink(Audit(audits.clone()));
    context.ensure_schema().await?;
    context.ensure_schema().await?;
    let roots = Q::catalogs()
        .limit(1)
        .comment("Find the provisioned validation root")
        .purpose("Verify generated schema bootstrap before graph validation")
        .execute_for_list(&context)
        .await?;
    assert_eq!(roots.data.len(), 1);
    let root_id = roots.data[0].id();
    for name in ["first", "second"] {
        let mut entry = Q::entries()
            .comment("Prepare a validation example row")
            .purpose("Create a complete typed example graph")
            .new_entity(&context);
        entry.update_catalog_id(root_id).update_name(name);
        entry
            .audit_as("Create an entry through the generated audited mutation API")
            .save(&context)
            .await?;
    }
    let parents = Q::catalogs()
        .with_id_is(root_id)
        .select_entry_list_with(
            Q::entries()
                .order_by_id_desc()
                .limit(2)
                .comment("Load the latest two validation entries"),
        )
        .limit(1)
        .comment("Load the unchanged root and its bounded entry graph")
        .purpose("Verify indexed Checker failures through generated Q and E")
        .execute_for_list(&context)
        .await?;
    let parent = parents.data[0].clone();
    let handle = parent.entry_list();
    let children = handle.value().ok_or("entry_list was not loaded")?;
    assert_eq!(children.data.len(), 2);
    let mut other = children.data[0].clone();
    let mut child = children.data[1].clone();
    assert_eq!(
        E::entry(&other).get_name().eval().as_deref(),
        Some("second")
    );
    assert_eq!(E::entry(&child).get_name().eval().as_deref(), Some("first"));
    let parent_version = parent.version();
    let version = child.version();
    let previous_update = E::entry(&child).get_update_time().eval();
    assert_eq!(previous_update, Some(Timestamp(a.timestamp_millis())));
    context.set_business_clock(FixedBusinessClock::new(b));
    let audit_before = audits.load(SeqCst);
    let sql_before = statements.load(SeqCst);
    other.update_name("changed");
    parent.include_pending_mutations_from(&other)?;
    for (language, expected) in [
        (Language::English, "The name of the 2nd entry is required."),
        (Language::Chinese, "请填写第2项明细的名称。"),
    ] {
        context.set_language(language);
        // Required rejects absent/null, not an otherwise valid empty string.
        child.update_name(teaql_core::Value::Null);
        parent.include_pending_mutations_from(&child)?;
        reject(
            &parent,
            &context,
            &statements,
            &audits,
            audit_before,
            expected,
        )
        .await;
        assert_eq!(E::entry(&child).get_update_time().eval(), previous_update);
    }
    context.set_language(Language::English);
    child.update_name(private_name());
    parent.include_pending_mutations_from(&child)?;
    reject(&parent, &context, &statements, &audits, audit_before,
        "The name of the 2nd entry must not exceed 100 Unicode scalar values; its current length is 101.").await;
    let renderer_calls = Arc::new(AtomicUsize::new(0));
    context.set_checker_message_renderer(Arc::new(PrivateRenderer(renderer_calls.clone())));
    reject(
        &parent,
        &context,
        &statements,
        &audits,
        audit_before,
        "Private [REDACTED]",
    )
    .await;
    assert_eq!(renderer_calls.load(SeqCst), 1);
    context.set_checker_message_renderer(Arc::new(StandardCheckerMessageRenderer));
    assert_eq!(statements.load(SeqCst), sql_before);
    assert_eq!(audits.load(SeqCst), audit_before);
    assert_eq!(child.version(), version);
    assert_eq!(E::entry(&child).get_update_time().eval(), previous_update);
    other.update_name("second");
    parent.include_pending_mutations_from(&other)?;
    child.update_name("first");
    parent.include_pending_mutations_from(&child)?;
    parent
        .clone()
        .audit_as("Cancel caller edits without writing automatic Context values")
        .save(&context)
        .await?;
    assert_eq!(statements.load(SeqCst), sql_before);
    assert_eq!(audits.load(SeqCst), audit_before);
    child.update_name("accepted");
    parent.include_pending_mutations_from(&child)?;
    let saved = parent
        .audit_as("Persist the valid corrected second child")
        .save(&context)
        .await?;
    assert_eq!(audits.load(SeqCst), audit_before + 1);
    assert_eq!(saved.version(), parent_version);
    let stored = Q::entries()
        .with_id_is(child.id())
        .limit(1)
        .comment("Reload the corrected entry")
        .purpose("Verify safe diagnostics preserve successful audited mutation")
        .execute_for_list(&context)
        .await?;
    assert_eq!(stored.data.len(), 1);
    assert_eq!(
        E::entry(&stored.data[0]).get_name().eval().as_deref(),
        Some("accepted")
    );
    assert_eq!(
        E::entry(&stored.data[0]).get_update_time().eval(),
        Some(Timestamp(b.timestamp_millis()))
    );
    assert_eq!(stored.data[0].version(), version + 1);
    println!(
        "RUST_NATURAL_CHECKER_SQLITE={}",
        serde_json::json!({
            "passed":true,"typedQ":true,"typedE":true,"automaticMetadata":true,"originalGeneratedChecker":true,
            "unchangedParent":true,"responses":4,"customerRendererCalls":renderer_calls.load(SeqCst),
            "providerCallsOnRejection":0,"transactionCallsOnRejection":0,"updatedAudits":audits.load(SeqCst)-audit_before,
            "version":stored.data[0].version(),"originalScalarLength":101
        })
    );
    Ok(())
}
