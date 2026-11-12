use context_fix_service_core::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering::SeqCst},
};
use teaql_core::Entity;
use teaql_runtime::checker_messages::CheckerPresentationModel;
use teaql_runtime::{
    CheckObjectStatus, CheckResult, CheckResults, InMemoryCheckerRegistry, JsonFieldNamingProfile,
    Language, ObjectLocation, RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink,
    TypedEntityChecker, UserContext,
};

struct BusinessRules;
impl EntryCheckerLogic for BusinessRules {
    fn check_entry(
        &self,
        _: &UserContext,
        entry: &Entry,
        _: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        let name = entry.name();
        if name.trim().is_empty() {
            results.push(CheckResult::required(location.clone().member("name")));
        }
        if name.chars().count() > 8 {
            results.push(CheckResult::max_str(
                location.clone().member("name"),
                8,
                name.clone(),
            ));
        }
        if name == "private-probe" {
            // Exercise a real model-masked value, not a synthetic rendered mask.
            let token = entry.guarded_token();
            if token.chars().count() > 4 {
                results.push(CheckResult::max_str(
                    location.clone().member("guarded_token"),
                    4,
                    token,
                ));
            }
        }
    }
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

fn new_entry(context: &UserContext, catalog: u64, name: &str) -> Entry {
    let mut entry = Q::entries()
        .comment("Prepare an entry")
        .purpose("Verify natural Checker messages")
        .new_entity(context);
    entry
        .update_catalog_id(catalog)
        .update_name(name)
        .update_source_ip("local")
        .update_last_ip("local")
        .update_opened_by("example")
        .update_changed_by("example");
    entry
}

fn rejected(error: RuntimeError, pointer: &str, expected: &str) -> serde_json::Value {
    let RuntimeError::Check(results) = error else {
        panic!("expected located Checker error: {error:?}")
    };
    let response = serde_json::json!({"violations":results.iter().map(|result| result.to_wire(JsonFieldNamingProfile::CamelCase)).collect::<Vec<_>>()});
    println!("{}", response);
    let result = results
        .iter()
        .find(|result| result.location.instance_path() == pointer)
        .expect("exact violation pointer");
    assert_eq!(result.entity_type.as_deref(), Some("Entry"));
    assert_eq!(result.message.as_deref(), Some(expected));
    response
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("TEAQL_NATURAL_MESSAGES_DB")?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(path)?));
    let mut context = service_runtime_from_pool(pool.clone()).await?;
    let metadata: serde_json::Value = serde_json::from_str(include_str!(
        "../../../teaql-runtime/tests/fixtures/natural-messages/cases-v1.json"
    ))?;
    let mut entities = serde_json::json!({"Catalog":metadata["entities"]["Catalog"],"Entry":metadata["entities"]["Entry"]});
    entities["Catalog"]["fields"]
        .as_array_mut()
        .unwrap()
        .retain(|field| matches!(field["name"].as_str(), Some("name" | "entry_list")));
    entities["Entry"]["fields"]
        .as_array_mut()
        .unwrap()
        .retain(|field| field["name"] == "name");
    entities["Entry"]["fields"].as_array_mut().unwrap().push(serde_json::json!({"name":"guarded_token","labels":{"en":"guarded token","zh-CN":"受保护令牌"}}));
    let model = CheckerPresentationModel::from_json(
        &serde_json::json!({
            "schema":"teaql.checker-model/v1", "revision":"generated-context-natural-example-v1",
            "entities":entities, "currentObject":metadata["currentObject"]
        })
        .to_string(),
    )?;
    // Explicit passive metadata until automatic generator emission is qualified.
    context.set_checker_presentation_model(Arc::new(model));
    context.set_checker_registry(
        InMemoryCheckerRegistry::new()
            .with_checker(TypedEntityChecker::<Catalog, _>::new(
                CatalogChecker::default(),
            ))
            .with_checker(TypedEntityChecker::<Entry, _>::new(EntryChecker::new(
                BusinessRules,
            ))),
    );
    let audits = Arc::new(AtomicUsize::new(0));
    context.set_custom_event_sink(Audit(audits.clone()));
    context.ensure_schema().await?;
    context.ensure_schema().await?;
    let mut parent = Q::catalogs()
        .comment("Prepare catalog")
        .purpose("Verify a two-child graph")
        .new_entity(&context);
    parent.update_name("catalog");
    let parent = parent
        .audit_as("Create example graph owner")
        .save(&context)
        .await?;
    for name in ["first", "second"] {
        new_entry(&context, parent.id(), name)
            .audit_as("Create a valid example child")
            .save(&context)
            .await?;
    }
    let parents = Q::catalogs()
        .with_id_is(parent.id())
        .select_entry_list_with(Q::entries().order_by_id_asc().limit(2))
        .limit(1)
        .comment("Load complete bounded graph")
        .purpose("Check generated Q and E before mutation")
        .execute_for_list(&context)
        .await?;
    let parent = parents.data[0].clone();
    let children = parent.entry_list();
    let children = children.value().unwrap();
    assert_eq!(children.data.len(), 2);
    let mut first = children.data[0].clone();
    let mut child = children.data[1].clone();
    assert_eq!(
        E::entry(&child).get_name().eval().as_deref(),
        Some("second")
    );
    let version = child.version();
    let token = child.guarded_token();
    let length = token.chars().count();
    let before = pool.lock().unwrap().total_changes();
    let audit_before = audits.load(SeqCst);
    // A graph of intended changes contains only participating children. Include
    // a valid first edit so the rejected child is second in that validation graph.
    first.update_name("changed");
    parent.include_pending_mutations_from(&first)?;
    for (language, expected) in [
        (Language::English, "The name of the 2nd entry is required."),
        (Language::Chinese, "请填写第2条明细的名称。"),
    ] {
        context.set_language(language);
        child.update_name("");
        parent.include_pending_mutations_from(&child)?;
        rejected(
            parent
                .clone()
                .audit_as("Reject an empty child name")
                .save(&context)
                .await
                .unwrap_err(),
            "/entryList/1/name",
            expected,
        );
        assert_eq!(pool.lock().unwrap().total_changes(), before);
        assert_eq!(audits.load(SeqCst), audit_before);
    }
    context.set_language(Language::English);
    child.update_name("private-probe");
    parent.include_pending_mutations_from(&child)?;
    let response = rejected(
        parent
            .clone()
            .audit_as("Reject masked and ordinary bounds")
            .save(&context)
            .await
            .unwrap_err(),
        "/entryList/1/guardedToken",
        &format!(
            "The guarded token of the 2nd entry must not exceed 4 Unicode scalar values; its current length is {length}."
        ),
    );
    assert!(!response.to_string().contains(&token));
    assert_eq!(pool.lock().unwrap().total_changes(), before);
    assert_eq!(audits.load(SeqCst), audit_before);
    assert_eq!(child.version(), version);
    first.update_name("first");
    parent.include_pending_mutations_from(&first)?;
    child.update_name("renamed");
    parent.include_pending_mutations_from(&child)?;
    parent
        .audit_as("Persist a corrected child")
        .save(&context)
        .await?;
    assert_eq!(audits.load(SeqCst), audit_before + 1);
    let rows = Q::entries()
        .with_id_is(child.id())
        .limit(1)
        .comment("Reload corrected child")
        .purpose("Verify stored state through E")
        .execute_for_list(&context)
        .await?;
    assert_eq!(
        E::entry(&rows.data[0]).get_name().eval().as_deref(),
        Some("renamed")
    );
    assert_eq!(rows.data[0].version(), version + 1);
    println!(
        "RUST_NATURAL_MESSAGES_SQLITE_PASS child={} same_graph=true languages=en,zh-CN masked=true",
        child.id()
    );
    Ok(())
}
