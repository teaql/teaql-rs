//! Native save admission: readonly result metadata is part of the view, not
//! mutation intent. These are runtime carriers, not generated API substitutes.
use super::*;
use crate::{EntityKey, EntityRuntimeState, InMemoryMetadataStore, LedgerEntity};
use std::sync::atomic::{AtomicUsize, Ordering};
use teaql_core::{CompactRow, CompactRowLayout, DataType, TeaqlEntity as _};
use teaql_macros::{TeaqlEntity, teaql_entity};

#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "RestorationView", indexed_layout)]
struct View {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    note: Option<String>,
    #[teaql(dynamic)]
    properties: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}
impl View {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "restoration-view-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2), ("note", 3)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
        ("note", "note", "note"),
    ];
    fn change(&mut self, field: &str, value: Value) {
        match (field, &value) {
            ("name", Value::Text(value)) => self.name = value.clone(),
            ("note", Value::Text(value)) => self.note = Some(value.clone()),
            ("note", Value::Null) => self.note = None,
            _ => panic!("invalid native fixture edit"),
        }
        self.__teaql_runtime_state
            .set(EntityKey::new("RestorationView", self.id), field, value);
    }
}
struct NoWrite(Arc<AtomicUsize>);
impl DynGraphSaver for NoWrite {
    fn save_graph_dyn<'a>(
        &'a self,
        _: &'a UserContext,
        _: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(RuntimeError::Graph("unexpected graph write".into())) })
    }
    fn save_ledger_dyn<'a>(
        &'a self,
        _: &'a UserContext,
        _: GraphNode,
        _: EntityRuntimeState,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(RuntimeError::Graph("unexpected ledger write".into())) })
    }
}
fn fixture(partial: bool) -> (UserContext, Arc<AtomicUsize>, [View; 2]) {
    let definitions = teaql_core::dynamic_properties::DynamicPropertyDefinitions::new([
        ("_count".into(), DataType::I64),
        ("_null".into(), DataType::Text),
        ("_missing".into(), DataType::I64),
        ("_flag".into(), DataType::Bool),
        ("_empty".into(), DataType::Text),
    ])
    .unwrap();
    let mut names = vec![
        "id".into(),
        "version".into(),
        "note".into(),
        "_count".into(),
        "_null".into(),
        "_flag".into(),
        "_empty".into(),
    ];
    if !partial {
        names.push("name".into());
    }
    let columns =
        CompactRowLayout::new(names.into()).with_dynamic_property_definitions(definitions);
    let make = |id, count| {
        let mut values = vec![
            Value::U64(id),
            Value::I64(9),
            Value::Null,
            count,
            Value::Null,
            Value::Bool(false),
            Value::from(""),
        ];
        if !partial {
            values.push(Value::from("native"));
        }
        View::from_compact_row(CompactRow::with_layout(columns.clone(), values)).unwrap()
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(View::entity_descriptor()));
    let saver: Arc<dyn DynGraphSaver> = Arc::new(NoWrite(calls.clone()));
    context.insert_resource(saver);
    (
        context,
        calls,
        [
            make(7, Value::I64(0)),
            make(8, Value::TypedNull(DataType::I64)),
        ],
    )
}
fn assert_view(view: &View, partial: bool, count: bool) {
    assert_eq!((view.id, view.version), (if count { 7 } else { 8 }, 9));
    assert_eq!(view.is_field_loaded("name"), !partial);
    assert!(view.is_field_loaded("note"));
    assert!(view.note.is_none());
    assert_eq!(view.dynamic_property_type("_count"), Some(DataType::I64));
    assert_eq!(view.dynamic_property_type("_missing"), Some(DataType::I64));
    assert!(view.has_dynamic_property("_count"));
    assert_eq!(
        view.dynamic_property("_count"),
        if count { Some(&Value::I64(0)) } else { None }
    );
    assert!(view.has_dynamic_property("_null"));
    assert!(view.dynamic_property("_null").is_none());
    assert!(!view.has_dynamic_property("_missing"));
    assert!(view.dynamic_property("_missing").is_none());
    assert_eq!(view.dynamic_property("_flag"), Some(&Value::Bool(false)));
    assert_eq!(view.dynamic_property("_empty"), Some(&Value::from("")));
    assert!(view.dirty_fields().is_none());
    assert!(
        view.entity_runtime_state()
            .unwrap()
            .current_change_set()
            .is_empty()
    );
}

#[tokio::test]
async fn unchanged_readonly_view_keeps_types_values_and_shared_shape() {
    let (context, calls, [first, second]) = fixture(false);
    let first = save_audited_ledger_entity(
        teaql_core::Audited::new(first, "retain readonly view"),
        &context,
    )
    .await
    .unwrap();
    let second = save_audited_ledger_entity(
        teaql_core::Audited::new(second, "retain another readonly view"),
        &context,
    )
    .await
    .unwrap();
    assert_view(&first, false, true);
    assert_view(&second, false, false);
    assert!(Arc::ptr_eq(
        &first.loaded_state_snapshot().unwrap(),
        &second.loaded_state_snapshot().unwrap()
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn restored_known_null_keeps_partial_mask_and_readonly_metadata() {
    let (context, calls, [mut first, mut second]) = fixture(true);
    for view in [&mut first, &mut second] {
        view.change("note", Value::from("temporary"));
        view.change("note", Value::Null);
    }
    let first = save_audited_ledger_entity(
        teaql_core::Audited::new(first, "restore known null without widening projection"),
        &context,
    )
    .await
    .unwrap();
    let second = save_audited_ledger_entity(
        teaql_core::Audited::new(second, "retain independently owned row values"),
        &context,
    )
    .await
    .unwrap();
    assert_view(&first, true, true);
    assert_view(&second, true, false);
    assert!(Arc::ptr_eq(
        &first.loaded_state_snapshot().unwrap(),
        &second.loaded_state_snapshot().unwrap()
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn empty_audit_intent_is_rejected_at_request_construction() {
    let (context, calls, [first, _]) = fixture(false);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| teaql_core::Audited::new(
            first, " "
        )))
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

fn reject_foreign_state(owner: &str, revision: &str) {
    let (_, calls, [mut view, _]) = fixture(false);
    let before = view.loaded_state_snapshot().unwrap();
    let layout = teaql_core::FieldLayout::from_generated(
        owner,
        revision,
        View::__TEAQL_FIXED_FIELD_INDEXES,
        View::__TEAQL_FIXED_FIELD_MAPPINGS,
        &[],
        &["id", "version", "name", "note"],
    )
    .unwrap();
    let foreign = teaql_core::LoadedSnapshot::projection(layout, ["id", "version"]).into_shared();
    assert!(view.install_loaded_state_snapshot(foreign).is_err());
    assert!(Arc::ptr_eq(&before, &view.loaded_state_snapshot().unwrap()));
    assert_view(&view, false, true);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn loaded_state_install_rejects_another_entity_without_changing_the_view() {
    reject_foreign_state("AnotherEntity", "restoration-view-v1");
}

#[test]
fn loaded_state_install_rejects_another_layout_revision_without_changing_the_view() {
    reject_foreign_state("RestorationView", "restoration-view-v2");
}
