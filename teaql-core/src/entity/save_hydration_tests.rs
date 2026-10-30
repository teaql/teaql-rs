use super::*;
struct ManualRow {
    id: u64,
    context: Option<u64>,
}
impl TeaqlEntity for ManualRow {
    const ENTITY_NAME: &'static str = "ManualRow";
    fn entity_descriptor() -> EntityDescriptor {
        EntityDescriptor::new(Self::ENTITY_NAME)
    }
}
impl Entity for ManualRow {
    fn from_compact_row(row: CompactRow) -> Result<Self, EntityError> {
        Ok(Self {
            id: row.get("id").and_then(Value::try_u64).unwrap(),
            context: None,
        })
    }
    fn into_values(self) -> MutationValues {
        BTreeMap::from([("id".into(), Value::U64(self.id))]).into()
    }
    fn on_loaded(&mut self, context: &dyn std::any::Any) {
        self.context = context.downcast_ref::<u64>().copied();
    }
}
#[test]
fn default_save_hydration_preserves_manual_carriers_context_behavior() {
    for preserve in [false, true] {
        let row = CompactRow::from_map(BTreeMap::from([("id".into(), Value::U64(42))]));
        let entity = ManualRow::from_compact_row_after_save(row, &77_u64, preserve).unwrap();
        assert_eq!(entity.id, 42);
        assert_eq!(entity.context, preserve.then_some(77));
    }
}
