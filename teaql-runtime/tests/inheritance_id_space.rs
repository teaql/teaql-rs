use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor as P};
use teaql_runtime::{InMemoryMetadataStore, InternalIdGenerator, RuntimeError, UserContext};

struct FamilyIds(Arc<Mutex<BTreeMap<String, u64>>>);
impl InternalIdGenerator for FamilyIds {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        let mut counters = self.0.lock().unwrap();
        let value = counters.entry(entity.into()).or_default();
        *value += 1;
        Ok(*value)
    }
    fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), RuntimeError> {
        let mut counters = self.0.lock().unwrap();
        let value = counters.entry(entity.into()).or_default();
        *value = (*value).max(floor);
        Ok(())
    }
}

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
struct IdentityProbe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

#[test]
fn family_root_owns_generated_ids_and_fixed_floors_but_unrelated_types_remain_independent() {
    let root = EntityDescriptor::new("School")
        .property(P::new("id", DataType::U64).id())
        .property(P::new("version", DataType::I64).version());
    let child = EntityDescriptor::new("Academy").inherits(root.clone());
    let leaf = EntityDescriptor::new("TrainingAcademy").inherits(child.clone());
    let counters = Arc::new(Mutex::new(BTreeMap::new()));
    let context = UserContext::new()
        .with_metadata(
            InMemoryMetadataStore::new()
                .with_entity(root)
                .with_entity(child)
                .with_entity(leaf)
                .with_entity(EntityDescriptor::new("Unrelated")),
        )
        .with_internal_id_generator(FamilyIds(counters.clone()));
    assert_eq!(context.next_id("Academy").unwrap(), 1);
    assert_eq!(context.next_id("School").unwrap(), 2);
    assert_eq!(context.next_id("TrainingAcademy").unwrap(), 3);
    assert_eq!(context.next_id("Unrelated").unwrap(), 1);
    let mut entity = IdentityProbe {
        id: 1001,
        version: 0,
        __load_state: teaql_core::eval::LoadState::NotLoaded,
    };
    context
        .initialize_generated_bootstrap_entity(&mut entity, "TrainingAcademy", 1001)
        .unwrap();
    assert_eq!(context.next_id("Academy").unwrap(), 1002);
    assert_eq!(context.next_id("Unrelated").unwrap(), 2);
    assert_eq!(
        counters.lock().unwrap().keys().cloned().collect::<Vec<_>>(),
        ["School", "Unrelated"]
    );
}
