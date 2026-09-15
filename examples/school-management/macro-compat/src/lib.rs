//! Small downstream compile fixture for both published and candidate reverse-relation macros.

use school_management_service_core::School;
use teaql_core::{CompactRow, EntityDescriptor, EntityError, TeaqlBoxedRelations, Value};
#[cfg(not(feature = "macro-hygiene"))]
use teaql_core::Entity;
use teaql_macros::TeaqlReverseRelations;

#[derive(TeaqlReverseRelations)]
pub struct ReverseSchoolList {
    #[teaql(relation(
        target = "School",
        local_key = "id",
        foreign_key = "platform_id",
        many
    ))]
    pub schools: Vec<School>,
}

pub fn exercise_reverse_relation_contract() -> Result<EntityDescriptor, EntityError> {
    let mut descriptor = EntityDescriptor::new("Platform");
    ReverseSchoolList::extend_descriptor(&mut descriptor);

    let row = CompactRow::from_map(Default::default());
    let relations = ReverseSchoolList::extract_from_values(&row)?;
    let mut values = std::collections::BTreeMap::<String, Value>::new();
    relations.inject_into_values(&mut values);
    Ok(descriptor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_relation_is_registered_and_empty_row_decodes() {
        let descriptor = exercise_reverse_relation_contract().unwrap();
        assert_eq!(descriptor.relations.len(), 1);
        assert_eq!(descriptor.relations[0].name, "schools");
        assert_eq!(descriptor.relations[0].target_entity, "School");
        assert!(descriptor.relations[0].many);
    }
}
