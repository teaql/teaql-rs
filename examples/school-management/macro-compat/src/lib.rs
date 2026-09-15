//! Small downstream compile fixture for both published and candidate reverse-relation macros.

use school_management_service_core::School;
#[cfg(not(feature = "macro-hygiene"))]
use teaql_core::Entity;
use teaql_core::{
    CompactRow, EntityDescriptor, EntityError, SmartList, TeaqlBoxedRelations, Value,
};
use teaql_macros::TeaqlReverseRelations;

#[derive(TeaqlReverseRelations)]
pub struct ReverseSchoolList {
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "platform_id", many))]
    pub schools: Vec<School>,
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "platform_id"))]
    pub boxed_school: Option<Box<School>>,
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "platform_id"))]
    pub direct_school: Option<School>,
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "platform_id", many))]
    pub smart_schools: SmartList<School>,
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

    fn populated_school() -> std::collections::BTreeMap<String, Value> {
        std::collections::BTreeMap::from([
            ("id".to_owned(), Value::U64(7)),
            ("name".to_owned(), Value::Text("Riverside".to_owned())),
            (
                "address".to_owned(),
                Value::Text("12 River Road".to_owned()),
            ),
            (
                "established_date".to_owned(),
                Value::Date("1995-09-01".parse().unwrap()),
            ),
            ("student_capacity".to_owned(), Value::I64(800)),
            ("active".to_owned(), Value::Bool(true)),
            (
                "create_time".to_owned(),
                Value::Timestamp(teaql_core::time::Timestamp(1_000)),
            ),
            (
                "update_time".to_owned(),
                Value::Timestamp(teaql_core::time::Timestamp(2_000)),
            ),
            ("version".to_owned(), Value::I64(3)),
            ("platform".to_owned(), Value::U64(1)),
            ("school_type".to_owned(), Value::U64(1001)),
        ])
    }

    #[test]
    fn reverse_relation_is_registered_and_empty_row_decodes() {
        let descriptor = exercise_reverse_relation_contract().unwrap();
        assert_eq!(descriptor.relations.len(), 4);
        assert_eq!(descriptor.relations[0].name, "schools");
        assert_eq!(descriptor.relations[0].target_entity, "School");
        assert!(descriptor.relations[0].many);
        assert_eq!(descriptor.relations[1].name, "boxed_school");
        assert!(!descriptor.relations[1].many);
        assert_eq!(descriptor.relations[2].name, "direct_school");
        assert!(!descriptor.relations[2].many);
        assert_eq!(descriptor.relations[3].name, "smart_schools");
        assert!(descriptor.relations[3].many);

        let empty = CompactRow::from_map(Default::default());
        let decoded = ReverseSchoolList::extract_from_values(&empty).unwrap();
        assert!(decoded.schools.is_empty());
        assert!(decoded.boxed_school.is_none());
        assert!(decoded.direct_school.is_none());
        assert!(decoded.smart_schools.data.is_empty());
    }

    #[test]
    fn populated_reverse_relations_decode_and_preserve_shapes() {
        let school = populated_school();
        let row = CompactRow::from_map(std::collections::BTreeMap::from([
            (
                "schools".to_owned(),
                Value::List(vec![Value::Object(school.clone())]),
            ),
            ("boxed_school".to_owned(), Value::Object(school.clone())),
            ("direct_school".to_owned(), Value::Object(school.clone())),
            (
                "smart_schools".to_owned(),
                Value::List(vec![Value::Object(school)]),
            ),
        ]));
        let decoded = ReverseSchoolList::extract_from_values(&row).unwrap();
        assert_eq!(decoded.schools.len(), 1);
        assert_eq!(decoded.schools[0].id(), 7);
        assert_eq!(decoded.schools[0].name(), "Riverside");
        assert_eq!(decoded.boxed_school.as_ref().unwrap().id(), 7);
        assert_eq!(decoded.direct_school.as_ref().unwrap().id(), 7);
        assert_eq!(decoded.smart_schools.data.len(), 1);
        assert_eq!(decoded.smart_schools.data[0].id(), 7);

        let mut injected = std::collections::BTreeMap::<String, Value>::new();
        decoded.inject_into_values(&mut injected);
        assert!(matches!(injected.get("schools"), Some(Value::List(rows)) if rows.len() == 1));
        assert!(matches!(
            injected.get("boxed_school"),
            Some(Value::Object(_))
        ));
        assert!(matches!(
            injected.get("direct_school"),
            Some(Value::Object(_))
        ));
        assert!(
            matches!(injected.get("smart_schools"), Some(Value::List(rows)) if rows.len() == 1)
        );
    }
}
