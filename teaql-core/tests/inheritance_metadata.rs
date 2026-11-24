use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};

fn school() -> EntityDescriptor {
    EntityDescriptor::new("School")
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("name", DataType::Text).column_name("Legacy Name"))
}

#[test]
fn joined_tables_keep_only_declared_fields_and_share_root_identity_version() {
    let academy = EntityDescriptor::new("Academy")
        .inherits(school())
        .property(PropertyDescriptor::new("campus", DataType::Text));
    let training = EntityDescriptor::new("TrainingAcademy")
        .inherits(academy.clone())
        .property(PropertyDescriptor::new("specialty", DataType::Text));
    assert_eq!(training.inheritance_root().name, "School");
    assert_eq!(training.properties.len(), 5);
    assert_eq!(
        training.property_by_name("name").unwrap().column_name,
        "Legacy Name"
    );
    assert!(training.version_property().is_some());
    for (entity, fields) in [
        (&academy, vec!["id", "campus"]),
        (&training, vec!["id", "specialty"]),
    ] {
        let storage = entity.storage_descriptor();
        assert_eq!(
            storage
                .properties
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            fields
        );
        assert!(storage.version_property().is_none());
        assert!(storage.joined_parent.is_none());
    }
    assert_eq!(school().storage_descriptor(), school());
}

#[test]
fn complete_generated_layout_is_preserved_without_duplicate_ancestor_fields() {
    let descriptor = school();
    let mut child = descriptor.clone();
    child.name = "Academy".into();
    child.table_name = "academy_data".into();
    let child = child.inherits(descriptor);
    assert_eq!(child.properties.len(), 3);
    assert_eq!(child.storage_descriptor().properties.len(), 1);
    assert_eq!(child.inheritance_root().name, "School");
}

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "School")]
struct SchoolMetadata {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "Academy", parent = SchoolMetadata)]
struct AcademyMetadata {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    campus: String,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

#[test]
fn derive_attests_and_retains_typed_parent_metadata() {
    use teaql_core::TeaqlEntity;
    assert_eq!(AcademyMetadata::JOINED_INHERITANCE_METADATA_REVISION, 1);
    let academy = AcademyMetadata::entity_descriptor();
    assert_eq!(academy.inheritance_root().name, "School");
    assert_eq!(academy.properties.len(), 4);
    assert_eq!(academy.storage_descriptor().properties.len(), 2);
}
