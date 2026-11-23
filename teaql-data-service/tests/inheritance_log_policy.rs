use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_data_service::{SqlIntentRedactions, SqlParameterLogPolicy as Policy};

#[test]
fn child_cannot_relax_an_ancestor_mask_but_can_strengthen_it() {
    let root = EntityDescriptor::new("School")
        .property(PropertyDescriptor::new("name", DataType::Text))
        .audit_mask_fields(vec!["name".into()]);
    let child = EntityDescriptor::new("Academy")
        .inherits(root)
        .property(PropertyDescriptor::new("campus", DataType::Text))
        .audit_mask_fields(vec!["campus".into()]);
    let leaf = EntityDescriptor::new("Training")
        .inherits(child)
        .property(PropertyDescriptor::new("specialty", DataType::Text))
        .audit_mask_fields(vec![]);
    assert_eq!(
        SqlIntentRedactions::field_policy(&leaf, "name"),
        Policy::Masked
    );
    assert_eq!(
        SqlIntentRedactions::field_policy(&leaf, "campus"),
        Policy::Masked
    );
    assert_eq!(
        SqlIntentRedactions::field_policy(&leaf, "specialty"),
        Policy::Plain
    );
}

#[test]
fn incomplete_ancestor_policy_is_fail_closed_only_for_its_inherited_fields() {
    let root =
        EntityDescriptor::new("School").property(PropertyDescriptor::new("name", DataType::Text));
    let child = EntityDescriptor::new("Academy")
        .inherits(root)
        .property(PropertyDescriptor::new("campus", DataType::Text))
        .audit_mask_fields(vec![]);
    assert_eq!(
        SqlIntentRedactions::field_policy(&child, "name"),
        Policy::Unknown
    );
    assert_eq!(
        SqlIntentRedactions::field_policy(&child, "campus"),
        Policy::Plain
    );
}
