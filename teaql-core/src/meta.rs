use crate::{DataType, default_table_name};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDescriptor {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub column_name: String,
    pub is_id: bool,
    pub is_version: bool,
}

impl PropertyDescriptor {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        Self {
            column_name: name.clone(),
            name,
            data_type,
            nullable: true,
            is_id: false,
            is_version: false,
        }
    }

    pub fn column_name(mut self, column_name: impl Into<String>) -> Self {
        self.column_name = column_name.into();
        self
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn id(mut self) -> Self {
        self.is_id = true;
        self
    }

    pub fn version(mut self) -> Self {
        self.is_version = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationDescriptor {
    pub name: String,
    pub target_entity: String,
    pub local_key: String,
    pub foreign_key: String,
    pub many: bool,
    pub attach: bool,
    pub delete_missing: bool,
}

impl RelationDescriptor {
    pub fn new(name: impl Into<String>, target_entity: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            target_entity: target_entity.into(),
            local_key: "id".to_owned(),
            foreign_key: "id".to_owned(),
            many: false,
            attach: true,
            delete_missing: true,
        }
    }

    pub fn local_key(mut self, key: impl Into<String>) -> Self {
        self.local_key = key.into();
        self
    }

    pub fn foreign_key(mut self, key: impl Into<String>) -> Self {
        self.foreign_key = key.into();
        self
    }

    pub fn many(mut self) -> Self {
        self.many = true;
        self
    }

    pub fn attach(mut self) -> Self {
        self.attach = true;
        self
    }

    pub fn detached(mut self) -> Self {
        self.attach = false;
        self
    }

    pub fn delete_missing(mut self) -> Self {
        self.delete_missing = true;
        self
    }

    pub fn keep_missing(mut self) -> Self {
        self.delete_missing = false;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDescriptor {
    pub name: String,
    pub table_name: String,
    pub data_service: Option<String>,
    pub properties: Vec<PropertyDescriptor>,
    pub relations: Vec<RelationDescriptor>,
    pub audit_mask_fields: Vec<String>,
    pub audit_value_max_len: Option<usize>,
}

impl EntityDescriptor {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            table_name: default_table_name(&name),
            name,
            data_service: None,
            properties: Vec::new(),
            relations: Vec::new(),
            audit_mask_fields: Vec::new(),
            audit_value_max_len: None,
        }
    }

    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub fn data_service(mut self, data_service: impl Into<String>) -> Self {
        self.data_service = Some(data_service.into());
        self
    }

    pub fn property(mut self, property: PropertyDescriptor) -> Self {
        self.properties.push(property);
        self
    }

    pub fn relation(mut self, relation: RelationDescriptor) -> Self {
        self.relations.push(relation);
        self
    }

    pub fn audit_mask_fields(mut self, fields: Vec<String>) -> Self {
        self.audit_mask_fields = fields;
        self
    }

    pub fn audit_value_max_len(mut self, max_len: Option<usize>) -> Self {
        self.audit_value_max_len = max_len;
        self
    }

    pub fn property_by_name(&self, name: &str) -> Option<&PropertyDescriptor> {
        self.properties
            .iter()
            .find(|property| property.name == name)
    }

    pub fn relation_by_name(&self, name: &str) -> Option<&RelationDescriptor> {
        self.relations.iter().find(|relation| relation.name == name)
    }

    pub fn id_property(&self) -> Option<&PropertyDescriptor> {
        self.properties.iter().find(|property| property.is_id)
    }

    pub fn version_property(&self) -> Option<&PropertyDescriptor> {
        self.properties.iter().find(|property| property.is_version)
    }

    pub fn writable_properties(&self) -> impl Iterator<Item = &PropertyDescriptor> {
        self.properties.iter().filter(|property| !property.is_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_descriptor_builder() {
        let prop = PropertyDescriptor::new("username", DataType::Text)
            .column_name("user_name")
            .not_null()
            .id()
            .version();

        assert_eq!(prop.name, "username");
        assert_eq!(prop.column_name, "user_name");
        assert_eq!(prop.data_type, DataType::Text);
        assert!(!prop.nullable);
        assert!(prop.is_id);
        assert!(prop.is_version);
    }

    #[test]
    fn test_relation_descriptor_builder() {
        let rel = RelationDescriptor::new("orders", "Order")
            .local_key("user_id")
            .foreign_key("customer_id")
            .many()
            .detached()
            .keep_missing();

        assert_eq!(rel.name, "orders");
        assert_eq!(rel.target_entity, "Order");
        assert_eq!(rel.local_key, "user_id");
        assert_eq!(rel.foreign_key, "customer_id");
        assert!(rel.many);
        assert!(!rel.attach);
        assert!(!rel.delete_missing);
    }

    #[test]
    fn test_entity_descriptor_builder_and_lookups() {
        let mut entity = EntityDescriptor::new("User")
            .table_name("users")
            .data_service("auth_db")
            .audit_mask_fields(vec!["password".to_string()])
            .audit_value_max_len(Some(255));

        let id_prop = PropertyDescriptor::new("id", DataType::I64).id();
        let name_prop = PropertyDescriptor::new("name", DataType::Text);
        let version_prop = PropertyDescriptor::new("version", DataType::I64).version();

        let orders_rel = RelationDescriptor::new("orders", "Order");

        entity = entity
            .property(id_prop.clone())
            .property(name_prop.clone())
            .property(version_prop.clone())
            .relation(orders_rel.clone());

        assert_eq!(entity.name, "User");
        assert_eq!(entity.table_name, "users");
        assert_eq!(entity.data_service, Some("auth_db".to_string()));
        assert_eq!(entity.audit_mask_fields, vec!["password".to_string()]);
        assert_eq!(entity.audit_value_max_len, Some(255));

        // Lookups
        assert_eq!(entity.property_by_name("name"), Some(&name_prop));
        assert_eq!(entity.property_by_name("missing"), None);

        assert_eq!(entity.relation_by_name("orders"), Some(&orders_rel));
        assert_eq!(entity.relation_by_name("missing"), None);

        assert_eq!(entity.id_property(), Some(&id_prop));
        assert_eq!(entity.version_property(), Some(&version_prop));

        let writable: Vec<_> = entity.writable_properties().collect();
        assert_eq!(writable.len(), 2);
        assert!(writable.contains(&&name_prop));
        assert!(writable.contains(&&version_prop));
    }
}
