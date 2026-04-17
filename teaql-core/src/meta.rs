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
}

impl RelationDescriptor {
    pub fn new(name: impl Into<String>, target_entity: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            target_entity: target_entity.into(),
            local_key: "id".to_owned(),
            foreign_key: "id".to_owned(),
            many: false,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDescriptor {
    pub name: String,
    pub table_name: String,
    pub properties: Vec<PropertyDescriptor>,
    pub relations: Vec<RelationDescriptor>,
}

impl EntityDescriptor {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            table_name: default_table_name(&name),
            name,
            properties: Vec::new(),
            relations: Vec::new(),
        }
    }

    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
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
