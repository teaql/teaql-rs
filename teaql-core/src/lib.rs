extern crate self as teaql_core;

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Bool,
    I64,
    F64,
    Text,
    Json,
    Date,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Object(BTreeMap<String, Value>),
    List(Vec<Value>),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

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
            table_name: name.to_lowercase(),
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
        self.properties.iter().find(|property| property.name == name)
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

pub trait TeaqlEntity {
    fn entity_descriptor() -> EntityDescriptor;

    fn register_into(store: &mut impl EntityDescriptorStore) {
        store.register_descriptor(Self::entity_descriptor());
    }
}

pub trait EntityDescriptorStore {
    fn register_descriptor(&mut self, descriptor: EntityDescriptor);
}

#[macro_export]
macro_rules! register_entities {
    ($store:expr, $($entity:ty),+ $(,)?) => {{
        $(
            <$entity as $crate::TeaqlEntity>::register_into($store);
        )+
    }};
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    In,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Value(Value),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Between {
        expr: Box<Expr>,
        lower: Box<Expr>,
        upper: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    pub fn value(value: impl Into<Value>) -> Self {
        Self::Value(value.into())
    }

    pub fn eq(column: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Binary {
            left: Box::new(Self::column(column)),
            op: BinaryOp::Eq,
            right: Box::new(Self::value(value)),
        }
    }

    pub fn in_list(column: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Self {
        Self::Binary {
            left: Box::new(Self::column(column)),
            op: BinaryOp::In,
            right: Box::new(Self::Value(Value::List(values.into_iter().collect()))),
        }
    }

    pub fn and(parts: impl IntoIterator<Item = Expr>) -> Self {
        Self::And(parts.into_iter().collect())
    }
}

impl Value {
    pub fn object(record: Record) -> Self {
        Self::Object(record)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    pub field: String,
    pub direction: SortDirection,
}

impl OrderBy {
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Desc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub function: AggregateFunction,
    pub field: String,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slice {
    pub limit: Option<u64>,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationLoad {
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
    Recover,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertCommand {
    pub entity: String,
    pub values: Record,
}

impl InsertCommand {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            values: Record::new(),
        }
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: Option<i64>,
    pub values: Record,
}

impl UpdateCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            values: Record::new(),
        }
    }

    pub fn expected_version(mut self, version: i64) -> Self {
        self.expected_version = Some(version);
        self
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: Option<i64>,
    pub soft_delete: bool,
}

impl DeleteCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            soft_delete: true,
        }
    }

    pub fn expected_version(mut self, version: i64) -> Self {
        self.expected_version = Some(version);
        self
    }

    pub fn hard_delete(mut self) -> Self {
        self.soft_delete = false;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoverCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: i64,
}

impl RecoverCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>, expected_version: i64) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub entity: String,
    pub projection: Vec<String>,
    pub filter: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub slice: Option<Slice>,
    pub aggregates: Vec<Aggregate>,
    pub group_by: Vec<String>,
    pub relations: Vec<RelationLoad>,
}

impl SelectQuery {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            projection: Vec::new(),
            filter: None,
            order_by: Vec::new(),
            slice: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            relations: Vec::new(),
        }
    }

    pub fn project(mut self, field: impl Into<String>) -> Self {
        self.projection.push(field.into());
        self
    }

    pub fn filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        let slice = self.slice.get_or_insert(Slice {
            limit: None,
            offset: 0,
        });
        slice.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        let slice = self.slice.get_or_insert(Slice {
            limit: None,
            offset: 0,
        });
        slice.offset = offset;
        self
    }
}

pub type Record = BTreeMap<String, Value>;

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_macros::TeaqlEntity;

    #[derive(Default)]
    struct TestStore {
        descriptors: Vec<EntityDescriptor>,
    }

    impl EntityDescriptorStore for TestStore {
        fn register_descriptor(&mut self, descriptor: EntityDescriptor) {
            self.descriptors.push(descriptor);
        }
    }

    #[allow(dead_code)]
    #[derive(TeaqlEntity)]
    #[teaql(entity = "Order", table = "orders")]
    struct OrderRow {
        #[teaql(id)]
        id: i64,
        #[teaql(version)]
        version: i64,
        #[teaql(column = "display_name")]
        name: String,
    }

    #[test]
    fn derive_entity_descriptor() {
        let descriptor = OrderRow::entity_descriptor();
        assert_eq!(descriptor.name, "Order");
        assert_eq!(descriptor.table_name, "orders");
        assert_eq!(descriptor.id_property().map(|p| p.name.as_str()), Some("id"));
        assert_eq!(
            descriptor.property_by_name("name").map(|p| p.column_name.as_str()),
            Some("display_name")
        );
    }

    #[allow(dead_code)]
    #[derive(TeaqlEntity)]
    #[teaql(entity = "OrderLine", table = "orderline")]
    struct OrderLineRow {
        #[teaql(id)]
        id: i64,
        #[teaql(column = "order_id")]
        order_id: i64,
        #[teaql(relation(target = "Product", local_key = "product_id", foreign_key = "id"))]
        product: Option<()>,
    }

    #[test]
    fn derive_relation_descriptor_and_register() {
        let descriptor = OrderLineRow::entity_descriptor();
        let relation = descriptor.relation_by_name("product").unwrap();
        assert_eq!(relation.target_entity, "Product");
        assert_eq!(relation.local_key, "product_id");
        assert_eq!(relation.foreign_key, "id");

        let mut store = TestStore::default();
        OrderLineRow::register_into(&mut store);
        assert_eq!(store.descriptors.len(), 1);
        assert_eq!(store.descriptors[0].name, "OrderLine");
    }

    #[test]
    fn register_entities_macro_registers_multiple_descriptors() {
        let mut store = TestStore::default();
        crate::register_entities!(&mut store, OrderRow, OrderLineRow);

        assert_eq!(store.descriptors.len(), 2);
        assert_eq!(store.descriptors[0].name, "Order");
        assert_eq!(store.descriptors[1].name, "OrderLine");
    }
}
