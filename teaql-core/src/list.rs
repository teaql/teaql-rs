use crate::{Entity, IdentifiableEntity, Record, Value, VersionedEntity};

#[derive(Debug, Clone, PartialEq)]
pub struct SmartList<T> {
    pub data: Vec<T>,
    pub total_count: Option<u64>,
    pub aggregations: Record,
    pub summary: Record,
}

impl<T> SmartList<T> {
    pub fn new(data: Vec<T>) -> Self {
        Self {
            data,
            total_count: None,
            aggregations: Record::new(),
            summary: Record::new(),
        }
    }

    pub fn with_total_count(mut self, total_count: u64) -> Self {
        self.total_count = Some(total_count);
        self
    }

    pub fn with_aggregation(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.aggregations.insert(key.into(), value.into());
        self
    }

    pub fn with_summary(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.summary.insert(key.into(), value.into());
        self
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn first(&self) -> Option<&T> {
        self.data.first()
    }

    pub fn into_vec(self) -> Vec<T> {
        self.data
    }

    pub fn map<U>(self, mapper: impl FnMut(T) -> U) -> SmartList<U> {
        SmartList {
            data: self.data.into_iter().map(mapper).collect(),
            total_count: self.total_count,
            aggregations: self.aggregations,
            summary: self.summary,
        }
    }

    pub fn into_records(self) -> SmartList<Record>
    where
        T: Entity,
    {
        SmartList {
            data: self.data.into_iter().map(Entity::into_record).collect(),
            total_count: self.total_count,
            aggregations: self.aggregations,
            summary: self.summary,
        }
    }
}

impl<T> SmartList<T>
where
    T: IdentifiableEntity,
{
    pub fn ids(&self) -> Vec<Value> {
        self.data.iter().map(IdentifiableEntity::id_value).collect()
    }
}

impl<T> SmartList<T>
where
    T: VersionedEntity,
{
    pub fn versions(&self) -> Vec<i64> {
        self.data.iter().map(VersionedEntity::version).collect()
    }
}

impl<T> From<Vec<T>> for SmartList<T> {
    fn from(data: Vec<T>) -> Self {
        Self::new(data)
    }
}

impl<T> Default for SmartList<T> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
