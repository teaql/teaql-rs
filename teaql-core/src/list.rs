use std::collections::BTreeMap;
use std::hash::Hash;
use std::ops::{Index, IndexMut};

use crate::{Entity, IdentifiableEntity, Record, Value, VersionedEntity};

#[derive(Debug, Clone, PartialEq)]
pub struct SmartList<T> {
    pub data: Vec<T>,
    pub total_count: Option<u64>,
    pub aggregations: Record,
    pub summary: Record,
}

impl<T> SmartList<T> {
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

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

    pub fn push(&mut self, value: T) {
        self.data.push(value);
    }

    pub fn extend(&mut self, values: impl IntoIterator<Item = T>) {
        self.data.extend(values);
    }

    pub fn set(&mut self, index: usize, value: T) -> Option<T> {
        if index >= self.data.len() {
            return None;
        }
        Some(std::mem::replace(&mut self.data[index], value))
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index)
    }

    pub fn last(&self) -> Option<&T> {
        self.data.last()
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

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.data.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.data.iter_mut()
    }

    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    pub fn retain(&mut self, filter: impl FnMut(&T) -> bool) {
        self.data.retain(filter);
    }

    pub fn total_count_or_len(&self) -> u64 {
        self.total_count.unwrap_or(self.data.len() as u64)
    }

    pub fn aggregation(&self, key: &str) -> Option<&Value> {
        self.aggregations.get(key)
    }

    pub fn summary(&self, key: &str) -> Option<&Value> {
        self.summary.get(key)
    }

    pub fn aggregation_json(&self) -> serde_json::Value {
        crate::record_to_json_value(&self.aggregations)
    }

    pub fn summary_json(&self) -> serde_json::Value {
        crate::record_to_json_value(&self.summary)
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

    pub fn to_list<U>(&self, mapper: impl FnMut(&T) -> U) -> Vec<U> {
        self.data.iter().map(mapper).collect()
    }

    pub fn to_set<U>(&self, mapper: impl FnMut(&T) -> U) -> std::collections::BTreeSet<U>
    where
        U: Ord,
    {
        self.data.iter().map(mapper).collect()
    }

    pub fn identity_map<K>(&self, mut key: impl FnMut(&T) -> K) -> BTreeMap<K, T>
    where
        K: Ord,
        T: Clone,
    {
        self.data
            .iter()
            .map(|item| (key(item), item.clone()))
            .collect()
    }

    pub fn group_by<K>(&self, mut key: impl FnMut(&T) -> K) -> BTreeMap<K, Vec<T>>
    where
        K: Ord,
        T: Clone,
    {
        let mut groups = BTreeMap::new();
        for item in &self.data {
            groups
                .entry(key(item))
                .or_insert_with(Vec::new)
                .push(item.clone());
        }
        groups
    }

    pub fn merge_by<K>(
        &mut self,
        incoming: impl IntoIterator<Item = T>,
        mut key: impl FnMut(&T) -> K,
    ) where
        K: Eq + Hash,
    {
        let mut positions = self
            .data
            .iter()
            .enumerate()
            .map(|(index, item)| (key(item), index))
            .collect::<std::collections::HashMap<_, _>>();
        for item in incoming {
            let item_key = key(&item);
            if let Some(index) = positions.get(&item_key).copied() {
                self.data[index] = item;
            } else {
                positions.insert(item_key, self.data.len());
                self.data.push(item);
            }
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

    pub fn map_by_id(&self) -> BTreeMap<String, T>
    where
        T: Clone,
    {
        self.data
            .iter()
            .map(|item| (id_key(&item.id_value()), item.clone()))
            .collect()
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

impl<T> IntoIterator for SmartList<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a SmartList<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut SmartList<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter_mut()
    }
}

impl<T> FromIterator<T> for SmartList<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl<T> Extend<T> for SmartList<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.data.extend(iter);
    }
}

impl<T> Index<usize> for SmartList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for SmartList<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
    }
}

fn id_key(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => format!("b:{value}"),
        Value::I64(value) => format!("i:{value}"),
        Value::U64(value) => format!("u:{value}"),
        Value::F64(value) => format!("f:{value}"),
        Value::Text(value) => format!("t:{value}"),
        Value::Json(value) => format!("j:{value}"),
        Value::Date(value) => format!("d:{value}"),
        Value::Timestamp(value) => format!("ts:{}", value.to_rfc3339()),
        Value::Object(_) => "object".to_owned(),
        Value::List(_) => "list".to_owned(),
    }
}
