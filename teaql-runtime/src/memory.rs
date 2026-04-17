use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use teaql_core::{
    AggregateFunction, BinaryOp, DeleteCommand, Entity, Expr, InsertCommand, Record,
    RecoverCommand, SelectQuery, SmartList, SortDirection, UpdateCommand, Value,
};

use crate::{InMemoryMetadataStore, MetadataStore, RepositoryError, RuntimeError};

#[derive(Debug)]
pub enum MemoryRepositoryError {
    Poisoned,
    UnsupportedExpression(String),
    UnsupportedAggregate(String),
}

impl std::fmt::Display for MemoryRepositoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Poisoned => write!(f, "memory repository lock poisoned"),
            Self::UnsupportedExpression(message) => {
                write!(f, "unsupported memory expression: {message}")
            }
            Self::UnsupportedAggregate(message) => {
                write!(f, "unsupported memory aggregate: {message}")
            }
        }
    }
}

impl std::error::Error for MemoryRepositoryError {}

#[derive(Debug, Clone)]
pub struct MemoryRepository<M = InMemoryMetadataStore> {
    metadata: M,
    data: Arc<Mutex<BTreeMap<String, Vec<Record>>>>,
}

impl<M> MemoryRepository<M>
where
    M: MetadataStore,
{
    pub fn new(metadata: M) -> Self {
        Self {
            metadata,
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_rows(mut self, entity: impl Into<String>, rows: Vec<Record>) -> Self {
        self.seed(entity, rows);
        self
    }

    pub fn seed(&mut self, entity: impl Into<String>, rows: Vec<Record>) {
        if let Ok(mut data) = self.data.lock() {
            data.insert(entity.into(), rows);
        }
    }

    pub fn fetch_all(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, RepositoryError<MemoryRepositoryError>> {
        self.require_entity(&query.entity)?;
        let data = self
            .data
            .lock()
            .map_err(|_| RepositoryError::Executor(MemoryRepositoryError::Poisoned))?;
        let mut rows = data.get(&query.entity).cloned().unwrap_or_default();
        drop(data);

        if let Some(filter) = &query.filter {
            rows = rows
                .into_iter()
                .filter_map(|row| match eval_filter(filter, &row) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(RepositoryError::Executor)?;
        }

        if !query.aggregates.is_empty() {
            return aggregate_rows(query, &rows).map_err(RepositoryError::Executor);
        }

        apply_ordering(&mut rows, query);
        rows = apply_slice(rows, query);
        if !query.projection.is_empty() {
            rows = rows
                .into_iter()
                .map(|row| project_row(row, &query.projection))
                .collect();
        }
        Ok(rows)
    }

    pub fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<MemoryRepositoryError>> {
        self.fetch_all(query).map(SmartList::from)
    }

    pub fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<MemoryRepositoryError>>
    where
        T: Entity,
    {
        self.fetch_all(query)?
            .into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub fn insert(
        &self,
        command: &InsertCommand,
    ) -> Result<u64, RepositoryError<MemoryRepositoryError>> {
        self.require_entity(&command.entity)?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| RepositoryError::Executor(MemoryRepositoryError::Poisoned))?;
        data.entry(command.entity.clone())
            .or_default()
            .push(command.values.clone());
        Ok(1)
    }

    pub fn update(
        &self,
        command: &UpdateCommand,
    ) -> Result<u64, RepositoryError<MemoryRepositoryError>> {
        let (id_property, version_property) = self.id_and_version_properties(&command.entity)?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| RepositoryError::Executor(MemoryRepositoryError::Poisoned))?;
        let rows = data.entry(command.entity.clone()).or_default();
        let Some(row) = rows
            .iter_mut()
            .find(|row| row.get(id_property) == Some(&command.id))
        else {
            return self.maybe_optimistic_conflict(
                command.expected_version,
                &command.entity,
                &command.id,
            );
        };

        if let Some(expected_version) = command.expected_version {
            if row.get(version_property) != Some(&Value::I64(expected_version)) {
                return Err(RepositoryError::Runtime(
                    RuntimeError::OptimisticLockConflict {
                        entity: command.entity.clone(),
                        id: format!("{:?}", command.id),
                    },
                ));
            }
            row.insert(
                version_property.to_owned(),
                Value::I64(expected_version + 1),
            );
        }

        for (key, value) in &command.values {
            row.insert(key.clone(), value.clone());
        }
        Ok(1)
    }

    pub fn delete(
        &self,
        command: &DeleteCommand,
    ) -> Result<u64, RepositoryError<MemoryRepositoryError>> {
        let (id_property, version_property) = self.id_and_version_properties(&command.entity)?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| RepositoryError::Executor(MemoryRepositoryError::Poisoned))?;
        let rows = data.entry(command.entity.clone()).or_default();
        let Some(index) = rows
            .iter()
            .position(|row| row.get(id_property) == Some(&command.id))
        else {
            return self.maybe_optimistic_conflict(
                command.expected_version,
                &command.entity,
                &command.id,
            );
        };

        if let Some(expected_version) = command.expected_version {
            if rows[index].get(version_property) != Some(&Value::I64(expected_version)) {
                return Err(RepositoryError::Runtime(
                    RuntimeError::OptimisticLockConflict {
                        entity: command.entity.clone(),
                        id: format!("{:?}", command.id),
                    },
                ));
            }
        }

        if command.soft_delete {
            let next_version = command
                .expected_version
                .or_else(|| read_i64(rows[index].get(version_property)))
                .map(|version| -(version.abs() + 1))
                .unwrap_or(-1);
            rows[index].insert(version_property.to_owned(), Value::I64(next_version));
        } else {
            rows.remove(index);
        }
        Ok(1)
    }

    pub fn recover(
        &self,
        command: &RecoverCommand,
    ) -> Result<u64, RepositoryError<MemoryRepositoryError>> {
        let (id_property, version_property) = self.id_and_version_properties(&command.entity)?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| RepositoryError::Executor(MemoryRepositoryError::Poisoned))?;
        let rows = data.entry(command.entity.clone()).or_default();
        let Some(row) = rows
            .iter_mut()
            .find(|row| row.get(id_property) == Some(&command.id))
        else {
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        };

        if row.get(version_property) != Some(&Value::I64(command.expected_version)) {
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        }

        row.insert(
            version_property.to_owned(),
            Value::I64(command.expected_version.abs() + 1),
        );
        Ok(1)
    }

    fn require_entity(&self, entity: &str) -> Result<(), RepositoryError<MemoryRepositoryError>> {
        self.metadata
            .entity(entity)
            .map(|_| ())
            .ok_or_else(|| RepositoryError::Runtime(RuntimeError::MissingEntity(entity.to_owned())))
    }

    fn id_and_version_properties(
        &self,
        entity: &str,
    ) -> Result<(&str, &str), RepositoryError<MemoryRepositoryError>> {
        let descriptor = self.metadata.entity(entity).ok_or_else(|| {
            RepositoryError::Runtime(RuntimeError::MissingEntity(entity.to_owned()))
        })?;
        let id = descriptor
            .id_property()
            .map(|property| property.name.as_str())
            .unwrap_or("id");
        let version = descriptor
            .version_property()
            .map(|property| property.name.as_str())
            .unwrap_or("version");
        Ok((id, version))
    }

    fn maybe_optimistic_conflict(
        &self,
        expected_version: Option<i64>,
        entity: &str,
        id: &Value,
    ) -> Result<u64, RepositoryError<MemoryRepositoryError>> {
        if expected_version.is_some() {
            Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: entity.to_owned(),
                    id: format!("{id:?}"),
                },
            ))
        } else {
            Ok(0)
        }
    }
}

fn eval_filter(expr: &Expr, row: &Record) -> Result<bool, MemoryRepositoryError> {
    match expr {
        Expr::Column(_) | Expr::Value(_) => value_truthy(&eval_value(expr, row)?),
        Expr::Binary { left, op, right } => {
            let left = eval_value(left, row)?;
            let right = eval_value(right, row)?;
            eval_binary(&left, *op, &right)
        }
        Expr::Between { expr, lower, upper } => {
            let value = eval_value(expr, row)?;
            let lower = eval_value(lower, row)?;
            let upper = eval_value(upper, row)?;
            Ok(compare_values(&value, &lower) != Some(Ordering::Less)
                && compare_values(&value, &upper) != Some(Ordering::Greater))
        }
        Expr::IsNull(expr) => Ok(matches!(eval_value(expr, row)?, Value::Null)),
        Expr::IsNotNull(expr) => Ok(!matches!(eval_value(expr, row)?, Value::Null)),
        Expr::And(parts) => {
            for part in parts {
                if !eval_filter(part, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::Or(parts) => {
            for part in parts {
                if eval_filter(part, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Not(expr) => Ok(!eval_filter(expr, row)?),
    }
}

fn eval_value(expr: &Expr, row: &Record) -> Result<Value, MemoryRepositoryError> {
    match expr {
        Expr::Column(column) => Ok(row.get(column).cloned().unwrap_or(Value::Null)),
        Expr::Value(value) => Ok(value.clone()),
        other => Err(MemoryRepositoryError::UnsupportedExpression(format!(
            "cannot evaluate {other:?} as a scalar value"
        ))),
    }
}

fn eval_binary(left: &Value, op: BinaryOp, right: &Value) -> Result<bool, MemoryRepositoryError> {
    match op {
        BinaryOp::Eq => Ok(values_equal(left, right)),
        BinaryOp::Ne => Ok(!values_equal(left, right)),
        BinaryOp::Gt => Ok(compare_values(left, right) == Some(Ordering::Greater)),
        BinaryOp::Gte => Ok(matches!(
            compare_values(left, right),
            Some(Ordering::Greater | Ordering::Equal)
        )),
        BinaryOp::Lt => Ok(compare_values(left, right) == Some(Ordering::Less)),
        BinaryOp::Lte => Ok(matches!(
            compare_values(left, right),
            Some(Ordering::Less | Ordering::Equal)
        )),
        BinaryOp::Like => match (left, right) {
            (Value::Text(value), Value::Text(pattern)) => Ok(like_matches(value, pattern)),
            _ => Ok(false),
        },
        BinaryOp::In => match right {
            Value::List(values) => Ok(values.iter().any(|value| values_equal(left, value))),
            _ => Err(MemoryRepositoryError::UnsupportedExpression(
                "IN expects a list value".to_owned(),
            )),
        },
    }
}

fn value_truthy(value: &Value) -> Result<bool, MemoryRepositoryError> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::Null => Ok(false),
        other => Err(MemoryRepositoryError::UnsupportedExpression(format!(
            "non-boolean expression result: {other:?}"
        ))),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::I64(left), Value::U64(right)) if *left >= 0 => *left as u64 == *right,
        (Value::U64(left), Value::I64(right)) if *right >= 0 => *left == *right as u64,
        _ => left == right,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::I64(left), Value::I64(right)) => Some(left.cmp(right)),
        (Value::U64(left), Value::U64(right)) => Some(left.cmp(right)),
        (Value::I64(left), Value::U64(right)) if *left >= 0 => Some((*left as u64).cmp(right)),
        (Value::U64(left), Value::I64(right)) if *right >= 0 => Some(left.cmp(&(*right as u64))),
        (Value::F64(left), Value::F64(right)) => left.partial_cmp(right),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Date(left), Value::Date(right)) => Some(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn like_matches(value: &str, pattern: &str) -> bool {
    if pattern == "%" {
        return true;
    }
    match (pattern.strip_prefix('%'), pattern.strip_suffix('%')) {
        (Some(inner), Some(_)) if pattern.len() >= 2 => value.contains(&inner[..inner.len() - 1]),
        (Some(suffix), None) => value.ends_with(suffix),
        (None, Some(prefix)) => value.starts_with(prefix),
        _ => value == pattern,
    }
}

fn apply_ordering(rows: &mut [Record], query: &SelectQuery) {
    for order in query.order_by.iter().rev() {
        rows.sort_by(|left, right| {
            let ordering = match (left.get(&order.field), right.get(&order.field)) {
                (Some(left), Some(right)) => compare_values(left, right).unwrap_or(Ordering::Equal),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            match order.direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
            }
        });
    }
}

fn apply_slice(rows: Vec<Record>, query: &SelectQuery) -> Vec<Record> {
    let Some(slice) = query.slice else {
        return rows;
    };
    let offset = usize::try_from(slice.offset).unwrap_or(usize::MAX);
    let limit = slice
        .limit
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    rows.into_iter().skip(offset).take(limit).collect()
}

fn project_row(row: Record, projection: &[String]) -> Record {
    projection
        .iter()
        .filter_map(|field| row.get(field).cloned().map(|value| (field.clone(), value)))
        .collect()
}

fn aggregate_rows(
    query: &SelectQuery,
    rows: &[Record],
) -> Result<Vec<Record>, MemoryRepositoryError> {
    let mut groups: BTreeMap<Vec<String>, Vec<&Record>> = BTreeMap::new();
    if query.group_by.is_empty() {
        groups.insert(Vec::new(), rows.iter().collect());
    } else {
        for row in rows {
            let key = query
                .group_by
                .iter()
                .map(|field| row.get(field).map(value_key).unwrap_or_default())
                .collect::<Vec<_>>();
            groups.entry(key).or_default().push(row);
        }
    }

    groups
        .into_values()
        .map(|rows| {
            let mut output = Record::new();
            if let Some(first) = rows.first() {
                for field in &query.group_by {
                    if let Some(value) = first.get(field) {
                        output.insert(field.clone(), value.clone());
                    }
                }
            }
            for aggregate in &query.aggregates {
                let value = match aggregate.function {
                    AggregateFunction::Count => Value::U64(rows.len() as u64),
                    AggregateFunction::Sum => numeric_sum(&rows, &aggregate.field)?,
                    AggregateFunction::Avg => numeric_avg(&rows, &aggregate.field)?,
                    AggregateFunction::Min => min_max(&rows, &aggregate.field, false)?,
                    AggregateFunction::Max => min_max(&rows, &aggregate.field, true)?,
                };
                output.insert(aggregate.alias.clone(), value);
            }
            Ok(output)
        })
        .collect()
}

fn numeric_sum(rows: &[&Record], field: &str) -> Result<Value, MemoryRepositoryError> {
    let mut float_sum = 0.0;
    let mut integer_sum: i128 = 0;
    let mut saw_float = false;
    for value in rows.iter().filter_map(|row| row.get(field)) {
        match value {
            Value::I64(value) => integer_sum += i128::from(*value),
            Value::U64(value) => integer_sum += i128::from(*value),
            Value::F64(value) => {
                saw_float = true;
                float_sum += *value;
            }
            Value::Null => {}
            other => {
                return Err(MemoryRepositoryError::UnsupportedAggregate(format!(
                    "SUM does not support {other:?}"
                )));
            }
        }
    }
    if saw_float {
        Ok(Value::F64(float_sum + integer_sum as f64))
    } else if integer_sum >= 0 {
        Ok(Value::U64(integer_sum as u64))
    } else {
        Ok(Value::I64(integer_sum as i64))
    }
}

fn numeric_avg(rows: &[&Record], field: &str) -> Result<Value, MemoryRepositoryError> {
    let mut sum = 0.0;
    let mut count = 0.0;
    for value in rows.iter().filter_map(|row| row.get(field)) {
        match value {
            Value::I64(value) => {
                sum += *value as f64;
                count += 1.0;
            }
            Value::U64(value) => {
                sum += *value as f64;
                count += 1.0;
            }
            Value::F64(value) => {
                sum += *value;
                count += 1.0;
            }
            Value::Null => {}
            other => {
                return Err(MemoryRepositoryError::UnsupportedAggregate(format!(
                    "AVG does not support {other:?}"
                )));
            }
        }
    }
    Ok(if count == 0.0 {
        Value::Null
    } else {
        Value::F64(sum / count)
    })
}

fn min_max(rows: &[&Record], field: &str, max: bool) -> Result<Value, MemoryRepositoryError> {
    let mut selected: Option<Value> = None;
    for value in rows.iter().filter_map(|row| row.get(field)) {
        if matches!(value, Value::Null) {
            continue;
        }
        match &selected {
            None => selected = Some(value.clone()),
            Some(current) => {
                let Some(ordering) = compare_values(value, current) else {
                    return Err(MemoryRepositoryError::UnsupportedAggregate(format!(
                        "MIN/MAX does not support {value:?}"
                    )));
                };
                if (max && ordering == Ordering::Greater) || (!max && ordering == Ordering::Less) {
                    selected = Some(value.clone());
                }
            }
        }
    }
    Ok(selected.unwrap_or(Value::Null))
}

fn value_key(value: &Value) -> String {
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

fn read_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::I64(value)) => Some(*value),
        _ => None,
    }
}
