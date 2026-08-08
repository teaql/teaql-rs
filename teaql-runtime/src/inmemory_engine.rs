use std::cmp::Ordering;
use std::time::SystemTime;

use teaql_core::{
    Aggregate, AggregateFunction, BinaryOp, Expr, OrderBy, Record, SelectQuery, SortDirection,
    Value,
};
use teaql_data_service::{DataServiceOperation, ExecutionMetadata, QueryResult};

/// A general-purpose in-memory query engine that executes [`SelectQuery`] against a
/// `Vec<Record>`. This replaces the database engine for non-SQL data sources.
pub struct InMemoryQueryEngine;

impl InMemoryQueryEngine {
    /// Execute a [`SelectQuery`] against the given rows and return a [`QueryResult`].
    ///
    /// Processing order: filter → aggregation (if any) → sort → paginate → project.
    pub fn execute(query: &SelectQuery, mut rows: Vec<Record>) -> QueryResult {
        let started_at = SystemTime::now();

        // 1. Filter
        if let Some(filter) = &query.filter {
            Self::filter(&mut rows, filter);
        }

        // 2. Aggregation and GROUP BY short-circuit the normal pipeline.
        if !query.aggregates.is_empty() || !query.group_by.is_empty() {
            let mut result = Self::aggregate(query, rows);
            result.metadata.started_at = started_at;
            result.metadata.ended_at = SystemTime::now();
            return result;
        }

        // 3. Sort
        if !query.order_by.is_empty() {
            Self::sort(&mut rows, &query.order_by);
        }

        // 4. Paginate
        if let Some(slice) = &query.slice {
            rows = Self::paginate(rows, slice);
        }

        // 5. Project
        if !query.projection.is_empty() {
            rows = Self::project(rows, &query.projection);
        }

        let count = rows.len();
        QueryResult {
            rows,
            metadata: ExecutionMetadata {
                debug_query: None,
                backend: "memory".to_owned(),
                operation: DataServiceOperation::Query,
                started_at,
                ended_at: SystemTime::now(),
                affected_rows: None,
                result_count: Some(count),
                trace_chain: Vec::new(),
                comment: None,
                backend_request_id: None,
            },
        }
    }

    /// Retain only the rows for which the expression evaluates to `true`.
    fn filter(rows: &mut Vec<Record>, expr: &Expr) {
        rows.retain(|row| ExprEvaluator::eval(expr, row));
    }

    /// Sort rows in-place according to the given [`OrderBy`] list (multi-column).
    fn sort(rows: &mut Vec<Record>, order_by: &[OrderBy]) {
        rows.sort_by(|a, b| {
            for ob in order_by {
                let va = a.get(&ob.field).unwrap_or(&Value::Null);
                let vb = b.get(&ob.field).unwrap_or(&Value::Null);
                let ord = compare_values(va, vb);
                let ord = match ob.direction {
                    SortDirection::Asc => ord,
                    SortDirection::Desc => ord.reverse(),
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
    }

    /// Apply offset/limit pagination.
    fn paginate(rows: Vec<Record>, slice: &teaql_core::Slice) -> Vec<Record> {
        let offset = slice.offset as usize;
        let iter = rows.into_iter().skip(offset);
        match slice.limit {
            Some(limit) => iter.take(limit as usize).collect(),
            None => iter.collect(),
        }
    }

    /// Keep only the specified fields in each record.
    fn project(rows: Vec<Record>, projection: &[String]) -> Vec<Record> {
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .filter(|(key, _)| projection.contains(key))
                    .collect()
            })
            .collect()
    }

    /// Compute aggregations over the (already-filtered) rows and return the result
    /// as a single-row [`QueryResult`].
    fn aggregate(query: &SelectQuery, rows: Vec<Record>) -> QueryResult {
        let started_at = SystemTime::now();

        // If there are group-by fields, partition the rows into groups.
        if !query.group_by.is_empty() {
            return Self::aggregate_grouped(query, rows, started_at);
        }

        // No group-by: single aggregation over all rows.
        let mut result_row = Record::new();
        for agg in &query.aggregates {
            let value = compute_aggregate(agg, &rows);
            result_row.insert(agg.alias.clone(), value);
        }

        let result_rows = vec![result_row];
        let count = result_rows.len();
        QueryResult {
            rows: result_rows,
            metadata: ExecutionMetadata {
                debug_query: None,
                backend: "memory".to_owned(),
                operation: DataServiceOperation::Query,
                started_at,
                ended_at: SystemTime::now(),
                affected_rows: None,
                result_count: Some(count),
                trace_chain: Vec::new(),
                comment: None,
                backend_request_id: None,
            },
        }
    }

    /// Aggregate with GROUP BY support.
    fn aggregate_grouped(
        query: &SelectQuery,
        rows: Vec<Record>,
        started_at: SystemTime,
    ) -> QueryResult {
        // Build groups keyed by the group-by field values.
        let mut groups: Vec<(Vec<Value>, Vec<Record>)> = Vec::new();

        for row in rows {
            let key: Vec<Value> = query
                .group_by
                .iter()
                .map(|gb| row.get(gb).cloned().unwrap_or(Value::Null))
                .collect();

            match groups.iter_mut().find(|(k, _)| k == &key) {
                Some((_k, group)) => group.push(row),
                None => groups.push((key, vec![row])),
            }
        }

        let mut result_rows = Vec::with_capacity(groups.len());
        for (key_values, group_rows) in &groups {
            let mut result_row = Record::new();

            // Include group-by fields in the output.
            for (i, gb) in query.group_by.iter().enumerate() {
                result_row.insert(gb.clone(), key_values[i].clone());
            }

            // Compute each aggregate over this group.
            for agg in &query.aggregates {
                let value = compute_aggregate(agg, group_rows);
                result_row.insert(agg.alias.clone(), value);
            }

            result_rows.push(result_row);
        }

        let count = result_rows.len();
        QueryResult {
            rows: result_rows,
            metadata: ExecutionMetadata {
                debug_query: None,
                backend: "memory".to_owned(),
                operation: DataServiceOperation::Query,
                started_at,
                ended_at: SystemTime::now(),
                affected_rows: None,
                result_count: Some(count),
                trace_chain: Vec::new(),
                comment: None,
                backend_request_id: None,
            },
        }
    }
}

/// Evaluates [`Expr`] trees against a single [`Record`].
pub struct ExprEvaluator;

impl ExprEvaluator {
    /// Evaluate an expression as a boolean predicate against a row.
    pub fn eval(expr: &Expr, row: &Record) -> bool {
        match expr {
            Expr::Binary { left, op, right } => {
                let lv = Self::resolve(left, row);
                let rv = Self::resolve(right, row);
                Self::compare_op(&lv, op, &rv)
            }
            Expr::And(parts) => parts.iter().all(|p| Self::eval(p, row)),
            Expr::Or(parts) => parts.iter().any(|p| Self::eval(p, row)),
            Expr::Not(inner) => !Self::eval(inner, row),
            Expr::IsNull(inner) => Self::resolve(inner, row) == Value::Null,
            Expr::IsNotNull(inner) => Self::resolve(inner, row) != Value::Null,
            Expr::Between {
                expr: inner,
                lower,
                upper,
            } => {
                let v = Self::resolve(inner, row);
                let lo = Self::resolve(lower, row);
                let hi = Self::resolve(upper, row);
                compare_values(&v, &lo) != Ordering::Less
                    && compare_values(&v, &hi) != Ordering::Greater
            }
            // SubQuery is not supported for in-memory evaluation; always false.
            Expr::SubQuery { .. } => false,
            // Function expressions are not boolean predicates in general.
            Expr::Function { .. } => false,
            // A bare column or value is truthy if it is a Bool(true).
            Expr::Column(_) | Expr::Value(_) => {
                matches!(Self::resolve(expr, row), Value::Bool(true))
            }
        }
    }

    /// Resolve an expression to a concrete [`Value`] given a row.
    pub fn resolve(expr: &Expr, row: &Record) -> Value {
        match expr {
            Expr::Column(name) => row.get(name).cloned().unwrap_or(Value::Null),
            Expr::Value(v) => v.clone(),
            Expr::Binary { left, op, right } => {
                let lv = Self::resolve(left, row);
                let rv = Self::resolve(right, row);
                Value::Bool(Self::compare_op(&lv, op, &rv))
            }
            Expr::And(parts) => Value::Bool(parts.iter().all(|p| Self::eval(p, row))),
            Expr::Or(parts) => Value::Bool(parts.iter().any(|p| Self::eval(p, row))),
            Expr::Not(inner) => Value::Bool(!Self::eval(inner, row)),
            Expr::IsNull(inner) => Value::Bool(Self::resolve(inner, row) == Value::Null),
            Expr::IsNotNull(inner) => Value::Bool(Self::resolve(inner, row) != Value::Null),
            Expr::Between {
                expr: inner,
                lower,
                upper,
            } => {
                let v = Self::resolve(inner, row);
                let lo = Self::resolve(lower, row);
                let hi = Self::resolve(upper, row);
                Value::Bool(
                    compare_values(&v, &lo) != Ordering::Less
                        && compare_values(&v, &hi) != Ordering::Greater,
                )
            }
            Expr::SubQuery { .. } => Value::Null,
            Expr::Function { .. } => Value::Null,
        }
    }

    /// Compare two values according to a [`BinaryOp`].
    fn compare_op(left: &Value, op: &BinaryOp, right: &Value) -> bool {
        match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Ne => left != right,
            BinaryOp::Gt => compare_values(left, right) == Ordering::Greater,
            BinaryOp::Gte => matches!(
                compare_values(left, right),
                Ordering::Greater | Ordering::Equal
            ),
            BinaryOp::Lt => compare_values(left, right) == Ordering::Less,
            BinaryOp::Lte => matches!(
                compare_values(left, right),
                Ordering::Less | Ordering::Equal
            ),
            BinaryOp::Like => match (left, right) {
                (Value::Text(text), Value::Text(pattern)) => Self::like_match(text, pattern),
                _ => false,
            },
            BinaryOp::NotLike => match (left, right) {
                (Value::Text(text), Value::Text(pattern)) => !Self::like_match(text, pattern),
                _ => true,
            },
            BinaryOp::In | BinaryOp::InLarge => match right {
                Value::List(items) => items.contains(left),
                _ => left == right,
            },
            BinaryOp::NotIn | BinaryOp::NotInLarge => match right {
                Value::List(items) => !items.contains(left),
                _ => left != right,
            },
        }
    }

    /// SQL LIKE matching without regex.
    ///
    /// - `%` matches any sequence of characters (including empty).
    /// - `_` matches exactly one character.
    fn like_match(text: &str, pattern: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();
        like_match_recursive(&text_chars, 0, &pattern_chars, 0)
    }
}

/// Recursive helper for SQL LIKE matching with memoisation-free DP-style
/// backtracking via iterative `%` expansion.
fn like_match_recursive(text: &[char], ti: usize, pattern: &[char], pi: usize) -> bool {
    let mut ti = ti;
    let mut pi = pi;

    loop {
        if pi == pattern.len() {
            return ti == text.len();
        }

        match pattern[pi] {
            '%' => {
                // Skip consecutive '%' characters.
                while pi < pattern.len() && pattern[pi] == '%' {
                    pi += 1;
                }
                // If '%' was the last character in pattern, match everything.
                if pi == pattern.len() {
                    return true;
                }
                // Try matching the rest of the pattern from every position.
                for start in ti..=text.len() {
                    if like_match_recursive(text, start, pattern, pi) {
                        return true;
                    }
                }
                return false;
            }
            '_' => {
                if ti >= text.len() {
                    return false;
                }
                ti += 1;
                pi += 1;
            }
            ch => {
                if ti >= text.len() || text[ti] != ch {
                    return false;
                }
                ti += 1;
                pi += 1;
            }
        }
    }
}

/// Compare a signed `i64` against an unsigned `u64`, handling the negative case.
fn compare_i64_u64(a: i64, b: u64) -> Ordering {
    match a < 0 {
        true => Ordering::Less,
        false => (a as u64).cmp(&b),
    }
}

/// Compare two [`Value`]s for ordering. Nulls sort first.
fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::I64(a), Value::I64(b)) => a.cmp(b),
        (Value::U64(a), Value::U64(b)) => a.cmp(b),
        (Value::I64(a), Value::U64(b)) => compare_i64_u64(*a, *b),
        (Value::U64(a), Value::I64(b)) => compare_i64_u64(*b, *a).reverse(),
        (Value::F64(a), Value::F64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        // Cross-type numeric comparisons via f64.
        _ => value_to_f64(a)
            .zip(value_to_f64(b))
            .and_then(|(fa, fb)| fa.partial_cmp(&fb))
            .unwrap_or(Ordering::Equal),
    }
}

/// Best-effort conversion of a [`Value`] to `f64` for cross-type numeric comparison.
fn value_to_f64(v: &Value) -> Option<f64> {
    v.try_f64()
}

/// Count rows, treating `"*"` as counting all rows and other fields as counting non-null values.
fn count_rows(rows: &[Record], field: &str) -> Value {
    let count = match field {
        "*" => rows.len(),
        _ => rows
            .iter()
            .filter(|r| r.get(field).map(|v| v != &Value::Null).unwrap_or(false))
            .count(),
    };
    Value::I64(count as i64)
}

/// Compute a single aggregate over a slice of rows.
fn compute_aggregate(agg: &Aggregate, rows: &[Record]) -> Value {
    match agg.function {
        AggregateFunction::Count => count_rows(rows, &agg.field),
        AggregateFunction::Sum => {
            let mut sum: f64 = 0.0;
            let mut found = false;
            for row in rows {
                if let Some(v) = row.get(&agg.field) {
                    if let Some(f) = v.try_f64() {
                        sum += f;
                        found = true;
                    }
                }
            }
            found.then(|| Value::F64(sum)).unwrap_or(Value::Null)
        }
        AggregateFunction::Avg => {
            let mut sum: f64 = 0.0;
            let mut count: u64 = 0;
            for row in rows {
                if let Some(v) = row.get(&agg.field) {
                    if let Some(f) = v.try_f64() {
                        sum += f;
                        count += 1;
                    }
                }
            }
            (count > 0)
                .then(|| Value::F64(sum / count as f64))
                .unwrap_or(Value::Null)
        }
        AggregateFunction::Max => {
            let mut max: Option<&Value> = None;
            for row in rows {
                if let Some(v) = row.get(&agg.field) {
                    if v == &Value::Null {
                        continue;
                    }
                    max = Some(match max {
                        Some(current) if compare_values(v, current) == Ordering::Greater => v,
                        Some(current) => current,
                        None => v,
                    });
                }
            }
            max.cloned().unwrap_or(Value::Null)
        }
        AggregateFunction::Min => {
            let mut min: Option<&Value> = None;
            for row in rows {
                if let Some(v) = row.get(&agg.field) {
                    if v == &Value::Null {
                        continue;
                    }
                    min = Some(match min {
                        Some(current) if compare_values(v, current) == Ordering::Less => v,
                        Some(current) => current,
                        None => v,
                    });
                }
            }
            min.cloned().unwrap_or(Value::Null)
        }
        // Unsupported aggregate functions return Null.
        AggregateFunction::Stddev
        | AggregateFunction::StddevPop
        | AggregateFunction::VarSamp
        | AggregateFunction::VarPop
        | AggregateFunction::BitAnd
        | AggregateFunction::BitOr
        | AggregateFunction::BitXor => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{Aggregate, AggregateFunction, Record, SelectQuery, Value};

    fn make_row(pairs: Vec<(&str, Value)>) -> Record {
        pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
    }

    fn sample_rows() -> Vec<Record> {
        vec![
            make_row(vec![
                ("id", Value::U64(1)),
                ("name", Value::Text("Alice".to_owned())),
                ("age", Value::I64(30)),
            ]),
            make_row(vec![
                ("id", Value::U64(2)),
                ("name", Value::Text("Bob".to_owned())),
                ("age", Value::I64(25)),
            ]),
            make_row(vec![
                ("id", Value::U64(3)),
                ("name", Value::Text("Charlie".to_owned())),
                ("age", Value::I64(35)),
            ]),
        ]
    }

    #[test]
    fn test_execute_no_filter() {
        let query = SelectQuery::new("User");
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.metadata.backend, "memory");
    }

    #[test]
    fn test_execute_with_eq_filter() {
        let query = SelectQuery::new("User").filter(Expr::eq("name", "Bob"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("name"),
            Some(&Value::Text("Bob".to_owned()))
        );
    }

    #[test]
    fn test_execute_with_gt_filter() {
        let query = SelectQuery::new("User").filter(Expr::gt("age", 28_i64));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows.len(), 2); // Alice(30) and Charlie(35)
    }

    #[test]
    fn test_sort_ascending() {
        let query = SelectQuery::new("User").order_by(teaql_core::OrderBy::asc("age"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        let ages: Vec<_> = result
            .rows
            .iter()
            .map(|r| r.get("age").unwrap().clone())
            .collect();
        assert_eq!(ages, vec![Value::I64(25), Value::I64(30), Value::I64(35)]);
    }

    #[test]
    fn test_sort_descending() {
        let query = SelectQuery::new("User").order_by(teaql_core::OrderBy::desc("age"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        let ages: Vec<_> = result
            .rows
            .iter()
            .map(|r| r.get("age").unwrap().clone())
            .collect();
        assert_eq!(ages, vec![Value::I64(35), Value::I64(30), Value::I64(25)]);
    }

    #[test]
    fn test_paginate() {
        let query = SelectQuery::new("User").page(1, 1);
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("name"),
            Some(&Value::Text("Bob".to_owned()))
        );
    }

    #[test]
    fn test_projection() {
        let query = SelectQuery::new("User").projects(["name"]);
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        for row in &result.rows {
            assert!(row.contains_key("name"));
            assert!(!row.contains_key("id"));
            assert!(!row.contains_key("age"));
        }
    }

    #[test]
    fn test_count_aggregate() {
        let query = SelectQuery::new("User").aggregate(Aggregate::count("total"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("total"), Some(&Value::I64(3)));
    }

    #[test]
    fn test_sum_aggregate() {
        let query = SelectQuery::new("User").aggregate(Aggregate::sum("age", "age_sum"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows[0].get("age_sum"), Some(&Value::F64(90.0)));
    }

    #[test]
    fn test_avg_aggregate() {
        let query = SelectQuery::new("User").aggregate(Aggregate::avg("age", "age_avg"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows[0].get("age_avg"), Some(&Value::F64(30.0)));
    }

    #[test]
    fn test_max_aggregate() {
        let query = SelectQuery::new("User").aggregate(Aggregate::max("age", "age_max"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows[0].get("age_max"), Some(&Value::I64(35)));
    }

    #[test]
    fn test_min_aggregate() {
        let query = SelectQuery::new("User").aggregate(Aggregate::min("age", "age_min"));
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows[0].get("age_min"), Some(&Value::I64(25)));
    }

    #[test]
    fn test_like_match_percent() {
        assert!(ExprEvaluator::like_match("hello world", "%world"));
        assert!(ExprEvaluator::like_match("hello world", "hello%"));
        assert!(ExprEvaluator::like_match("hello world", "%lo wo%"));
        assert!(ExprEvaluator::like_match("hello world", "%"));
        assert!(!ExprEvaluator::like_match("hello world", "%xyz%"));
    }

    #[test]
    fn test_like_match_underscore() {
        assert!(ExprEvaluator::like_match("abc", "a_c"));
        assert!(!ExprEvaluator::like_match("abbc", "a_c"));
        assert!(ExprEvaluator::like_match("abc", "___"));
        assert!(!ExprEvaluator::like_match("ab", "___"));
    }

    #[test]
    fn test_like_match_combined() {
        assert!(ExprEvaluator::like_match("foobar", "f%r"));
        assert!(ExprEvaluator::like_match("foobar", "f__b%"));
        assert!(!ExprEvaluator::like_match("foobar", "f__x%"));
    }

    #[test]
    fn test_and_or_not() {
        let row = make_row(vec![("a", Value::I64(10)), ("b", Value::I64(20))]);
        let expr_and = Expr::and([Expr::eq("a", 10_i64), Expr::eq("b", 20_i64)]);
        assert!(ExprEvaluator::eval(&expr_and, &row));

        let expr_or = Expr::or([Expr::eq("a", 99_i64), Expr::eq("b", 20_i64)]);
        assert!(ExprEvaluator::eval(&expr_or, &row));

        let expr_not = Expr::negate(Expr::eq("a", 99_i64));
        assert!(ExprEvaluator::eval(&expr_not, &row));
    }

    #[test]
    fn test_is_null_is_not_null() {
        let row = make_row(vec![("x", Value::Null), ("y", Value::I64(1))]);
        assert!(ExprEvaluator::eval(&Expr::is_null("x"), &row));
        assert!(!ExprEvaluator::eval(&Expr::is_not_null("x"), &row));
        assert!(ExprEvaluator::eval(&Expr::is_not_null("y"), &row));
    }

    #[test]
    fn test_between() {
        let row = make_row(vec![("age", Value::I64(30))]);
        assert!(ExprEvaluator::eval(
            &Expr::between("age", Value::I64(25), Value::I64(35)),
            &row
        ));
        assert!(!ExprEvaluator::eval(
            &Expr::between("age", Value::I64(31), Value::I64(35)),
            &row
        ));
    }

    #[test]
    fn test_in_list() {
        let row = make_row(vec![("status", Value::Text("active".to_owned()))]);
        let expr = Expr::in_list(
            "status",
            vec![
                Value::Text("active".to_owned()),
                Value::Text("pending".to_owned()),
            ],
        );
        assert!(ExprEvaluator::eval(&expr, &row));

        let expr_miss = Expr::in_list("status", vec![Value::Text("closed".to_owned())]);
        assert!(!ExprEvaluator::eval(&expr_miss, &row));
    }

    #[test]
    fn test_execute_group_by() {
        let mut query = SelectQuery::new("User");
        query.group_by = vec!["age".to_owned()];
        query.aggregates = vec![Aggregate::sum("age", "sum_age")];

        let rows = vec![
            make_row(vec![("age", Value::I64(30))]),
            make_row(vec![("age", Value::I64(30))]),
            make_row(vec![("age", Value::I64(25))]),
        ];
        let result = InMemoryQueryEngine::execute(&query, rows);
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_execute_group_by_no_aggregate() {
        let mut query = SelectQuery::new("User");
        query.group_by = vec!["age".to_owned()];

        let rows = vec![
            make_row(vec![("age", Value::I64(30))]),
            make_row(vec![("age", Value::I64(30))]),
        ];
        let result = InMemoryQueryEngine::execute(&query, rows);
        // aggregate_grouped includes group-by fields even if no aggregates are provided
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("age"), Some(&Value::I64(30)));
    }

    #[test]
    fn test_aggregates_unsupported() {
        let unsupported = vec![
            AggregateFunction::Stddev,
            AggregateFunction::StddevPop,
            AggregateFunction::VarSamp,
            AggregateFunction::VarPop,
            AggregateFunction::BitAnd,
            AggregateFunction::BitOr,
            AggregateFunction::BitXor,
        ];
        for func in unsupported {
            let mut query = SelectQuery::new("User");
            query.aggregates = vec![Aggregate {
                function: func,
                field: "age".to_owned(),
                alias: "res".to_owned(),
            }];
            let result = InMemoryQueryEngine::execute(&query, sample_rows());
            assert_eq!(result.rows[0].get("res"), Some(&Value::Null));
        }
    }

    #[test]
    fn test_aggregate_min_max_null_handling() {
        let mut query = SelectQuery::new("User");
        query.aggregates = vec![
            Aggregate::max("age", "max_age"),
            Aggregate::min("age", "min_age"),
        ];
        let rows = vec![
            make_row(vec![("age", Value::Null)]),
            make_row(vec![("age", Value::I64(10))]),
        ];
        let result = InMemoryQueryEngine::execute(&query, rows);
        assert_eq!(result.rows[0].get("max_age"), Some(&Value::I64(10)));
        assert_eq!(result.rows[0].get("min_age"), Some(&Value::I64(10)));
    }

    #[test]
    fn test_sort_multiple_columns() {
        let mut query = SelectQuery::new("User");
        query.order_by = vec![
            teaql_core::OrderBy::asc("age"),
            teaql_core::OrderBy::desc("id"),
        ];
        let rows = vec![
            make_row(vec![("id", Value::U64(1)), ("age", Value::I64(30))]),
            make_row(vec![("id", Value::U64(2)), ("age", Value::I64(30))]),
        ];
        let result = InMemoryQueryEngine::execute(&query, rows);
        assert_eq!(result.rows[0].get("id"), Some(&Value::U64(2)));
    }

    #[test]
    fn test_paginate_no_limit() {
        let mut query = SelectQuery::new("User");
        query.slice = Some(teaql_core::Slice {
            offset: 1,
            limit: None,
        });
        let result = InMemoryQueryEngine::execute(&query, sample_rows());
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_compare_values_edge_cases() {
        use chrono::{NaiveDate, TimeZone, Utc};
        use rust_decimal::Decimal;
        use std::cmp::Ordering;

        assert_eq!(
            super::compare_values(&Value::Null, &Value::Null),
            Ordering::Equal
        );
        assert_eq!(
            super::compare_values(&Value::Null, &Value::I64(1)),
            Ordering::Less
        );
        assert_eq!(
            super::compare_values(&Value::I64(1), &Value::Null),
            Ordering::Greater
        );

        assert_eq!(
            super::compare_values(&Value::Bool(false), &Value::Bool(true)),
            Ordering::Less
        );

        assert_eq!(
            super::compare_values(&Value::I64(-1), &Value::U64(1)),
            Ordering::Less
        );
        assert_eq!(
            super::compare_values(&Value::U64(1), &Value::I64(-1)),
            Ordering::Greater
        );

        // F64 vs F64 with nan fallback
        assert_eq!(
            super::compare_values(&Value::F64(f64::NAN), &Value::F64(f64::NAN)),
            Ordering::Equal
        );

        let d1 = Value::Decimal(Decimal::new(100, 2));
        let d2 = Value::Decimal(Decimal::new(200, 2));
        assert_eq!(super::compare_values(&d1, &d2), Ordering::Less);

        let date1 = Value::Date(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        let date2 = Value::Date(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap());
        assert_eq!(super::compare_values(&date1, &date2), Ordering::Less);

        let t1 = Value::Timestamp(teaql_core::time::Timestamp(100));
        let t2 = Value::Timestamp(teaql_core::time::Timestamp(200));
        assert_eq!(super::compare_values(&t1, &t2), Ordering::Less);

        assert_eq!(
            super::compare_values(&Value::Text("a".into()), &Value::Text("b".into())),
            Ordering::Less
        );

        // Cross type numeric (e.g. I64 vs F64)
        assert_eq!(
            super::compare_values(&Value::I64(1), &Value::F64(2.0)),
            Ordering::Less
        );
    }

    #[test]
    fn test_expr_eval_additional_ops() {
        let row = make_row(vec![
            ("a", Value::I64(10)),
            ("b", Value::Text("foo".to_owned())),
        ]);

        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::Ne,
                right: Box::new(Expr::Value(Value::I64(20)))
            },
            &row
        ));
        assert!(!ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Value(Value::I64(10)))
            },
            &row
        ));
        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::Gte,
                right: Box::new(Expr::Value(Value::I64(10)))
            },
            &row
        ));
        assert!(!ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Value(Value::I64(10)))
            },
            &row
        ));
        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::Lte,
                right: Box::new(Expr::Value(Value::I64(10)))
            },
            &row
        ));

        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("b".into())),
                op: BinaryOp::Like,
                right: Box::new(Expr::Value(Value::Text("f%".into())))
            },
            &row
        ));
        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("b".into())),
                op: BinaryOp::NotLike,
                right: Box::new(Expr::Value(Value::Text("x%".into())))
            },
            &row
        ));
        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::NotLike,
                right: Box::new(Expr::Value(Value::Text("x%".into())))
            },
            &row
        ));
        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::InLarge,
                right: Box::new(Expr::Value(Value::List(vec![Value::I64(10)])))
            },
            &row
        ));
        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::NotInLarge,
                right: Box::new(Expr::Value(Value::List(vec![Value::I64(20)])))
            },
            &row
        ));

        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::In,
                right: Box::new(Expr::Value(Value::I64(10)))
            },
            &row
        ));
        assert!(ExprEvaluator::eval(
            &Expr::Binary {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::NotIn,
                right: Box::new(Expr::Value(Value::I64(20)))
            },
            &row
        ));
    }

    #[test]
    fn test_expr_resolve() {
        let row = make_row(vec![("a", Value::Bool(true)), ("b", Value::Bool(false))]);

        assert_eq!(
            ExprEvaluator::resolve(&Expr::Column("a".into()), &row),
            Value::Bool(true)
        );
        assert_eq!(
            ExprEvaluator::resolve(&Expr::Value(Value::I64(10)), &row),
            Value::I64(10)
        );
        assert_eq!(
            ExprEvaluator::resolve(
                &Expr::Binary {
                    left: Box::new(Expr::Column("a".into())),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Value(Value::Bool(true)))
                },
                &row
            ),
            Value::Bool(true)
        );

        assert_eq!(
            ExprEvaluator::resolve(
                &Expr::and([Expr::Column("a".into()), Expr::Column("a".into())]),
                &row
            ),
            Value::Bool(true)
        );
        assert_eq!(
            ExprEvaluator::resolve(
                &Expr::or([Expr::Column("a".into()), Expr::Column("b".into())]),
                &row
            ),
            Value::Bool(true)
        );
        assert_eq!(
            ExprEvaluator::resolve(&Expr::negate(Expr::Column("b".into())), &row),
            Value::Bool(true)
        );

        assert_eq!(
            ExprEvaluator::resolve(&Expr::is_null("a"), &row),
            Value::Bool(false)
        );
        assert_eq!(
            ExprEvaluator::resolve(&Expr::is_not_null("a"), &row),
            Value::Bool(true)
        );
        assert_eq!(
            ExprEvaluator::resolve(
                &Expr::between("a", Value::Bool(false), Value::Bool(true)),
                &row
            ),
            Value::Bool(true)
        );

        assert_eq!(
            ExprEvaluator::resolve(
                &Expr::SubQuery {
                    left: Box::new(Expr::Column("a".into())),
                    op: BinaryOp::In,
                    entity: teaql_core::EntityDescriptor::new("User"),
                    query: Box::new(SelectQuery::new("User"))
                },
                &row
            ),
            Value::Null
        );
        assert_eq!(
            ExprEvaluator::resolve(
                &Expr::Function {
                    function: teaql_core::ExprFunction::Count,
                    args: vec![]
                },
                &row
            ),
            Value::Null
        );
        assert!(!ExprEvaluator::eval(
            &Expr::SubQuery {
                left: Box::new(Expr::Column("a".into())),
                op: BinaryOp::In,
                entity: teaql_core::EntityDescriptor::new("User"),
                query: Box::new(SelectQuery::new("User"))
            },
            &row
        ));
        assert!(!ExprEvaluator::eval(
            &Expr::Function {
                function: teaql_core::ExprFunction::Count,
                args: vec![]
            },
            &row
        ));

        assert!(ExprEvaluator::eval(&Expr::Column("a".into()), &row));
    }

    #[test]
    fn test_like_match_wildcards() {
        assert!(super::ExprEvaluator::like_match("hello", "hello%"));
        assert!(super::ExprEvaluator::like_match("hello", "he%o"));
        assert!(super::ExprEvaluator::like_match("hello", "h_ll_"));
        assert!(!super::ExprEvaluator::like_match("hello", "h_ll_x"));
        assert!(!super::ExprEvaluator::like_match("hello", "x%"));
        assert!(super::ExprEvaluator::like_match("hello", "he%%o"));
    }

    #[test]
    fn test_like_match_underscore_out_of_bounds() {
        assert!(!super::ExprEvaluator::like_match("a", "a_"));
    }

    #[test]
    fn test_like_match_mismatch() {
        assert!(!super::ExprEvaluator::like_match("a", "b"));
    }

    #[test]
    fn test_aggregate_count_field_nulls() {
        let mut query = SelectQuery::new("User");
        query.aggregates = vec![Aggregate {
            function: AggregateFunction::Count,
            field: "age".to_owned(),
            alias: "count_age".to_owned(),
        }];
        let rows = vec![
            make_row(vec![("age", Value::I64(30))]),
            make_row(vec![("age", Value::Null)]),
        ];
        let result = InMemoryQueryEngine::execute(&query, rows);
        assert_eq!(result.rows[0].get("count_age"), Some(&Value::I64(1)));
    }

    #[test]
    fn test_aggregate_sum_avg_nulls() {
        let mut query = SelectQuery::new("User");
        query.aggregates = vec![
            Aggregate::sum("age", "sum_age"),
            Aggregate::avg("age", "avg_age"),
        ];
        let rows = vec![
            make_row(vec![("age", Value::Null)]),
            make_row(vec![("age", Value::I64(10))]),
            make_row(vec![("age", Value::Text("not_a_number".into()))]),
        ];
        let result = InMemoryQueryEngine::execute(&query, rows);
        assert_eq!(result.rows[0].get("sum_age"), Some(&Value::F64(10.0)));
        assert_eq!(result.rows[0].get("avg_age"), Some(&Value::F64(10.0)));
    }

    #[test]
    fn test_aggregate_empty_rows() {
        let mut query = SelectQuery::new("User");
        query.aggregates = vec![
            Aggregate::sum("age", "sum_age"),
            Aggregate::avg("age", "avg_age"),
            Aggregate::max("age", "max_age"),
            Aggregate::min("age", "min_age"),
        ];
        let result = InMemoryQueryEngine::execute(&query, vec![]);
        assert_eq!(result.rows[0].get("sum_age"), Some(&Value::Null));
        assert_eq!(result.rows[0].get("avg_age"), Some(&Value::Null));
        assert_eq!(result.rows[0].get("max_age"), Some(&Value::Null));
        assert_eq!(result.rows[0].get("min_age"), Some(&Value::Null));
    }
}
