#![allow(clippy::manual_async_fn)]
#![allow(async_fn_in_trait)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use teaql_core::{
    CompactRow, EntityDescriptor, EntitySnapshot, Expr, GeneratedValues, SelectQuery, Value,
};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

use crate::{CompiledQuery, SqlCompileError, SqlDialect};

pub trait SqlTransport: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> impl std::future::Future<Output = Result<Vec<CompactRow>, Self::Error>> + Send;
    fn fetch_repeated_compact_sql(
        &self,
        template: &CompiledQuery,
        param_index: usize,
        values: &[Value],
    ) -> impl std::future::Future<Output = Result<Vec<CompactRow>, Self::Error>> + Send {
        async move {
            let mut rows = Vec::new();
            for value in values {
                let mut query = template.clone();
                query.params[param_index] = value.clone();
                rows.extend(self.fetch_all_compact_sql(&query).await?);
            }
            Ok(rows)
        }
    }
    fn execute_sql(
        &self,
        query: &CompiledQuery,
    ) -> impl std::future::Future<Output = Result<u64, Self::Error>> + Send;
}

pub trait StreamingSqlTransport: SqlTransport {
    fn stream_sql(
        &self,
        query: CompiledQuery,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error>;
}

pub trait SqlTransactionTransport: SqlTransport {
    type Tx<'a>: SqlTransport<Error = Self::Error>
        + SqlTransaction<Error = Self::Error>
        + Send
        + Sync
        + 'a
    where
        Self: 'a;

    fn begin_sql(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Tx<'_>, Self::Error>> + Send;
}

pub trait SqlTransaction {
    type Error: std::error::Error + Send + Sync + 'static;
    fn commit_sql(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
    fn rollback_sql(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug)]
pub enum SqlExecutorError<E: std::error::Error + Send + Sync + 'static> {
    Compile(SqlCompileError),
    Transport(E),
    PersistedRecord(String),
}

impl<E: std::error::Error + Send + Sync + 'static> std::fmt::Display for SqlExecutorError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlExecutorError::Compile(e) => write!(f, "SQL compile error: {}", e),
            SqlExecutorError::Transport(e) => write!(f, "Transport error: {}", e),
            SqlExecutorError::PersistedRecord(e) => write!(f, "Persisted record error: {}", e),
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> std::error::Error for SqlExecutorError<E> {}

#[derive(Clone)]
pub struct SqlDataServiceExecutor<D, T, S> {
    pub dialect: D,
    pub transport: T,
    pub schema_provider: S,
    descriptor_cache: Arc<RwLock<HashMap<String, Arc<teaql_core::EntityDescriptor>>>>,
    select_plan_cache: Arc<RwLock<Vec<(SelectQuery, String)>>>,
}

impl<D, T, S> SqlDataServiceExecutor<D, T, S> {
    pub fn new(dialect: D, transport: T, schema_provider: S) -> Self {
        Self {
            dialect,
            transport,
            schema_provider,
            descriptor_cache: Arc::new(RwLock::new(HashMap::new())),
            select_plan_cache: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<D, T, S> SqlDataServiceExecutor<D, T, S>
where
    D: SqlDialect,
    S: teaql_data_service::SchemaProvider,
{
    fn compile_select_cached(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
    ) -> Result<CompiledQuery, SqlCompileError> {
        compile_select_with_cache(&self.dialect, &self.select_plan_cache, entity, query)
    }
}

fn compile_select_with_cache<D: SqlDialect>(
    dialect: &D,
    plan_cache: &RwLock<Vec<(SelectQuery, String)>>,
    entity: &EntityDescriptor,
    query: &SelectQuery,
) -> Result<CompiledQuery, SqlCompileError> {
    if let Ok(cache) = plan_cache.read()
        && let Some((_, sql)) = cache
            .iter()
            .find(|(candidate, _)| select_plan_matches(candidate, query))
    {
        return Ok(CompiledQuery {
            sql: sql.clone(),
            params: collect_select_params(entity, query, dialect.large_in_uses_array_param()),
            comment: query.comment.clone(),
        });
    }

    let key = select_plan_key(query);
    let compiled = dialect.compile_select(entity, query)?;
    if let Ok(mut cache) = plan_cache.write() {
        if cache.len() >= 256 {
            cache.remove(0);
        }
        if !cache
            .iter()
            .any(|(candidate, _)| select_plan_matches(candidate, query))
        {
            cache.push((key, compiled.sql.clone()));
        }
    }
    Ok(compiled)
}

fn select_plan_matches(key: &SelectQuery, query: &SelectQuery) -> bool {
    key.hard_limit == query.hard_limit
        && key.entity == query.entity
        && key.projection == query.projection
        && key.expr_projection.len() == query.expr_projection.len()
        && key
            .expr_projection
            .iter()
            .zip(&query.expr_projection)
            .all(|(left, right)| {
                left.alias == right.alias && expr_plan_matches(&left.expr, &right.expr)
            })
        && key.search_with_text.is_some() == query.search_with_text.is_some()
        && optional_expr_plan_matches(key.filter.as_ref(), query.filter.as_ref())
        && optional_expr_plan_matches(key.having.as_ref(), query.having.as_ref())
        && key.order_by.len() == query.order_by.len()
        && key
            .order_by
            .iter()
            .zip(&query.order_by)
            .all(|(left, right)| {
                left.field == right.field
                    && left.direction == right.direction
                    && optional_expr_plan_matches(left.expr.as_ref(), right.expr.as_ref())
            })
        && key.slice == query.slice
        && key.partition_by == query.partition_by
        && key.aggregates == query.aggregates
        && key.group_by == query.group_by
        && key.relations == query.relations
        && key.aggregation_cache == query.aggregation_cache
        && key.raw_sql == query.raw_sql
        && key.raw_sql_search_criteria == query.raw_sql_search_criteria
        && key.dynamic_properties == query.dynamic_properties
        && key.raw_projections == query.raw_projections
        && key.object_group_bys == query.object_group_bys
        && key.child_enhancements == query.child_enhancements
        && key.stream_config == query.stream_config
        && key.continuous_page_fetch == query.continuous_page_fetch
}

fn optional_expr_plan_matches(key: Option<&Expr>, query: Option<&Expr>) -> bool {
    match (key, query) {
        (Some(key), Some(query)) => expr_plan_matches(key, query),
        (None, None) => true,
        _ => false,
    }
}

fn expr_plan_matches(key: &Expr, query: &Expr) -> bool {
    match (key, query) {
        (Expr::Column(left), Expr::Column(right)) => left == right,
        (Expr::Value(Value::List(left)), Expr::Value(Value::List(right))) => {
            left.len() == right.len()
        }
        (Expr::Value(Value::List(_)), Expr::Value(_))
        | (Expr::Value(_), Expr::Value(Value::List(_))) => false,
        (Expr::Value(_), Expr::Value(_)) => true,
        (
            Expr::Function {
                function: left_function,
                args: left_args,
            },
            Expr::Function {
                function: right_function,
                args: right_args,
            },
        ) => left_function == right_function && expr_slice_plan_matches(left_args, right_args),
        (
            Expr::Binary {
                left: left_left,
                op: left_op,
                right: left_right,
            },
            Expr::Binary {
                left: right_left,
                op: right_op,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && expr_plan_matches(left_left, right_left)
                && expr_plan_matches(left_right, right_right)
        }
        (
            Expr::SubQuery {
                left: left_expr,
                op: left_op,
                entity: left_entity,
                query: left_query,
            },
            Expr::SubQuery {
                left: right_expr,
                op: right_op,
                entity: right_entity,
                query: right_query,
            },
        ) => {
            left_op == right_op
                && left_entity == right_entity
                && expr_plan_matches(left_expr, right_expr)
                && select_plan_matches(left_query, right_query)
        }
        (
            Expr::Between {
                expr: left_expr,
                lower: left_lower,
                upper: left_upper,
            },
            Expr::Between {
                expr: right_expr,
                lower: right_lower,
                upper: right_upper,
            },
        ) => {
            expr_plan_matches(left_expr, right_expr)
                && expr_plan_matches(left_lower, right_lower)
                && expr_plan_matches(left_upper, right_upper)
        }
        (Expr::IsNull(left), Expr::IsNull(right))
        | (Expr::IsNotNull(left), Expr::IsNotNull(right))
        | (Expr::Not(left), Expr::Not(right)) => expr_plan_matches(left, right),
        (Expr::And(left), Expr::And(right)) | (Expr::Or(left), Expr::Or(right)) => {
            expr_slice_plan_matches(left, right)
        }
        _ => false,
    }
}

fn expr_slice_plan_matches(left: &[Expr], right: &[Expr]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| expr_plan_matches(left, right))
}

fn select_plan_key(query: &SelectQuery) -> SelectQuery {
    let mut key = query.clone();
    key.comment = None;
    key.trace_chain.clear();
    if key.search_with_text.is_some() {
        key.search_with_text = Some(String::new());
    }
    for projection in &mut key.expr_projection {
        normalize_expr_values(&mut projection.expr);
    }
    if let Some(expr) = &mut key.filter {
        normalize_expr_values(expr);
    }
    if let Some(expr) = &mut key.having {
        normalize_expr_values(expr);
    }
    for order in &mut key.order_by {
        if let Some(expr) = &mut order.expr {
            normalize_expr_values(expr);
        }
    }
    key
}

fn normalize_expr_values(expr: &mut Expr) {
    match expr {
        Expr::Value(Value::List(values)) => {
            values.fill(Value::Null);
        }
        Expr::Value(value) => *value = Value::Null,
        Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
            for arg in args {
                normalize_expr_values(arg);
            }
        }
        Expr::Binary { left, right, .. } => {
            normalize_expr_values(left);
            normalize_expr_values(right);
        }
        Expr::SubQuery { left, query, .. } => {
            normalize_expr_values(left);
            **query = select_plan_key(query);
        }
        Expr::Between { expr, lower, upper } => {
            normalize_expr_values(expr);
            normalize_expr_values(lower);
            normalize_expr_values(upper);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::Not(expr) => {
            normalize_expr_values(expr);
        }
        Expr::Column(_) => {}
    }
}

fn collect_select_params(
    entity: &EntityDescriptor,
    query: &SelectQuery,
    large_in_uses_array_param: bool,
) -> Vec<Value> {
    let mut params = Vec::new();
    if query.raw_sql.is_some() {
        return params;
    }
    for projection in &query.expr_projection {
        collect_expr_params(&projection.expr, &mut params, large_in_uses_array_param);
    }
    let partitioned = query.partition_by.is_some() && query.slice.is_some();
    if partitioned {
        for order in &query.order_by {
            if let Some(expr) = &order.expr {
                collect_expr_params(expr, &mut params, large_in_uses_array_param);
            }
        }
    }
    if let Some(filter) = &query.filter {
        collect_expr_params(filter, &mut params, large_in_uses_array_param);
    }
    if let Some(search_text) = &query.search_with_text {
        let value = Value::from(format!("%{search_text}%"));
        params.extend(
            entity
                .properties
                .iter()
                .filter(|property| {
                    matches!(
                        property.data_type,
                        teaql_core::DataType::Text | teaql_core::DataType::LargeText
                    )
                })
                .map(|_| value.clone()),
        );
    }
    if partitioned {
        return params;
    }
    if let Some(having) = &query.having {
        collect_expr_params(having, &mut params, large_in_uses_array_param);
    }
    for order in &query.order_by {
        if let Some(expr) = &order.expr {
            collect_expr_params(expr, &mut params, large_in_uses_array_param);
        }
    }
    params
}

fn collect_expr_params(expr: &Expr, params: &mut Vec<Value>, large_in_uses_array_param: bool) {
    match expr {
        Expr::Column(_) => {}
        Expr::Value(value) => params.push(value.clone()),
        Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
            for arg in args {
                collect_expr_params(arg, params, large_in_uses_array_param);
            }
        }
        Expr::Binary { left, op, right } => {
            collect_expr_params(left, params, large_in_uses_array_param);
            if let Expr::Value(Value::List(values)) = right.as_ref()
                && matches!(
                    op,
                    teaql_core::BinaryOp::In
                        | teaql_core::BinaryOp::NotIn
                        | teaql_core::BinaryOp::InLarge
                        | teaql_core::BinaryOp::NotInLarge
                )
            {
                if large_in_uses_array_param
                    && matches!(
                        op,
                        teaql_core::BinaryOp::InLarge | teaql_core::BinaryOp::NotInLarge
                    )
                {
                    params.push(Value::List(values.clone()));
                } else {
                    params.extend(values.iter().cloned());
                }
            } else {
                collect_expr_params(right, params, large_in_uses_array_param);
            }
        }
        Expr::SubQuery {
            left,
            entity,
            query,
            ..
        } => {
            collect_expr_params(left, params, large_in_uses_array_param);
            params.extend(collect_select_params(
                entity,
                query,
                large_in_uses_array_param,
            ));
        }
        Expr::Between { expr, lower, upper } => {
            collect_expr_params(expr, params, large_in_uses_array_param);
            collect_expr_params(lower, params, large_in_uses_array_param);
            collect_expr_params(upper, params, large_in_uses_array_param);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::Not(expr) => {
            collect_expr_params(expr, params, large_in_uses_array_param);
        }
    }
}

fn partition_probe_values(query: &SelectQuery) -> Option<Vec<Value>> {
    let field = query.partition_by.as_deref()?;
    fn find(expr: &Expr, field: &str) -> Option<Vec<Value>> {
        match expr {
            Expr::Binary { left, op, right }
                if matches!(op, teaql_core::BinaryOp::In | teaql_core::BinaryOp::InLarge)
                    && matches!(left.as_ref(), Expr::Column(column) if column == field) =>
            {
                match right.as_ref() {
                    Expr::Value(Value::List(values)) => Some(values.clone()),
                    _ => None,
                }
            }
            Expr::And(parts) => parts.iter().find_map(|part| find(part, field)),
            _ => None,
        }
    }
    find(query.filter.as_ref()?, field)
}

fn scalar_partition_probe_query(query: &SelectQuery, value: Value) -> Option<SelectQuery> {
    let field = query.partition_by.as_deref()?;
    fn replace(expr: &mut Expr, field: &str, value: &Value) -> bool {
        match expr {
            Expr::Binary { left, op, right }
                if matches!(op, teaql_core::BinaryOp::In | teaql_core::BinaryOp::InLarge)
                    && matches!(left.as_ref(), Expr::Column(column) if column == field) =>
            {
                *op = teaql_core::BinaryOp::Eq;
                **right = Expr::Value(value.clone());
                true
            }
            Expr::And(parts) => parts.iter_mut().any(|part| replace(part, field, value)),
            _ => false,
        }
    }

    let mut scalar = query.clone();
    if !replace(scalar.filter.as_mut()?, field, &value) {
        return None;
    }
    scalar.partition_by = None;
    Some(scalar)
}

impl<D, T, S> SqlDataServiceExecutor<D, T, S>
where
    S: teaql_data_service::SchemaProvider,
{
    fn entity_descriptor(&self, name: &str) -> Option<Arc<teaql_core::EntityDescriptor>> {
        if let Ok(cache) = self.descriptor_cache.read()
            && let Some(descriptor) = cache.get(name)
        {
            return Some(descriptor.clone());
        }
        let descriptor = self.schema_provider.get_entity(name)?;
        if let Ok(mut cache) = self.descriptor_cache.write() {
            return Some(
                cache
                    .entry(name.to_owned())
                    .or_insert_with(|| descriptor.clone())
                    .clone(),
            );
        }
        Some(descriptor)
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> DataServiceExecutor for SqlDataServiceExecutor<D, T, S>
{
    type Error = SqlExecutorError<T::Error>;

    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities {
            query: true,
            mutation: true,
            transaction: false, // Override if T implements SqlTransactionTransport
            schema: false,
            id_generation: false,
            batch_mutation: true,
            returning: false,
            small_parent_relation_probes: self.dialect.prefers_small_parent_relation_probes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};

    #[derive(Clone, Copy)]
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn kind(&self) -> crate::DatabaseKind {
            crate::DatabaseKind::PostgreSql
        }

        fn quote_ident(&self, ident: &str) -> String {
            format!("\"{ident}\"")
        }

        fn placeholder(&self, index: usize) -> String {
            format!("${index}")
        }
    }

    #[derive(Clone, Copy)]
    struct ArrayTestDialect;

    impl SqlDialect for ArrayTestDialect {
        fn kind(&self) -> crate::DatabaseKind {
            crate::DatabaseKind::PostgreSql
        }

        fn quote_ident(&self, ident: &str) -> String {
            format!("\"{ident}\"")
        }

        fn placeholder(&self, index: usize) -> String {
            format!("${index}")
        }

        fn large_in_uses_array_param(&self) -> bool {
            true
        }

        fn compile_in(
            &self,
            entity: &EntityDescriptor,
            left: &Expr,
            op: teaql_core::BinaryOp,
            right: &Expr,
            params: &mut Vec<Value>,
        ) -> Result<String, SqlCompileError> {
            if matches!(
                op,
                teaql_core::BinaryOp::InLarge | teaql_core::BinaryOp::NotInLarge
            ) && let Expr::Value(Value::List(values)) = right
            {
                let lhs = self.compile_expr(entity, left, params)?;
                params.push(Value::List(values.clone()));
                let operator = if op == teaql_core::BinaryOp::InLarge {
                    "= ANY"
                } else {
                    "<> ALL"
                };
                return Ok(format!("({lhs} {operator}(${}))", params.len()));
            }
            Err(SqlCompileError::InvalidFunctionArguments(
                "array test dialect only supports large IN".to_owned(),
            ))
        }
    }

    #[derive(Clone, Copy)]
    struct EmptyTransport;

    impl SqlTransport for EmptyTransport {
        type Error = std::io::Error;

        async fn fetch_all_compact_sql(
            &self,
            _query: &CompiledQuery,
        ) -> Result<Vec<CompactRow>, Self::Error> {
            Ok(Vec::new())
        }

        async fn execute_sql(&self, _query: &CompiledQuery) -> Result<u64, Self::Error> {
            Ok(0)
        }
    }

    #[derive(Clone)]
    struct RepeatedProbeTransport {
        calls: Arc<AtomicUsize>,
        single_calls: Arc<AtomicUsize>,
    }

    impl SqlTransport for RepeatedProbeTransport {
        type Error = std::io::Error;

        async fn fetch_all_compact_sql(
            &self,
            _query: &CompiledQuery,
        ) -> Result<Vec<CompactRow>, Self::Error> {
            self.single_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn fetch_repeated_compact_sql(
            &self,
            template: &CompiledQuery,
            param_index: usize,
            values: &[Value],
        ) -> Result<Vec<CompactRow>, Self::Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(values, [Value::U64(7), Value::U64(9)]);
            assert_eq!(template.params[param_index], Value::U64(7));
            Ok(Vec::new())
        }

        async fn execute_sql(&self, _query: &CompiledQuery) -> Result<u64, Self::Error> {
            Ok(0)
        }
    }

    #[derive(Clone, Copy)]
    struct ProbeDialect;

    impl SqlDialect for ProbeDialect {
        fn kind(&self) -> crate::DatabaseKind {
            crate::DatabaseKind::Sqlite
        }

        fn quote_ident(&self, ident: &str) -> String {
            format!("\"{ident}\"")
        }

        fn placeholder(&self, _index: usize) -> String {
            "?".to_owned()
        }

        fn prefers_small_parent_relation_probes(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    struct CountingSchemaProvider {
        lookups: Arc<AtomicUsize>,
    }

    impl teaql_data_service::SchemaProvider for CountingSchemaProvider {
        fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            (name == "Order").then(|| Arc::new(test_entity()))
        }
    }

    fn test_entity() -> EntityDescriptor {
        EntityDescriptor::new("Order")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("name", DataType::Text))
    }

    fn query_request(capture_debug_query: bool) -> QueryRequest {
        QueryRequest {
            query: SelectQuery::new("Order"),
            trace_chain: Vec::new(),
            comment: None,
            capture_debug_query,
            capture_execution_metadata: true,
        }
    }

    #[tokio::test]
    async fn caches_entity_descriptors_across_executor_clones() {
        let lookups = Arc::new(AtomicUsize::new(0));
        let executor = SqlDataServiceExecutor::new(
            TestDialect,
            EmptyTransport,
            CountingSchemaProvider {
                lookups: lookups.clone(),
            },
        );

        let result = executor.query(query_request(false)).await.unwrap();
        executor.clone().query(query_request(true)).await.unwrap();

        assert_eq!(lookups.load(Ordering::Relaxed), 1);
        assert!(result.metadata.debug_query.is_none());
    }

    #[tokio::test]
    async fn skips_execution_metadata_when_caller_will_discard_it() {
        let executor = SqlDataServiceExecutor::new(
            TestDialect,
            EmptyTransport,
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );
        let mut request = query_request(false);
        request.capture_execution_metadata = false;

        let result = executor.query(request).await.unwrap();

        assert!(result.metadata.backend.is_empty());
        assert_eq!(result.metadata.started_at, SystemTime::UNIX_EPOCH);
        assert!(result.metadata.parameterized_query.is_none());
        assert!(result.metadata.params.is_empty());
        assert!(result.metadata.trace_chain.is_empty());
    }

    #[tokio::test]
    async fn executes_trace_off_partition_query_as_one_repeated_probe() {
        let calls = Arc::new(AtomicUsize::new(0));
        let single_calls = Arc::new(AtomicUsize::new(0));
        let executor = SqlDataServiceExecutor::new(
            ProbeDialect,
            RepeatedProbeTransport {
                calls: calls.clone(),
                single_calls: single_calls.clone(),
            },
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );
        let mut request = query_request(false);
        request.capture_execution_metadata = false;
        request.query = request
            .query
            .filter(Expr::in_list("id", [Value::U64(7), Value::U64(9)]))
            .order_desc("id")
            .limit(1)
            .partition_by("id");

        let result = executor.query(request).await.unwrap();

        assert!(result.rows.is_empty());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(single_calls.load(Ordering::Relaxed), 0);
        assert_eq!(result.metadata.started_at, SystemTime::UNIX_EPOCH);
    }

    #[tokio::test]
    async fn keeps_partition_query_observable_when_metadata_is_enabled() {
        let calls = Arc::new(AtomicUsize::new(0));
        let single_calls = Arc::new(AtomicUsize::new(0));
        let executor = SqlDataServiceExecutor::new(
            ProbeDialect,
            RepeatedProbeTransport {
                calls: calls.clone(),
                single_calls: single_calls.clone(),
            },
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );
        let mut request = query_request(false);
        request.query = request
            .query
            .filter(Expr::in_list("id", [Value::U64(7), Value::U64(9)]))
            .order_desc("id")
            .limit(1)
            .partition_by("id");

        executor.query(request).await.unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(single_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn explicit_zero_threshold_forces_window_for_probe_provider() {
        let calls = Arc::new(AtomicUsize::new(0));
        let single_calls = Arc::new(AtomicUsize::new(0));
        let executor = SqlDataServiceExecutor::new(
            ProbeDialect,
            RepeatedProbeTransport {
                calls: calls.clone(),
                single_calls: single_calls.clone(),
            },
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );
        let mut request = query_request(false);
        request.capture_execution_metadata = false;
        request.query = request
            .query
            .filter(Expr::in_list("id", [Value::U64(7), Value::U64(9)]))
            .order_desc("id")
            .limit(1)
            .partition_by("id")
            .top_n_probe_parent_threshold(0);

        executor.query(request).await.unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(single_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cached_select_plan_rebinds_values_and_separates_in_list_lengths() {
        let lookups = Arc::new(AtomicUsize::new(0));
        let executor = SqlDataServiceExecutor::new(
            TestDialect,
            EmptyTransport,
            CountingSchemaProvider { lookups },
        );
        let request = |filter| QueryRequest {
            query: SelectQuery::new("Order").filter(filter),
            trace_chain: Vec::new(),
            comment: None,
            capture_debug_query: false,
            capture_execution_metadata: true,
        };

        let first = executor
            .query(request(Expr::eq("id", 7_u64)))
            .await
            .unwrap();
        let second = executor
            .query(request(Expr::eq("id", 9_u64)))
            .await
            .unwrap();
        assert_eq!(
            first.metadata.parameterized_query,
            second.metadata.parameterized_query
        );
        assert_eq!(first.metadata.params, vec![Value::U64(7)]);
        assert_eq!(second.metadata.params, vec![Value::U64(9)]);

        let short = executor
            .query(request(Expr::in_list("id", [Value::U64(1), Value::U64(2)])))
            .await
            .unwrap();
        let long = executor
            .query(request(Expr::in_list(
                "id",
                [Value::U64(1), Value::U64(2), Value::U64(3)],
            )))
            .await
            .unwrap();
        assert_ne!(
            short.metadata.parameterized_query,
            long.metadata.parameterized_query
        );
        assert_eq!(short.metadata.params.len(), 2);
        assert_eq!(long.metadata.params.len(), 3);
    }

    #[tokio::test]
    async fn cached_select_plan_rebinds_large_in_as_one_array_parameter() {
        let executor = SqlDataServiceExecutor::new(
            ArrayTestDialect,
            EmptyTransport,
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );
        let request = |values: Vec<Value>| QueryRequest {
            query: SelectQuery::new("Order").filter(Expr::in_list("id", values)),
            trace_chain: Vec::new(),
            comment: None,
            capture_debug_query: false,
            capture_execution_metadata: true,
        };
        let first_values = (1_u64..=21).map(Value::from).collect::<Vec<_>>();
        let second_values = (101_u64..=121).map(Value::from).collect::<Vec<_>>();

        let first = executor.query(request(first_values.clone())).await.unwrap();
        let second = executor
            .query(request(second_values.clone()))
            .await
            .unwrap();

        assert_eq!(
            first.metadata.parameterized_query,
            second.metadata.parameterized_query
        );
        assert_eq!(first.metadata.params, vec![Value::List(first_values)]);
        assert_eq!(second.metadata.params, vec![Value::List(second_values)]);
    }

    #[tokio::test]
    async fn cached_select_plan_preserves_parameter_order_for_supported_query_shapes() {
        let executor = SqlDataServiceExecutor::new(
            TestDialect,
            EmptyTransport,
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );

        async fn assert_rebound(
            executor: &SqlDataServiceExecutor<TestDialect, EmptyTransport, CountingSchemaProvider>,
            warm: SelectQuery,
            current: SelectQuery,
        ) {
            let request = |query| QueryRequest {
                query,
                trace_chain: Vec::new(),
                comment: None,
                capture_debug_query: false,
                capture_execution_metadata: true,
            };
            executor.query(request(warm)).await.unwrap();
            let actual = executor.query(request(current.clone())).await.unwrap();
            let expected = TestDialect
                .compile_select(&test_entity(), &current)
                .unwrap();
            assert_eq!(actual.metadata.parameterized_query, Some(expected.sql));
            assert_eq!(actual.metadata.params, expected.params);
        }

        assert_rebound(
            &executor,
            SelectQuery::new("Order").search_with_text("first"),
            SelectQuery::new("Order").search_with_text("second"),
        )
        .await;
        assert_rebound(
            &executor,
            SelectQuery::new("Order")
                .project_expr("marker", Expr::value(1_i64))
                .filter(Expr::eq("id", 2_u64))
                .having(Expr::gt("id", 3_u64))
                .order_by(teaql_core::OrderBy::asc_expr(Expr::value(4_i64))),
            SelectQuery::new("Order")
                .project_expr("marker", Expr::value(11_i64))
                .filter(Expr::eq("id", 12_u64))
                .having(Expr::gt("id", 13_u64))
                .order_by(teaql_core::OrderBy::asc_expr(Expr::value(14_i64))),
        )
        .await;
        assert_rebound(
            &executor,
            SelectQuery::new("Order")
                .filter(Expr::eq("id", 1_u64))
                .order_by(teaql_core::OrderBy::asc_expr(Expr::value(2_i64)))
                .page(0, 10)
                .partition_by("name"),
            SelectQuery::new("Order")
                .filter(Expr::eq("id", 3_u64))
                .order_by(teaql_core::OrderBy::asc_expr(Expr::value(4_i64)))
                .page(0, 10)
                .partition_by("name"),
        )
        .await;
        assert_rebound(
            &executor,
            SelectQuery::new("Order").filter(Expr::in_subquery(
                "id",
                test_entity(),
                SelectQuery::new("Order").filter(Expr::gt("id", 20_u64)),
                "id",
            )),
            SelectQuery::new("Order").filter(Expr::in_subquery(
                "id",
                test_entity(),
                SelectQuery::new("Order").filter(Expr::gt("id", 30_u64)),
                "id",
            )),
        )
        .await;
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> QueryExecutor for SqlDataServiceExecutor<D, T, S>
{
    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send {
        async move {
            let entity_desc = self
                .entity_descriptor(&request.query.entity)
                .ok_or_else(|| {
                    SqlExecutorError::Compile(SqlCompileError::UnknownEntity(
                        request.query.entity.clone(),
                    ))
                })?;

            if !request.capture_execution_metadata
                && self.dialect.prefers_small_parent_relation_probes()
                && request.query.top_n_probe_parent_threshold.is_none()
                && let Some(values) = partition_probe_values(&request.query)
                && values.len() >= 2
                && let (Some(first_query), Some(second_query)) = (
                    scalar_partition_probe_query(&request.query, values[0].clone()),
                    scalar_partition_probe_query(&request.query, values[1].clone()),
                )
            {
                let first = self
                    .compile_select_cached(&entity_desc, &first_query)
                    .map_err(SqlExecutorError::Compile)?;
                let second_params = collect_select_params(
                    &entity_desc,
                    &second_query,
                    self.dialect.large_in_uses_array_param(),
                );
                if let Some(param_index) = first
                    .params
                    .iter()
                    .zip(&second_params)
                    .position(|(left, right)| left != right)
                {
                    let rows = self
                        .transport
                        .fetch_repeated_compact_sql(&first, param_index, &values)
                        .await
                        .map_err(SqlExecutorError::Transport)?;
                    return Ok(QueryResult {
                        metadata: ExecutionMetadata::unrecorded_query(rows.len()),
                        rows,
                    });
                }
            }

            let compiled = self
                .compile_select_cached(&entity_desc, &request.query)
                .map_err(SqlExecutorError::Compile)?;
            let start = request.capture_execution_metadata.then(SystemTime::now);
            let rows = self
                .transport
                .fetch_all_compact_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = request.capture_execution_metadata.then(SystemTime::now);
            let debug_query = request
                .capture_debug_query
                .then(|| compiled.debug_sql(self.dialect.kind()));
            let metadata = if request.capture_execution_metadata {
                let CompiledQuery { sql, params, .. } = compiled;
                ExecutionMetadata {
                    backend: "sql".to_string(),
                    operation: DataServiceOperation::Query,
                    started_at: start.expect("captured query start"),
                    ended_at: end.expect("captured query end"),
                    affected_rows: None,
                    result_count: Some(rows.len()),
                    trace_chain: request.trace_chain,
                    comment: request.comment,
                    backend_request_id: None,
                    parameterized_query: Some(sql),
                    params,
                    debug_query,
                }
            } else {
                ExecutionMetadata::unrecorded_query(rows.len())
            };

            Ok(QueryResult { rows, metadata })
        }
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> MutationExecutor for SqlDataServiceExecutor<D, T, S>
{
    fn mutate(
        &self,
        request: MutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send {
        async move {
            let entity_name = match &request {
                MutationRequest::Insert(cmd) => &cmd.entity,
                MutationRequest::Update(cmd) => &cmd.entity,
                MutationRequest::Delete(cmd) => &cmd.entity,
                MutationRequest::Recover(cmd) => &cmd.entity,
                MutationRequest::Batch(mutations) => {
                    let mut total_affected = 0;
                    let mut parameterized_queries = Vec::new();
                    let mut params = Vec::new();
                    let mut debug_queries = Vec::new();
                    let start = SystemTime::now();
                    for req in mutations {
                        let res = Box::pin(self.mutate(req.clone())).await?;
                        total_affected += res.affected_rows;
                        if let Some(query) = res.metadata.parameterized_query {
                            parameterized_queries.push(query);
                        }
                        params.extend(res.metadata.params);
                        if let Some(query) = res.metadata.debug_query {
                            debug_queries.push(query);
                        }
                    }
                    let end = SystemTime::now();
                    return Ok(MutationResult {
                        affected_rows: total_affected,
                        generated_values: GeneratedValues::default(),
                        persisted_snapshot: None,
                        metadata: ExecutionMetadata {
                            backend: "sql".to_string(),
                            operation: DataServiceOperation::Batch,
                            started_at: start,
                            ended_at: end,
                            affected_rows: Some(total_affected),
                            result_count: None,
                            trace_chain: Vec::new(),
                            comment: None,
                            backend_request_id: None,
                            parameterized_query: (!parameterized_queries.is_empty())
                                .then(|| parameterized_queries.join("; ")),
                            params,
                            debug_query: (!debug_queries.is_empty())
                                .then(|| debug_queries.join("; ")),
                        },
                    });
                }
            };

            let entity_desc = self.entity_descriptor(entity_name).ok_or_else(|| {
                SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone()))
            })?;

            let compiled = match &request {
                MutationRequest::Insert(cmd) => self
                    .dialect
                    .compile_insert(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Update(cmd) => self
                    .dialect
                    .compile_update(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Delete(cmd) => self
                    .dialect
                    .compile_delete(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Recover(cmd) => self
                    .dialect
                    .compile_recover(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Batch(_) => unreachable!(),
            };

            let start = SystemTime::now();
            let affected_rows = self
                .transport
                .execute_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = SystemTime::now();

            let operation = match &request {
                MutationRequest::Insert(_) => DataServiceOperation::Insert,
                MutationRequest::Update(_) => DataServiceOperation::Update,
                MutationRequest::Delete(_) => DataServiceOperation::Delete,
                MutationRequest::Recover(_) => DataServiceOperation::Recover,
                MutationRequest::Batch(_) => DataServiceOperation::Batch,
            };

            let metadata = ExecutionMetadata {
                backend: "sql".to_string(),
                operation,
                started_at: start,
                ended_at: end,
                affected_rows: Some(affected_rows),
                result_count: None,
                trace_chain: request.trace_chain().to_vec(),
                comment: request.comment().map(|s| s.to_owned()),
                backend_request_id: None,
                parameterized_query: Some(compiled.sql.clone()),
                params: compiled.params.clone(),
                debug_query: Some(compiled.debug_sql(self.dialect.kind())),
            };

            Ok(MutationResult {
                affected_rows,
                generated_values: GeneratedValues::default(),
                persisted_snapshot: None,
                metadata,
            })
        }
    }
}

#[derive(Clone)]
pub struct SqlDataServiceTransaction<'a, D, Tx: SqlTransport + SqlTransaction, S> {
    pub dialect: &'a D,
    pub transport: Tx,
    pub schema_provider: &'a S,
    descriptor_cache: Arc<RwLock<HashMap<String, Arc<teaql_core::EntityDescriptor>>>>,
    select_plan_cache: Arc<RwLock<Vec<(SelectQuery, String)>>>,
}

impl<'a, D, Tx: SqlTransport + SqlTransaction, S> SqlDataServiceTransaction<'a, D, Tx, S>
where
    S: teaql_data_service::SchemaProvider,
{
    fn entity_descriptor(&self, name: &str) -> Option<Arc<teaql_core::EntityDescriptor>> {
        if let Ok(cache) = self.descriptor_cache.read()
            && let Some(descriptor) = cache.get(name)
        {
            return Some(descriptor.clone());
        }
        let descriptor = self.schema_provider.get_entity(name)?;
        if let Ok(mut cache) = self.descriptor_cache.write() {
            return Some(
                cache
                    .entry(name.to_owned())
                    .or_insert_with(|| descriptor.clone())
                    .clone(),
            );
        }
        Some(descriptor)
    }

    fn compile_select_cached(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
    ) -> Result<CompiledQuery, SqlCompileError>
    where
        D: SqlDialect,
    {
        compile_select_with_cache(self.dialect, &self.select_plan_cache, entity, query)
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> DataServiceExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    type Error = SqlExecutorError<<Tx as SqlTransport>::Error>;

    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities {
            query: true,
            mutation: true,
            transaction: false,
            schema: false,
            id_generation: false,
            batch_mutation: true,
            returning: false,
            small_parent_relation_probes: self.dialect.prefers_small_parent_relation_probes(),
        }
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> QueryExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send {
        async move {
            let entity_desc = self
                .entity_descriptor(&request.query.entity)
                .ok_or_else(|| {
                    SqlExecutorError::Compile(SqlCompileError::UnknownEntity(
                        request.query.entity.clone(),
                    ))
                })?;

            let compiled = self
                .compile_select_cached(&entity_desc, &request.query)
                .map_err(SqlExecutorError::Compile)?;
            let start = SystemTime::now();
            let rows = self
                .transport
                .fetch_all_compact_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = SystemTime::now();

            let metadata = ExecutionMetadata {
                backend: "sql".to_string(),
                operation: DataServiceOperation::Query,
                started_at: start,
                ended_at: end,
                affected_rows: None,
                result_count: Some(rows.len()),
                trace_chain: request.trace_chain,
                comment: request.comment,
                backend_request_id: None,
                parameterized_query: Some(compiled.sql.clone()),
                params: compiled.params.clone(),
                debug_query: request
                    .capture_debug_query
                    .then(|| compiled.debug_sql(self.dialect.kind())),
            };

            Ok(QueryResult { rows, metadata })
        }
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> MutationExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn mutate(
        &self,
        request: MutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send {
        async move {
            let entity_name = match &request {
                MutationRequest::Insert(cmd) => &cmd.entity,
                MutationRequest::Update(cmd) => &cmd.entity,
                MutationRequest::Delete(cmd) => &cmd.entity,
                MutationRequest::Recover(cmd) => &cmd.entity,
                MutationRequest::Batch(mutations) => {
                    let mut total_affected = 0;
                    let mut parameterized_queries = Vec::new();
                    let mut params = Vec::new();
                    let mut debug_queries = Vec::new();
                    let start = SystemTime::now();
                    for req in mutations {
                        let res = Box::pin(self.mutate(req.clone())).await?;
                        total_affected += res.affected_rows;
                        if let Some(query) = res.metadata.parameterized_query {
                            parameterized_queries.push(query);
                        }
                        params.extend(res.metadata.params);
                        if let Some(query) = res.metadata.debug_query {
                            debug_queries.push(query);
                        }
                    }
                    let end = SystemTime::now();
                    return Ok(MutationResult {
                        affected_rows: total_affected,
                        generated_values: GeneratedValues::default(),
                        persisted_snapshot: None,
                        metadata: ExecutionMetadata {
                            backend: "sql".to_string(),
                            operation: DataServiceOperation::Batch,
                            started_at: start,
                            ended_at: end,
                            affected_rows: Some(total_affected),
                            result_count: None,
                            trace_chain: Vec::new(),
                            comment: None,
                            backend_request_id: None,
                            parameterized_query: (!parameterized_queries.is_empty())
                                .then(|| parameterized_queries.join("; ")),
                            params,
                            debug_query: (!debug_queries.is_empty())
                                .then(|| debug_queries.join("; ")),
                        },
                    });
                }
            };

            let entity_desc = self.entity_descriptor(entity_name).ok_or_else(|| {
                SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone()))
            })?;

            let compiled = match &request {
                MutationRequest::Insert(cmd) => self
                    .dialect
                    .compile_insert(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Update(cmd) => self
                    .dialect
                    .compile_update(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Delete(cmd) => self
                    .dialect
                    .compile_delete(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Recover(cmd) => self
                    .dialect
                    .compile_recover(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Batch(_) => unreachable!("batch handled above"),
            };

            let start = SystemTime::now();
            let affected_rows = self
                .transport
                .execute_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = SystemTime::now();

            let operation = match &request {
                MutationRequest::Insert(_) => DataServiceOperation::Insert,
                MutationRequest::Update(_) => DataServiceOperation::Update,
                MutationRequest::Delete(_) => DataServiceOperation::Delete,
                MutationRequest::Recover(_) => DataServiceOperation::Recover,
                MutationRequest::Batch(_) => DataServiceOperation::Batch,
            };

            let persisted_id = match &request {
                MutationRequest::Insert(cmd) => cmd.values.get("id").cloned(),
                MutationRequest::Update(cmd) => Some(cmd.id.clone()),
                MutationRequest::Delete(cmd) if cmd.soft_delete => Some(cmd.id.clone()),
                MutationRequest::Recover(cmd) => Some(cmd.id.clone()),
                MutationRequest::Delete(_) | MutationRequest::Batch(_) => None,
            };
            let persisted_snapshot = if affected_rows == 1 {
                if let Some(id) = persisted_id {
                    let query = SelectQuery::new(entity_name.clone()).filter(Expr::eq("id", id));
                    let compiled_readback = self
                        .compile_select_cached(&entity_desc, &query)
                        .map_err(SqlExecutorError::Compile)?;
                    let mut rows = self
                        .transport
                        .fetch_all_compact_sql(&compiled_readback)
                        .await
                        .map_err(SqlExecutorError::Transport)?;
                    if rows.len() != 1 {
                        return Err(SqlExecutorError::PersistedRecord(format!(
                            "persisted {entity_name} record could not be read back"
                        )));
                    }
                    rows.pop().map(|row| EntitySnapshot::from(row.into_map()))
                } else {
                    None
                }
            } else {
                None
            };

            let metadata = ExecutionMetadata {
                backend: "sql".to_string(),
                operation,
                started_at: start,
                ended_at: end,
                affected_rows: Some(affected_rows),
                result_count: None,
                trace_chain: request.trace_chain().to_vec(),
                comment: request.comment().map(|s| s.to_owned()),
                backend_request_id: None,
                parameterized_query: Some(compiled.sql.clone()),
                params: compiled.params.clone(),
                debug_query: Some(compiled.debug_sql(self.dialect.kind())),
            };

            Ok(MutationResult {
                affected_rows,
                generated_values: GeneratedValues::default(),
                persisted_snapshot,
                metadata,
            })
        }
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> teaql_data_service::Transaction for SqlDataServiceTransaction<'a, D, Tx, S>
{
    type Error = SqlExecutorError<<Tx as SqlTransport>::Error>;

    fn commit(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.transport
                .commit_sql()
                .await
                .map_err(SqlExecutorError::Transport)
        }
    }

    fn rollback(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.transport
                .rollback_sql()
                .await
                .map_err(SqlExecutorError::Transport)
        }
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransactionTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> teaql_data_service::TransactionExecutor for SqlDataServiceExecutor<D, T, S>
{
    type Tx<'a>
        = SqlDataServiceTransaction<'a, D, T::Tx<'a>, S>
    where
        Self: 'a;

    fn begin(&self) -> impl std::future::Future<Output = Result<Self::Tx<'_>, Self::Error>> + Send {
        async move {
            let tx = self
                .transport
                .begin_sql()
                .await
                .map_err(SqlExecutorError::Transport)?;
            Ok(SqlDataServiceTransaction {
                dialect: &self.dialect,
                transport: tx,
                schema_provider: &self.schema_provider,
                descriptor_cache: self.descriptor_cache.clone(),
                select_plan_cache: self.select_plan_cache.clone(),
            })
        }
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: StreamingSqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> teaql_data_service::StreamQueryExecutor for SqlDataServiceExecutor<D, T, S>
{
    fn query_stream(
        &self,
        request: teaql_data_service::QueryRequest,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        use futures_util::StreamExt;
        let entity = match self.entity_descriptor(&request.query.entity) {
            Some(entity) => entity,
            None => {
                return Box::pin(futures_util::stream::once(async {
                    Err(SqlExecutorError::Compile(SqlCompileError::UnknownEntity(
                        request.query.entity,
                    )))
                }));
            }
        };
        match self.compile_select_cached(&entity, &request.query) {
            Ok(compiled) => Box::pin(
                self.transport
                    .stream_sql(compiled, chunk_size)
                    .map(|r| r.map_err(SqlExecutorError::Transport)),
            ),
            Err(error) => Box::pin(futures_util::stream::once(async {
                Err(SqlExecutorError::Compile(error))
            })),
        }
    }
}
