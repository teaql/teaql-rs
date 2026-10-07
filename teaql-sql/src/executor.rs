#![allow(clippy::manual_async_fn)]
#![allow(async_fn_in_trait)]

#[cfg(test)]
#[path = "execution_tests.rs"]
mod diagnostic_tests;
#[cfg(test)]
#[path = "mutation_execution_tests.rs"]
mod mutation_diagnostic_tests;
#[path = "mutation_execution.rs"]
mod mutation_execution;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use teaql_core::{
    CompactRow, EntityDescriptor, EntitySnapshot, Expr, GeneratedValues, SelectQuery, Value,
};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    GuardedMutationExecutor, GuardedMutationRequest, MutationExecutor, MutationRequest,
    MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

use crate::{CompiledQuery, SqlCompileError, SqlDialect};

pub trait SqlTransport: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    #[doc(hidden)]
    fn dynamic_field_store(
        &self,
    ) -> Option<&dyn teaql_data_service::dynamic_fields::DynamicFieldStore> {
        None
    }

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
    select_plan_cache: Arc<RwLock<Vec<CachedSelectPlan>>>,
}

type CachedSelectPlan = (
    EntityDescriptor,
    SelectQuery,
    String,
    teaql_data_service::SqlLogContext,
);

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
    plan_cache: &RwLock<Vec<CachedSelectPlan>>,
    entity: &EntityDescriptor,
    query: &SelectQuery,
) -> Result<CompiledQuery, SqlCompileError> {
    if let Ok(cache) = plan_cache.read()
        && let Some((_, _, sql, log_context)) =
            cache.iter().find(|(descriptor, candidate, _, _)| {
                descriptor == entity && select_plan_matches(candidate, query)
            })
    {
        let params = collect_select_params(entity, query, dialect.large_in_uses_array_param());
        return Ok(CompiledQuery {
            log_context: params.rebind_log_context(log_context),
            sql: sql.clone(),
            params: params.into_values(),
            comment: query.comment.clone(),
        });
    }

    let key = select_plan_key(query);
    let compiled = dialect.compile_select(entity, query)?;
    if let Ok(mut cache) = plan_cache.write() {
        if cache.len() >= 256 {
            cache.remove(0);
        }
        if !cache.iter().any(|(descriptor, candidate, _, _)| {
            descriptor == entity && select_plan_matches(candidate, query)
        }) {
            let mut log_context = compiled.log_context.clone();
            log_context.intent_redactions.clear();
            cache.push((entity.clone(), key, compiled.sql.clone(), log_context));
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
        && key.dynamic_property_definitions == query.dynamic_property_definitions
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
        (Expr::LikePattern { .. }, Expr::LikePattern { .. }) => true,
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
        Expr::LikePattern { pattern, original } => {
            pattern.clear();
            original.clear();
        }
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
) -> crate::SqlBindings {
    let mut params = crate::SqlBindings::new();
    append_select_params(entity, query, large_in_uses_array_param, &mut params);
    params
}

fn append_select_params(
    entity: &EntityDescriptor,
    query: &SelectQuery,
    large_in_uses_array_param: bool,
    params: &mut crate::SqlBindings,
) {
    if query.raw_sql.is_some() {
        return;
    }
    for projection in &query.expr_projection {
        collect_expr_params(&projection.expr, params, large_in_uses_array_param);
    }
    let partitioned = query.partition_by.is_some() && query.slice.is_some();
    if partitioned {
        for order in &query.order_by {
            if let Some(expr) = &order.expr {
                collect_expr_params(expr, params, large_in_uses_array_param);
            }
        }
    }
    if let Some(filter) = &query.filter {
        collect_expr_params(filter, params, large_in_uses_array_param);
    }
    if let Some(search_text) = &query.search_with_text {
        let value = Value::from(format!("%{search_text}%"));
        for _ in entity.properties.iter().filter(|property| {
            matches!(
                property.data_type,
                teaql_core::DataType::Text | teaql_core::DataType::LargeText
            )
        }) {
            params.push(value.clone());
        }
    }
    if let Some(having) = &query.having {
        collect_expr_params(having, params, large_in_uses_array_param);
    }
    // GROUP BY/HAVING belong inside the partition wrapper. Window ordering
    // was collected above, so only the ordinary trailing ORDER BY is skipped.
    if partitioned {
        return;
    }
    for order in &query.order_by {
        if let Some(expr) = &order.expr {
            collect_expr_params(expr, params, large_in_uses_array_param);
        }
    }
}

fn collect_expr_params(
    expr: &Expr,
    params: &mut crate::SqlBindings,
    large_in_uses_array_param: bool,
) {
    match expr {
        Expr::Column(_) => {}
        Expr::Value(value) => params.push(value.clone()),
        Expr::LikePattern { pattern, .. } => params.push(Value::from(pattern.clone())),
        Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
            for arg in args {
                collect_expr_params(arg, params, large_in_uses_array_param);
            }
        }
        Expr::Binary { left, op, right } => {
            collect_expr_params(left, params, large_in_uses_array_param);
            if let Some((pattern, original)) = crate::bindings::like_operand(*op, right) {
                params.push_like_pattern(pattern, original);
            } else if let Expr::Value(Value::List(values)) = right.as_ref()
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
                    for value in values {
                        params.push(value.clone());
                    }
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
            append_select_params(entity, query, large_in_uses_array_param, params);
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
            params: &mut crate::SqlBindings,
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
            .audit_mask_fields(vec![])
    }

    fn query_request(capture_debug_query: bool) -> QueryRequest {
        QueryRequest {
            query: SelectQuery::new("Order"),
            trace_chain: Vec::new(),
            intent: teaql_core::QueryIntent::new(
                "verify bounded provider query",
                "verify provider query behavior",
            )
            .unwrap(),
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

    #[test]
    fn cached_query_keeps_binding_policies_and_checks_descriptor_identity() {
        struct CountingDialect(Arc<AtomicUsize>);
        impl SqlDialect for CountingDialect {
            fn kind(&self) -> crate::DatabaseKind {
                crate::DatabaseKind::PostgreSql
            }
            fn quote_ident(&self, ident: &str) -> String {
                TestDialect.quote_ident(ident)
            }
            fn placeholder(&self, index: usize) -> String {
                TestDialect.placeholder(index)
            }
            fn compile_select(
                &self,
                entity: &EntityDescriptor,
                query: &SelectQuery,
            ) -> Result<CompiledQuery, SqlCompileError> {
                self.0.fetch_add(1, Ordering::Relaxed);
                TestDialect.compile_select(entity, query)
            }
        }
        use teaql_data_service::SqlParameterLogPolicy::{Masked, Plain};
        let counter = Arc::new(AtomicUsize::new(0));
        let dialect = CountingDialect(counter.clone());
        let cache = RwLock::new(Vec::new());
        let private = test_entity().audit_mask_fields(vec!["name".into()]);
        for value in ["first", "second"] {
            let compiled = compile_select_with_cache(
                &dialect,
                &cache,
                &private,
                &SelectQuery::new("Order").filter(Expr::eq("name", value)),
            )
            .unwrap();
            assert_eq!(compiled.params, vec![Value::from(value)]);
            assert_eq!(compiled.log_context.parameter_policies, [Masked]);
            assert!(compiled.log_context.generated_sql);
        }
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "second call must actually hit cache"
        );
        let public = compile_select_with_cache(
            &dialect,
            &cache,
            &test_entity(),
            &SelectQuery::new("Order").filter(Expr::eq("name", "public")),
        )
        .unwrap();
        assert_eq!(public.log_context.parameter_policies, [Plain]);
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn batch_metadata_keeps_each_statement_and_its_numbered_bindings() {
        let executor = SqlDataServiceExecutor::new(
            TestDialect,
            EmptyTransport,
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );
        let result = executor
            .mutate(
                teaql_data_service::MutationCommand::Batch(vec![
                    teaql_data_service::MutationCommand::Insert(
                        teaql_core::InsertCommand::new("Order").value("name", "first"),
                    )
                    .request("audited provider conformance test")
                    .unwrap(),
                    teaql_data_service::MutationCommand::Insert(
                        teaql_core::InsertCommand::new("Order").value("name", "second"),
                    )
                    .request("audited provider conformance test")
                    .unwrap(),
                ])
                .request("audited provider conformance test")
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result.metadata.statements.len(), 2);
        for (child, value) in result.metadata.statements.iter().zip(["first", "second"]) {
            assert_eq!(child.params, [Value::from(value)]);
            assert!(child.parameterized_query.as_ref().unwrap().contains("$1"));
            assert_eq!(
                child.sql_log.parameter_policies,
                [teaql_data_service::SqlParameterLogPolicy::Plain]
            );
            assert!(child.debug_query.is_none());
        }
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
    async fn sql_executor_never_eagerly_expands_private_values_even_when_diagnostics_requested() {
        let executor = SqlDataServiceExecutor::new(
            TestDialect,
            EmptyTransport,
            CountingSchemaProvider {
                lookups: Arc::new(AtomicUsize::new(0)),
            },
        );
        let mut request = query_request(true);
        request.query = request
            .query
            .filter(Expr::eq("name", "PRIVATE-BINDING-CANARY"));
        let result = executor.query(request).await.unwrap();
        assert!(result.metadata.debug_query.is_none());
        assert!(
            !result
                .metadata
                .parameterized_query
                .as_ref()
                .unwrap()
                .contains("PRIVATE-BINDING-CANARY")
        );
        assert_eq!(
            result.metadata.params,
            vec![Value::from("PRIVATE-BINDING-CANARY")]
        );
        let inserted = executor
            .mutate(
                teaql_data_service::MutationCommand::Insert(
                    teaql_core::InsertCommand::new("Order").value("name", "PRIVATE-BINDING-CANARY"),
                )
                .request("audited provider conformance test")
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(inserted.metadata.debug_query.is_none());
        assert_eq!(
            inserted.metadata.params,
            vec![Value::from("PRIVATE-BINDING-CANARY")]
        );
    }

    #[tokio::test]
    async fn ordinary_sql_query_future_keeps_partition_probe_state_out_of_frame() {
        let lookups = Arc::new(AtomicUsize::new(0));
        let repeated = Arc::new(AtomicUsize::new(0));
        let singles = Arc::new(AtomicUsize::new(0));
        let executor = SqlDataServiceExecutor::new(
            ProbeDialect,
            RepeatedProbeTransport {
                calls: repeated.clone(),
                single_calls: singles.clone(),
            },
            CountingSchemaProvider {
                lookups: lookups.clone(),
            },
        );
        let mut request = query_request(false);
        request.capture_execution_metadata = false;
        request.query = request
            .query
            .limit(1)
            .comment("what: ordinary SQL future size");
        let future = executor.query(request);
        let bytes = std::mem::size_of_val(&future);
        println!("ORDINARY_SQL_QUERY_FRAME_BYTES={bytes}");
        assert!(
            bytes <= 4608,
            "ordinary SQL future retains partition-probe state: {bytes}"
        );
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "unpolled query must not execute"
        );
        assert_eq!(singles.load(Ordering::Relaxed), 0);
        future.await.unwrap();
        assert_eq!(lookups.load(Ordering::Relaxed), 1);
        assert_eq!(singles.load(Ordering::Relaxed), 1);
        assert_eq!(repeated.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn partition_probe_fallback_keeps_identical_single_and_explicit_policy_queries() {
        for (values, threshold) in [
            (vec![Value::U64(7), Value::U64(7)], None),
            (vec![Value::U64(7)], None),
            (vec![Value::U64(7), Value::U64(9)], Some(32)),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let singles = Arc::new(AtomicUsize::new(0));
            let executor = SqlDataServiceExecutor::new(
                ProbeDialect,
                RepeatedProbeTransport {
                    calls: calls.clone(),
                    single_calls: singles.clone(),
                },
                CountingSchemaProvider {
                    lookups: Arc::new(AtomicUsize::new(0)),
                },
            );
            let mut request = query_request(false);
            request.capture_execution_metadata = false;
            request.query = request
                .query
                .filter(Expr::in_list("id", values))
                .order_desc("id")
                .limit(1)
                .partition_by("id");
            request.query.top_n_probe_parent_threshold = threshold;
            assert!(executor.query(request).await.unwrap().rows.is_empty());
            assert_eq!(calls.load(Ordering::Relaxed), 0);
            assert_eq!(singles.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test]
    async fn topn_004_011_sqlite_reuses_one_repeated_probe_boundary() {
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
    async fn topn_001_explicit_zero_threshold_forces_window_for_probe_provider() {
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

    #[test]
    fn topn_007_probe_rewrites_only_partition_membership_filter() {
        let policy = Expr::eq("tenant_id", 7_u64);
        let visibility = Expr::gt("version", 0_i64);
        let business = Expr::eq("status", "ACTIVE");
        let query = SelectQuery::new("Order")
            .filter(Expr::and([
                Expr::in_list("owner_id", [Value::U64(11), Value::U64(12)]),
                policy.clone(),
                visibility.clone(),
                business.clone(),
            ]))
            .order_desc("id")
            .limit(3)
            .partition_by("owner_id");

        let probe = scalar_partition_probe_query(&query, Value::U64(11)).unwrap();
        let expected = Expr::and([Expr::eq("owner_id", 11_u64), policy, visibility, business]);

        assert_eq!(probe.filter, Some(expected));
        assert!(probe.partition_by.is_none());
        assert_eq!(probe.order_by, query.order_by);
        assert_eq!(probe.slice, query.slice);
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
            intent: teaql_core::QueryIntent::new(
                "verify bounded provider query",
                "verify provider query behavior",
            )
            .unwrap(),
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

    #[test]
    fn cached_like_plan_rebinds_originals_across_all_expression_positions() {
        let entity = test_entity().audit_mask_fields(vec!["name".into()]);
        let cache = RwLock::new(Vec::new());
        for prefix in ["FIRST-SECRET", "SECOND-SECRET"] {
            let query = SelectQuery::new("Order")
                .project_expr("public", Expr::value(format!("PLAIN-{prefix}")))
                .filter(Expr::and([
                    Expr::contain("name", format!("{prefix}-FILTER")),
                    Expr::in_list("id", [Value::U64(1), Value::U64(2)]),
                    Expr::in_subquery(
                        "id",
                        entity.clone(),
                        SelectQuery::new("Order")
                            .filter(Expr::begin_with("name", format!("{prefix}-SUBQUERY"))),
                        "id",
                    ),
                ]))
                .having(Expr::not_end_with("name", format!("{prefix}-HAVING")))
                .order_by(teaql_core::OrderBy::asc_expr(Expr::end_with(
                    "name",
                    format!("{prefix}-ORDER"),
                )));
            let actual = compile_select_with_cache(&TestDialect, &cache, &entity, &query).unwrap();
            assert_eq!(actual, TestDialect.compile_select(&entity, &query).unwrap());
            let mut secrets = Vec::new();
            actual
                .log_context
                .intent_redactions
                .extend_secrets(false, &mut secrets);
            for suffix in ["FILTER", "SUBQUERY", "HAVING", "ORDER"] {
                assert!(secrets.contains(&format!("{prefix}-{suffix}")));
            }
            assert!(secrets.iter().all(|secret| secret.starts_with(prefix)));
            let plans = cache.read().unwrap();
            assert_eq!(plans.len(), 1, "same shape must reuse its plan");
            assert!(plans[0].3.intent_redactions.is_empty());
            assert!(!format!("{:?}", plans[0].1).contains("SECRET"));
        }
    }

    #[test]
    fn cached_partition_having_rebinds_numeric_and_private_like_operands() {
        struct CountingDialect(Arc<AtomicUsize>);
        impl SqlDialect for CountingDialect {
            fn kind(&self) -> crate::DatabaseKind {
                TestDialect.kind()
            }
            fn quote_ident(&self, ident: &str) -> String {
                TestDialect.quote_ident(ident)
            }
            fn placeholder(&self, index: usize) -> String {
                TestDialect.placeholder(index)
            }
            fn compile_select(
                &self,
                entity: &EntityDescriptor,
                query: &SelectQuery,
            ) -> Result<CompiledQuery, SqlCompileError> {
                self.0.fetch_add(1, Ordering::Relaxed);
                TestDialect.compile_select(entity, query)
            }
        }

        let entity = test_entity().audit_mask_fields(vec!["name".into()]);
        let compilations = Arc::new(AtomicUsize::new(0));
        let dialect = CountingDialect(compilations.clone());
        let cache = RwLock::new(Vec::new());
        for (number, original) in [(1_i64, "FIRST-SECRET"), (11, "SECOND-SECRET")] {
            let query = SelectQuery::new("Order")
                .project_expr("marker", Expr::value(number))
                .filter(Expr::gt("id", number + 1))
                .search_with_text(format!("SEARCH-{number}"))
                .group_by("id")
                .group_by("name")
                .count("n")
                .having(Expr::and([
                    Expr::binary(
                        Expr::count_all(),
                        teaql_core::BinaryOp::Gt,
                        Expr::value(number + 2),
                    ),
                    Expr::contain("name", original),
                ]))
                .order_by(teaql_core::OrderBy::asc_expr(Expr::value(number + 3)))
                .page(0, 10)
                .partition_by("id");
            let fresh = TestDialect.compile_select(&entity, &query).unwrap();
            assert!(fresh.sql.contains("ROW_NUMBER() OVER (PARTITION BY \"id\""));
            assert!(fresh.sql.contains(" GROUP BY "));
            assert!(fresh.sql.contains(" HAVING "));
            assert_eq!(
                fresh.params,
                vec![
                    Value::I64(number),
                    Value::I64(number + 3),
                    Value::I64(number + 1),
                    Value::from(format!("%SEARCH-{number}%")),
                    Value::I64(number + 2),
                    Value::from(format!("%{original}%")),
                ],
                "projection, window order, WHERE, search, then HAVING bindings"
            );
            let cached = compile_select_with_cache(&dialect, &cache, &entity, &query).unwrap();
            assert_eq!(
                cached, fresh,
                "cold/warm plans must match fresh compilation"
            );
            assert_eq!(compilations.load(Ordering::Relaxed), 1, "warm cache hit");
            let mut secrets = Vec::new();
            cached
                .log_context
                .intent_redactions
                .extend_secrets(false, &mut secrets);
            assert_eq!(secrets, [original]);
            let plans = cache.read().unwrap();
            assert_eq!(plans.len(), 1);
            assert!(plans[0].3.intent_redactions.is_empty());
            assert!(!format!("{:?}", plans[0].1).contains("SECRET"));
        }
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
            intent: teaql_core::QueryIntent::new(
                "verify bounded provider query",
                "verify provider query behavior",
            )
            .unwrap(),
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
                intent: teaql_core::QueryIntent::new(
                    "verify bounded provider query",
                    "verify provider query behavior",
                )
                .unwrap(),
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
    fn dynamic_field_store(
        &self,
    ) -> Option<&dyn teaql_data_service::dynamic_fields::DynamicFieldStore> {
        self.transport.dynamic_field_store()
    }
    fn query_log_intent(&self, query: &SelectQuery) -> teaql_data_service::SqlIntentRedactions {
        self.entity_descriptor(&query.entity)
            .and_then(|entity| self.compile_select_cached(&entity, query).ok())
            .map(|compiled| {
                teaql_data_service::SqlIntentRedactions::from_bindings(
                    &compiled.log_context,
                    &compiled.params,
                    &compiled.sql,
                )
            })
            .unwrap_or_else(|| {
                teaql_data_service::SqlIntentRedactions::from_unclassified_query(query)
            })
    }

    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send {
        self.query_observed(request, None)
    }

    fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<teaql_data_service::ExecutionObserver<'a>>,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send + 'a {
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
            {
                // The two cloned SelectQueries and repeated transport future
                // belong to this optional path, not every ordinary read frame.
                if let Some(result) = Box::pin(self.fetch_partition_probe_query(
                    &entity_desc,
                    &request.query,
                    &values,
                ))
                .await?
                {
                    return Ok(result);
                }
            }

            let compiled = self
                .compile_select_cached(&entity_desc, &request.query)
                .map_err(SqlExecutorError::Compile)?;
            execute_compiled_query(&self.dialect, &self.transport, compiled, request, observer)
                .await
        }
    }
}

impl<D, T, S> SqlDataServiceExecutor<D, T, S>
where
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
{
    async fn fetch_partition_probe_query(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
        values: &[Value],
    ) -> Result<Option<QueryResult>, SqlExecutorError<T::Error>> {
        let (Some(first_query), Some(second_query)) = (
            scalar_partition_probe_query(query, values[0].clone()),
            scalar_partition_probe_query(query, values[1].clone()),
        ) else {
            return Ok(None);
        };
        let first = self
            .compile_select_cached(entity, &first_query)
            .map_err(SqlExecutorError::Compile)?;
        let second_params = collect_select_params(
            entity,
            &second_query,
            self.dialect.large_in_uses_array_param(),
        );
        let Some(param_index) = first
            .params
            .iter()
            .zip(second_params.iter())
            .position(|(left, right)| left != right)
        else {
            return Ok(None);
        };
        let rows = self
            .transport
            .fetch_repeated_compact_sql(&first, param_index, values)
            .await
            .map_err(SqlExecutorError::Transport)?;
        Ok(Some(QueryResult {
            metadata: ExecutionMetadata::unrecorded_query(rows.len()),
            rows,
        }))
    }
}

async fn execute_compiled_query<D: SqlDialect + Sync, T: SqlTransport>(
    dialect: &D,
    transport: &T,
    compiled: CompiledQuery,
    request: QueryRequest,
    observer: Option<teaql_data_service::ExecutionObserver<'_>>,
) -> Result<QueryResult, SqlExecutorError<T::Error>> {
    let metadata = request
        .capture_execution_metadata
        .then(|| query_diagnostic_metadata(dialect, &compiled, &request));
    let mut diagnostic = crate::diagnostic_execution::StatementDiagnostic::new(metadata, observer);
    let result = transport.fetch_all_compact_sql(&compiled).await;
    let rows = result.map_err(|error| {
        diagnostic.fail();
        SqlExecutorError::Transport(error)
    })?;
    let mut metadata = diagnostic
        .success()
        .unwrap_or_else(|| ExecutionMetadata::unrecorded_query(rows.len()));
    metadata.result_count = Some(rows.len());
    Ok(QueryResult { rows, metadata })
}

fn query_diagnostic_metadata<D: SqlDialect>(
    dialect: &D,
    compiled: &CompiledQuery,
    request: &QueryRequest,
) -> ExecutionMetadata {
    let now = SystemTime::now();
    ExecutionMetadata {
        statements: Vec::new(),
        sql_log: compiled.log_context.clone(),
        backend: format!("{:?}", dialect.kind()).to_ascii_lowercase(),
        operation: DataServiceOperation::Query,
        started_at: now,
        ended_at: now,
        affected_rows: None,
        result_count: None,
        trace_chain: request.execution_trace_chain(),
        comment: Some(request.intent.comment().to_owned()),
        backend_request_id: None,
        parameterized_query: Some(compiled.sql.clone()),
        params: compiled.params.clone(),
        debug_query: None,
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
        self.mutate_observed(request, None)
    }

    fn mutate_observed<'scope>(
        &'scope self,
        request: MutationRequest,
        observer: Option<teaql_data_service::ExecutionObserver<'scope>>,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send + 'scope
    {
        async move {
            mutation_execution::execute(
                &self.dialect,
                &self.transport,
                &|name| self.entity_descriptor(name),
                &self.select_plan_cache,
                request,
                false,
                observer,
            )
            .await
        }
    }
}

fn guarded_mutation_entity_name(request: &MutationRequest) -> Result<&str, SqlCompileError> {
    match &request.command {
        teaql_data_service::MutationCommand::Update(command) => Ok(&command.entity),
        teaql_data_service::MutationCommand::Delete(command) => Ok(&command.entity),
        teaql_data_service::MutationCommand::Recover(command) => Ok(&command.entity),
        teaql_data_service::MutationCommand::Insert(_)
        | teaql_data_service::MutationCommand::Batch(_) => {
            Err(SqlCompileError::InvalidFunctionArguments(
                "guarded mutation supports update, delete, and recover only".to_owned(),
            ))
        }
    }
}

async fn execute_guarded_mutation<D, T>(
    dialect: &D,
    transport: &T,
    entity: &EntityDescriptor,
    select_plan_cache: &RwLock<Vec<CachedSelectPlan>>,
    request: GuardedMutationRequest,
) -> Result<MutationResult, SqlExecutorError<T::Error>>
where
    D: SqlDialect + Sync,
    T: SqlTransport + Sync,
{
    let GuardedMutationRequest { mutation, guard } = request;
    let (entity_name, operation, persisted_id) = match &mutation.command {
        teaql_data_service::MutationCommand::Update(command) => (
            &command.entity,
            DataServiceOperation::Update,
            Some(command.id.clone()),
        ),
        teaql_data_service::MutationCommand::Delete(command) => (
            &command.entity,
            DataServiceOperation::Delete,
            command.soft_delete.then(|| command.id.clone()),
        ),
        teaql_data_service::MutationCommand::Recover(command) => (
            &command.entity,
            DataServiceOperation::Recover,
            Some(command.id.clone()),
        ),
        teaql_data_service::MutationCommand::Insert(_)
        | teaql_data_service::MutationCommand::Batch(_) => unreachable!(),
    };
    let compiled = match &mutation.command {
        teaql_data_service::MutationCommand::Update(command) => {
            dialect.compile_guarded_update(entity, command, &guard)
        }
        teaql_data_service::MutationCommand::Delete(command) => {
            dialect.compile_guarded_delete(entity, command, &guard)
        }
        teaql_data_service::MutationCommand::Recover(command) => {
            dialect.compile_guarded_recover(entity, command, &guard)
        }
        teaql_data_service::MutationCommand::Insert(_)
        | teaql_data_service::MutationCommand::Batch(_) => unreachable!(),
    }
    .map_err(SqlExecutorError::Compile)?;

    let start = SystemTime::now();
    let affected_rows = transport
        .execute_sql(&compiled)
        .await
        .map_err(SqlExecutorError::Transport)?;
    let end = SystemTime::now();

    let persisted_snapshot = if affected_rows == 1 {
        if let Some(id) = persisted_id {
            let query = SelectQuery::new(entity_name.clone())
                .filter(Expr::and([Expr::eq("id", id), guard.clone()]));
            let compiled_readback =
                compile_select_with_cache(dialect, select_plan_cache, entity, &query)
                    .map_err(SqlExecutorError::Compile)?;
            let mut rows = transport
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
        statements: Vec::new(),
        sql_log: compiled.log_context.clone(),
        backend: format!("{:?}", dialect.kind()).to_ascii_lowercase(),
        operation,
        started_at: start,
        ended_at: end,
        affected_rows: Some(affected_rows),
        result_count: None,
        trace_chain: mutation.trace_chain().to_vec(),
        comment: Some(mutation.comment().to_owned()),
        backend_request_id: None,
        parameterized_query: Some(compiled.sql.clone()),
        params: compiled.params.clone(),
        debug_query: None,
    };

    Ok(MutationResult {
        affected_rows,
        generated_values: GeneratedValues::default(),
        persisted_snapshot,
        metadata,
    })
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> GuardedMutationExecutor for SqlDataServiceExecutor<D, T, S>
{
    fn mutate_guarded(
        &self,
        request: GuardedMutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send {
        self.mutate_guarded_observed(request, None)
    }
    fn mutate_guarded_observed<'scope>(
        &'scope self,
        request: GuardedMutationRequest,
        observer: Option<teaql_data_service::ExecutionObserver<'scope>>,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send + 'scope
    {
        async move {
            let entity_name = guarded_mutation_entity_name(&request.mutation)
                .map_err(SqlExecutorError::Compile)?;
            let entity = self.entity_descriptor(entity_name).ok_or_else(|| {
                SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.to_owned()))
            })?;
            mutation_execution::guarded(
                &self.dialect,
                &self.transport,
                &entity,
                &self.select_plan_cache,
                request,
                observer,
            )
            .await
        }
    }
}

#[derive(Clone)]
pub struct SqlDataServiceTransaction<'a, D, Tx: SqlTransport + SqlTransaction, S> {
    pub dialect: &'a D,
    pub transport: Tx,
    pub schema_provider: &'a S,
    descriptor_cache: Arc<RwLock<HashMap<String, Arc<teaql_core::EntityDescriptor>>>>,
    select_plan_cache: Arc<RwLock<Vec<CachedSelectPlan>>>,
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
> GuardedMutationExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn mutate_guarded(
        &self,
        request: GuardedMutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send {
        self.mutate_guarded_observed(request, None)
    }
    fn mutate_guarded_observed<'scope>(
        &'scope self,
        request: GuardedMutationRequest,
        observer: Option<teaql_data_service::ExecutionObserver<'scope>>,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send + 'scope
    {
        async move {
            let entity_name = guarded_mutation_entity_name(&request.mutation)
                .map_err(SqlExecutorError::Compile)?;
            let entity = self.entity_descriptor(entity_name).ok_or_else(|| {
                SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.to_owned()))
            })?;
            mutation_execution::guarded(
                self.dialect,
                &self.transport,
                &entity,
                &self.select_plan_cache,
                request,
                observer,
            )
            .await
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
    fn dynamic_field_store(
        &self,
    ) -> Option<&dyn teaql_data_service::dynamic_fields::DynamicFieldStore> {
        self.transport.dynamic_field_store()
    }
    fn query_log_intent(&self, query: &SelectQuery) -> teaql_data_service::SqlIntentRedactions {
        self.entity_descriptor(&query.entity)
            .and_then(|entity| self.compile_select_cached(&entity, query).ok())
            .map(|compiled| {
                teaql_data_service::SqlIntentRedactions::from_bindings(
                    &compiled.log_context,
                    &compiled.params,
                    &compiled.sql,
                )
            })
            .unwrap_or_else(|| {
                teaql_data_service::SqlIntentRedactions::from_unclassified_query(query)
            })
    }

    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send {
        self.query_observed(request, None)
    }

    fn query_observed<'b>(
        &'b self,
        request: QueryRequest,
        observer: Option<teaql_data_service::ExecutionObserver<'b>>,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send + 'b {
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
            execute_compiled_query(self.dialect, &self.transport, compiled, request, observer).await
        }
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> teaql_data_service::StreamQueryExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn query_stream_observed<'b>(
        &'b self,
        mut request: QueryRequest,
        chunk_size: usize,
        observer: teaql_data_service::ExecutionObserver<'b>,
    ) -> teaql_data_service::QueryStream<'b, Self::Error> {
        if !request.capture_execution_metadata {
            return self.query_stream(request, chunk_size);
        }
        let compiled = self
            .entity_descriptor(&request.query.entity)
            .ok_or_else(|| SqlCompileError::UnknownEntity(request.query.entity.clone()))
            .and_then(|entity| self.compile_select_cached(&entity, &request.query));
        let compiled = match compiled {
            Ok(compiled) => compiled,
            Err(error) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(SqlExecutorError::Compile(error))
                }));
            }
        };
        let metadata = query_diagnostic_metadata(self.dialect, &compiled, &request);
        // This adapter buffers the query before chunk delivery. Its one terminal
        // diagnostic counts delivered chunks, not the full buffer returned by SQL.
        request.capture_execution_metadata = false;
        let source = self.query_stream(request, chunk_size);
        crate::diagnostic_stream::DiagnosticStream::wrap(source, metadata, observer)
    }

    fn query_stream(
        &self,
        request: QueryRequest,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        use std::collections::VecDeque;

        let chunk_size = chunk_size.max(1);
        Box::pin(futures_util::stream::try_unfold(
            (Some(request), VecDeque::new(), 0_usize),
            move |(request, mut rows, chunk_index)| async move {
                if let Some(request) = request {
                    rows = QueryExecutor::query(self, request).await?.rows.into();
                }
                if rows.is_empty() {
                    return Ok(None);
                }
                let take = rows.len().min(chunk_size);
                let chunk_rows = rows.drain(..take).collect();
                let is_last = rows.is_empty();
                Ok(Some((
                    teaql_data_service::StreamChunk {
                        rows: chunk_rows,
                        chunk_index,
                        is_last,
                    },
                    (None, rows, chunk_index + 1),
                )))
            },
        ))
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
        self.mutate_observed(request, None)
    }

    fn mutate_observed<'scope>(
        &'scope self,
        request: MutationRequest,
        observer: Option<teaql_data_service::ExecutionObserver<'scope>>,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send + 'scope
    {
        async move {
            mutation_execution::execute(
                self.dialect,
                &self.transport,
                &|name| self.entity_descriptor(name),
                &self.select_plan_cache,
                request,
                true,
                observer,
            )
            .await
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
    fn query_stream_observed<'a>(
        &'a self,
        request: QueryRequest,
        chunk_size: usize,
        observer: teaql_data_service::ExecutionObserver<'a>,
    ) -> teaql_data_service::QueryStream<'a, Self::Error> {
        use futures_util::StreamExt;
        if !request.capture_execution_metadata {
            return self.query_stream(request, chunk_size);
        }
        let compiled = self
            .entity_descriptor(&request.query.entity)
            .ok_or_else(|| SqlCompileError::UnknownEntity(request.query.entity.clone()))
            .and_then(|entity| self.compile_select_cached(&entity, &request.query));
        let compiled = match compiled {
            Ok(compiled) => compiled,
            // No SQL was compiled/executed, so do not invent an SQL diagnostic.
            Err(error) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(SqlExecutorError::Compile(error))
                }));
            }
        };
        let metadata = query_diagnostic_metadata(&self.dialect, &compiled, &request);
        let source = Box::pin(
            self.transport
                .stream_sql(compiled, chunk_size)
                .map(|item| item.map_err(SqlExecutorError::Transport)),
        );
        crate::diagnostic_stream::DiagnosticStream::wrap(source, metadata, observer)
    }

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
