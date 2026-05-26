use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use teaql_core::{EntityDescriptor, Record, UpdateCommand, Value};
use teaql_sql::{CompiledQuery, DatabaseKind, SqlDialect};

use crate::{
    CheckResults, CheckerRegistry, ContextError, EntityEvent, EntityEventSink, GraphNode,
    InternalIdGenerator, Language, MetadataStore, ObjectLocation, RepositoryBehavior,
    RepositoryBehaviorRegistry, RepositoryRegistry, RequestPolicy, RuntimeError,
    local_id_generator, translate_check_result,
};
use crate::{EntityRoot, QueryExecutor, RepositoryError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlLogOperation {
    Select,
    Insert,
    Update,
    Delete,
    Recover,
}

impl SqlLogOperation {
    pub fn is_select(self) -> bool {
        matches!(self, Self::Select)
    }

    pub fn is_mutation(self) -> bool {
        !self.is_select()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlLogOptions {
    pub select: bool,
    pub mutation: bool,
}

impl SqlLogOptions {
    pub fn disabled() -> Self {
        Self {
            select: false,
            mutation: false,
        }
    }

    pub fn select_only() -> Self {
        Self {
            select: true,
            mutation: false,
        }
    }

    pub fn mutation_only() -> Self {
        Self {
            select: false,
            mutation: true,
        }
    }

    pub fn all() -> Self {
        Self {
            select: true,
            mutation: true,
        }
    }

    pub fn enabled_for(self, operation: SqlLogOperation) -> bool {
        if operation.is_select() {
            self.select
        } else {
            self.mutation
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlLogEntry {
    pub operation: SqlLogOperation,
    pub sql: String,
    pub params: Vec<Value>,
    pub debug_sql: String,
    pub pretty_sql: String,
    pub started_at: SystemTime,
    pub ended_at: SystemTime,
    pub elapsed: Duration,
    pub result_count: Option<usize>,
    pub result_type: Option<String>,
    pub affected_rows: Option<u64>,
    pub result_summary: String,
    pub user_identifier: Option<String>,
    pub comment: Option<String>,
}

pub trait SchemaProvider: Send + Sync {
    fn ensure_schema<'a>(
        &'a self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>>;
}

pub struct UserContext {
    pub(crate) metadata: Option<Box<dyn MetadataStore>>,
    pub(crate) repository_registry: Option<Box<dyn RepositoryRegistry>>,
    pub(crate) repository_behavior_registry: Option<Box<dyn RepositoryBehaviorRegistry>>,
    pub(crate) request_policy: Option<Box<dyn RequestPolicy>>,
    pub(crate) checker_registry: Option<Box<dyn CheckerRegistry>>,
    pub(crate) event_sink: Option<Box<dyn EntityEventSink>>,
    pub(crate) internal_id_generator: Option<Box<dyn InternalIdGenerator>>,
    schema_provider: Option<Box<dyn SchemaProvider>>,
    language: Language,
    typed_resources: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    named_resources: BTreeMap<String, Box<dyn Any + Send + Sync>>,
    locals: BTreeMap<String, Value>,
    pub(crate) initial_graphs: Vec<GraphNode>,
    entity_root: EntityRoot,
    sql_log_options: SqlLogOptions,
    sql_log_entries: Mutex<Vec<SqlLogEntry>>,
    user_identifier: Option<String>,
    pub(crate) comment_stack: Mutex<Vec<String>>,
}

impl Default for UserContext {
    fn default() -> Self {
        let pid = std::process::id();
        let thread_id_str = format!("{:?}", std::thread::current().id());
        let numeric_thread_id = thread_id_str
            .strip_prefix("ThreadId(")
            .and_then(|s| s.strip_suffix(")"))
            .unwrap_or(&thread_id_str);
        let os_user = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "main".to_owned());
        let user_id = format!("{os_user}@pid-{pid}.tid-{numeric_thread_id}");
        Self {
            metadata: None,
            repository_registry: None,
            repository_behavior_registry: None,
            request_policy: None,
            checker_registry: None,
            event_sink: None,
            internal_id_generator: None,
            schema_provider: None,
            language: Language::default(),
            typed_resources: HashMap::new(),
            named_resources: BTreeMap::new(),
            locals: BTreeMap::new(),
            initial_graphs: Vec::new(),
            entity_root: EntityRoot::default(),
            sql_log_options: SqlLogOptions::all(),
            sql_log_entries: Mutex::new(Vec::new()),
            user_identifier: Some(user_id),
            comment_stack: Mutex::new(Vec::new()),
        }
    }
}

impl UserContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn user_identifier(&self) -> Option<&str> {
        self.user_identifier.as_deref()
    }

    pub fn set_user_identifier(&mut self, user_identifier: impl Into<String>) {
        self.user_identifier = Some(user_identifier.into());
    }

    pub fn with_user_identifier(mut self, user_identifier: impl Into<String>) -> Self {
        self.user_identifier = Some(user_identifier.into());
        self
    }

    pub fn set_user_identifier_option(&mut self, user_identifier: Option<String>) {
        self.user_identifier = user_identifier;
    }

    pub fn with_user_identifier_option(mut self, user_identifier: Option<String>) -> Self {
        self.user_identifier = user_identifier;
        self
    }

    pub fn with_module(mut self, module: crate::RuntimeModule) -> Self {
        module.apply_to(&mut self);
        self
    }

    pub fn entity_root(&self) -> EntityRoot {
        self.entity_root.clone()
    }

    pub fn initial_graphs(&self) -> &[GraphNode] {
        &self.initial_graphs
    }

    pub fn set_initial_graphs(&mut self, graphs: Vec<GraphNode>) {
        self.initial_graphs = graphs;
    }

    pub fn with_metadata(mut self, metadata: impl MetadataStore + 'static) -> Self {
        self.metadata = Some(Box::new(metadata));
        self
    }

    pub fn set_metadata(&mut self, metadata: impl MetadataStore + 'static) {
        self.metadata = Some(Box::new(metadata));
    }

    pub fn with_repository_registry(mut self, registry: impl RepositoryRegistry + 'static) -> Self {
        self.repository_registry = Some(Box::new(registry));
        self
    }

    pub fn set_repository_registry(&mut self, registry: impl RepositoryRegistry + 'static) {
        self.repository_registry = Some(Box::new(registry));
    }

    pub fn with_repository_behavior_registry(
        mut self,
        registry: impl RepositoryBehaviorRegistry + 'static,
    ) -> Self {
        self.repository_behavior_registry = Some(Box::new(registry));
        self
    }

    pub fn set_repository_behavior_registry(
        &mut self,
        registry: impl RepositoryBehaviorRegistry + 'static,
    ) {
        self.repository_behavior_registry = Some(Box::new(registry));
    }

    pub fn with_request_policy(mut self, policy: impl RequestPolicy + 'static) -> Self {
        self.request_policy = Some(Box::new(policy));
        self
    }

    pub fn set_request_policy(&mut self, policy: impl RequestPolicy + 'static) {
        self.request_policy = Some(Box::new(policy));
    }

    pub fn clear_request_policy(&mut self) {
        self.request_policy = None;
    }

    pub fn with_checker_registry(mut self, registry: impl CheckerRegistry + 'static) -> Self {
        self.checker_registry = Some(Box::new(registry));
        self
    }

    pub fn set_checker_registry(&mut self, registry: impl CheckerRegistry + 'static) {
        self.checker_registry = Some(Box::new(registry));
    }

    pub fn with_event_sink(mut self, sink: impl EntityEventSink + 'static) -> Self {
        self.event_sink = Some(Box::new(sink));
        self
    }

    pub fn set_event_sink(&mut self, sink: impl EntityEventSink + 'static) {
        self.event_sink = Some(Box::new(sink));
    }

    pub fn with_internal_id_generator(
        mut self,
        generator: impl InternalIdGenerator + 'static,
    ) -> Self {
        self.internal_id_generator = Some(Box::new(generator));
        self
    }

    pub fn set_internal_id_generator(&mut self, generator: impl InternalIdGenerator + 'static) {
        self.internal_id_generator = Some(Box::new(generator));
    }

    pub fn with_schema_provider(mut self, provider: impl SchemaProvider + 'static) -> Self {
        self.schema_provider = Some(Box::new(provider));
        self
    }

    pub fn set_schema_provider(&mut self, provider: impl SchemaProvider + 'static) {
        self.schema_provider = Some(Box::new(provider));
    }

    pub async fn ensure_schema(&self) -> Result<(), RuntimeError> {
        let provider = self
            .schema_provider
            .as_ref()
            .ok_or_else(|| RuntimeError::Schema("missing schema provider".to_owned()))?;
        provider.ensure_schema(self).await
    }

    pub fn with_language(mut self, language: Language) -> Self {
        self.language = language;
        self
    }

    pub fn set_language(&mut self, language: Language) {
        self.language = language;
    }

    pub fn with_sql_log_options(mut self, options: SqlLogOptions) -> Self {
        self.sql_log_options = options;
        self
    }

    pub fn set_sql_log_options(&mut self, options: SqlLogOptions) {
        self.sql_log_options = options;
    }

    pub fn enable_select_sql_log(&mut self) {
        self.sql_log_options.select = true;
    }

    pub fn enable_mutation_sql_log(&mut self) {
        self.sql_log_options.mutation = true;
    }

    pub fn enable_all_sql_log(&mut self) {
        self.sql_log_options = SqlLogOptions::all();
    }

    pub fn disable_sql_log(&mut self) {
        self.sql_log_options = SqlLogOptions::disabled();
        self.clear_sql_logs();
    }

    pub fn sql_log_options(&self) -> SqlLogOptions {
        self.sql_log_options
    }

    pub fn sql_logs(&self) -> Vec<SqlLogEntry> {
        self.sql_log_entries
            .lock()
            .map(|entries| entries.clone())
            .unwrap_or_default()
    }

    pub fn clear_sql_logs(&self) {
        if let Ok(mut entries) = self.sql_log_entries.lock() {
            entries.clear();
        }
    }

    pub(crate) fn record_sql_log(
        &self,
        operation: SqlLogOperation,
        query: &CompiledQuery,
        database_kind: DatabaseKind,
        started_at: SystemTime,
        ended_at: SystemTime,
        elapsed: Duration,
        result_count: Option<usize>,
        result_type: Option<String>,
        affected_rows: Option<u64>,
        comment: Option<String>,
    ) {
        if !self.sql_log_options.enabled_for(operation) {
            return;
        }
        let debug_sql = query.debug_sql(database_kind);
        let result_summary = sql_result_summary(
            operation,
            result_count,
            result_type.as_deref(),
            affected_rows,
        );

        // Resolve final comment from stack or parameter
        let stack_comment = self.comment_stack.lock().ok().and_then(|stack| {
            if stack.is_empty() {
                None
            } else {
                Some(stack.join("->"))
            }
        });
        let final_comment = stack_comment.or(comment);

        // Append log line to app.log file
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("app.log")
        {
            use std::io::Write;
            let local_time: chrono::DateTime<chrono::Local> = started_at.into();
            let timestamp_str = local_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
            let user_id_str = self.user_identifier.as_deref().unwrap_or("");
            let comment_part = if let Some(ref c) = final_comment {
                format!(" - [{c}]")
            } else {
                "".to_owned()
            };
            let mut clean_debug_sql = debug_sql.clone();
            if clean_debug_sql.starts_with("/*") {
                if let Some(pos) = clean_debug_sql.find("*/") {
                    clean_debug_sql = clean_debug_sql[pos + 2..].trim_start().to_owned();
                }
            }
            let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
            let log_line = format!(
                "{timestamp_str}-[{user_id_str}]--DEBUG - SqlLogEntry{comment_part} - [{result_summary}] {} (took {:.3}ms)\n",
                clean_debug_sql, elapsed_ms
            );
            let _ = file.write_all(log_line.as_bytes());
        }

        if let Ok(mut entries) = self.sql_log_entries.lock() {
            entries.push(SqlLogEntry {
                operation,
                sql: query.sql.clone(),
                params: query.params.clone(),
                pretty_sql: pretty_sql(&debug_sql),
                debug_sql,
                started_at,
                ended_at,
                elapsed,
                result_summary,
                result_count,
                result_type,
                affected_rows,
                user_identifier: self.user_identifier.clone(),
                comment: final_comment,
            });
        }
    }

    pub fn language(&self) -> Language {
        self.language
    }

    pub fn set_language_code(&mut self, code: &str) -> Result<(), RuntimeError> {
        let Some(language) = Language::from_code(code) else {
            return Err(RuntimeError::Language(format!(
                "unsupported language code: {code}"
            )));
        };
        self.language = language;
        Ok(())
    }

    pub fn generate_id(&self, entity: &str) -> Result<Option<u64>, RuntimeError> {
        self.internal_id_generator
            .as_ref()
            .map(|generator| generator.generate_id(entity))
            .transpose()
    }

    pub fn next_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        match self.generate_id(entity)? {
            Some(id) => Ok(id),
            None => local_id_generator().generate_id(entity),
        }
    }

    pub fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.entity(name))
    }

    pub fn all_entities(&self) -> Vec<&EntityDescriptor> {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.all_entities())
            .unwrap_or_default()
    }

    pub fn require_entity(&self, name: &str) -> Result<&EntityDescriptor, RuntimeError> {
        self.entity(name)
            .ok_or_else(|| RuntimeError::MissingEntity(name.to_owned()))
    }

    pub fn insert_resource<T>(&mut self, resource: T)
    where
        T: Send + Sync + 'static,
    {
        self.typed_resources
            .insert(TypeId::of::<T>(), Box::new(resource));
    }

    pub fn get_resource<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.typed_resources
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn require_resource<T>(&self) -> Result<&T, ContextError>
    where
        T: Send + Sync + 'static,
    {
        self.get_resource::<T>()
            .ok_or(ContextError::MissingTypedResource(
                std::any::type_name::<T>(),
            ))
    }

    pub fn insert_named_resource<T>(&mut self, name: impl Into<String>, resource: T)
    where
        T: Send + Sync + 'static,
    {
        self.named_resources.insert(name.into(), Box::new(resource));
    }

    pub fn get_named_resource<T>(&self, name: &str) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.named_resources
            .get(name)
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn require_named_resource<T>(&self, name: &str) -> Result<&T, ContextError>
    where
        T: Send + Sync + 'static,
    {
        self.get_named_resource::<T>(name)
            .ok_or_else(|| ContextError::MissingResource(name.to_owned()))
    }

    pub fn put_local(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.locals.insert(key.into(), value.into());
    }

    pub fn local(&self, key: &str) -> Option<&Value> {
        self.locals.get(key)
    }

    pub fn remove_local(&mut self, key: &str) -> Option<Value> {
        self.locals.remove(key)
    }

    pub fn has_repository(&self, entity: &str) -> bool {
        let in_registry = self
            .repository_registry
            .as_ref()
            .map(|registry| registry.contains(entity))
            .unwrap_or(false);
        in_registry || self.entity(entity).is_some()
    }

    pub fn repository_behavior(
        &self,
        entity: &str,
    ) -> Option<std::sync::Arc<dyn RepositoryBehavior>> {
        self.repository_behavior_registry
            .as_ref()
            .and_then(|registry| registry.behavior(entity))
    }

    pub fn has_checker(&self, entity: &str) -> bool {
        self.checker_registry
            .as_ref()
            .and_then(|registry| registry.checker(entity))
            .is_some()
    }

    pub fn check_and_fix_record(
        &self,
        entity: &str,
        record: &mut Record,
    ) -> Result<(), RuntimeError> {
        self.check_and_fix_record_at(entity, record, &ObjectLocation::root())
    }

    pub fn check_and_fix_record_at(
        &self,
        entity: &str,
        record: &mut Record,
        location: &ObjectLocation,
    ) -> Result<(), RuntimeError> {
        let Some(checker) = self
            .checker_registry
            .as_ref()
            .and_then(|registry| registry.checker(entity))
        else {
            return Ok(());
        };
        let mut results = CheckResults::new();
        checker.check_and_fix(self, record, location, &mut results);
        if results.is_empty() {
            Ok(())
        } else {
            self.translate_check_results(&mut results);
            Err(RuntimeError::Check(results))
        }
    }

    pub fn translate_check_results(&self, results: &mut CheckResults) {
        for result in results {
            result.message = Some(translate_check_result(self.language, result));
        }
    }

    pub fn send_event(&self, event: EntityEvent) -> Result<(), RuntimeError> {
        let Some(sink) = self.event_sink.as_ref() else {
            return Ok(());
        };
        sink.on_event(self, &event)
    }

    pub fn commit_changes<D, E>(&self) -> Result<(), RepositoryError<E::Error>>
    where
        D: SqlDialect + Send + Sync + 'static,
        E: QueryExecutor + Send + Sync + 'static,
    {
        let dialect = self.require_resource::<D>().map_err(|err| {
            RepositoryError::Runtime(RuntimeError::Graph(format!(
                "cannot commit changes without dialect: {err}"
            )))
        })?;
        let executor = self.require_resource::<E>().map_err(|err| {
            RepositoryError::Runtime(RuntimeError::Graph(format!(
                "cannot commit changes without executor: {err}"
            )))
        })?;
        let change_set = self.entity_root.current_change_set();

        for (key, changes) in change_set.changes() {
            if changes.is_empty() {
                continue;
            }
            let entity = self
                .require_entity(&key.entity)
                .map_err(RepositoryError::Runtime)?;
            let mut command = UpdateCommand::new(&key.entity, key.id.clone());
            for (field, value) in changes {
                command = command.value(field.clone(), value.clone());
            }
            let query = dialect
                .compile_update(entity, &command)
                .map_err(RuntimeError::from)
                .map_err(RepositoryError::Runtime)?;
            executor
                .execute(&query)
                .map_err(RepositoryError::Executor)?;
        }

        self.entity_root.clear_current_change_set();
        Ok(())
    }
}

fn sql_result_summary(
    operation: SqlLogOperation,
    result_count: Option<usize>,
    result_type: Option<&str>,
    affected_rows: Option<u64>,
) -> String {
    match operation {
        SqlLogOperation::Select => {
            let count = result_count.unwrap_or(0);
            if count == 0 {
                "NO ROWS".to_owned()
            } else if count > 1 {
                match result_type {
                    Some(result_type) => format!("{count}*{result_type}"),
                    None => format!("{count}*rows"),
                }
            } else {
                match result_type {
                    Some(result_type) => result_type.to_owned(),
                    None => "row".to_owned(),
                }
            }
        }
        _ => {
            let affected = affected_rows.unwrap_or(0);
            format!("{affected} UPDATED")
        }
    }
}

fn pretty_sql(sql: &str) -> String {
    let mut pretty = sql.to_owned();
    for keyword in [
        " FROM ",
        " WHERE ",
        " GROUP BY ",
        " HAVING ",
        " ORDER BY ",
        " LIMIT ",
        " OFFSET ",
        " RETURNING ",
    ] {
        pretty = pretty.replace(keyword, &format!("\n{}", keyword.trim_start()));
    }
    pretty
        .replace(" AND ", "\n  AND ")
        .replace(" OR ", "\n  OR ")
}

pub struct QueryCommentGuard<'a> {
    context: &'a UserContext,
    has_pushed: bool,
}

impl<'a> QueryCommentGuard<'a> {
    pub fn new(context: &'a UserContext, comment: Option<String>) -> Self {
        let mut has_pushed = false;
        if let Some(comment) = comment {
            if !comment.is_empty() {
                if let Ok(mut stack) = context.comment_stack.lock() {
                    if stack.last() != Some(&comment) {
                        stack.push(comment);
                        has_pushed = true;
                    }
                }
            }
        }
        Self { context, has_pushed }
    }
}

impl<'a> Drop for QueryCommentGuard<'a> {
    fn drop(&mut self) {
        if self.has_pushed {
            if let Ok(mut stack) = self.context.comment_stack.lock() {
                stack.pop();
            }
        }
    }
}
