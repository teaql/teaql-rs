use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeAttributeValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

impl From<&str> for RuntimeAttributeValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for RuntimeAttributeValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i64> for RuntimeAttributeValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<usize> for RuntimeAttributeValue {
    fn from(value: usize) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<bool> for RuntimeAttributeValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeOperation {
    pub family: String,
    pub name: String,
    pub attributes: BTreeMap<String, RuntimeAttributeValue>,
}

impl RuntimeOperation {
    pub fn new(family: impl Into<String>, name: impl Into<String>) -> Self {
        let family = family.into();
        let name = name.into();
        let mut attributes = BTreeMap::new();
        attributes.insert("teaql.operation.family".into(), family.clone().into());
        attributes.insert("teaql.operation.name".into(), name.clone().into());
        Self {
            family,
            name,
            attributes,
        }
    }

    pub fn attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<RuntimeAttributeValue>,
    ) -> Self {
        let key = key.into();
        if !is_forbidden_attribute(&key) {
            self.attributes.insert(key, value.into());
        }
        self
    }
}

fn is_forbidden_attribute(key: &str) -> bool {
    matches!(
        key,
        "teaql.entity.id"
            | "teaql.user.id"
            | "teaql.tenant.id"
            | "teaql.query.parameters"
            | "teaql.field.values"
            | "teaql.audit.reason"
            | "db.query.parameter_values"
            | "http.request.body"
            | "url.full"
    )
}

/// Stable category derived from a native error type, never its message.
pub fn runtime_error_category(error_type: &str) -> &'static str {
    let error_type = error_type.to_ascii_lowercase();
    for (category, terms) in [
        ("cancelled", &["cancelled", "canceled", "cancellation"][..]),
        ("timeout", &["timeout", "deadline"][..]),
        (
            "authorization",
            &[
                "authentication",
                "authorization",
                "unauthorized",
                "forbidden",
                "permission",
            ],
        ),
        (
            "validation",
            &[
                "validation",
                "invalidargument",
                "valueerror",
                "parse",
                "format",
            ],
        ),
        (
            "conflict",
            &[
                "conflict",
                "optimistic",
                "version",
                "duplicate",
                "alreadyexists",
            ],
        ),
        (
            "transport",
            &[
                "transport",
                "network",
                "connection",
                "socket",
                "http",
                "ioerror",
            ],
        ),
        ("provider", &["provider", "sql", "database", "jdbc"]),
    ] {
        if terms.iter().any(|term| error_type.contains(term)) {
            return category;
        }
    }
    "internal"
}

pub trait RuntimeTelemetryScope: Send {
    fn correlation(
        &self,
    ) -> Option<teaql_data_service::statement_telemetry::SqlExecutionCorrelation> {
        None
    }
    fn with_context(&self, callback: &mut dyn FnMut()) {
        callback();
    }
    fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>);
    fn failure(&mut self, error_type: &str);
    fn failure_with_attributes(
        &mut self,
        error_type: &str,
        _attributes: BTreeMap<String, RuntimeAttributeValue>,
    ) {
        self.failure(error_type);
    }
}

pub trait RuntimeTelemetry: Send + Sync {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope>;
    fn is_noop(&self) -> bool {
        false
    }
    fn records_sql_diagnostics(&self) -> bool {
        false
    }
    fn record_diagnostic(&self, _event: RuntimeDiagnosticEvent) {}
    fn inject_context(&self, _carrier: &mut BTreeMap<String, String>) {}
    fn extract_context(
        &self,
        _carrier: &BTreeMap<String, String>,
    ) -> Box<dyn RuntimeTelemetryPropagationContext> {
        Box::new(NoopRuntimeTelemetryPropagationContext)
    }
    fn flush(&self) {}
    fn shutdown(&self) {}
}

pub trait RuntimeTelemetryPropagationContext: Send + Sync {
    fn with_context(&self, callback: &mut dyn FnMut()) {
        callback();
    }
}

struct NoopRuntimeTelemetryPropagationContext;
impl RuntimeTelemetryPropagationContext for NoopRuntimeTelemetryPropagationContext {}

pub struct FailOpenRuntimeTelemetryPropagationContext {
    delegate: Option<Box<dyn RuntimeTelemetryPropagationContext>>,
}

impl FailOpenRuntimeTelemetryPropagationContext {
    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        futures_util::pin_mut!(future);
        futures_util::future::poll_fn(|task_context| {
            let Some(delegate) = self.delegate.as_ref() else {
                return future.as_mut().poll(task_context);
            };
            let mut result = None;
            let mut invoked = false;
            let mut callback = || {
                invoked = true;
                result = Some(future.as_mut().poll(task_context));
            };
            let context_result = catch_unwind(AssertUnwindSafe(|| {
                delegate.with_context(&mut callback);
            }));
            match (context_result, result) {
                (_, Some(result)) => result,
                (Err(payload), None) if invoked => std::panic::resume_unwind(payload),
                _ => future.as_mut().poll(task_context),
            }
        })
        .await
    }
}

pub fn extract_runtime_context(
    telemetry: &Arc<dyn RuntimeTelemetry>,
    carrier: &BTreeMap<String, String>,
) -> FailOpenRuntimeTelemetryPropagationContext {
    let delegate = catch_unwind(AssertUnwindSafe(|| telemetry.extract_context(carrier))).ok();
    FailOpenRuntimeTelemetryPropagationContext { delegate }
}

#[derive(Default)]
pub struct NoopRuntimeTelemetry;

impl RuntimeTelemetry for NoopRuntimeTelemetry {
    fn start(&self, _operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        Box::new(NoopRuntimeTelemetryScope)
    }

    fn is_noop(&self) -> bool {
        true
    }
}

struct NoopRuntimeTelemetryScope;

impl RuntimeTelemetryScope for NoopRuntimeTelemetryScope {
    fn success(&mut self, _attributes: BTreeMap<String, RuntimeAttributeValue>) {}
    fn failure(&mut self, _error_type: &str) {}
}

pub struct FailOpenRuntimeTelemetryScope {
    delegate: Mutex<Option<Box<dyn RuntimeTelemetryScope>>>,
}

#[derive(Debug, Clone)]
pub struct RuntimeDiagnosticEvent {
    pub correlation: teaql_data_service::statement_telemetry::SqlExecutionCorrelation,
    pub attributes: BTreeMap<String, RuntimeAttributeValue>,
}

impl FailOpenRuntimeTelemetryScope {
    pub fn correlation(
        &self,
    ) -> Option<teaql_data_service::statement_telemetry::SqlExecutionCorrelation> {
        let delegate = self.delegate.lock().ok()?;
        catch_unwind(AssertUnwindSafe(|| delegate.as_ref()?.correlation()))
            .ok()
            .flatten()
    }
    pub fn with_context(&self, callback: &mut dyn FnMut()) {
        let Ok(delegate) = self.delegate.lock() else {
            callback();
            return;
        };
        let Some(scope) = delegate.as_ref() else {
            callback();
            return;
        };
        let mut invoked = false;
        let mut completed = false;
        let mut observed = || {
            if invoked {
                return;
            }
            invoked = true;
            callback();
            completed = true;
        };
        let result = catch_unwind(AssertUnwindSafe(|| scope.with_context(&mut observed)));
        // Resume a business panic only after releasing our lock. The adapter
        // mutex must not become poisoned by a caller's stream/future panic.
        drop(delegate);
        if let Err(payload) = result {
            if invoked && !completed {
                std::panic::resume_unwind(payload);
            }
            if !invoked {
                callback();
            }
        } else if !invoked {
            callback();
        }
    }
    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        futures_util::pin_mut!(future);
        futures_util::future::poll_fn(|task_context| {
            let mut result = None;
            self.with_context(&mut || {
                result = Some(future.as_mut().poll(task_context));
            });
            result.expect("fail-open telemetry must poll once")
        })
        .await
    }

    pub fn success(&self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
        self.finish(|scope| scope.success(attributes));
    }

    pub fn failure(&self, error_type: &str) {
        self.finish(|scope| scope.failure(error_type));
    }

    pub fn failure_with_attributes(
        &self,
        error_type: &str,
        attributes: BTreeMap<String, RuntimeAttributeValue>,
    ) {
        self.finish(|scope| scope.failure_with_attributes(error_type, attributes));
    }

    fn finish(&self, action: impl FnOnce(&mut dyn RuntimeTelemetryScope)) {
        let Ok(mut delegate) = self.delegate.lock() else {
            return;
        };
        let Some(mut scope) = delegate.take() else {
            return;
        };
        let _ = catch_unwind(AssertUnwindSafe(|| action(scope.as_mut())));
    }
}

pub fn start_runtime_operation(
    telemetry: &Arc<dyn RuntimeTelemetry>,
    operation: RuntimeOperation,
) -> FailOpenRuntimeTelemetryScope {
    let delegate = catch_unwind(AssertUnwindSafe(|| telemetry.start(operation))).ok();
    FailOpenRuntimeTelemetryScope {
        delegate: Mutex::new(delegate),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_native_error_types_without_messages() {
        assert_eq!(runtime_error_category("DatabaseTimeoutError"), "timeout");
        assert_eq!(runtime_error_category("PermissionError"), "authorization");
        assert_eq!(runtime_error_category("UnknownTeaQLError"), "internal");
        assert_eq!(
            runtime_error_category("sql_execution_cancelled"),
            "cancelled"
        );
    }

    struct BrokenTelemetry;
    impl RuntimeTelemetry for BrokenTelemetry {
        fn start(&self, _operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
            panic!("adapter failed")
        }
    }

    struct BrokenContextTelemetry;
    impl RuntimeTelemetry for BrokenContextTelemetry {
        fn start(&self, _operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
            Box::new(BrokenContextScope)
        }
    }
    struct BrokenContextScope;
    impl RuntimeTelemetryScope for BrokenContextScope {
        fn with_context(&self, _callback: &mut dyn FnMut()) {
            panic!("context adapter failed")
        }
        fn success(&mut self, _attributes: BTreeMap<String, RuntimeAttributeValue>) {}
        fn failure(&mut self, _error_type: &str) {}
    }

    #[test]
    fn strips_forbidden_attributes_and_is_fail_open() {
        let operation = RuntimeOperation::new("query", "School.list")
            .attribute("teaql.entity.type", "School")
            .attribute("teaql.entity.id", 42_i64);
        assert_eq!(
            operation.attributes.get("teaql.entity.type"),
            Some(&"School".into())
        );
        assert!(!operation.attributes.contains_key("teaql.entity.id"));

        let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(BrokenTelemetry);
        let scope = start_runtime_operation(&telemetry, operation);
        scope.success(BTreeMap::new());
        scope.failure("late");
    }

    #[tokio::test]
    async fn context_activation_is_fail_open() {
        let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(BrokenContextTelemetry);
        let scope =
            start_runtime_operation(&telemetry, RuntimeOperation::new("query", "School.list"));
        assert_eq!(scope.run(async { 42 }).await, 42);
    }

    struct DuplicateContextTelemetry;
    struct DuplicateContextScope;
    impl RuntimeTelemetry for DuplicateContextTelemetry {
        fn start(&self, _: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
            Box::new(DuplicateContextScope)
        }
    }
    impl RuntimeTelemetryScope for DuplicateContextScope {
        fn with_context(&self, callback: &mut dyn FnMut()) {
            callback();
            callback();
            panic!("adapter failed after callback");
        }
        fn success(&mut self, _: BTreeMap<String, RuntimeAttributeValue>) {}
        fn failure(&mut self, _: &str) {}
    }

    #[tokio::test]
    async fn duplicate_or_post_callback_adapter_fault_cannot_repeat_business_work() {
        let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(DuplicateContextTelemetry);
        let scope = start_runtime_operation(&telemetry, RuntimeOperation::new("query", "Probe"));
        let mut polls = 0;
        assert_eq!(
            scope
                .run(async {
                    polls += 1;
                    42
                })
                .await,
            42
        );
        assert_eq!(polls, 1);
        let mut calls = 0;
        scope.with_context(&mut || calls += 1);
        assert_eq!(calls, 1);
        scope.success(BTreeMap::new());
    }
}
