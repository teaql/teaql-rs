//! Application-owned structured logging. Only privacy-projected fields belong
//! here; this destination never sees execution bindings or error messages.
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Default, Clone)]
pub struct RuntimeLoggingOptions {
    pub log_name: Option<String>,
    pub log_format: Option<String>,
    pub model_names: Vec<String>,
    pub environment: Option<BTreeMap<String, String>>,
    pub working_directory: Option<PathBuf>,
}

#[derive(Debug)]
pub struct RuntimeLoggingError;
impl std::fmt::Display for RuntimeLoggingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Runtime logging initialization failed")
    }
}
impl std::error::Error for RuntimeLoggingError {}

pub struct RuntimeLogger {
    pub log_name: String,
    pub log_format: String,
    pub path: Option<PathBuf>,
    pub max_size: u64,
    pub max_files: u32,
    stream: String,
    gate: Mutex<()>,
}

fn invalid() -> io::Error {
    io::Error::other("invalid_logging_configuration")
}
fn environment() -> BTreeMap<String, String> {
    [
        "TEAQL_DOMAIN",
        "TEAQL_LOG_ENDPOINT",
        "TEAQL_LOG_MAX_SIZE",
        "TEAQL_LOG_MAX_FILES",
        "TEAQL_LOG_FORMAT",
    ]
    .into_iter()
    .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
    .collect()
}

fn valid_name(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let device = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && matches!(stem.as_bytes()[3], b'1'..=b'9'));
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.ends_with(['.', ' '])
        && !device
        && !name
            .chars()
            .any(|c| c.is_control() || "<>:\"/\\|?*".contains(c))
}

pub fn parse_log_size(value: &str) -> Result<u64, RuntimeLoggingError> {
    let value = value.trim().to_ascii_uppercase();
    let boundary = value
        .bytes()
        .position(|c| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let amount = value[..boundary]
        .parse::<u64>()
        .map_err(|_| RuntimeLoggingError)?;
    let multiplier = match value[boundary..].trim() {
        "" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024 * 1024,
        "G" | "GB" => 1024 * 1024 * 1024,
        _ => return Err(RuntimeLoggingError),
    };
    amount
        .checked_mul(multiplier)
        .filter(|value| *value > 0 && *value <= i64::MAX as u64)
        .ok_or(RuntimeLoggingError)
}

pub(crate) fn failure(name: &str, code: &str) {
    let record = json!({"timestamp": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true), "level": "ERROR", "log_name": name,
        "runtime": "rust", "event": "logging.failure", "message": "Runtime logging failed", "error_code": code});
    let _ = writeln!(io::stderr().lock(), "{record}");
}

impl RuntimeLogger {
    pub fn new(options: RuntimeLoggingOptions) -> Result<Self, RuntimeLoggingError> {
        let mut valid = "teaql".to_owned();
        let result = (|| -> io::Result<Self> {
            let env = options.environment.unwrap_or_else(environment);
            let name = options
                .log_name
                .or_else(|| {
                    env.get("TEAQL_DOMAIN")
                        .filter(|value| !value.is_empty())
                        .cloned()
                })
                .or_else(|| {
                    (options.model_names.len() == 1).then(|| options.model_names[0].clone())
                })
                .unwrap_or_else(|| "teaql".into());
            if !valid_name(&name) {
                return Err(invalid());
            }
            valid = name.clone();
            let log_format = options
                .log_format
                .or_else(|| env.get("TEAQL_LOG_FORMAT").cloned())
                .unwrap_or_else(|| "json".into());
            if !matches!(log_format.as_str(), "json" | "human") {
                return Err(invalid());
            }
            let max_size = parse_log_size(
                env.get("TEAQL_LOG_MAX_SIZE")
                    .map(String::as_str)
                    .unwrap_or("50M"),
            )
            .map_err(|_| invalid())?;
            let retention = env
                .get("TEAQL_LOG_MAX_FILES")
                .map(String::as_str)
                .unwrap_or("7");
            if retention.is_empty() || !retention.bytes().all(|c| c.is_ascii_digit()) {
                return Err(invalid());
            }
            let max_files = retention
                .parse::<u32>()
                .ok()
                .filter(|value| *value <= i32::MAX as u32)
                .ok_or_else(invalid)?;
            let endpoint = env
                .get("TEAQL_LOG_ENDPOINT")
                .filter(|value| !value.is_empty())
                .cloned()
                .unwrap_or_else(|| format!("./logs/{name}.log"));
            let (path, stream) = if matches!(endpoint.as_str(), "stdout" | "stderr" | "-") {
                (None, endpoint)
            } else {
                let endpoint = if endpoint.starts_with("file:") {
                    let url = url::Url::parse(&endpoint).map_err(|_| invalid())?;
                    if url.query().is_some()
                        || url.fragment().is_some()
                        || url.host_str().is_some_and(|host| host != "localhost")
                    {
                        return Err(invalid());
                    }
                    url.to_file_path().map_err(|_| invalid())?
                } else {
                    if endpoint.contains("://") {
                        return Err(invalid());
                    }
                    PathBuf::from(endpoint)
                };
                let base = options
                    .working_directory
                    .unwrap_or(std::env::current_dir()?);
                let path = if endpoint.is_absolute() {
                    endpoint
                } else {
                    base.join(endpoint)
                };
                let parent = path.parent().ok_or_else(invalid)?;
                fs::create_dir_all(parent)?;
                let path = fs::canonicalize(parent)?.join(path.file_name().ok_or_else(invalid)?);
                let path = match fs::canonicalize(&path) {
                    Ok(resolved) => resolved,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => path,
                    Err(error) => return Err(error),
                };
                (Some(path), String::new())
            };
            let logger = Self {
                log_name: name,
                log_format,
                path,
                max_size,
                max_files,
                stream,
                gate: Mutex::new(()),
            };
            logger.emit(logger.record(
                "INFO",
                "runtime.start",
                "TeaQL runtime started",
                json!({}),
            ))?;
            Ok(logger)
        })();
        result.map_err(|_| {
            failure(&valid, "log_destination_unavailable");
            RuntimeLoggingError
        })
    }

    pub(crate) fn default_logger() -> Arc<Self> {
        static DEFAULTS: OnceLock<Mutex<BTreeMap<String, Arc<RuntimeLogger>>>> = OnceLock::new();
        let env = environment();
        let cwd = std::env::current_dir().unwrap_or_else(|_| {
            failure("teaql", "log_destination_unavailable");
            panic!("Runtime logging initialization failed")
        });
        let key = format!("{}:{:?}", cwd.display(), env);
        let mut defaults = DEFAULTS
            .get_or_init(Mutex::default)
            .lock()
            .expect("runtime logging configuration lock poisoned");
        defaults
            .entry(key)
            .or_insert_with(|| {
                Arc::new(
                    Self::new(RuntimeLoggingOptions {
                        environment: Some(env),
                        working_directory: Some(cwd),
                        ..Default::default()
                    })
                    .unwrap_or_else(|_| panic!("Runtime logging initialization failed")),
                )
            })
            .clone()
    }

    fn record(&self, level: &str, event: &str, message: &str, fields: Value) -> Value {
        let mut record = fields.as_object().cloned().unwrap_or_default();
        record.extend(Map::from_iter([
            (
                "timestamp".into(),
                json!(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)),
            ),
            ("level".into(), json!(level)),
            ("log_name".into(), json!(self.log_name)),
            ("runtime".into(), json!("rust")),
            ("event".into(), json!(event)),
            ("message".into(), json!(message)),
        ]));
        Value::Object(record)
    }

    pub fn write(&self, level: &str, event: &str, message: &str, safe_fields: Value) -> bool {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if !matches!(level, "INFO" | "WARN" | "ERROR") {
                return Err(invalid());
            }
            self.emit(self.record(level, event, message, safe_fields))
        }));
        if matches!(result, Ok(Ok(()))) {
            true
        } else {
            failure(&self.log_name, "log_write_failed");
            false
        }
    }

    fn archives(path: &Path) -> io::Result<Vec<(u32, PathBuf)>> {
        let prefix = format!(
            "{}.",
            path.file_name().ok_or_else(invalid)?.to_string_lossy()
        );
        let mut files = Vec::new();
        for entry in fs::read_dir(path.parent().ok_or_else(invalid)?)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(suffix) = name.to_str().and_then(|name| name.strip_prefix(&prefix)) else {
                continue;
            };
            if suffix.is_empty()
                || suffix.starts_with('0')
                || !suffix.bytes().all(|c| c.is_ascii_digit())
            {
                continue;
            }
            if !entry.file_type()?.is_file() {
                return Err(invalid());
            }
            files.push((suffix.parse::<u32>().map_err(|_| invalid())?, entry.path()));
        }
        files.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(files)
    }

    fn emit(&self, record: Value) -> io::Result<()> {
        let bytes = if self.log_format == "human" {
            human_record(&record).into_bytes()
        } else {
            let mut bytes = serde_json::to_vec(&record)?;
            bytes.push(b'\n');
            bytes
        };
        let _guard = self.gate.lock().map_err(|_| invalid())?;
        let Some(path) = &self.path else {
            return if self.stream == "stderr" {
                io::stderr().lock().write_all(&bytes)
            } else {
                io::stdout().lock().write_all(&bytes)
            };
        };
        let _ownership = DestinationLock::acquire(path)?;
        self.check_presentation(path)?; // Never rotate/prune on a format mismatch.
        let archives = Self::archives(path)?;
        for (number, file) in &archives {
            if *number > self.max_files {
                fs::remove_file(file)?;
            }
        }
        let active = match fs::metadata(path) {
            Ok(value) => Some(value),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        if let Some(active) = active {
            if !active.is_file() {
                return Err(invalid());
            }
            let old_day: DateTime<Utc> = active.modified()?.into();
            if active.len() > 0
                && (active.len().saturating_add(bytes.len() as u64) > self.max_size
                    || old_day.date_naive() != Utc::now().date_naive())
            {
                for (number, file) in archives
                    .iter()
                    .filter(|(number, _)| *number <= self.max_files)
                {
                    if *number == self.max_files {
                        fs::remove_file(file)?;
                    } else {
                        fs::rename(file, archive_path(path, number + 1))?;
                    }
                }
                if self.max_files == 0 {
                    fs::remove_file(path)?;
                } else {
                    fs::rename(path, archive_path(path, 1))?;
                }
            }
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?
            .write_all(&bytes)
    }

    fn check_presentation(&self, path: &Path) -> io::Result<()> {
        let active = match fs::metadata(path) {
            Ok(active) => active,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if !active.is_file() {
            return Err(invalid());
        }
        if active.len() == 0 {
            return Ok(());
        }
        let mut first = String::new();
        BufReader::new(File::open(path)?).read_line(&mut first)?;
        let Some(first) = first.strip_suffix('\n') else {
            return Err(invalid());
        };
        if self.log_format == "human" && human_header(first) {
            return Ok(());
        }
        if self.log_format == "json" {
            if let Ok(Value::Object(record)) = serde_json::from_str(first) {
                if EVENT_FIELDS
                    .iter()
                    .all(|key| record.get(*key).is_some_and(Value::is_string))
                    && matches!(record["level"].as_str(), Some("INFO" | "WARN" | "ERROR"))
                {
                    return Ok(());
                }
            }
        }
        Err(invalid())
    }

    pub(crate) fn sql(&self, entry: &crate::SqlLogEntry) {
        let safe = crate::log_privacy::sql_entry(entry, false);
        let outcome = safe
            .log_context
            .execution_outcome
            .map(|outcome| outcome.as_str())
            .unwrap_or("unknown");
        let level = if matches!(outcome, "failure" | "cancelled") {
            "ERROR"
        } else {
            "INFO"
        };
        let trace_id = safe
            .log_context
            .telemetry
            .as_ref()
            .and_then(|state| state.correlation.as_ref())
            .map(|identity| identity.trace_id.clone());
        let mut fields = json!({ "category": "SQL", "outcome": outcome,
            "sql": safe.debug_sql, "comment": safe.comment, "purpose": safe.purpose, "audit_reason": safe.audit_reason,
            "trace_chain": trace(&safe.trace_chain), "trace_path": trace(&safe.trace_path), "omission_reason": safe.log_context.omission_reason,
            "result_count": safe.result_count, "affected_rows": safe.affected_rows, "elapsed_us": safe.elapsed.as_micros() });
        if let Some(trace_id) = trace_id {
            fields["trace_id"] = trace_id.into();
        }
        self.write(
            level,
            &format!("sql.{outcome}"),
            "TeaQL SQL execution",
            fields,
        );
    }

    pub(crate) fn audit(&self, safe: &crate::SafeAuditEvent) {
        self.write("INFO", "audit.event", "TeaQL audit event", json!({"category": "AUDIT", "outcome": "success", "entity": safe.entity,
            "entity_id": safe.entity_id, "trace_chain": trace(&safe.trace_chain), "fields": safe.fields.iter().map(|field| json!({"field": field.name, "value": field.value, "masked": field.masked, "truncated": field.truncated})).collect::<Vec<_>>() }));
    }
}

fn trace(nodes: &[teaql_core::TraceNode]) -> Vec<Value> {
    nodes.iter().map(|node| json!({"kind": format!("{:?}", node.kind), "name": node.entity_type, "detail": node.comment})).collect()
}
const EVENT_FIELDS: [&str; 6] = [
    "timestamp",
    "level",
    "log_name",
    "runtime",
    "event",
    "message",
];

fn human_text(value: &str, escape_backslash: bool) -> String {
    let mut output = String::new();
    for c in value.chars() {
        if escape_backslash && c == '\\' {
            output.push_str("\\\\");
        } else if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') {
            let _ = write!(output, "\\u{:04x}", c as u32);
        } else {
            output.push(c);
        }
    }
    output
}

fn human_record(record: &Value) -> String {
    let text = |key: &str| human_text(record[key].as_str().unwrap_or_default(), true);
    let mut output = format!(
        "{} [TeaQL][{}][{}] {} runtime={}\n  {}\n",
        text("timestamp"),
        text("log_name"),
        text("level"),
        text("event"),
        text("runtime"),
        text("message")
    );
    let fields = record.as_object().expect("runtime event is an object");
    let mut keys: Vec<_> = fields
        .keys()
        .filter(|key| !EVENT_FIELDS.contains(&key.as_str()))
        .collect();
    keys.sort();
    for key in keys {
        if let Some(sql) = fields[key].as_str().filter(|_| key.as_str() == "sql") {
            output.push_str("  sql:\n");
            for line in sql.split('\n') {
                output.push_str(&format!("    {}\n", human_text(line, true)));
            }
        } else {
            output.push_str(&format!(
                "  {}={}\n",
                human_text(key, true),
                human_text(&fields[key].to_string(), false)
            ));
        }
    }
    output
}

fn human_header(line: &str) -> bool {
    let Some((timestamp, rest)) = line.split_once(' ') else {
        return false;
    };
    let utc = DateTime::parse_from_rfc3339(timestamp)
        .is_ok_and(|date| date.offset().local_minus_utc() == 0);
    let Some((event, runtime)) = rest.rsplit_once(" runtime=") else {
        return false;
    };
    utc && rest.starts_with("[TeaQL][")
        && !runtime.is_empty()
        && !runtime.chars().any(char::is_whitespace)
        && ["][INFO] ", "][WARN] ", "][ERROR] "]
            .iter()
            .any(|level| event.contains(level))
        && !line
            .chars()
            .any(|c| c.is_control() || matches!(c, '\u{2028}' | '\u{2029}'))
}

fn archive_path(path: &Path, number: u32) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(format!(".{number}"));
    PathBuf::from(value)
}

struct DestinationLock {
    file: Option<File>,
    path: PathBuf,
}
impl DestinationLock {
    fn acquire(path: &Path) -> io::Result<Self> {
        let mut value = path.as_os_str().to_os_string();
        value.push(".lock");
        let path = PathBuf::from(value);
        let started = Instant::now();
        loop {
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        file: Some(file),
                        path,
                    });
                }
                Err(error)
                    if error.kind() == io::ErrorKind::AlreadyExists
                        && started.elapsed() < Duration::from_secs(30) =>
                {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(error) => return Err(error),
            }
        }
    }
}
impl Drop for DestinationLock {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.path);
    }
}

const CATEGORIES: &[&str] = &[
    "internal",
    "validation",
    "authorization",
    "conflict",
    "provider",
    "transport",
    "timeout",
    "cancelled",
];

pub(crate) fn error_type_category<E>(_: &E) -> &'static str {
    crate::runtime_error_category(std::any::type_name::<E>())
}

pub(crate) fn runtime_error_category(error: &crate::RuntimeError) -> &'static str {
    match error {
        crate::RuntimeError::Check(_)
        | crate::RuntimeError::RequestIntent(_)
        | crate::RuntimeError::DynamicField(_) => "validation",
        crate::RuntimeError::Policy(_) => "authorization",
        crate::RuntimeError::OptimisticLockConflict { .. } => "conflict",
        crate::RuntimeError::SqlCompile(_) | crate::RuntimeError::Transaction(_) => "provider",
        _ => "internal",
    }
}

pub(crate) fn data_service_error_category<E>(error: &crate::DataServiceError<E>) -> &'static str {
    match error {
        crate::DataServiceError::Runtime(error) => runtime_error_category(error),
        crate::DataServiceError::Entity(_) => "validation",
        crate::DataServiceError::Executor(_) => "provider",
    }
}

impl crate::UserContext {
    // The disabled SDK path keeps its inline future and never constructs an SDK
    // scope or evaluates an entity metadata thunk. Logging owns no business data.
    pub(crate) fn observe_local_result<T, E, F>(
        &self,
        family: &'static str,
        future: F,
        classify: fn(&E) -> &'static str,
    ) -> impl std::future::Future<Output = Result<T, E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        use futures_util::FutureExt;
        future.map(move |result| {
            let category = result.as_ref().err().map(classify);
            let outcome = if result.is_ok() { "success" } else { "failure" };
            let level = match category {
                None => "INFO",
                Some("validation" | "authorization" | "conflict") => "WARN",
                _ => "ERROR",
            };
            let mut fields = json!({"category": family, "outcome": outcome});
            if let Some(category) = category {
                fields["error_code"] = category.into();
            }
            self.runtime_logger.write(
                level,
                &format!("operation.{outcome}"),
                &format!("TeaQL operation {outcome}"),
                fields,
            );
            result
        })
    }

    pub(crate) fn start_classified_runtime_operation(
        &self,
        operation: crate::RuntimeOperation,
    ) -> ClassifiedRuntimeScope {
        let error_category = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let telemetry: Arc<dyn crate::RuntimeTelemetry> = Arc::new(LocalLogTelemetry {
            delegate: self.runtime_telemetry().clone(),
            logger: self.runtime_logger.clone(),
            error_category: Some(error_category.clone()),
        });
        ClassifiedRuntimeScope {
            scope: crate::start_runtime_operation(&telemetry, operation),
            error_category,
        }
    }
}

// A local category override belongs to this one operation. It is never a
// shared context flag, SDK attribute, or replacement for the SDK error type.
pub(crate) struct ClassifiedRuntimeScope {
    scope: crate::FailOpenRuntimeTelemetryScope,
    error_category: Arc<std::sync::atomic::AtomicU8>,
}
impl std::ops::Deref for ClassifiedRuntimeScope {
    type Target = crate::FailOpenRuntimeTelemetryScope;
    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}
impl ClassifiedRuntimeScope {
    pub(crate) fn failure_with_category(&self, error_type: &str, category: &'static str) {
        let index = CATEGORIES
            .iter()
            .position(|value| *value == category)
            .unwrap_or(0);
        self.error_category
            .store(index as u8, std::sync::atomic::Ordering::Relaxed);
        self.scope.failure(error_type);
    }
}

pub(crate) struct LocalLogTelemetry {
    pub delegate: Arc<dyn crate::RuntimeTelemetry>,
    pub logger: Arc<RuntimeLogger>,
    pub error_category: Option<Arc<std::sync::atomic::AtomicU8>>,
}
impl crate::RuntimeTelemetry for LocalLogTelemetry {
    fn start(&self, operation: crate::RuntimeOperation) -> Box<dyn crate::RuntimeTelemetryScope> {
        let delegate = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.delegate.start(operation.clone())
        }))
        .unwrap_or_else(|_| {
            crate::RuntimeTelemetry::start(&crate::NoopRuntimeTelemetry, operation.clone())
        });
        Box::new(LocalLogScope {
            delegate,
            operation,
            logger: self.logger.clone(),
            error_category: self.error_category.clone(),
            ended: false,
        })
    }
}
struct LocalLogScope {
    delegate: Box<dyn crate::RuntimeTelemetryScope>,
    operation: crate::RuntimeOperation,
    logger: Arc<RuntimeLogger>,
    error_category: Option<Arc<std::sync::atomic::AtomicU8>>,
    ended: bool,
}
impl LocalLogScope {
    fn log(&mut self, error: Option<&str>) -> bool {
        if self.ended {
            return false;
        }
        self.ended = true;
        let category = error.map(|kind| {
            let index = self
                .error_category
                .as_ref()
                .map(|slot| slot.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(0);
            CATEGORIES
                .get(index as usize)
                .copied()
                .filter(|_| index != 0)
                .unwrap_or_else(|| crate::runtime_error_category(kind))
        });
        let level = match category {
            None => "INFO",
            Some("validation" | "authorization" | "conflict") => "WARN",
            _ => "ERROR",
        };
        let outcome = if error.is_none() {
            "success"
        } else {
            "failure"
        };
        let correlation =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.delegate.correlation()))
                .ok()
                .flatten();
        self.logger.write(level, &format!("operation.{outcome}"), &format!("TeaQL operation {outcome}"), json!({"category": self.operation.family.to_ascii_uppercase(), "outcome": outcome, "error_code": category, "trace_id": correlation.map(|value| value.trace_id) }));
        true
    }
}
impl crate::RuntimeTelemetryScope for LocalLogScope {
    fn correlation(
        &self,
    ) -> Option<teaql_data_service::statement_telemetry::SqlExecutionCorrelation> {
        self.delegate.correlation()
    }
    fn with_context(&self, callback: &mut dyn FnMut()) {
        self.delegate.with_context(callback);
    }
    fn success(&mut self, attributes: BTreeMap<String, crate::RuntimeAttributeValue>) {
        if self.log(None) {
            self.delegate.success(attributes);
        }
    }
    fn failure(&mut self, error_type: &str) {
        if self.log(Some(error_type)) {
            self.delegate.failure(error_type);
        }
    }
    fn failure_with_attributes(
        &mut self,
        error_type: &str,
        attributes: BTreeMap<String, crate::RuntimeAttributeValue>,
    ) {
        if self.log(Some(error_type)) {
            self.delegate
                .failure_with_attributes(error_type, attributes);
        }
    }
}

#[cfg(test)]
mod tests;
