use super::*;
mod presentations;
use crate::{RuntimeOperation, UserContext};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

struct TestDirectory(PathBuf);
impl TestDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "teaql-logging-rust-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn logger(directory: &Path, values: &[(&str, &str)]) -> RuntimeLogger {
    RuntimeLogger::new(RuntimeLoggingOptions {
        working_directory: Some(directory.to_owned()),
        environment: Some(
            values
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        ),
        ..Default::default()
    })
    .unwrap()
}
fn records(path: &Path) -> Vec<Value> {
    let bytes = fs::read(path).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .map(|line| {
            let value: Value = serde_json::from_str(line).unwrap();
            for field in [
                "timestamp",
                "level",
                "log_name",
                "runtime",
                "event",
                "message",
            ] {
                assert!(!value[field].as_str().unwrap().is_empty());
            }
            assert_eq!(value["runtime"], "rust");
            assert!(value["timestamp"].as_str().unwrap().ends_with('Z'));
            DateTime::parse_from_rfc3339(value["timestamp"].as_str().unwrap()).unwrap();
            value
        })
        .collect()
}

#[test]
fn default_startup_and_append_across_two_starts() {
    let directory = TestDirectory::new();
    let first = logger(&directory.0, &[]);
    assert_eq!(
        first.path.as_ref().unwrap(),
        &directory.0.join("logs/teaql.log")
    );
    assert_eq!(first.max_size, 50 * 1024 * 1024);
    assert_eq!(first.max_files, 7);
    assert_eq!(
        records(first.path.as_ref().unwrap())[0]["event"],
        "runtime.start"
    );
    let second = logger(&directory.0, &[]);
    assert!(second.write("INFO", "probe", "Completed", json!({})));
    assert_eq!(records(first.path.as_ref().unwrap()).len(), 3);
}

#[test]
fn name_precedence_override_unicode_and_portable_validation() {
    for (explicit, domain, models, expected) in [
        (Some("application"), "domain", vec!["model"], "application"),
        (None, "domain", vec!["model"], "domain"),
        (None, "", vec!["model"], "model"),
        (None, "", vec!["one", "two"], "teaql"),
        (Some("订单"), "", vec![], "订单"),
    ] {
        let directory = TestDirectory::new();
        let log = RuntimeLogger::new(RuntimeLoggingOptions {
            log_name: explicit.map(str::to_owned),
            model_names: models.into_iter().map(str::to_owned).collect(),
            working_directory: Some(directory.0.clone()),
            environment: Some(BTreeMap::from([
                ("TEAQL_DOMAIN".into(), domain.into()),
                ("TEAQL_LOG_ENDPOINT".into(), "other/override.jsonl".into()),
            ])),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(log.log_name, expected);
        assert_eq!(records(log.path.as_ref().unwrap())[0]["log_name"], expected);
        assert_eq!(log.path.unwrap(), directory.0.join("other/override.jsonl"));
    }
    for name in [
        "",
        ".",
        "..",
        "../secret",
        "a/b",
        "a\\b",
        "bad\nname",
        "NUL",
        "COM1.txt",
        "trailing.",
        "trailing ",
    ] {
        assert!(!valid_name(name), "{name:?}");
    }
    let directory = TestDirectory::new();
    assert!(
        RuntimeLogger::new(RuntimeLoggingOptions {
            log_name: Some("../secret".into()),
            working_directory: Some(directory.0.clone()),
            environment: Some(BTreeMap::new()),
            ..Default::default()
        })
        .is_err()
    );
}

#[test]
fn configuration_units_and_invalid_values() {
    for (value, expected) in [
        ("1", 1),
        ("2K", 2048),
        ("3KB", 3072),
        ("2m", 2097152),
        ("1MB", 1048576),
        ("1G", 1073741824),
        ("1GB", 1073741824),
    ] {
        assert_eq!(parse_log_size(value).unwrap(), expected);
    }
    for value in ["0", "-1", "1.5M", "1TB", "999999999999999999999G"] {
        assert!(parse_log_size(value).is_err());
    }
    for value in ["-1", "invalid", "1.5", "2147483648"] {
        let directory = TestDirectory::new();
        assert!(
            RuntimeLogger::new(RuntimeLoggingOptions {
                working_directory: Some(directory.0.clone()),
                environment: Some(BTreeMap::from([(
                    "TEAQL_LOG_MAX_FILES".into(),
                    value.into()
                )])),
                ..Default::default()
            })
            .is_err()
        );
    }
}

#[test]
fn utf8_size_oversized_whole_records_archive_order_and_zero_retention() {
    let directory = TestDirectory::new();
    let log = logger(
        &directory.0,
        &[("TEAQL_LOG_MAX_SIZE", "256"), ("TEAQL_LOG_MAX_FILES", "2")],
    );
    for i in 0..5 {
        assert!(log.write("INFO", "probe", &"界".repeat(200), json!({"sequence": i})));
    }
    let path = log.path.as_ref().unwrap();
    for (suffix, expected) in [(0, 4), (1, 3), (2, 2)] {
        let file = if suffix == 0 {
            path.clone()
        } else {
            archive_path(path, suffix)
        };
        let content = records(&file);
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["sequence"], expected);
        assert_eq!(content[0]["message"], "界".repeat(200));
    }
    assert!(!archive_path(path, 3).exists());
    let zero = logger(
        &directory.0,
        &[("TEAQL_LOG_MAX_SIZE", "256"), ("TEAQL_LOG_MAX_FILES", "0")],
    );
    zero.write("INFO", "probe", &"x".repeat(400), json!({}));
    assert!(RuntimeLogger::archives(path).unwrap().is_empty());
    assert_eq!(records(path).len(), 1);
}

#[test]
fn utc_day_rotation_and_restart() {
    for restart in [false, true] {
        let directory = TestDirectory::new();
        let log = logger(&directory.0, &[]);
        let path = log.path.as_ref().unwrap();
        let before = fs::read(path).unwrap();
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(86400))
            .unwrap();
        if restart {
            let _ = logger(&directory.0, &[]);
        } else {
            assert!(log.write("INFO", "next.day", "Next day", json!({})));
        }
        assert_eq!(fs::read(archive_path(path, 1)).unwrap(), before);
        assert_eq!(records(path).len(), 1);
    }
}

#[test]
fn ordinary_utf8_records_respect_whole_line_byte_limit() {
    let directory = TestDirectory::new();
    let log = logger(
        &directory.0,
        &[
            ("TEAQL_LOG_MAX_SIZE", "1K"),
            ("TEAQL_LOG_MAX_FILES", "1000"),
        ],
    );
    for index in 0..40 {
        assert!(log.write("INFO", "regular", &"界".repeat(30), json!({"index": index})));
    }
    let files = fs::read_dir(directory.0.join("logs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert!(files.len() > 1);
    let mut all = Vec::new();
    for file in files {
        let bytes = fs::metadata(&file).unwrap().len();
        assert!(bytes > 0 && bytes <= 1024);
        all.extend(records(&file));
    }
    assert_eq!(all.len(), 41);
    let indices = all
        .iter()
        .filter_map(|record| record["index"].as_u64())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(indices, (0..40).collect());
}

#[tokio::test]
async fn rotation_failure_preserves_foreign_archive_and_business_outcome() {
    let directory = TestDirectory::new();
    let log = Arc::new(logger(&directory.0, &[]));
    let path = log.path.as_ref().unwrap();
    let before = fs::read(path).unwrap();
    let archive = archive_path(path, 1);
    fs::create_dir(&archive).unwrap();
    fs::write(archive.join("marker"), "preserve").unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_times(
            fs::FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(172800)),
        )
        .unwrap();
    let context = UserContext::new_with_logger(log.clone());
    let scope = context.start_runtime_operation(RuntimeOperation::new("query", "probe"));
    assert_eq!(scope.run(async { 42 }).await, 42);
    scope.success(BTreeMap::new());
    assert!(!log.write("INFO", "probe", "Completed", json!({})));
    assert_eq!(fs::read(path).unwrap(), before);
    assert_eq!(
        fs::read_to_string(archive.join("marker")).unwrap(),
        "preserve"
    );
}

#[tokio::test]
async fn startup_failure_and_post_startup_result_error_and_spi_identity() {
    let directory = TestDirectory::new();
    fs::create_dir_all(directory.0.join("logs/teaql.log")).unwrap();
    assert!(
        RuntimeLogger::new(RuntimeLoggingOptions {
            working_directory: Some(directory.0.clone()),
            environment: Some(BTreeMap::new()),
            ..Default::default()
        })
        .is_err()
    );
    fs::remove_dir(directory.0.join("logs/teaql.log")).unwrap();
    let log = Arc::new(logger(&directory.0, &[]));
    let path = log.path.as_ref().unwrap();
    let telemetry: Arc<dyn crate::RuntimeTelemetry> = Arc::new(crate::NoopRuntimeTelemetry);
    let context =
        UserContext::new_with_logger(log.clone()).with_runtime_telemetry(telemetry.clone());
    assert!(Arc::ptr_eq(context.runtime_telemetry(), &telemetry));
    fs::remove_file(path).unwrap();
    fs::create_dir(path).unwrap();
    let scope = context.start_runtime_operation(RuntimeOperation::new("query", "probe"));
    assert_eq!(scope.run(async { 42 }).await, 42);
    scope.success(BTreeMap::new());
    let original = io::Error::other("private-original-error");
    let scope = context.start_runtime_operation(RuntimeOperation::new("query", "probe"));
    let result: io::Result<()> = scope.run(async { Err(original) }).await;
    scope.failure("IOError");
    assert_eq!(result.unwrap_err().to_string(), "private-original-error");
    assert!(!log.write("INFO", "probe", "Completed", json!({})));
}

#[test]
fn native_scope_severity_idempotence_and_value_free_failures() {
    let directory = TestDirectory::new();
    let log = Arc::new(logger(&directory.0, &[]));
    let context = UserContext::new_with_logger(log.clone());
    for error in [None, Some("ValidationError"), Some("IOError")] {
        let scope = context.start_runtime_operation(RuntimeOperation::new("query", "probe"));
        if let Some(error) = error {
            scope.failure(error);
        } else {
            scope.success(BTreeMap::new());
        }
        scope.success(BTreeMap::new());
    }
    let content = records(log.path.as_ref().unwrap());
    assert_eq!(content.len(), 4);
    assert_eq!(content[1]["level"], "INFO");
    assert_eq!(content[2]["level"], "WARN");
    assert_eq!(content[3]["level"], "ERROR");
}

fn assert_all(directory: &Path, expected: usize, starts: usize) {
    let mut all = Vec::new();
    for entry in fs::read_dir(directory.join("logs")).unwrap() {
        all.extend(records(&entry.unwrap().path()));
    }
    let ids = all
        .iter()
        .filter_map(|record| record["id"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), expected);
    assert_eq!(all.len(), expected + starts);
}

#[test]
fn concurrent_threads_rotate_and_reopen() {
    let directory = TestDirectory::new();
    let mut children = Vec::new();
    for worker in 0..8 {
        let log = logger(
            &directory.0,
            &[
                ("TEAQL_LOG_MAX_SIZE", "1K"),
                ("TEAQL_LOG_MAX_FILES", "1000"),
            ],
        );
        children.push(std::thread::spawn(move || {
            for i in 0..30 {
                assert!(log.write(
                    "INFO",
                    "concurrent",
                    "Completed",
                    json!({"id": format!("{worker}/{i}")})
                ));
            }
        }));
    }
    for child in children {
        child.join().unwrap();
    }
    assert_all(&directory.0, 240, 8);
}

#[test]
fn process_helper() {
    let Ok(directory) = std::env::var("TEAQL_LOGGING_TEST_DIRECTORY") else {
        return;
    };
    let worker = std::env::var("TEAQL_LOGGING_TEST_WORKER").unwrap();
    let log = logger(
        Path::new(&directory),
        &[
            ("TEAQL_LOG_MAX_SIZE", "1K"),
            ("TEAQL_LOG_MAX_FILES", "1000"),
        ],
    );
    for i in 0..30 {
        assert!(log.write(
            "INFO",
            "concurrent",
            "Completed",
            json!({"id": format!("{worker}/{i}")})
        ));
    }
}

struct Children(Vec<Child>);
impl Drop for Children {
    fn drop(&mut self) {
        for child in &mut self.0 {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}
#[test]
fn separate_processes_coordinate_rotation_and_append() {
    let directory = TestDirectory::new();
    let mut children = Children(Vec::new());
    for worker in 0..4 {
        children.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime_logging::tests::process_helper",
                    "--nocapture",
                ])
                .env_clear()
                .env("TEAQL_LOGGING_TEST_DIRECTORY", &directory.0)
                .env("TEAQL_LOGGING_TEST_WORKER", worker.to_string())
                .spawn()
                .unwrap(),
        );
    }
    for child in &mut children.0 {
        assert!(child.wait().unwrap().success());
    }
    assert_all(&directory.0, 120, 4);
}

#[test]
fn legacy_public_sinks_share_the_structured_destination() {
    let directory = TestDirectory::new();
    let path = directory.0.join("legacy.jsonl");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "log_privacy::tests::file_log_child",
            "--ignored",
            "--nocapture",
        ])
        .env_clear()
        .env("TEAQL_DOMAIN", "legacy-app")
        .env("TEAQL_LOG_ENDPOINT", &path)
        .env("TMPDIR", &directory.0)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = records(&path);
    assert!(rows.len() >= 5);
    assert_eq!(rows[0]["event"], "runtime.start");
    assert!(rows.iter().all(|row| row["log_name"] == "legacy-app"));
    assert!(rows.iter().any(|row| row["category"] == "AUDIT"));
    assert!(rows.iter().any(|row| row["category"] == "SQL"));
    let content = fs::read_to_string(path).unwrap();
    for canary in ["PRIVATE-CUSTOMER", "TOKEN-CANARY", "RETAINED-WRITE-CANARY"] {
        assert!(!content.contains(canary));
    }
}

#[tokio::test]
async fn no_sdk_generic_execution_logs_without_sql_metadata_or_metadata_thunks() {
    let directory = TestDirectory::new();
    let log = Arc::new(logger(&directory.0, &[]));
    let context = UserContext::new_with_logger(log.clone());
    assert!(context.runtime_telemetry_is_noop());
    assert_eq!(
        context
            .observe_query_request("Probe", async { Ok::<_, ()>(42) })
            .await,
        Ok(42)
    );
    let rows = context
        .observe_generated_list("Probe", async {
            Ok::<_, ()>(teaql_core::SmartList::from(vec![7]))
        })
        .await
        .unwrap();
    assert_eq!(rows.data, vec![7]);
    assert_eq!(
        context
            .observe_mutation_request(|| panic!("No SDK metadata thunk"), async { Ok::<_, ()>(9) })
            .await,
        Ok(9)
    );
    let original: Arc<str> = Arc::from("private-executor-value");
    let returned = context
        .observe_query_request("Probe", async { Err::<(), _>(original.clone()) })
        .await
        .unwrap_err();
    assert!(Arc::ptr_eq(&returned, &original));
    let rows = records(log.path.as_ref().unwrap());
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter()
            .map(|row| row["level"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["INFO", "INFO", "INFO", "INFO", "ERROR"]
    );
    assert!(
        !fs::read_to_string(log.path.as_ref().unwrap())
            .unwrap()
            .contains("private-executor-value")
    );
}

#[tokio::test]
async fn native_rule_categories_are_local_and_sdk_errors_remain_unchanged() {
    struct Adapter(Arc<Mutex<Vec<String>>>);
    struct Scope(Arc<Mutex<Vec<String>>>);
    impl crate::RuntimeTelemetry for Adapter {
        fn start(&self, _: crate::RuntimeOperation) -> Box<dyn crate::RuntimeTelemetryScope> {
            Box::new(Scope(self.0.clone()))
        }
    }
    impl crate::RuntimeTelemetryScope for Scope {
        fn correlation(
            &self,
        ) -> Option<teaql_data_service::statement_telemetry::SqlExecutionCorrelation> {
            Some(
                teaql_data_service::statement_telemetry::SqlExecutionCorrelation {
                    trace_id: "0123456789abcdef0123456789abcdef".into(),
                    span_id: "0123456789abcdef".into(),
                },
            )
        }
        fn success(&mut self, _: BTreeMap<String, crate::RuntimeAttributeValue>) {}
        fn failure(&mut self, kind: &str) {
            self.0.lock().unwrap().push(kind.into());
        }
    }
    for sdk in [false, true] {
        let directory = TestDirectory::new();
        let log = Arc::new(logger(&directory.0, &[]));
        let sdk_errors = Arc::new(Mutex::new(Vec::new()));
        let adapter: Arc<dyn crate::RuntimeTelemetry> = if sdk {
            Arc::new(Adapter(sdk_errors.clone()))
        } else {
            Arc::new(crate::NoopRuntimeTelemetry)
        };
        let context =
            UserContext::new_with_logger(log.clone()).with_runtime_telemetry(adapter.clone());
        assert!(Arc::ptr_eq(context.runtime_telemetry(), &adapter));
        for error in [
            crate::RuntimeError::Check(vec![]),
            crate::RuntimeError::Policy("private-policy-value".into()),
            crate::RuntimeError::Transaction("private-commit-value".into()),
        ] {
            let returned = context
                .observe_runtime_mutation_request(
                    || {
                        assert!(sdk, "No SDK metadata thunk");
                        "Probe".into()
                    },
                    async { Err::<(), _>(error) },
                )
                .await
                .unwrap_err();
            assert!(matches!(
                returned,
                crate::RuntimeError::Check(_)
                    | crate::RuntimeError::Policy(_)
                    | crate::RuntimeError::Transaction(_)
            ));
        }
        let returned = context
            .observe_data_service_request("Probe", async {
                Err::<(), _>(crate::DataServiceError::<Arc<str>>::Runtime(
                    crate::RuntimeError::Check(vec![]),
                ))
            })
            .await
            .unwrap_err();
        assert!(matches!(
            returned,
            crate::DataServiceError::Runtime(crate::RuntimeError::Check(_))
        ));
        let original: Arc<str> = Arc::from("private-executor-value");
        let returned = context
            .observe_data_service_request("Probe", async {
                Err::<(), _>(crate::DataServiceError::Executor(original.clone()))
            })
            .await
            .unwrap_err();
        if let crate::DataServiceError::Executor(returned) = returned {
            assert!(Arc::ptr_eq(&returned, &original));
        } else {
            panic!("Changed executor error");
        }
        let rows = records(log.path.as_ref().unwrap());
        assert_eq!(
            rows.iter()
                .map(|row| row["level"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["INFO", "WARN", "WARN", "ERROR", "WARN", "ERROR"]
        );
        for row in rows.iter().skip(1) {
            if sdk {
                assert_eq!(row["trace_id"], "0123456789abcdef0123456789abcdef");
            } else {
                assert!(row.get("trace_id").is_none());
            }
        }
        let text = fs::read_to_string(log.path.as_ref().unwrap()).unwrap();
        for canary in [
            "private-policy-value",
            "private-commit-value",
            "private-executor-value",
        ] {
            assert!(!text.contains(canary));
        }
        let errors = sdk_errors.lock().unwrap();
        if sdk {
            assert_eq!(
                *errors,
                [
                    "mutation_request_error",
                    "mutation_request_error",
                    "mutation_request_error",
                    "query_error",
                    "query_error"
                ]
            );
        } else {
            assert!(errors.is_empty());
        }
    }
}

#[tokio::test]
async fn no_sdk_generated_owner_and_failed_local_output_keep_execution_results() {
    let directory = TestDirectory::new();
    let log = Arc::new(logger(&directory.0, &[]));
    let context = UserContext::new_with_logger(log.clone());
    context
        .observe_generated_list("Probe", async {
            context
                .observe_query_request("ProbeCount", async { Ok::<_, ()>(7) })
                .await?;
            Ok::<_, ()>(teaql_core::SmartList::<i32>::from(vec![]))
        })
        .await
        .unwrap();
    let rows = records(log.path.as_ref().unwrap());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["level"], "INFO");
    let path = log.path.as_ref().unwrap();
    fs::remove_file(path).unwrap();
    fs::create_dir(path).unwrap();
    assert_eq!(
        context
            .observe_query_request("Probe", async { Ok::<_, ()>(42) })
            .await,
        Ok(42)
    );
    let original: Arc<str> = Arc::from("private-original-error");
    let returned = context
        .observe_query_request("Probe", async { Err::<(), _>(original.clone()) })
        .await
        .unwrap_err();
    assert!(Arc::ptr_eq(&returned, &original));
}

#[test]
fn sql_file_keeps_available_trace_correlation() {
    let directory = TestDirectory::new();
    let log = logger(&directory.0, &[]);
    let now = SystemTime::now();
    let entry = crate::SqlLogEntry {
        log_context: teaql_data_service::SqlLogContext {
            telemetry: Some(Arc::new(teaql_data_service::SqlTelemetryState {
                correlation: Some(
                    teaql_data_service::statement_telemetry::SqlExecutionCorrelation {
                        trace_id: "0123456789abcdef0123456789abcdef".into(),
                        span_id: "0123456789abcdef".into(),
                    },
                ),
                ..Default::default()
            })),
            ..Default::default()
        },
        operation: crate::SqlLogOperation::Select,
        comment: None,
        purpose: None,
        audit_reason: None,
        trace_path: vec![],
        trace_chain: vec![],
        sql: "SELECT id FROM customer".into(),
        params: vec![],
        debug_sql: String::new(),
        pretty_sql: String::new(),
        started_at: now,
        ended_at: now,
        elapsed: Duration::ZERO,
        result_count: Some(1),
        result_type: None,
        affected_rows: None,
        result_summary: "1 row".into(),
    };
    log.sql(&entry);
    let rows = records(log.path.as_ref().unwrap());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["trace_id"], "0123456789abcdef0123456789abcdef");
}
