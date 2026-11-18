use super::*;

fn scalar(value: &str) -> String {
    serde_json::from_str(&format!("\"{}\"", value.replace('"', "\\\""))).unwrap()
}

fn parse(bytes: &[u8], format: &str) -> Vec<Value> {
    assert_eq!(bytes.last(), Some(&b'\n'));
    let text = std::str::from_utf8(bytes).unwrap();
    if format == "json" {
        return text
            .split_terminator('\n')
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    }
    let mut rows: Vec<Value> = Vec::new();
    let mut sql: Option<Vec<String>> = None;
    for line in text.split_terminator('\n') {
        assert!(
            !line
                .chars()
                .any(|c| c.is_control() || matches!(c, '\u{2028}' | '\u{2029}'))
        );
        if !line.starts_with("  ") {
            let (timestamp, header) = line.split_once(' ').unwrap();
            let date = DateTime::parse_from_rfc3339(timestamp).unwrap();
            assert_eq!(date.offset().local_minus_utc(), 0);
            let (prefix, event) = header.split_once("] ").unwrap();
            let (name, level) = prefix
                .strip_prefix("[TeaQL][")
                .unwrap()
                .rsplit_once("][")
                .unwrap();
            assert!(matches!(level, "INFO" | "WARN" | "ERROR"));
            let (event, runtime) = event.rsplit_once(" runtime=").unwrap();
            rows.push(json!({"timestamp":timestamp,"level":level,"log_name":scalar(name),"event":scalar(event),"runtime":runtime}));
            sql = None;
        } else {
            let row = rows.last_mut().expect("continuation without header");
            if row.get("message").is_none() {
                row["message"] = json!(scalar(&line[2..]));
            } else if line == "  sql:" {
                sql = Some(Vec::new());
                row["sql"] = json!("");
            } else if line.starts_with("    ") && sql.is_some() {
                let lines = sql.as_mut().unwrap();
                lines.push(scalar(&line[4..]));
                row["sql"] = json!(lines.join("\n"));
            } else {
                let (key, value) = line[2..].split_once('=').unwrap();
                row[scalar(key)] = serde_json::from_str(value).unwrap();
                sql = None;
            }
        }
    }
    for row in &rows {
        for key in EVENT_FIELDS {
            assert!(row[key].is_string(), "missing field {key}");
        }
    }
    rows
}
fn read(path: &Path, format: &str) -> Vec<Value> {
    parse(&fs::read(path).unwrap(), format)
}

#[test]
fn positive_retention_keeps_numeric_newest_order_and_whole_blocks() {
    for format in ["json", "human"] {
        let dir = TestDirectory::new();
        let log = logger(
            &dir.0,
            &[
                ("TEAQL_LOG_FORMAT", format),
                ("TEAQL_LOG_MAX_SIZE", "256"),
                ("TEAQL_LOG_MAX_FILES", "2"),
            ],
        );
        for sequence in 0..5 {
            assert!(log.write(
                "INFO",
                "retention",
                &"界".repeat(200),
                json!({"sequence":sequence,"sql":format!("SELECT\n{sequence}")})
            ));
        }
        let path = log.path.as_ref().unwrap();
        for index in 0..3 {
            let file = if index == 0 {
                path.clone()
            } else {
                archive_path(path, index)
            };
            let rows = read(&file, format);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["sequence"], 4 - index);
            assert_eq!(rows[0]["sql"], format!("SELECT\n{}", 4 - index));
        }
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 3);
        assert!(!archive_path(path, 3).exists());
    }
}

#[test]
fn presentation_stream_worker() {
    let Ok(format) = std::env::var("TEAQL_PRESENTATION_STREAM_FORMAT") else {
        return;
    };
    let endpoint = std::env::var("TEAQL_PRESENTATION_STREAM_ENDPOINT").unwrap();
    io::stdout()
        .lock()
        .write_all(b"TEAQL_STREAM_BEGIN\n")
        .unwrap();
    let log = RuntimeLogger::new(RuntimeLoggingOptions {
        log_format: Some(format),
        environment: Some(BTreeMap::from([("TEAQL_LOG_ENDPOINT".into(), endpoint)])),
        ..Default::default()
    })
    .unwrap();
    assert!(log.write(
        "WARN",
        "sql.retry",
        "Retry scheduled",
        json!({"category":"SQL","sql":"SELECT\nprobe"})
    ));
    io::stdout()
        .lock()
        .write_all(b"TEAQL_STREAM_END\n")
        .unwrap();
}

#[test]
fn both_console_streams_use_the_selected_presentation() {
    for format in ["json", "human"] {
        for endpoint in ["stdout", "stderr"] {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime_logging::tests::presentations::presentation_stream_worker",
                    "--nocapture",
                ])
                .env_clear()
                .env("TEAQL_PRESENTATION_STREAM_FORMAT", format)
                .env("TEAQL_PRESENTATION_STREAM_ENDPOINT", endpoint)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let rows = if endpoint == "stderr" {
                parse(&output.stderr, format)
            } else {
                let stdout = std::str::from_utf8(&output.stdout).unwrap();
                let body = stdout
                    .split_once("TEAQL_STREAM_BEGIN\n")
                    .unwrap()
                    .1
                    .split_once("TEAQL_STREAM_END\n")
                    .unwrap()
                    .0;
                parse(body.as_bytes(), format)
            };
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[1]["level"], "WARN");
            assert_eq!(rows[1]["sql"], "SELECT\nprobe");
        }
    }
}
fn all(directory: &Path, format: &str) -> Vec<Value> {
    fs::read_dir(directory)
        .unwrap()
        .flat_map(|entry| read(&entry.unwrap().path(), format))
        .collect()
}

#[test]
fn selection_precedence_freezing_default_paths_override_and_two_starts() {
    let default = TestDirectory::new();
    assert_eq!(logger(&default.0, &[]).log_format, "json");
    for format in ["json", "human"] {
        for (explicit, domain, models, name) in [
            (Some("application"), "domain", vec!["model"], "application"),
            (None, "domain", vec!["model"], "domain"),
            (None, "", vec!["model"], "model"),
            (None, "", vec![], "teaql"),
            (None, "", vec!["one", "two"], "teaql"),
        ] {
            let dir = TestDirectory::new();
            let env = BTreeMap::from([
                ("TEAQL_DOMAIN".into(), domain.into()),
                ("TEAQL_LOG_FORMAT".into(), format.into()),
            ]);
            let log = Arc::new(
                RuntimeLogger::new(RuntimeLoggingOptions {
                    log_name: explicit.map(str::to_owned),
                    model_names: models.into_iter().map(str::to_owned).collect(),
                    environment: Some(env),
                    working_directory: Some(dir.0.clone()),
                    ..Default::default()
                })
                .unwrap(),
            );
            assert_eq!(log.log_name, name);
            assert_eq!(log.log_format, format);
            assert_eq!(
                log.path.as_ref().unwrap(),
                &dir.0.join(format!("logs/{name}.log"))
            );
            let context = UserContext::new_with_logger(log.clone());
            context
                .start_runtime_operation(RuntimeOperation::new("query", "probe"))
                .success(BTreeMap::new());
            let _second = RuntimeLogger::new(RuntimeLoggingOptions {
                log_name: Some(name.into()),
                log_format: Some(format.into()),
                environment: Some(BTreeMap::from([(
                    "TEAQL_LOG_FORMAT".into(),
                    "invalid-unused".into(),
                )])),
                working_directory: Some(dir.0.clone()),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(read(log.path.as_ref().unwrap(), format).len(), 3);
            let override_log = logger(
                &dir.0,
                &[
                    ("TEAQL_LOG_FORMAT", format),
                    ("TEAQL_LOG_ENDPOINT", "override/custom.log"),
                ],
            );
            assert_eq!(
                override_log.path.unwrap(),
                dir.0.join("override/custom.log")
            );
        }
    }
    for value in ["", "debug", "JSON", "human\nFORMAT-CANARY"] {
        for app in [false, true] {
            let dir = TestDirectory::new();
            let result = RuntimeLogger::new(RuntimeLoggingOptions {
                log_format: app.then(|| value.into()),
                working_directory: Some(dir.0.clone()),
                environment: Some(if app {
                    BTreeMap::new()
                } else {
                    BTreeMap::from([("TEAQL_LOG_FORMAT".into(), value.into())])
                }),
                ..Default::default()
            });
            assert!(result.is_err());
            assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 0);
        }
    }
}

#[test]
fn identical_projected_event_types_unknown_event_sql_and_control_escaping() {
    for format in ["json", "human"] {
        let dir = TestDirectory::new();
        let log = logger(&dir.0, &[("TEAQL_LOG_FORMAT", format)]);
        let record=log.record("WARN","unknown.kind","Readable \\u001b\nFORGED\u{1b}\r\u{85}\u{2029}",json!({
            "category":"SQL","trace_id":"0123456789abcdef0123456789abcdef","outcome":"retrying","error_code":"connection_timeout",
            "entity_id":9007199254740993_u64,"result_count":0,"elapsed_us":12,"enabled":true,"omission_reason":null,
            "changes":[{"field":"password","value":"***"}],"sql":"SELECT id\nFROM customer\n\u{1b}\u{85}\u{2028}","trace_chain":["root","child"]}));
        log.emit(record.clone()).unwrap();
        assert_eq!(read(log.path.as_ref().unwrap(), format)[1], record);
    }
}

#[tokio::test]
async fn native_execution_preserves_values_error_identity_and_levels() {
    for format in ["json", "human"] {
        let dir = TestDirectory::new();
        let log = Arc::new(logger(&dir.0, &[("TEAQL_LOG_FORMAT", format)]));
        let context = UserContext::new_with_logger(log.clone());
        assert_eq!(
            context
                .observe_query_request("Probe", async { Ok::<_, ()>(42) })
                .await,
            Ok(42)
        );
        let original: Arc<str> = Arc::from("PRIVATE-EXECUTOR-CANARY");
        let returned = context
            .observe_query_request("Probe", async { Err::<(), _>(original.clone()) })
            .await
            .unwrap_err();
        assert!(Arc::ptr_eq(&original, &returned));
        context
            .start_runtime_operation(RuntimeOperation::new("query", "probe"))
            .failure("ValidationError");
        let rows = read(log.path.as_ref().unwrap(), format);
        assert_eq!(
            rows.iter()
                .map(|r| r["level"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["INFO", "INFO", "ERROR", "WARN"]
        );
        assert!(
            !fs::read_to_string(log.path.as_ref().unwrap())
                .unwrap()
                .contains("CANARY")
        );
    }
}

#[test]
fn native_legacy_sql_audit_privacy_and_exact_ack_stay_separate_from_format() {
    for format in ["json", "human"] {
        for ack in [
            "",
            "true",
            "I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK",
        ] {
            let dir = TestDirectory::new();
            let path = dir.0.join("legacy.log");
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
                .env("TEAQL_LOG_FORMAT", format)
                .env("TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS", ack)
                .env("TMPDIR", &dir.0)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let rows = read(&path, format);
            assert!(rows.iter().any(|r| r["category"] == "SQL"));
            assert!(rows.iter().any(|r| r["category"] == "AUDIT"));
            let text = fs::read_to_string(path).unwrap();
            for secret in ["PRIVATE-CUSTOMER", "TOKEN-CANARY", "RETAINED-WRITE-CANARY"] {
                assert!(!text.contains(secret));
            }
        }
    }
}

#[test]
fn utf8_multiline_size_day_restart_archives_zero_and_oversized_events() {
    for format in ["json", "human"] {
        for override_path in [false, true] {
            let dir = TestDirectory::new();
            let mut env = vec![
                ("TEAQL_LOG_FORMAT", format),
                ("TEAQL_LOG_MAX_SIZE", "1K"),
                ("TEAQL_LOG_MAX_FILES", "100"),
            ];
            if override_path {
                env.push(("TEAQL_LOG_ENDPOINT", "override/custom.log"));
            }
            let log = logger(&dir.0, &env);
            let path = log.path.as_ref().unwrap();
            for index in 0..40 {
                assert!(log.write(
                    "INFO",
                    "regular",
                    &"界".repeat(25),
                    json!({"index":index,"sql":"SELECT id\nFROM customer"})
                ));
            }
            let rows = all(path.parent().unwrap(), format);
            assert_eq!(rows.len(), 41);
            let ids = rows
                .iter()
                .filter_map(|r| r["index"].as_u64())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(ids, (0..40).collect());
            for file in fs::read_dir(path.parent().unwrap()).unwrap() {
                let size = file.unwrap().metadata().unwrap().len();
                assert!(size > 0 && size <= 1024);
            }
            assert!(log.write(
                "WARN",
                "oversize",
                "whole",
                json!({"sql":"界".repeat(2000)+"\nLAST"})
            ));
            let before = fs::read(path).unwrap();
            assert!(before.len() > 1024);
            assert_eq!(parse(&before, format).len(), 1);
            assert!(log.write("INFO", "after", "next", json!({})));
            assert_eq!(fs::read(archive_path(path, 1)).unwrap(), before);
            for restart in [false, true] {
                File::options()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_modified(SystemTime::now() - Duration::from_secs(172800))
                    .unwrap();
                let before = fs::read(path).unwrap();
                if restart {
                    let _ = logger(&dir.0, &env);
                } else {
                    assert!(log.write("INFO", "day", "UTC rotation", json!({})));
                }
                assert_eq!(fs::read(archive_path(path, 1)).unwrap(), before);
                assert_eq!(read(path, format).len(), 1);
            }
            env[2] = ("TEAQL_LOG_MAX_FILES", "0");
            let zero = logger(&dir.0, &env);
            assert!(zero.write("INFO", "last", &"界".repeat(2000), json!({})));
            assert!(RuntimeLogger::archives(path).unwrap().is_empty());
            assert_eq!(read(path, format).len(), 1);
        }
    }
}

fn assert_blocks(directory: &Path, format: &str, count: usize, starts: usize) {
    let rows = all(&directory.join("logs"), format);
    assert_eq!(rows.len(), count + starts);
    let ids = rows
        .iter()
        .filter_map(|r| r["id"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), count);
    for row in &rows {
        if let Some(id) = row["id"].as_str() {
            assert_eq!(row["sql"], format!("SELECT\n{id}"));
        }
    }
}
#[test]
fn concurrent_threads_keep_sql_continuations_with_their_header() {
    for format in ["json", "human"] {
        let dir = TestDirectory::new();
        let mut children = Vec::new();
        for worker in 0..4 {
            let log = logger(
                &dir.0,
                &[
                    ("TEAQL_LOG_FORMAT", format),
                    ("TEAQL_LOG_MAX_SIZE", "2K"),
                    ("TEAQL_LOG_MAX_FILES", "100"),
                ],
            );
            children.push(std::thread::spawn(move || {
                for i in 0..30 {
                    let id = format!("{worker}/{i}");
                    assert!(log.write(
                        "INFO",
                        "parallel",
                        "多线程",
                        json!({"id":id,"sql":format!("SELECT\n{id}")})
                    ));
                }
            }));
        }
        for child in children {
            child.join().unwrap();
        }
        assert_blocks(&dir.0, format, 120, 4);
    }
}
#[test]
fn presentation_process_worker() {
    let Ok(dir) = std::env::var("TEAQL_PRESENTATION_DIRECTORY") else {
        return;
    };
    let format = std::env::var("TEAQL_PRESENTATION_FORMAT").unwrap();
    let log = logger(
        Path::new(&dir),
        &[
            ("TEAQL_LOG_FORMAT", &format),
            ("TEAQL_LOG_MAX_SIZE", "2K"),
            ("TEAQL_LOG_MAX_FILES", "100"),
        ],
    );
    let worker = std::env::var("TEAQL_PRESENTATION_WORKER").unwrap();
    for i in 0..30 {
        let id = format!("{worker}/{i}");
        assert!(log.write(
            "INFO",
            "parallel",
            "原生并发",
            json!({"id":id,"sql":format!("SELECT\n{id}")})
        ));
    }
}
#[test]
fn native_processes_and_following_start_preserve_complete_blocks() {
    for format in ["json", "human"] {
        let dir = TestDirectory::new();
        let mut children = Children(Vec::new());
        for worker in 0..4 {
            children.0.push(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "runtime_logging::tests::presentations::presentation_process_worker",
                        "--nocapture",
                    ])
                    .env_clear()
                    .env("TEAQL_PRESENTATION_DIRECTORY", &dir.0)
                    .env("TEAQL_PRESENTATION_FORMAT", format)
                    .env("TEAQL_PRESENTATION_WORKER", worker.to_string())
                    .spawn()
                    .unwrap(),
            );
        }
        for child in &mut children.0 {
            assert!(child.wait().unwrap().success());
        }
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime_logging::tests::presentations::presentation_process_worker",
                "--nocapture",
            ])
            .env_clear()
            .env("TEAQL_PRESENTATION_DIRECTORY", &dir.0)
            .env("TEAQL_PRESENTATION_FORMAT", format)
            .env("TEAQL_PRESENTATION_WORKER", "following")
            .status()
            .unwrap();
        assert!(status.success());
        assert_blocks(&dir.0, format, 150, 5);
    }
}

#[tokio::test]
async fn mixed_format_rejection_precedes_rotation_and_preserves_business_result() {
    for format in ["json", "human"] {
        let dir = TestDirectory::new();
        let log = Arc::new(logger(&dir.0, &[("TEAQL_LOG_FORMAT", format)]));
        let path = log.path.as_ref().unwrap();
        let other = if format == "json" { "human" } else { "json" };
        let archive = archive_path(path, 10);
        fs::write(&archive, "preserve archive bytes").unwrap();
        let before = fs::read(path).unwrap();
        assert!(
            RuntimeLogger::new(RuntimeLoggingOptions {
                environment: Some(BTreeMap::from([
                    ("TEAQL_LOG_FORMAT".into(), other.into()),
                    ("TEAQL_LOG_MAX_SIZE".into(), "1".into()),
                    ("TEAQL_LOG_MAX_FILES".into(), "0".into())
                ])),
                working_directory: Some(dir.0.clone()),
                ..Default::default()
            })
            .is_err()
        );
        assert_eq!(fs::read(path).unwrap(), before);
        assert_eq!(
            fs::read_to_string(&archive).unwrap(),
            "preserve archive bytes"
        );
        let replacement_dir = TestDirectory::new();
        let replacement = logger(&replacement_dir.0, &[("TEAQL_LOG_FORMAT", other)]);
        let before = fs::read(replacement.path.as_ref().unwrap()).unwrap();
        fs::write(path, &before).unwrap();
        let context = UserContext::new_with_logger(log.clone());
        assert_eq!(
            context
                .observe_query_request("Probe", async { Ok::<_, ()>(42) })
                .await,
            Ok(42)
        );
        let original: Arc<str> = Arc::from("SECRET-BUSINESS-ERROR");
        let returned = context
            .observe_query_request("Probe", async { Err::<(), _>(original.clone()) })
            .await
            .unwrap_err();
        assert!(Arc::ptr_eq(&returned, &original));
        assert_eq!(fs::read(path).unwrap(), before);
        assert_eq!(
            fs::read_to_string(&archive).unwrap(),
            "preserve archive bytes"
        );
        fs::remove_file(path).unwrap();
        fs::create_dir(path).unwrap();
        assert!(!log.write("ERROR", "later", "I/O failure", json!({})));
        assert!(
            RuntimeLogger::new(RuntimeLoggingOptions {
                log_format: Some(format.into()),
                working_directory: Some(dir.0.clone()),
                environment: Some(BTreeMap::new()),
                ..Default::default()
            })
            .is_err()
        );
    }
}
