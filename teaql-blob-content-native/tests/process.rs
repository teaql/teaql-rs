use futures_util::stream;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use teaql_blob_content_native::*;
use teaql_blob_reference::*;
use tempfile::TempDir;

fn temp() -> TempDir {
    tempfile::Builder::new()
        .prefix("teaql-r5-worker-")
        .tempdir()
        .unwrap()
}
fn fixture(extension: &str) -> Vec<u8> {
    fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/fixtures/files/sample.{extension}")),
    )
    .unwrap()
}
fn content(bytes: Vec<u8>) -> BlobContentStream {
    Box::pin(stream::iter(
        bytes
            .chunks(37)
            .map(|p| Ok(Bytes::copy_from_slice(p)))
            .collect::<Vec<_>>(),
    ))
}
fn hash(path: &Path) -> String {
    let mut file = File::open(path).unwrap();
    let mut digest = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    format!("{:x}", digest.finalize())
}
fn executable(path: &Path) -> WorkerExecutable {
    WorkerExecutable::new(path, hash(path)).unwrap()
}
fn production() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_teaql-blob-content-worker"))
}
async fn configured(
    path: &Path,
    limits: VerificationLimits,
    resources: WorkerResourceLimits,
) -> (TempDir, ProcessBlobContentVerifier) {
    let root = temp();
    let installed = root.path().join("parser-worker");
    fs::copy(path, &installed).unwrap();
    fs::set_permissions(&installed, fs::Permissions::from_mode(0o500)).unwrap();
    let verifier = ProcessBlobContentVerifier::new(
        root.path().join("scratch"),
        limits,
        executable(&installed),
        resources,
    )
    .unwrap();
    assert!(!root.path().join("scratch").exists());
    assert_eq!(verifier.stats().started, 0);
    assert!(verifier.validate_ready().await.is_err());
    assert!(!root.path().join("scratch").exists());
    verifier.initialize().await.unwrap();
    (root, verifier)
}
#[track_caller]
fn code<T>(result: BlobResult<T>, expected: BlobErrorCode) {
    match result {
        Err(e) => assert_eq!(e.code, expected),
        Ok(_) => panic!("unexpected acceptance"),
    }
}
fn reaped(verifier: &ProcessBlobContentVerifier) {
    let stats = verifier.stats();
    assert!(stats.running.is_empty(), "owned worker still running");
    assert_eq!(stats.started, stats.reaped);
}

#[tokio::test]
async fn production_worker_parses_ten_formats_and_seven_shared_cases() {
    let (_root, verifier) = configured(
        &production(),
        VerificationLimits::default(),
        WorkerResourceLimits::default(),
    )
    .await;
    for extension in [
        "jpg", "jpeg", "png", "mp3", "mp4", "doc", "docx", "xls", "xlsx", "pdf",
    ] {
        let bytes = fixture(extension);
        let size = bytes.len() as u64;
        let sha = format!("{:x}", Sha256::digest(&bytes));
        let result = verifier
            .verify(content(bytes), extension, size, false)
            .await
            .unwrap();
        assert_eq!(result.size_bytes(), size);
        assert_eq!(result.sha256(), sha);
        assert_eq!(
            result.content_type(),
            teaql_core::blob::content_type(extension).unwrap()
        );
    }
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let cases: serde_json::Value =
        serde_json::from_slice(&fs::read(fixtures.join("upload-cases.json")).unwrap()).unwrap();
    assert_eq!(cases["cases"].as_array().unwrap().len(), 7);
    for case in cases["cases"].as_array().unwrap() {
        let extension = case["fileName"]
            .as_str()
            .unwrap()
            .rsplit('.')
            .next()
            .unwrap();
        let bytes = fs::read(fixtures.join(case["path"].as_str().unwrap())).unwrap();
        let result = verifier
            .verify(
                content(bytes),
                extension,
                case["maxSizeBytes"].as_u64().unwrap(),
                false,
            )
            .await;
        if case.get("expected").is_some() {
            assert!(result.is_ok(), "{}", case["id"]);
        } else {
            assert_eq!(
                result.unwrap_err().code.code(),
                case["error"].as_str().unwrap()
            );
        }
    }
    reaped(&verifier);
}

#[tokio::test]
async fn process_limits_and_stream_failures_preserve_closed_facts() {
    let limits = VerificationLimits {
        maximum_text_characters: 1,
        ..VerificationLimits::default()
    };
    let (_root, verifier) =
        configured(&production(), limits, WorkerResourceLimits::default()).await;
    code(
        verifier
            .verify(content(fixture("pdf")), "pdf", 1000000, false)
            .await,
        BlobErrorCode::SizeExceeded,
    );
    code(
        verifier
            .verify(content(fixture("docx")), "docx", 1000000, false)
            .await,
        BlobErrorCode::SizeExceeded,
    );
    let started = verifier.stats().started;
    code(
        verifier
            .verify(content(fixture("pdf")), "pdf", 1, false)
            .await,
        BlobErrorCode::SizeExceeded,
    );
    code(
        verifier.verify(content(vec![]), "pdf", 1000, false).await,
        BlobErrorCode::SizeExceeded,
    );
    code(
        verifier
            .verify(content(fixture("pdf")), "PDF", 1000000, false)
            .await,
        BlobErrorCode::FormatNotAllowed,
    );
    let failed: BlobContentStream =
        Box::pin(stream::iter(vec![Err(BlobErrorCode::StoreFailed.into())]));
    code(
        verifier.verify(failed, "pdf", 1000000, false).await,
        BlobErrorCode::StoreFailed,
    );
    assert_eq!(verifier.stats().started, started);
    reaped(&verifier);
}

#[tokio::test]
async fn pinned_artifact_readiness_rejects_missing_mismatched_and_symlink_paths() {
    let root = temp();
    let install = root.path().join("worker");
    fs::copy(production(), &install).unwrap();
    fs::set_permissions(&install, fs::Permissions::from_mode(0o500)).unwrap();
    let pin = executable(&install);
    assert_eq!(format!("{pin:?}"), "WorkerExecutable([redacted])");
    let verifier = ProcessBlobContentVerifier::new(
        root.path().join("scratch"),
        VerificationLimits::default(),
        pin,
        WorkerResourceLimits::default(),
    )
    .unwrap();
    verifier.initialize().await.unwrap();
    // Same deployment path replaced after readiness: every invocation reopens and hashes.
    let replaced = root.path().join("replaced");
    fs::rename(&install, &replaced).unwrap();
    fs::write(&install, b"not an ELF artifact").unwrap();
    fs::set_permissions(&install, fs::Permissions::from_mode(0o500)).unwrap();
    code(
        verifier.validate_ready().await,
        BlobErrorCode::CapabilityMissing,
    );
    code(
        verifier
            .verify(content(fixture("pdf")), "pdf", 1000000, false)
            .await,
        BlobErrorCode::CapabilityMissing,
    );
    fs::remove_file(&install).unwrap();
    code(
        verifier.validate_ready().await,
        BlobErrorCode::CapabilityMissing,
    );
    symlink(&replaced, &install).unwrap();
    code(
        verifier.validate_ready().await,
        BlobErrorCode::CapabilityMissing,
    );
    fs::remove_file(&install).unwrap();
    fs::rename(&replaced, &install).unwrap();
    verifier.validate_ready().await.unwrap();
    let wrong = ProcessBlobContentVerifier::new(
        root.path().join("scratch"),
        VerificationLimits::default(),
        WorkerExecutable::new(&install, "0".repeat(64)).unwrap(),
        WorkerResourceLimits::default(),
    )
    .unwrap();
    code(wrong.initialize().await, BlobErrorCode::CapabilityMissing);
    assert_eq!(wrong.stats().started, 0);
    let linked = root.path().join("linked");
    symlink(root.path(), &linked).unwrap();
    let through_link = ProcessBlobContentVerifier::new(
        root.path().join("scratch"),
        VerificationLimits::default(),
        WorkerExecutable::new(linked.join("worker"), hash(&install)).unwrap(),
        WorkerResourceLimits::default(),
    )
    .unwrap();
    code(
        through_link.initialize().await,
        BlobErrorCode::CapabilityMissing,
    );
    assert_eq!(through_link.stats().started, 0);
    for mode in [0o520, 0o700, 0o4500] {
        fs::set_permissions(&install, fs::Permissions::from_mode(mode)).unwrap();
        code(
            verifier.validate_ready().await,
            BlobErrorCode::CapabilityMissing,
        );
    }
    reaped(&verifier);
    assert_eq!(verifier.stats().started, 2);
}

#[test]
fn invalid_config_is_passive_and_direct_worker_has_no_uncontained_mode() {
    assert!(WorkerExecutable::new("relative", "a".repeat(64)).is_err());
    assert!(WorkerExecutable::new("/a/../b", "a".repeat(64)).is_err());
    assert!(WorkerExecutable::new("/missing", "A".repeat(64)).is_err());
    let root = temp();
    let resources = WorkerResourceLimits {
        cpu_seconds: 0,
        ..WorkerResourceLimits::default()
    };
    code(
        ProcessBlobContentVerifier::new(
            root.path().join("scratch"),
            VerificationLimits::default(),
            executable(&production()),
            resources,
        ),
        BlobErrorCode::PolicyInvalid,
    );
    assert!(!root.path().join("scratch").exists());
    for args in [
        vec!["--probe-v1"],
        vec!["--fault"],
        vec!["--verify-v1", "pdf", "{}"],
    ] {
        let output = Command::new(production())
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stderr.is_empty());
        assert!(output.stdout.len() <= 4096);
        let reply: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(reply["body"]["kind"], "Failure");
    }
}

#[cfg(feature = "process-fixtures")]
mod faults {
    use super::*;
    use std::{
        os::unix::process::ExitStatusExt,
        process::Child,
        time::{Duration, Instant},
    };
    fn fault_worker() -> PathBuf {
        PathBuf::from(env!("CARGO_BIN_EXE_teaql-blob-content-test-worker"))
    }
    async fn setup(resources: WorkerResourceLimits) -> (TempDir, ProcessBlobContentVerifier) {
        configured(
            &fault_worker(),
            VerificationLimits {
                concurrent_verifications: 1,
                ..VerificationLimits::default()
            },
            resources,
        )
        .await
    }
    async fn active(verifier: &ProcessBlobContentVerifier) -> u32 {
        active_within(verifier, Duration::from_secs(10)).await
    }
    async fn active_within(verifier: &ProcessBlobContentVerifier, timeout: Duration) -> u32 {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(pid) = verifier.stats().running.first() {
                return *pid;
            }
            assert!(Instant::now() < deadline, "worker never started");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    async fn drained(verifier: &ProcessBlobContentVerifier) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !verifier.stats().running.is_empty() {
            assert!(Instant::now() < deadline, "worker not reaped");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        reaped(verifier);
    }
    #[tokio::test]
    async fn wall_timeout_kills_and_reaps_then_restores_capacity() {
        let resources = WorkerResourceLimits {
            // Includes hashing the installed unoptimized ELF, while the other
            // real-process tests contend for CPU. The worker itself never exits.
            wall_time: Duration::from_secs(10),
            ..WorkerResourceLimits::default()
        };
        let (_root, verifier) = setup(resources).await;
        let clone = verifier.clone();
        let work = tokio::spawn(async move {
            clone
                .verify(content(b"H".to_vec()), "pdf", 1000, false)
                .await
        });
        let pid = active(&verifier).await;
        code(work.await.unwrap(), BlobErrorCode::FormatNotAllowed);
        assert_eq!(
            verifier.stats().last_outcome,
            Some(ProcessWorkerOutcome::TimedOut)
        );
        reaped(&verifier);
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        verifier
            .verify(content(fixture("pdf")), "pdf", 1000000, false)
            .await
            .unwrap();
        reaped(&verifier);
    }
    #[tokio::test]
    async fn cancellation_keeps_capacity_until_actual_kill_and_reap() {
        let (_root, verifier) = setup(WorkerResourceLimits::default()).await;
        let clone = verifier.clone();
        let work = tokio::spawn(async move {
            clone
                .verify(content(b"H".to_vec()), "pdf", 1000, false)
                .await
        });
        let pid = active(&verifier).await;
        code(
            verifier
                .verify(content(fixture("pdf")), "pdf", 1000000, false)
                .await,
            BlobErrorCode::VerificationBusy,
        );
        // Independent kernel oracle while the owned process is alive.
        let limits = fs::read_to_string(format!("/proc/{pid}/limits")).unwrap();
        for (name, expected) in [
            ("Max address space", "1073741824"),
            ("Max cpu time", "15"),
            ("Max core file size", "0"),
            ("Max file size", "0"),
            ("Max processes", "0"),
            ("Max open files", "32"),
            ("Max stack size", "8388608"),
        ] {
            let line = limits.lines().find(|l| l.starts_with(name)).unwrap();
            let fields: Vec<_> = line[name.len()..].split_whitespace().collect();
            assert_eq!(&fields[..2], &[expected, expected]);
        }
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        drained(&verifier).await;
        assert_eq!(
            verifier.stats().last_outcome,
            Some(ProcessWorkerOutcome::Cancelled)
        );
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        verifier
            .verify(content(fixture("pdf")), "pdf", 1000000, false)
            .await
            .unwrap();
        reaped(&verifier);
    }
    #[tokio::test]
    async fn kernel_cpu_and_address_space_limits_terminate_real_workers() {
        for (payload, resources, signal) in [
            (
                b'C',
                WorkerResourceLimits {
                    cpu_seconds: 1,
                    wall_time: Duration::from_secs(10),
                    ..WorkerResourceLimits::default()
                },
                libc::SIGKILL,
            ),
            (
                b'M',
                WorkerResourceLimits {
                    address_space_bytes: 67108864,
                    wall_time: Duration::from_secs(10),
                    ..WorkerResourceLimits::default()
                },
                libc::SIGABRT,
            ),
        ] {
            let (_root, verifier) = setup(resources).await;
            code(
                verifier
                    .verify(content(vec![payload]), "pdf", 1000, false)
                    .await,
                BlobErrorCode::FormatNotAllowed,
            );
            assert_eq!(
                verifier.stats().last_outcome,
                Some(ProcessWorkerOutcome::Exited {
                    code: None,
                    signal: Some(signal)
                })
            );
            reaped(&verifier);
            verifier
                .verify(content(fixture("pdf")), "pdf", 1000000, false)
                .await
                .unwrap();
            reaped(&verifier);
        }
    }
    #[tokio::test]
    async fn malformed_forged_version_duplicate_trailing_and_large_replies_fail_closed() {
        let (_root, verifier) = setup(WorkerResourceLimits::default()).await;
        for payload in *b"JBDTXVO" {
            let expected = if payload == b'V' {
                BlobErrorCode::CapabilityMissing
            } else {
                BlobErrorCode::FormatNotAllowed
            };
            code(
                verifier
                    .verify(content(vec![payload]), "pdf", 1000, false)
                    .await,
                expected,
            );
            if payload == b'O' {
                assert_eq!(
                    verifier.stats().last_outcome,
                    Some(ProcessWorkerOutcome::OutputLimit)
                );
            }
            reaped(&verifier);
        }
        verifier
            .verify(content(fixture("pdf")), "pdf", 1000000, false)
            .await
            .unwrap();
        reaped(&verifier);
    }
    #[test]
    fn environment_directory_and_unmarked_host_descriptors_are_not_inherited() {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        const ROLE: &str = "TEAQL_WORKER_FD_TEST_ROLE";
        if std::env::var(ROLE).as_deref() == Ok("controller") {
            // Isolate the intentionally non-CLOEXEC descriptor in this helper.
            let canary = tempfile::tempfile().unwrap();
            // SAFETY: F_DUPFD chooses an unused descriptor; it never overwrites
            // a harness-owned descriptor, and OwnedFd closes it on every exit.
            let fd = unsafe { libc::fcntl(canary.as_raw_fd(), libc::F_DUPFD, 100) };
            assert!(fd >= 100);
            let inherited = unsafe { OwnedFd::from_raw_fd(fd) };
            assert_eq!(
                unsafe { libc::fcntl(inherited.as_raw_fd(), libc::F_GETFD) },
                0
            );
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let (_root, verifier) = setup(WorkerResourceLimits::default()).await;
                    code(
                        verifier
                            .verify(content(b"E".to_vec()), "pdf", 1000, false)
                            .await,
                        BlobErrorCode::SizeExceeded,
                    );
                    assert_eq!(
                        verifier.stats().last_outcome,
                        Some(ProcessWorkerOutcome::Exited {
                            code: Some(2),
                            signal: None
                        })
                    );
                    reaped(&verifier);
                });
        } else {
            let mut child = ChildGuard(Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "faults::environment_directory_and_unmarked_host_descriptors_are_not_inherited", "--nocapture"])
                .env(ROLE, "controller").env("TEAQL_WORKER_TEST_CANARY", "must-never-reach-worker")
                .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap());
            assert!(wait(&mut child.0, Duration::from_secs(60)).success());
        }
    }

    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn wait(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "owned helper timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    // Each role runs in a separate owned test executable. The real test harness
    // never changes its subreaper setting or waits for an unrelated process.
    #[test]
    fn host_death_kernel_kills_worker_and_owned_subreaper_reaps_it() {
        const TEST: &str = "faults::host_death_kernel_kills_worker_and_owned_subreaper_reaps_it";
        const ROLE: &str = "TEAQL_WORKER_TEST_ROLE";
        match std::env::var(ROLE).ok().as_deref() {
            Some("controller") => {
                let root = PathBuf::from(std::env::var_os("TEAQL_WORKER_TEST_ROOT").unwrap());
                let installed = root.join("parser-worker");
                fs::copy(fault_worker(), &installed).unwrap();
                fs::set_permissions(&installed, fs::Permissions::from_mode(0o500)).unwrap();
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let verifier = ProcessBlobContentVerifier::new(
                            root.join("scratch"),
                            VerificationLimits::default(),
                            executable(&installed),
                            WorkerResourceLimits::default(),
                        )
                        .unwrap();
                        verifier.initialize().await.unwrap();
                        let cloned = verifier.clone();
                        let work = tokio::spawn(async move {
                            cloned
                                .verify(content(b"H".to_vec()), "pdf", 1000, false)
                                .await
                        });
                        let pid = active_within(&verifier, Duration::from_secs(60)).await;
                        fs::write(root.join("ready"), pid.to_string()).unwrap();
                        let _ = work.await;
                    });
                panic!("controller unexpectedly survived its owned kill");
            }
            Some("witness") => {
                // SAFETY: fixed process-local setting in this isolated helper only.
                assert_eq!(
                    unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) },
                    0
                );
                let root = temp();
                let mut controller = ChildGuard(
                    Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", TEST, "--nocapture"])
                        .env(ROLE, "controller")
                        .env("TEAQL_WORKER_TEST_ROOT", root.path())
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::inherit())
                        .spawn()
                        .unwrap(),
                );
                // Copying and hashing the pinned, unoptimized worker can take
                // longer while the other process-fault tests run in parallel.
                let deadline = Instant::now() + Duration::from_secs(120);
                let pid: i32 = loop {
                    if let Ok(text) = fs::read_to_string(root.path().join("ready"))
                        && let Ok(pid) = text.parse()
                    {
                        break pid;
                    }
                    assert!(
                        controller.0.try_wait().unwrap().is_none(),
                        "controller exited before ready"
                    );
                    assert!(Instant::now() < deadline, "controller never became ready");
                    std::thread::sleep(Duration::from_millis(10));
                };
                controller.0.kill().unwrap();
                assert_eq!(
                    wait(&mut controller.0, Duration::from_secs(3)).signal(),
                    Some(libc::SIGKILL)
                );
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut status = 0;
                loop {
                    // SAFETY: pid came from our own controller and is now our
                    // adopted child; only this exact PID is ever waited here.
                    let result = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
                    if result == pid {
                        break;
                    }
                    assert_eq!(result, 0, "owned worker was not adopted");
                    if Instant::now() >= deadline {
                        // SAFETY: still-unreaped owned child cannot have reused its PID.
                        unsafe {
                            libc::kill(pid, libc::SIGKILL);
                            libc::waitpid(pid, &mut status, 0);
                        }
                        panic!("kernel parent-death signal failed");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                assert!(libc::WIFSIGNALED(status));
                assert_eq!(libc::WTERMSIG(status), libc::SIGKILL);
                assert!(!Path::new(&format!("/proc/{pid}")).exists());
            }
            None => {
                let mut witness = ChildGuard(
                    Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", TEST, "--nocapture"])
                        .env(ROLE, "witness")
                        .env("TEAQL_WORKER_TEST_CANARY", "must-never-reach-worker")
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::inherit())
                        .spawn()
                        .unwrap(),
                );
                assert!(wait(&mut witness.0, Duration::from_secs(150)).success());
            }
            _ => panic!("invalid helper role"),
        }
    }
}
