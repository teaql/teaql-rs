use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime},
};
use teaql_blob_reference::*;
use teaql_blob_store_file::FileBlobStore;
use tempfile::TempDir;

const MAX: u64 = 1024 * 1024;
fn store(path: &Path) -> FileBlobStore {
    FileBlobStore::new(path, MAX, 16).unwrap()
}
async fn initialized() -> (TempDir, FileBlobStore) {
    let temp = tempfile::tempdir().unwrap();
    let store = store(&temp.path().join("store"));
    store.initialize().await.unwrap();
    (temp, store)
}
fn bytes(data: impl Into<Bytes>) -> BlobContentStream {
    Box::pin(stream::iter([Ok(data.into())]))
}
async fn read(mut content: BlobRangeContent) -> Vec<u8> {
    let mut result = Vec::new();
    while let Some(chunk) = content.stream.next().await {
        result.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(result.len() as u64, content.length);
    result
}
fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}
fn count(path: &Path) -> usize {
    fs::read_dir(path).unwrap().count()
}
fn assert_code<T>(result: BlobResult<T>, code: BlobErrorCode) {
    match result {
        Err(error) => assert_eq!(error.code, code),
        Ok(_) => panic!("operation unexpectedly succeeded"),
    }
}

#[tokio::test]
async fn passive_explicit_private_initialization_and_no_retarget() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("store");
    let provider = store(&path);
    assert!(!path.exists());
    assert_code(
        provider.validate_ready().await,
        BlobErrorCode::CapabilityMissing,
    );
    let target = provider.create_upload("server-generated-id").await.unwrap();
    assert!(!path.exists());
    assert_eq!(target.location().len(), 64);
    let capabilities = provider.capabilities();
    assert!(capabilities.range && capabilities.version_conditional_delete);
    assert!(
        !capabilities.direct_upload
            && !capabilities.signed_reads
            && !capabilities.context_checked_reads
    );
    provider.initialize().await.unwrap();
    provider.initialize().await.unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let old = temp.path().join("old");
    fs::rename(&path, &old).unwrap();
    let replacement = store(&path);
    replacement.initialize().await.unwrap();
    assert_code(
        provider.validate_ready().await,
        BlobErrorCode::PolicyInvalid,
    );
    assert_code(provider.initialize().await, BlobErrorCode::PolicyInvalid);
    replacement.validate_ready().await.unwrap();
    for invalid in ["", "../x", "a/b", "x\0", "å", &"a".repeat(129)] {
        assert_code(
            provider.create_upload(invalid).await,
            BlobErrorCode::ReferenceInvalid,
        );
    }
    assert!(FileBlobStore::new("relative", MAX, 1).is_err());
    assert!(FileBlobStore::new(temp.path().join("bad/../root"), MAX, 1).is_err());
    assert!(FileBlobStore::new(temp.path(), 0, 1).is_err());
    assert!(FileBlobStore::new(temp.path(), MAX, 65).is_err());
}

#[tokio::test]
async fn real_ten_format_bytes_hashes_and_bounded_ranges() {
    let (_temp, provider) = initialized().await;
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let input: serde_json::Value =
        serde_json::from_slice(&fs::read(fixtures.join("files.json")).unwrap()).unwrap();
    let files = input["files"].as_array().unwrap();
    assert_eq!(files.len(), 10);
    for fixture in files {
        let data = fs::read(fixtures.join(fixture["path"].as_str().unwrap())).unwrap();
        let target = provider
            .create_upload(&format!(
                "actual-{}",
                fixture["extension"].as_str().unwrap()
            ))
            .await
            .unwrap();
        let chunks: Vec<_> = data
            .chunks(17)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        let stored = provider
            .write(&target, Box::pin(stream::iter(chunks)), data.len() as u64)
            .await
            .unwrap();
        assert_eq!(stored.size_bytes(), fixture["sizeBytes"].as_u64().unwrap());
        assert_eq!(stored.sha256(), fixture["sha256"].as_str().unwrap());
        assert_eq!(stored.sha256(), hex(&Sha256::digest(&data)));
        assert_eq!(
            provider.stat(target.location()).await.unwrap().unwrap(),
            stored
        );
        assert_eq!(
            read(
                provider
                    .open_range(
                        target.location(),
                        stored.content_version(),
                        0,
                        stored.size_bytes()
                    )
                    .await
                    .unwrap()
            )
            .await,
            data
        );
        for (offset, length) in [
            (0, 0),
            (0, 7),
            (7, 13),
            (stored.size_bytes() - 5, 5),
            (stored.size_bytes(), 0),
        ] {
            let ranged = read(
                provider
                    .open_range(target.location(), stored.content_version(), offset, length)
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(ranged, data[offset as usize..(offset + length) as usize]);
        }
        assert_code(
            provider
                .open_range(
                    target.location(),
                    stored.content_version(),
                    stored.size_bytes() + 1,
                    0,
                )
                .await,
            BlobErrorCode::ReferenceInvalid,
        );
        assert_code(
            provider
                .open_range(target.location(), stored.content_version(), 1, u64::MAX)
                .await,
            BlobErrorCode::ReferenceInvalid,
        );
        let same = provider.write(&target, bytes(data), MAX).await.unwrap();
        assert_eq!(stored, same);
    }
}

#[tokio::test]
async fn source_failures_limits_empty_and_cancellation_leave_no_publication() {
    let (temp, provider) = initialized().await;
    let target = provider.create_upload("bounded").await.unwrap();
    assert_code(
        provider.write(&target, bytes("too large"), 3).await,
        BlobErrorCode::SizeExceeded,
    );
    let failed = Box::pin(stream::iter([
        Ok(Bytes::from_static(b"partial")),
        Err(BlobErrorCode::ReadDenied.into()),
    ]));
    assert_code(
        provider.write(&target, failed, MAX).await,
        BlobErrorCode::StoreFailed,
    );
    assert_code(
        provider.write(&target, bytes("x"), 0).await,
        BlobErrorCode::PolicyInvalid,
    );
    assert_code(
        provider.write(&target, bytes("x"), MAX + 1).await,
        BlobErrorCode::PolicyInvalid,
    );
    let forged = BlobUploadTarget::new("other", target.location()).unwrap();
    assert_code(
        provider.write(&forged, bytes("x"), MAX).await,
        BlobErrorCode::ReferenceInvalid,
    );
    let (started, received) = tokio::sync::oneshot::channel();
    let pending = Box::pin(stream::once(async move {
        let _ = started.send(());
        std::future::pending::<BlobResult<Bytes>>().await
    }));
    let p = provider.clone();
    let t = target.clone();
    let work = tokio::spawn(async move { p.write(&t, pending, MAX).await });
    received.await.unwrap();
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    assert!(provider.stat(target.location()).await.unwrap().is_none());
    assert_eq!(count(&temp.path().join("store/staging")), 0);
    let empty = provider
        .write(&target, bytes(Bytes::new()), 1)
        .await
        .unwrap();
    assert_eq!(empty.size_bytes(), 0); // Format/allowEmpty policy is the verifier's responsibility.
    assert_eq!(empty.sha256(), hex(&Sha256::digest([])));
    assert!(
        read(
            provider
                .open_range(target.location(), empty.content_version(), 0, 0)
                .await
                .unwrap()
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn exact_version_delete_and_open_inode_survive_replacement() {
    let (_temp, provider) = initialized().await;
    let target = provider.create_upload("versions").await.unwrap();
    let old = provider
        .write(&target, bytes("old content"), MAX)
        .await
        .unwrap();
    let opened = provider
        .open_range(
            target.location(),
            old.content_version(),
            0,
            old.size_bytes(),
        )
        .await
        .unwrap();
    assert_eq!(
        provider
            .delete_if_version(target.location(), &"0".repeat(32))
            .await
            .unwrap(),
        BlobDeleteResult::VersionMismatch
    );
    assert_eq!(
        provider
            .delete_if_version(target.location(), old.content_version())
            .await
            .unwrap(),
        BlobDeleteResult::Deleted
    );
    let new = provider
        .write(&target, bytes("new content"), MAX)
        .await
        .unwrap();
    assert_ne!(old.content_version(), new.content_version());
    assert_eq!(read(opened).await, b"old content");
    assert_code(
        provider
            .open_range(
                target.location(),
                old.content_version(),
                0,
                old.size_bytes(),
            )
            .await,
        BlobErrorCode::NotFound,
    );
    assert_eq!(
        provider
            .delete_if_version(target.location(), old.content_version())
            .await
            .unwrap(),
        BlobDeleteResult::VersionMismatch
    );
    assert_eq!(
        read(
            provider
                .open_range(
                    target.location(),
                    new.content_version(),
                    0,
                    new.size_bytes()
                )
                .await
                .unwrap()
        )
        .await,
        b"new content"
    );
    assert_eq!(
        provider
            .delete_if_version(target.location(), new.content_version())
            .await
            .unwrap(),
        BlobDeleteResult::Deleted
    );
    assert_eq!(
        provider
            .delete_if_version(target.location(), new.content_version())
            .await
            .unwrap(),
        BlobDeleteResult::AlreadyMissing
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_instances_concurrent_retry_and_conflict() {
    let (temp, one) = initialized().await;
    let two = store(&temp.path().join("store"));
    two.initialize().await.unwrap();
    let target = one.create_upload("concurrent").await.unwrap();
    let mut tasks = Vec::new();
    for index in 0..12 {
        let provider = if index % 2 == 0 {
            one.clone()
        } else {
            two.clone()
        };
        let target = target.clone();
        tasks.push(tokio::spawn(async move {
            provider
                .write(&target, bytes("immutable"), MAX)
                .await
                .unwrap()
        }));
    }
    let mut version = None;
    for task in tasks {
        let content = task.await.unwrap();
        if let Some(version) = &version {
            assert_eq!(content.content_version(), version);
        } else {
            version = Some(content.content_version().to_owned());
        }
    }
    assert_code(
        two.write(&target, bytes("different"), MAX).await,
        BlobErrorCode::IdempotencyConflict,
    );
    let target = one.create_upload("two-different").await.unwrap();
    let other = target.clone();
    let a = tokio::spawn(async move { one.write(&target, bytes("A"), MAX).await });
    let b = tokio::spawn(async move { two.write(&other, bytes("B"), MAX).await });
    let results = [a.await.unwrap(), b.await.unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| result
                .as_ref()
                .is_err_and(|e| e.code == BlobErrorCode::IdempotencyConflict))
            .count(),
        1
    );
    assert_eq!(count(&temp.path().join("store/staging")), 0);
}

#[tokio::test]
async fn durable_seal_blocks_missing_and_inflight_late_publication() {
    let (temp, provider) = initialized().await;
    let target = provider.create_upload("late").await.unwrap();
    let (started, received) = tokio::sync::oneshot::channel();
    let (resume, wait) = tokio::sync::oneshot::channel();
    let input = Box::pin(stream::once(async move {
        let _ = started.send(());
        wait.await.unwrap();
        Ok(Bytes::from_static(b"late content"))
    }));
    let p = provider.clone();
    let t = target.clone();
    let work = tokio::spawn(async move { p.write(&t, input, MAX).await });
    received.await.unwrap();
    assert!(
        provider
            .seal_for_deletion(target.location(), None)
            .await
            .unwrap()
            .is_none()
    );
    resume.send(()).unwrap();
    assert_code(work.await.unwrap(), BlobErrorCode::UploadExpired);
    assert!(provider.stat(target.location()).await.unwrap().is_none());
    let restarted = store(&temp.path().join("store"));
    restarted.initialize().await.unwrap();
    assert_code(
        restarted.write(&target, bytes("retry"), MAX).await,
        BlobErrorCode::UploadExpired,
    );
    let target = provider.create_upload("known").await.unwrap();
    let content = provider.write(&target, bytes("known"), MAX).await.unwrap();
    assert_code(
        provider
            .seal_for_deletion(target.location(), Some(&"0".repeat(32)))
            .await,
        BlobErrorCode::IdempotencyConflict,
    );
    assert_eq!(
        provider.write(&target, bytes("known"), MAX).await.unwrap(),
        content
    );
    assert_eq!(
        provider
            .seal_for_deletion(target.location(), Some(content.content_version()))
            .await
            .unwrap(),
        Some(content.clone())
    );
    assert_eq!(
        provider
            .delete_if_version(target.location(), content.content_version())
            .await
            .unwrap(),
        BlobDeleteResult::Deleted
    );
    assert!(
        provider
            .seal_for_deletion(target.location(), Some(content.content_version()))
            .await
            .unwrap()
            .is_none()
    );
    assert_code(
        provider.write(&target, bytes("known"), MAX).await,
        BlobErrorCode::UploadExpired,
    );
    assert_eq!(count(&temp.path().join("store/staging")), 0);
}

#[tokio::test]
async fn private_namespace_symlink_hardlink_corruption_and_foreign_format_reject() {
    let temp = tempfile::tempdir().unwrap();
    let unrelated = temp.path().join("unrelated");
    fs::create_dir(&unrelated).unwrap();
    fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(unrelated.join("keep"), "unchanged").unwrap();
    assert!(store(&unrelated).initialize().await.is_err());
    assert_eq!(fs::read(unrelated.join("keep")).unwrap(), b"unchanged");
    let linked = temp.path().join("linked");
    symlink(&unrelated, &linked).unwrap();
    assert!(store(&linked).initialize().await.is_err());
    let public = temp.path().join("public");
    fs::create_dir(&public).unwrap();
    fs::set_permissions(&public, fs::Permissions::from_mode(0o755)).unwrap();
    assert_code(
        store(&public).initialize().await,
        BlobErrorCode::PolicyInvalid,
    );
    let foreign = temp.path().join("foreign");
    fs::create_dir(&foreign).unwrap();
    fs::set_permissions(&foreign, fs::Permissions::from_mode(0o700)).unwrap();
    let mut marker = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(foreign.join("store.format"))
        .unwrap();
    marker.write_all(b"teaql.file.blob-store/v1\n").unwrap();
    assert_code(
        store(&foreign).initialize().await,
        BlobErrorCode::PolicyInvalid,
    );
    let (owned, provider) = initialized().await;
    let target = provider.create_upload("protected").await.unwrap();
    let path = owned.path().join("store/objects").join(target.location());
    let external = owned.path().join("private-original");
    fs::write(&external, b"never read this").unwrap();
    fs::set_permissions(&external, fs::Permissions::from_mode(0o600)).unwrap();
    symlink(&external, &path).unwrap();
    assert_code(
        provider.stat(target.location()).await,
        BlobErrorCode::StoreFailed,
    );
    fs::remove_file(&path).unwrap();
    fs::hard_link(&external, &path).unwrap();
    assert_code(
        provider.stat(target.location()).await,
        BlobErrorCode::StoreFailed,
    );
    fs::remove_file(&path).unwrap();
    let content = provider.write(&target, bytes("valid"), MAX).await.unwrap();
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.write_all(b"BADMAGIC").unwrap();
    assert_code(
        provider.stat(target.location()).await,
        BlobErrorCode::StoreFailed,
    );
    assert_code(
        provider
            .delete_if_version(target.location(), content.content_version())
            .await,
        BlobErrorCode::StoreFailed,
    );
    let objects = owned.path().join("store/objects");
    fs::rename(&objects, owned.path().join("old-objects")).unwrap();
    fs::create_dir(&objects).unwrap();
    fs::set_permissions(&objects, fs::Permissions::from_mode(0o700)).unwrap();
    assert_code(
        provider.validate_ready().await,
        BlobErrorCode::PolicyInvalid,
    );
    assert_code(
        provider.stat(target.location()).await,
        BlobErrorCode::PolicyInvalid,
    );
    assert_eq!(fs::read(external).unwrap(), b"never read this");
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn child(test: &str, mode: &str, root: &Path, location: &str, ready: &Path) -> Process {
    Process(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env("TEAQL_FILE_CHILD_MODE", mode)
            .env("TEAQL_FILE_CHILD_ROOT", root)
            .env("TEAQL_FILE_CHILD_LOCATION", location)
            .env("TEAQL_FILE_CHILD_READY", ready)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
async fn ready(path: &Path, process: &mut Process) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.exists() {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "child exited before readiness"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
fn signal_ready() {
    fs::write(
        std::env::var_os("TEAQL_FILE_CHILD_READY").unwrap(),
        b"ready",
    )
    .unwrap();
}
fn wait_input() {
    let mut input = [0];
    std::io::stdin().read_exact(&mut input).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_process_lock_blocks_publication_and_wait_cancels() {
    if std::env::var("TEAQL_FILE_CHILD_MODE").as_deref() == Ok("lock") {
        let root = PathBuf::from(std::env::var_os("TEAQL_FILE_CHILD_ROOT").unwrap());
        let location = std::env::var("TEAQL_FILE_CHILD_LOCATION").unwrap();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(root.join("locks").join(format!("{}.lock", &location[..2])))
            .unwrap();
        rustix::fs::flock(&file, rustix::fs::FlockOperation::LockExclusive).unwrap();
        signal_ready();
        wait_input();
        return;
    }
    let (temp, provider) = initialized().await;
    let target = provider.create_upload("cross-process").await.unwrap();
    let ready_path = temp.path().join("ready");
    let mut process = child(
        "cross_process_lock_blocks_publication_and_wait_cancels",
        "lock",
        &temp.path().join("store"),
        target.location(),
        &ready_path,
    );
    ready(&ready_path, &mut process).await;
    let p = provider.clone();
    let t = target.clone();
    let waiting = tokio::spawn(async move { p.write(&t, bytes("cancelled"), MAX).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    let p = provider.clone();
    let t = target.clone();
    let waiting = tokio::spawn(async move { p.write(&t, bytes("cross-process"), MAX).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    process.0.stdin.take().unwrap().write_all(b"x").unwrap();
    assert!(process.0.wait().unwrap().success());
    let content = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(content.size_bytes(), 13);
    assert_eq!(
        provider.stat(target.location()).await.unwrap(),
        Some(content)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_cross_process_scratch_survives_cleanup_and_crash_is_recovered() {
    if std::env::var("TEAQL_FILE_CHILD_MODE").as_deref() == Ok("scratch") {
        let provider = store(&PathBuf::from(
            std::env::var_os("TEAQL_FILE_CHILD_ROOT").unwrap(),
        ));
        provider.initialize().await.unwrap();
        let target = provider.create_upload("scratch-child").await.unwrap();
        let input = Box::pin(stream::once(async {
            signal_ready();
            tokio::task::spawn_blocking(wait_input).await.unwrap();
            Ok(Bytes::from_static(b"after crash"))
        }));
        provider.write(&target, input, MAX).await.unwrap();
        return;
    }
    let (temp, provider) = initialized().await;
    let path = temp.path().join("store");
    let ready_path = temp.path().join("ready");
    let mut process = child(
        "active_cross_process_scratch_survives_cleanup_and_crash_is_recovered",
        "scratch",
        &path,
        "unused",
        &ready_path,
    );
    ready(&ready_path, &mut process).await;
    assert_eq!(count(&path.join("staging")), 1);
    let now = SystemTime::now() + Duration::from_secs(3600);
    assert_eq!(
        provider
            .cleanup_staging(now, Duration::from_secs(1))
            .await
            .unwrap(),
        0
    );
    process.0.kill().unwrap();
    assert!(!process.0.wait().unwrap().success());
    assert_eq!(
        provider
            .cleanup_staging(now, Duration::from_secs(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(count(&path.join("staging")), 0);
    let target = provider.create_upload("scratch-child").await.unwrap();
    assert!(provider.stat(target.location()).await.unwrap().is_none());
    let restarted = store(&path);
    restarted.initialize().await.unwrap();
    let content = restarted
        .write(&target, bytes("recovered"), MAX)
        .await
        .unwrap();
    assert_eq!(
        read(
            restarted
                .open_range(
                    target.location(),
                    content.content_version(),
                    0,
                    content.size_bytes()
                )
                .await
                .unwrap()
        )
        .await,
        b"recovered"
    );
}

#[test]
fn transport_values_and_errors_do_not_print_private_locations_or_versions() {
    let target = BlobUploadTarget::new("PRIVATE-ID", "PRIVATE-LOCATION").unwrap();
    let content =
        StoredBlob::new("PRIVATE-LOCATION", "PRIVATE-VERSION", 1, "a".repeat(64)).unwrap();
    assert!(!format!("{target:?}{content:?}").contains("PRIVATE"));
    let error: BlobError = BlobErrorCode::StoreFailed.into();
    assert_eq!(error.to_string(), "BLOB_STORE_FAILED");
    assert!(StoredBlob::new("x", "v", 1, "A".repeat(64)).is_err());
    assert!(BlobUploadTarget::new("x", "bad\r\nheader").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_generation_survives_process_halt_and_new_instance() {
    if std::env::var("TEAQL_FILE_CHILD_MODE").as_deref() == Ok("published") {
        let provider = store(&PathBuf::from(
            std::env::var_os("TEAQL_FILE_CHILD_ROOT").unwrap(),
        ));
        provider.initialize().await.unwrap();
        let target = provider.create_upload("published-child").await.unwrap();
        provider
            .write(&target, bytes("durable published bytes"), MAX)
            .await
            .unwrap();
        signal_ready();
        tokio::task::spawn_blocking(wait_input).await.unwrap();
        return;
    }
    let (temp, provider) = initialized().await;
    let path = temp.path().join("store");
    let ready_path = temp.path().join("ready");
    let mut process = child(
        "published_generation_survives_process_halt_and_new_instance",
        "published",
        &path,
        "unused",
        &ready_path,
    );
    ready(&ready_path, &mut process).await;
    let target = provider.create_upload("published-child").await.unwrap();
    let before = provider.stat(target.location()).await.unwrap().unwrap();
    process.0.kill().unwrap();
    assert!(!process.0.wait().unwrap().success());
    drop(provider);
    let restarted = store(&path);
    restarted.initialize().await.unwrap();
    let after = restarted.stat(target.location()).await.unwrap().unwrap();
    assert_eq!(before, after);
    assert_eq!(
        read(
            restarted
                .open_range(
                    target.location(),
                    after.content_version(),
                    0,
                    after.size_bytes()
                )
                .await
                .unwrap()
        )
        .await,
        b"durable published bytes"
    );
    assert_eq!(count(&path.join("staging")), 0);
}
