//! Optional Linux filesystem provider. No listener, initialization or worker starts on construction.
#![forbid(unsafe_code)]
#[cfg(feature = "http-gateway")]
mod gateway;
#[cfg(feature = "http-gateway")]
pub use gateway::{BlobGatewayClock, FileBlobHttpGateway, SystemBlobGatewayClock};
#[cfg(not(target_os = "linux"))]
compile_error!(
    "teaql-blob-store-file currently requires Linux; choose another BlobStore provider on other targets"
);

use futures_util::{StreamExt, stream};
use rustix::fs::{self, AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags};
use sha2::{Digest, Sha256};
use std::{
    ffi::OsStr,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime},
};
use teaql_blob_contracts::{
    BlobContentStream, BlobDeleteResult, BlobError, BlobErrorCode as Code, BlobRangeContent,
    BlobResult, BlobStore, BlobStoreCapabilities, BlobUploadTarget, Bytes, StoredBlob,
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Semaphore,
};

const FORMAT: &[u8] = b"teaql.rust.file.blob-store/v1\n";
const MAGIC: &[u8; 8] = b"TQRBLB01";
const HEADER: u64 = 64;
const CHUNK: usize = 65536;
const PRIVATE_FILE: Mode = Mode::RUSR.union(Mode::WUSR);
const PRIVATE_DIR: Mode = PRIVATE_FILE.union(Mode::XUSR);
const SAFE_OPEN: OFlags = OFlags::CLOEXEC
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK);

/// Own a dedicated private root. Its on-disk format and locks are specific to this
/// Rust provider; sharing a Java FileBlobStore directory is deliberately rejected.
#[derive(Clone)]
pub struct FileBlobStore {
    configured_root: Arc<PathBuf>,
    root: Arc<OnceLock<Arc<Root>>>,
    writes: Arc<Semaphore>,
    maximum_write_bytes: u64,
}
struct Root {
    directory: File,
    objects: File,
    locks: File,
    staging: File,
    seals: File,
}
impl FileBlobStore {
    /// Passive. The parent must already exist; initialize creates only the final root.
    pub fn new(
        root: impl Into<PathBuf>,
        maximum_write_bytes: u64,
        maximum_concurrent_writes: usize,
    ) -> BlobResult<Self> {
        let root = root.into();
        if !root.is_absolute()
            || root.components().any(|p| matches!(p, Component::ParentDir))
            || maximum_write_bytes == 0
            || maximum_write_bytes > i64::MAX as u64 - HEADER
            || !(1..=64).contains(&maximum_concurrent_writes)
        {
            return Err(Code::PolicyInvalid.into());
        }
        Ok(Self {
            configured_root: Arc::new(root),
            root: Arc::new(OnceLock::new()),
            writes: Arc::new(Semaphore::new(maximum_concurrent_writes)),
            maximum_write_bytes,
        })
    }
    /// Explicit provisioning. Refuses unrelated/nonprivate/symlink roots and foreign formats.
    pub async fn initialize(&self) -> BlobResult<()> {
        let path = self.configured_root.clone();
        let root = blocking(move || initialize(&path)).await?;
        // Reinitialization may not retarget an already installed store to a replacement inode.
        if let Some(existing) = self.root.get() {
            if !same_file(&existing.directory, &root.directory)? {
                return Err(Code::PolicyInvalid.into());
            }
        } else {
            let _ = self.root.set(Arc::new(root));
        }
        self.validate_ready().await
    }
    fn ready(&self) -> BlobResult<Arc<Root>> {
        self.root
            .get()
            .cloned()
            .ok_or_else(|| Code::CapabilityMissing.into())
    }
    /// Explicit crash-scratch recovery. Active leases are skipped, including other
    /// processes' writers. Only recognized files inside this retained staging directory
    /// are eligible. No thread/timer or request-context authority is installed.
    pub async fn cleanup_staging(
        &self,
        now: SystemTime,
        minimum_age: Duration,
    ) -> BlobResult<usize> {
        if minimum_age.is_zero() {
            return Err(Code::PolicyInvalid.into());
        }
        let root = self.ready()?;
        let gate = acquire_lock(root.clone(), "staging.lock".into()).await?;
        blocking(move || {
            root.check()?;
            let mut removed = 0;
            let entries = fs::Dir::read_from(&root.staging).map_err(io_error)?;
            for entry in entries {
                let entry = entry.map_err(io_error)?;
                let bytes = entry.file_name().to_bytes();
                if bytes == b"." || bytes == b".." {
                    continue;
                }
                let name =
                    std::str::from_utf8(bytes).map_err(|_| BlobError::from(Code::StoreFailed))?;
                if name.len() != 38 || !name.ends_with(".stage") || !is_hex(&name[..32], 32) {
                    return Err(Code::StoreFailed.into());
                }
                let Some(file) = open_optional(&root.staging, name, OFlags::RDWR)? else {
                    continue;
                };
                if !try_lock(&file)? {
                    continue;
                }
                // A successful writer may unlink its scratch while this cleaner opens
                // it. The lease is released only after unlink; no active file is removed.
                if fs::fstat(&file).map_err(io_error)?.st_nlink == 0 {
                    continue;
                }
                check_file(&file)?;
                let modified = file
                    .metadata()
                    .map_err(io_error)?
                    .modified()
                    .map_err(io_error)?;
                if now
                    .duration_since(modified)
                    .is_ok_and(|age| age >= minimum_age)
                {
                    fs::unlinkat(&root.staging, name, AtFlags::empty()).map_err(io_error)?;
                    removed += 1;
                }
            }
            fs::fsync(&root.staging).map_err(io_error)?;
            drop(gate);
            Ok(removed)
        })
        .await
    }
}

#[async_trait::async_trait]
impl BlobStore for FileBlobStore {
    async fn validate_ready(&self) -> BlobResult<()> {
        let root = self.ready()?;
        let configured = self.configured_root.clone();
        blocking(move || {
            root.check()?;
            let current = open_path(&configured, false)?;
            if !same_file(&root.directory, &current)? {
                return Err(Code::PolicyInvalid.into());
            }
            Ok(())
        })
        .await
    }
    fn capabilities(&self) -> BlobStoreCapabilities {
        BlobStoreCapabilities {
            direct_upload: false,
            range: true,
            signed_reads: false,
            context_checked_reads: false,
            version_conditional_delete: true,
            maximum_read_ttl: Duration::ZERO,
        }
    }
    fn maximum_write_bytes(&self) -> u64 {
        self.maximum_write_bytes
    }
    async fn create_upload(&self, upload_id: &str) -> BlobResult<BlobUploadTarget> {
        if upload_id.is_empty()
            || upload_id.len() > 128
            || !upload_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(Code::ReferenceInvalid.into());
        }
        BlobUploadTarget::new(upload_id, hex(&Sha256::digest(upload_id.as_bytes())))
    }
    async fn write(
        &self,
        target: &BlobUploadTarget,
        mut source: BlobContentStream,
        maximum_bytes: u64,
    ) -> BlobResult<StoredBlob> {
        if maximum_bytes == 0 || maximum_bytes > self.maximum_write_bytes {
            return Err(Code::PolicyInvalid.into());
        }
        if self.create_upload(target.upload_id()).await?.location() != target.location() {
            return Err(Code::ReferenceInvalid.into());
        }
        let _permit = self
            .writes
            .clone()
            .try_acquire_owned()
            .map_err(|_| BlobError::from(Code::StoreFailed))?;
        let root = self.ready()?;
        let location = target.location().to_owned();
        // A seal is checked again at publication under the same lock as deletion.
        {
            let _lock = acquire_location(root.clone(), &location).await?;
            let r = root.clone();
            let l = location.clone();
            blocking(move || {
                r.check()?;
                require_unsealed(&r, &l)
            })
            .await?;
        }
        let stage_gate = acquire_lock(root.clone(), "staging.lock".into()).await?;
        let r = root.clone();
        let (stage, file) = blocking(move || {
            r.check()?;
            create_stage(r)
        })
        .await?;
        drop(stage_gate);
        let mut output = tokio::fs::File::from_std(file);
        output
            .seek(SeekFrom::Start(HEADER))
            .await
            .map_err(io_error)?;
        let version = random_hex()?;
        let mut digest = Sha256::new();
        let mut size = 0u64;
        while let Some(chunk) = source.next().await {
            let chunk = chunk.map_err(|_| BlobError::from(Code::StoreFailed))?;
            if chunk.is_empty() {
                tokio::task::yield_now().await;
                continue;
            }
            if chunk.len() as u64 > maximum_bytes - size {
                return Err(Code::SizeExceeded.into());
            }
            // Bound each filesystem submission even when the caller supplies a large chunk.
            for bytes in chunk.chunks(CHUNK) {
                output.write_all(bytes).await.map_err(io_error)?;
                digest.update(bytes);
            }
            size += chunk.len() as u64;
        }
        let sha = digest.finalize();
        let mut header = Vec::with_capacity(HEADER as usize);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&unhex_version(&version)?);
        header.extend_from_slice(&size.to_be_bytes());
        header.extend_from_slice(&sha);
        output.seek(SeekFrom::Start(0)).await.map_err(io_error)?;
        output.write_all(&header).await.map_err(io_error)?;
        output.sync_all().await.map_err(io_error)?;
        drop(output);
        let stored = StoredBlob::new(&location, version, size, hex(&sha))?;
        let lock = acquire_location(root.clone(), &location).await?;
        blocking(move || {
            root.check()?;
            require_unsealed(&root, &location)?;
            if let Some(mut file) = open_optional(&root.objects, &location, OFlags::RDONLY)? {
                let existing = metadata(&mut file, &location)?;
                if existing.size_bytes() != stored.size_bytes()
                    || existing.sha256() != stored.sha256()
                {
                    return Err(Code::IdempotencyConflict.into());
                }
                return Ok(existing);
            }
            // No overwrite fallback, even if an unexpected uncoordinated name appears.
            fs::renameat_with(
                &root.staging,
                &stage.name,
                &root.objects,
                &location,
                RenameFlags::NOREPLACE,
            )
            .map_err(io_error)?;
            fs::fsync(&root.objects).map_err(io_error)?;
            fs::fsync(&root.staging).map_err(io_error)?;
            drop(stage);
            drop(lock);
            Ok(stored)
        })
        .await
    }
    async fn stat(&self, location: &str) -> BlobResult<Option<StoredBlob>> {
        require_location(location)?;
        let root = self.ready()?;
        let location = location.to_owned();
        let lock = acquire_location(root.clone(), &location).await?;
        blocking(move || {
            root.check()?;
            let result = stat(&root, &location);
            drop(lock);
            result
        })
        .await
    }
    async fn seal_for_deletion(
        &self,
        location: &str,
        expected_version: Option<&str>,
    ) -> BlobResult<Option<StoredBlob>> {
        require_location(location)?;
        if let Some(version) = expected_version {
            require_version(version)?;
        }
        let root = self.ready()?;
        let location = location.to_owned();
        let expected = expected_version.map(str::to_owned);
        let stage_gate = acquire_lock(root.clone(), "staging.lock".into()).await?;
        let lock = acquire_location(root.clone(), &location).await?;
        blocking(move || {
            root.check()?;
            let current = stat(&root, &location)?;
            if let (Some(expected), Some(current)) = (&expected, &current)
                && current.content_version() != expected
            {
                return Err(Code::IdempotencyConflict.into());
            }
            if let Some(sealed) = read_seal(&root, &location)? {
                if let Some(expected) = &expected
                    && sealed != "-"
                    && sealed != *expected
                {
                    return Err(Code::IdempotencyConflict.into());
                }
            } else {
                let version = current
                    .as_ref()
                    .map(StoredBlob::content_version)
                    .or(expected.as_deref())
                    .unwrap_or("-");
                let (stage, mut seal) = create_stage(root.clone())?;
                seal.write_all(format!("TEAQL-SEAL1\n{version}\n").as_bytes())
                    .map_err(io_error)?;
                seal.sync_all().map_err(io_error)?;
                fs::renameat_with(
                    &root.staging,
                    &stage.name,
                    &root.seals,
                    &location,
                    RenameFlags::NOREPLACE,
                )
                .map_err(io_error)?;
                fs::fsync(&root.seals).map_err(io_error)?;
                fs::fsync(&root.staging).map_err(io_error)?;
                drop(stage);
            }
            drop(lock);
            drop(stage_gate);
            Ok(current)
        })
        .await
    }
    async fn open_range(
        &self,
        location: &str,
        expected_version: &str,
        offset: u64,
        length: u64,
    ) -> BlobResult<BlobRangeContent> {
        require_location(location)?;
        require_version(expected_version)?;
        let root = self.ready()?;
        let location = location.to_owned();
        let expected = expected_version.to_owned();
        let lock = acquire_location(root.clone(), &location).await?;
        let (content, file) = blocking(move || {
            root.check()?;
            let mut file = open_optional(&root.objects, &location, OFlags::RDONLY)?
                .ok_or_else(|| BlobError::from(Code::NotFound))?;
            let content = metadata(&mut file, &location)?;
            if content.content_version() != expected {
                return Err(Code::NotFound.into());
            }
            if offset > content.size_bytes() || length > content.size_bytes() - offset {
                return Err(Code::ReferenceInvalid.into());
            }
            file.seek(SeekFrom::Start(HEADER + offset))
                .map_err(io_error)?;
            drop(lock);
            Ok((content, file))
        })
        .await?;
        let stream = stream::try_unfold(
            (tokio::fs::File::from_std(file), length),
            |(mut file, remaining)| async move {
                if remaining == 0 {
                    return Ok(None);
                }
                let mut buffer = vec![0; remaining.min(CHUNK as u64) as usize];
                file.read_exact(&mut buffer).await.map_err(io_error)?;
                let next = remaining - buffer.len() as u64;
                Ok(Some((Bytes::from(buffer), (file, next))))
            },
        );
        Ok(BlobRangeContent {
            content,
            offset,
            length,
            stream: Box::pin(stream),
        })
    }
    async fn delete_if_version(
        &self,
        location: &str,
        expected_version: &str,
    ) -> BlobResult<BlobDeleteResult> {
        require_location(location)?;
        require_version(expected_version)?;
        let root = self.ready()?;
        let location = location.to_owned();
        let expected = expected_version.to_owned();
        let lock = acquire_location(root.clone(), &location).await?;
        blocking(move || {
            root.check()?;
            let result = match stat(&root, &location)? {
                None => BlobDeleteResult::AlreadyMissing,
                Some(content) if content.content_version() != expected => {
                    BlobDeleteResult::VersionMismatch
                }
                Some(_) => {
                    fs::unlinkat(&root.objects, &location, AtFlags::empty()).map_err(io_error)?;
                    fs::fsync(&root.objects).map_err(io_error)?;
                    BlobDeleteResult::Deleted
                }
            };
            drop(lock);
            Ok(result)
        })
        .await
    }
}

struct Stage {
    root: Arc<Root>,
    name: String,
    _lease: File,
}
fn create_stage(root: Arc<Root>) -> BlobResult<(Stage, File)> {
    let name = format!("{}.stage", random_hex()?);
    let file = create_file(&root.staging, &name)?;
    let stage = Stage {
        root,
        name,
        _lease: file,
    };
    // This lease remains locked through streaming, publication and Drop cleanup.
    fs::flock(&stage._lease, FlockOperation::NonBlockingLockExclusive).map_err(io_error)?;
    let output = stage._lease.try_clone().map_err(io_error)?;
    Ok((stage, output))
}
impl Drop for Stage {
    fn drop(&mut self) {
        let _ = fs::unlinkat(&self.root.staging, self.name.as_str(), AtFlags::empty());
    }
}
struct Lock(File);
impl Drop for Lock {
    fn drop(&mut self) {
        let _ = fs::flock(&self.0, FlockOperation::Unlock);
    }
}
async fn acquire_location(root: Arc<Root>, location: &str) -> BlobResult<Lock> {
    require_location(location)?;
    acquire_lock(root, format!("{}.lock", &location[..2])).await
}
async fn acquire_lock(root: Arc<Root>, name: String) -> BlobResult<Lock> {
    let file = blocking(move || {
        root.check()?;
        let file = open(&root.locks, name.as_str(), OFlags::RDWR | OFlags::CREATE)?;
        check_file(&file)?;
        Ok(file)
    })
    .await?;
    // Never leave an uncancellable blocking flock waiting after the future is dropped.
    retry_lock(file).await
}
async fn retry_lock(mut file: File) -> BlobResult<Lock> {
    loop {
        let (next, acquired) = blocking(move || {
            let acquired = try_lock(&file)?;
            Ok((file, acquired))
        })
        .await?;
        if acquired {
            return Ok(Lock(next));
        }
        file = next;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn try_lock(file: &File) -> BlobResult<bool> {
    match fs::flock(file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::WOULDBLOCK) => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> BlobResult<T> + Send + 'static,
) -> BlobResult<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| BlobError::from(Code::StoreFailed))?
}

impl Root {
    fn check(&self) -> BlobResult<()> {
        check_dir(&self.directory)?;
        check_format(&self.directory)?;
        for (name, retained) in [
            ("objects", &self.objects),
            ("locks", &self.locks),
            ("staging", &self.staging),
            ("seals", &self.seals),
        ] {
            check_dir(retained)?;
            let current = open_directory(&self.directory, name)?;
            if !same_file(retained, &current)? {
                return Err(Code::PolicyInvalid.into());
            }
        }
        Ok(())
    }
}
fn initialize(path: &Path) -> BlobResult<Root> {
    let directory = open_path(path, true)?;
    check_dir(&directory)?;
    // Inspect before creating our initialization lock in an existing directory.
    if open_optional(&directory, "store.format", OFlags::RDONLY)?.is_some() {
        check_format(&directory)?;
    } else {
        for entry in fs::Dir::read_from(&directory).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if ![b".".as_slice(), b"..", b".initialize.lock"]
                .contains(&entry.file_name().to_bytes())
            {
                return Err(Code::PolicyInvalid.into());
            }
        }
    }
    let init_lock = open(
        &directory,
        ".initialize.lock",
        OFlags::RDWR | OFlags::CREATE,
    )?;
    check_file(&init_lock)?;
    // Provisioning runs explicitly. Concurrent initialization fails instead of blocking forever.
    if !try_lock(&init_lock)? {
        return Err(Code::StoreFailed.into());
    }
    if open_optional(&directory, "store.format", OFlags::RDONLY)?.is_none() {
        for entry in fs::Dir::read_from(&directory).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if ![b".".as_slice(), b"..", b".initialize.lock"]
                .contains(&entry.file_name().to_bytes())
            {
                return Err(Code::PolicyInvalid.into());
            }
        }
        let mut marker = create_file(&directory, "store.format")?;
        marker.write_all(FORMAT).map_err(io_error)?;
        marker.sync_all().map_err(io_error)?;
    }
    check_format(&directory)?;
    let mut directories = Vec::new();
    for name in ["objects", "locks", "staging", "seals"] {
        match fs::mkdirat(&directory, name, PRIVATE_DIR) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => (),
            Err(error) => return Err(io_error(error)),
        }
        let child = open_directory(&directory, name)?;
        check_dir(&child)?;
        directories.push(child);
    }
    fs::fsync(&directory).map_err(io_error)?;
    let mut iter = directories.into_iter();
    Ok(Root {
        directory,
        objects: iter.next().unwrap(),
        locks: iter.next().unwrap(),
        staging: iter.next().unwrap(),
        seals: iter.next().unwrap(),
    })
}
fn open_path(path: &Path, create_final: bool) -> BlobResult<File> {
    let mut directory = File::from(
        fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | SAFE_OPEN,
            Mode::empty(),
        )
        .map_err(io_error)?,
    );
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        match component {
            Component::RootDir | Component::CurDir => (),
            Component::Normal(name) => {
                if create_final && index == components.len() - 1 {
                    match fs::mkdirat(&directory, *name, PRIVATE_DIR) {
                        Ok(()) | Err(rustix::io::Errno::EXIST) => (),
                        Err(error) => return Err(io_error(error)),
                    }
                }
                directory = open_directory(&directory, name)?;
            }
            _ => return Err(Code::PolicyInvalid.into()),
        }
    }
    Ok(directory)
}
fn open_directory(parent: &File, name: impl AsRef<OsStr>) -> BlobResult<File> {
    Ok(File::from(
        fs::openat(
            parent,
            name.as_ref(),
            OFlags::RDONLY | OFlags::DIRECTORY | SAFE_OPEN,
            Mode::empty(),
        )
        .map_err(io_error)?,
    ))
}
fn open(parent: &File, name: &str, flags: OFlags) -> BlobResult<File> {
    Ok(File::from(
        fs::openat(parent, name, flags | SAFE_OPEN, PRIVATE_FILE).map_err(io_error)?,
    ))
}
fn create_file(parent: &File, name: &str) -> BlobResult<File> {
    open(parent, name, OFlags::RDWR | OFlags::CREATE | OFlags::EXCL)
}
fn open_optional(parent: &File, name: &str, flags: OFlags) -> BlobResult<Option<File>> {
    match fs::openat(parent, name, flags | SAFE_OPEN, Mode::empty()) {
        Ok(fd) => Ok(Some(File::from(fd))),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}
fn check_dir(file: &File) -> BlobResult<()> {
    let stat = fs::fstat(file).map_err(io_error)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
        || stat.st_uid != rustix::process::geteuid().as_raw()
        || stat.st_mode & 0o077 != 0
    {
        return Err(Code::PolicyInvalid.into());
    }
    Ok(())
}
fn check_file(file: &File) -> BlobResult<()> {
    let stat = fs::fstat(file).map_err(io_error)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || stat.st_nlink != 1
        || stat.st_uid != rustix::process::geteuid().as_raw()
        || stat.st_mode & 0o077 != 0
    {
        return Err(Code::StoreFailed.into());
    }
    Ok(())
}
fn same_file(a: &File, b: &File) -> BlobResult<bool> {
    let a = fs::fstat(a).map_err(io_error)?;
    let b = fs::fstat(b).map_err(io_error)?;
    Ok(a.st_dev == b.st_dev && a.st_ino == b.st_ino)
}
fn check_format(directory: &File) -> BlobResult<()> {
    let mut marker = open(directory, "store.format", OFlags::RDONLY)?;
    check_file(&marker)?;
    let mut bytes = vec![0; FORMAT.len()];
    if marker.metadata().map_err(io_error)?.len() != FORMAT.len() as u64 {
        return Err(Code::PolicyInvalid.into());
    }
    marker.read_exact(&mut bytes).map_err(io_error)?;
    if bytes != FORMAT {
        return Err(Code::PolicyInvalid.into());
    }
    Ok(())
}
fn stat(root: &Root, location: &str) -> BlobResult<Option<StoredBlob>> {
    open_optional(&root.objects, location, OFlags::RDONLY)?
        .map(|mut file| metadata(&mut file, location))
        .transpose()
}
fn metadata(file: &mut File, location: &str) -> BlobResult<StoredBlob> {
    check_file(file)?;
    let mut header = [0; HEADER as usize];
    file.read_exact(&mut header).map_err(io_error)?;
    let size = u64::from_be_bytes(header[24..32].try_into().unwrap());
    if &header[..8] != MAGIC
        || size > i64::MAX as u64 - HEADER
        || file.metadata().map_err(io_error)?.len() != HEADER + size
    {
        return Err(Code::StoreFailed.into());
    }
    StoredBlob::new(location, hex(&header[8..24]), size, hex(&header[32..]))
}
fn read_seal(root: &Root, location: &str) -> BlobResult<Option<String>> {
    let Some(mut file) = open_optional(&root.seals, location, OFlags::RDONLY)? else {
        return Ok(None);
    };
    check_file(&file)?;
    let size = file.metadata().map_err(io_error)?.len();
    if size != 14 && size != 45 {
        return Err(Code::StoreFailed.into());
    }
    let mut bytes = String::new();
    file.read_to_string(&mut bytes).map_err(io_error)?;
    let version = bytes
        .strip_prefix("TEAQL-SEAL1\n")
        .and_then(|s| s.strip_suffix('\n'))
        .ok_or_else(|| BlobError::from(Code::StoreFailed))?;
    if version != "-" && !is_hex(version, 32) {
        return Err(Code::StoreFailed.into());
    }
    Ok(Some(version.to_owned()))
}
fn require_unsealed(root: &Root, location: &str) -> BlobResult<()> {
    if read_seal(root, location)?.is_some() {
        Err(Code::UploadExpired.into())
    } else {
        Ok(())
    }
}
fn require_location(value: &str) -> BlobResult<()> {
    if is_hex(value, 64) {
        Ok(())
    } else {
        Err(Code::ReferenceInvalid.into())
    }
}
fn require_version(value: &str) -> BlobResult<()> {
    if is_hex(value, 32) {
        Ok(())
    } else {
        Err(Code::ReferenceInvalid.into())
    }
}
fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn random_hex() -> BlobResult<String> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| BlobError::from(Code::StoreFailed))?;
    Ok(hex(&bytes))
}
fn unhex_version(value: &str) -> BlobResult<[u8; 16]> {
    require_version(value)?;
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| BlobError::from(Code::StoreFailed))?;
    }
    Ok(bytes)
}
fn io_error(_: impl std::fmt::Debug) -> BlobError {
    Code::StoreFailed.into()
}
