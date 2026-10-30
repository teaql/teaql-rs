//! Resource-contained native worker. No process, file or probe on construction.
use crate::{
    VerificationLimits,
    anonymous::ScratchArea,
    io_error, stage_input,
    wire::{self, Body, LimitsWire},
};
use rustix::{
    fs::{self, FileType, Mode, OFlags},
    process::{self, Resource, Rlimit, Signal},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fmt,
    fs::File,
    io::Read,
    os::{
        fd::AsRawFd,
        unix::process::{CommandExt, ExitStatusExt},
    },
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use teaql_blob_contracts::{
    BlobContentStream, BlobContentVerifier, BlobError, BlobErrorCode as Code, BlobResult,
    VerifiedBlobContent,
};
use tokio::sync::Semaphore;
const MAX_REPLY: usize = 4096;
const MAX_EXECUTABLE: u64 = 134217728;
const SAFE: OFlags = OFlags::NOFOLLOW
    .union(OFlags::CLOEXEC)
    .union(OFlags::NONBLOCK);

#[derive(Clone)]
pub struct WorkerExecutable {
    path: PathBuf,
    sha256: String,
}
impl WorkerExecutable {
    /// Pin an explicitly installed immutable ELF artifact. Never searches PATH.
    pub fn new(path: impl Into<PathBuf>, sha256: impl Into<String>) -> BlobResult<Self> {
        let path = path.into();
        let sha256 = sha256.into();
        if !path.is_absolute()
            || path.components().any(|c| matches!(c, Component::ParentDir))
            || sha256.len() != 64
            || !sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Code::PolicyInvalid.into());
        }
        Ok(Self { path, sha256 })
    }
    fn open(&self, cancel: &AtomicBool, deadline: Instant) -> BlobResult<File> {
        let file = open_executable(&self.path)?;
        let stat = fs::fstat(&file).map_err(io_error)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
            || stat.st_mode & 0o6222 != 0
            || stat.st_mode & 0o111 == 0
            || (stat.st_uid != 0 && stat.st_uid != process::geteuid().as_raw())
            || stat.st_size <= 0
            || stat.st_size as u64 > MAX_EXECUTABLE
        {
            return Err(Code::CapabilityMissing.into());
        }
        let mut reader = &file;
        let mut digest = Sha256::new();
        let mut buffer = [0; 65536];
        let mut count = 0;
        loop {
            if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
                return Err(Code::FormatNotAllowed.into());
            }
            let n = reader.read(&mut buffer).map_err(io_error)?;
            if n == 0 {
                break;
            }
            if count == 0 && buffer[..n].get(..4) != Some(b"\x7fELF") {
                return Err(Code::CapabilityMissing.into());
            }
            count += n as u64;
            if count > MAX_EXECUTABLE {
                return Err(Code::CapabilityMissing.into());
            }
            digest.update(&buffer[..n]);
        }
        if count != stat.st_size as u64 || format!("{:x}", digest.finalize()) != self.sha256 {
            return Err(Code::CapabilityMissing.into());
        }
        Ok(file)
    }
}
impl fmt::Debug for WorkerExecutable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WorkerExecutable([redacted])")
    }
}
#[derive(Clone, Copy, Debug)]
pub struct WorkerResourceLimits {
    pub address_space_bytes: u64,
    pub cpu_seconds: u64,
    pub wall_time: Duration,
}
impl Default for WorkerResourceLimits {
    fn default() -> Self {
        Self {
            address_space_bytes: 1073741824,
            cpu_seconds: 15,
            wall_time: Duration::from_secs(30),
        }
    }
}
impl WorkerResourceLimits {
    fn validate(self) -> BlobResult<()> {
        if !(67108864..=17179869184).contains(&self.address_space_bytes)
            || !self.address_space_bytes.is_multiple_of(4096)
            || !(1..=3600).contains(&self.cpu_seconds)
            || self.wall_time.is_zero()
            || self.wall_time > Duration::from_secs(3600)
        {
            Err(Code::PolicyInvalid.into())
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessWorkerOutcome {
    Exited {
        code: Option<i32>,
        signal: Option<i32>,
    },
    Cancelled,
    TimedOut,
    OutputLimit,
    IoFailed,
}
#[derive(Clone, Debug, Default)]
pub struct ProcessVerifierStats {
    pub started: u64,
    pub reaped: u64,
    pub running: BTreeSet<u32>,
    pub last_outcome: Option<ProcessWorkerOutcome>,
}
#[derive(Clone)]
pub struct ProcessBlobContentVerifier {
    scratch: ScratchArea,
    limits: VerificationLimits,
    resources: WorkerResourceLimits,
    worker: WorkerExecutable,
    permits: Arc<Semaphore>,
    stats: Arc<Mutex<ProcessVerifierStats>>,
}
impl ProcessBlobContentVerifier {
    pub fn new(
        scratch_root: impl Into<PathBuf>,
        limits: VerificationLimits,
        worker: WorkerExecutable,
        resources: WorkerResourceLimits,
    ) -> BlobResult<Self> {
        limits.validate()?;
        resources.validate()?;
        Ok(Self {
            scratch: ScratchArea::new(scratch_root.into())?,
            permits: Arc::new(Semaphore::new(limits.concurrent_verifications)),
            limits,
            resources,
            worker,
            stats: Default::default(),
        })
    }
    pub async fn initialize(&self) -> BlobResult<()> {
        self.scratch.initialize().await?;
        self.validate_ready().await
    }
    /// Bounded host diagnostics: no payload, path, digest, principal or storage locator.
    pub fn stats(&self) -> ProcessVerifierStats {
        self.stats.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    async fn invoke(&self, input: Option<File>, extension: Option<String>) -> BlobResult<Body> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| BlobError::from(Code::VerificationBusy))?;
        self.invoke_with_permit(input, extension, permit).await
    }
    async fn invoke_with_permit(
        &self,
        input: Option<File>,
        extension: Option<String>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> BlobResult<Body> {
        let cancel = Arc::new(AtomicBool::new(false));
        let _guard = Cancel(cancel.clone());
        let worker = self.worker.clone();
        let limits = self.limits.clone();
        let resources = self.resources;
        let stats = self.stats.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let (status, bytes) =
                execute(worker, input, extension, limits, resources, cancel, stats)?;
            wire::decode(&bytes, status, resources)
        })
        .await
        .map_err(|_| BlobError::from(Code::FormatNotAllowed))?
    }
}
#[async_trait::async_trait]
impl BlobContentVerifier for ProcessBlobContentVerifier {
    async fn validate_ready(&self) -> BlobResult<()> {
        self.scratch.validate_ready().await?;
        match self.invoke(None, None).await? {
            Body::Probe { .. } => Ok(()),
            _ => Err(Code::CapabilityMissing.into()),
        }
    }
    async fn verify(
        &self,
        source: BlobContentStream,
        extension: &str,
        maximum_bytes: u64,
        allow_empty: bool,
    ) -> BlobResult<VerifiedBlobContent> {
        let expected = teaql_core::blob::content_type(extension).ok_or(Code::FormatNotAllowed)?;
        if maximum_bytes == 0 {
            return Err(Code::PolicyInvalid.into());
        }
        // This slot also covers streaming input, executable validation and actual reaping.
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| BlobError::from(Code::VerificationBusy))?;
        let (file, size, sha256) = stage_input(
            &self.scratch,
            source,
            maximum_bytes.min(self.limits.maximum_input_bytes),
            allow_empty,
        )
        .await?;
        match self
            .invoke_with_permit(Some(file), Some(extension.to_owned()), permit)
            .await?
        {
            Body::Verified {
                content_type,
                size_bytes,
                sha256: verified_sha,
            } if content_type == expected && size_bytes == size && verified_sha == sha256 => {
                VerifiedBlobContent::new(content_type, size, sha256)
            }
            _ => Err(Code::FormatNotAllowed.into()),
        }
    }
}
struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct OwnedChild {
    child: Child,
    stats: Arc<Mutex<ProcessVerifierStats>>,
    outcome: Option<ProcessWorkerOutcome>,
    recorded: bool,
}
impl OwnedChild {
    fn new(child: Child, stats: Arc<Mutex<ProcessVerifierStats>>) -> Self {
        let mut state = stats.lock().unwrap_or_else(|e| e.into_inner());
        state.started = state.started.saturating_add(1);
        state.running.insert(child.id());
        drop(state);
        Self {
            child,
            stats,
            outcome: None,
            recorded: false,
        }
    }
    fn record(&mut self, status: ExitStatus) {
        let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        stats.running.remove(&self.child.id());
        stats.reaped = stats.reaped.saturating_add(1);
        stats.last_outcome = Some(self.outcome.unwrap_or(ProcessWorkerOutcome::Exited {
            code: status.code(),
            signal: status.signal(),
        }));
        self.recorded = true;
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.recorded {
            // The unreaped child owns this fresh session/group, so its PID cannot be reused.
            if let Some(pid) = process::Pid::from_raw(self.child.id() as i32) {
                let _ = process::kill_process_group(pid, Signal::KILL);
            }
            let _ = self.child.kill();
            if let Ok(status) = self.child.wait() {
                self.record(status);
            }
        }
    }
}
fn execute(
    worker: WorkerExecutable,
    input: Option<File>,
    extension: Option<String>,
    limits: VerificationLimits,
    resources: WorkerResourceLimits,
    cancel: Arc<AtomicBool>,
    stats: Arc<Mutex<ProcessVerifierStats>>,
) -> BlobResult<(ExitStatus, Vec<u8>)> {
    let deadline = Instant::now()
        .checked_add(resources.wall_time)
        .ok_or(Code::PolicyInvalid)?;
    let executable = worker.open(&cancel, deadline)?;
    // Execute the exact opened/hash-checked inode. Atomic deployment path replacement
    // cannot substitute another executable between validation and execve.
    let mut command = Command::new(format!("/proc/self/fd/{}", executable.as_raw_fd()));
    command
        .arg0("teaql-blob-content-worker")
        .current_dir("/")
        .env_clear()
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(extension) = extension {
        command.arg("--verify-v1").arg(extension).arg(
            serde_json::to_string(&LimitsWire::from(&limits))
                .map_err(|_| BlobError::from(Code::PolicyInvalid))?,
        );
    } else {
        command.arg("--probe-v1");
    }
    let input = input
        .map(|file| {
            // Drop the staging writer before spawn; even inherited fd 0 is read-only.
            fs::open(
                format!("/proc/self/fd/{}", file.as_raw_fd()),
                OFlags::RDONLY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(io_error)
        })
        .transpose()?;
    command.stdin(input.map_or_else(Stdio::null, Stdio::from));
    let parent = process::getpid();
    // SAFETY: after fork this callback uses only direct async-signal-safe syscalls,
    // copied primitive limits/PID and raw errno construction. No allocation, lock,
    // logger, environment access or Rust runtime operation occurs before exec.
    unsafe {
        command.pre_exec(move || {
            // Preserve the exec-error pipe and hash-pinned executable until exec,
            // but inherit no host descriptor beyond stdin/stdout/stderr afterwards.
            if libc::syscall(
                libc::SYS_close_range,
                3u32,
                u32::MAX,
                libc::CLOSE_RANGE_CLOEXEC,
            ) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            for (resource, value) in [
                (Resource::As, resources.address_space_bytes),
                (Resource::Cpu, resources.cpu_seconds),
                (Resource::Core, 0),
                (Resource::Fsize, 0),
                (Resource::Nproc, 0),
                (Resource::Nofile, 32),
                (Resource::Stack, 8388608),
            ] {
                process::setrlimit(
                    resource,
                    Rlimit {
                        current: Some(value),
                        maximum: Some(value),
                    },
                )
                .map_err(errno)?;
            }
            process::setsid().map_err(errno)?;
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
                || libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            if process::getppid() != Some(parent) {
                return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
            }
            Ok(())
        });
    }
    if cancel.load(Ordering::Acquire) {
        return Err(Code::FormatNotAllowed.into());
    }
    let child = command
        .spawn()
        .map_err(|_| BlobError::from(Code::CapabilityMissing))?;
    let mut child = OwnedChild::new(child, stats);
    let mut stdout = child.child.stdout.take().ok_or(Code::CapabilityMissing)?;
    let flags = fs::fcntl_getfl(&stdout).map_err(io_error)?;
    fs::fcntl_setfl(&stdout, flags | OFlags::NONBLOCK).map_err(io_error)?;
    let mut output = OutputState::default();
    loop {
        if cancel.load(Ordering::Acquire) {
            child.outcome = Some(ProcessWorkerOutcome::Cancelled);
            return Err(Code::FormatNotAllowed.into());
        }
        if Instant::now() >= deadline {
            child.outcome = Some(ProcessWorkerOutcome::TimedOut);
            return Err(Code::FormatNotAllowed.into());
        }
        match poll_output(&mut stdout, &mut output, || {
            let status = child.child.try_wait()?;
            if let Some(status) = status {
                child.record(status);
            }
            Ok(status)
        }) {
            Ok(Some(status)) => return Ok((status, output.bytes)),
            Ok(None) => (),
            Err(error) => {
                child.outcome = Some(if matches!(error, OutputError::Limit) {
                    ProcessWorkerOutcome::OutputLimit
                } else {
                    ProcessWorkerOutcome::IoFailed
                });
                return Err(match error {
                    OutputError::Wait(error) => io_error(error),
                    _ => Code::FormatNotAllowed.into(),
                });
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
#[derive(Default)]
struct OutputState {
    bytes: Vec<u8>,
    eof: bool,
    status: Option<ExitStatus>,
}
#[derive(Debug)]
enum OutputError {
    Limit,
    Read,
    Wait(std::io::Error),
}
fn poll_output(
    stdout: &mut impl Read,
    output: &mut OutputState,
    mut observe_exit: impl FnMut() -> std::io::Result<Option<ExitStatus>>,
) -> Result<Option<ExitStatus>, OutputError> {
    let mut buffer = [0; 512];
    loop {
        match stdout.read(&mut buffer) {
            Ok(0) => {
                output.eof = true;
                break;
            }
            Ok(n) => {
                if n > MAX_REPLY - output.bytes.len() {
                    return Err(OutputError::Limit);
                }
                output.bytes.extend_from_slice(&buffer[..n]);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(OutputError::Read),
        }
    }
    // Exit and pipe EOF are independent observations. The worker can publish
    // its reply between the nonblocking read and try_wait; retain its reaped
    // status and drain the pipe on following polls instead of rejecting bytes.
    if output.status.is_none() {
        output.status = observe_exit().map_err(OutputError::Wait)?;
    }
    Ok(if output.eof { output.status } else { None })
}

#[cfg(test)]
mod output_tests;

fn errno(error: rustix::io::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error.raw_os_error())
}
fn open_executable(path: &Path) -> BlobResult<File> {
    let mut directory = File::from(
        fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | SAFE,
            Mode::empty(),
        )
        .map_err(io_error)?,
    );
    let parts: Vec<_> = path.components().collect();
    for (index, part) in parts.iter().enumerate() {
        match part {
            Component::RootDir | Component::CurDir => (),
            Component::Normal(name) => {
                let flags = if index == parts.len() - 1 {
                    OFlags::RDONLY
                } else {
                    OFlags::RDONLY | OFlags::DIRECTORY
                };
                directory = File::from(
                    fs::openat(&directory, *name, flags | SAFE, Mode::empty())
                        .map_err(|_| BlobError::from(Code::CapabilityMissing))?,
                );
            }
            _ => return Err(Code::PolicyInvalid.into()),
        }
    }
    Ok(directory)
}
