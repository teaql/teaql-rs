//! Private bounded worker protocol. No request principal, locator, filename or key.
use crate::{VerificationLimits, io_error, parse_completed, process::WorkerResourceLimits};
use rustix::{
    fs::{self, FileType, Mode, OFlags},
    process::{self, Resource},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    process::{ExitCode, ExitStatus},
};
use teaql_blob_contracts::{BlobError, BlobErrorCode as Code, BlobResult};
pub(crate) const FORMATS: [&str; 10] = [
    "jpg", "jpeg", "png", "mp4", "mp3", "doc", "docx", "xls", "xlsx", "pdf",
];
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LimitsWire {
    input: u64,
    expanded: usize,
    entries: usize,
    text: usize,
    cells: usize,
}
impl From<&VerificationLimits> for LimitsWire {
    fn from(limits: &VerificationLimits) -> Self {
        Self {
            input: limits.maximum_input_bytes,
            expanded: limits.maximum_expanded_bytes,
            entries: limits.maximum_package_entries,
            text: limits.maximum_text_characters,
            cells: limits.maximum_spreadsheet_cells,
        }
    }
}
impl LimitsWire {
    fn into_limits(self) -> BlobResult<VerificationLimits> {
        let limits = VerificationLimits {
            maximum_input_bytes: self.input,
            maximum_expanded_bytes: self.expanded,
            maximum_package_entries: self.entries,
            maximum_text_characters: self.text,
            maximum_spreadsheet_cells: self.cells,
            concurrent_verifications: 1,
        };
        limits.validate()?;
        Ok(limits)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Envelope {
    schema: u8,
    version: String,
    body: Body,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(crate) enum Body {
    Probe {
        formats: Vec<String>,
        address_space: u64,
        cpu: u64,
        core: u64,
        file_size: u64,
        processes: u64,
        files: u64,
        stack: u64,
        no_new_privileges: bool,
        parent_death_signal: i32,
        euid: u32,
        effective_capabilities: u64,
        dumpable: bool,
    },
    Verified {
        content_type: String,
        size_bytes: u64,
        sha256: String,
    },
    Failure {
        code: String,
    },
}
pub(crate) fn decode(
    bytes: &[u8],
    status: ExitStatus,
    resources: WorkerResourceLimits,
) -> BlobResult<Body> {
    let reply: Envelope =
        serde_json::from_slice(bytes).map_err(|_| BlobError::from(Code::FormatNotAllowed))?;
    if reply.schema != 1 || reply.version != env!("CARGO_PKG_VERSION") {
        return Err(Code::CapabilityMissing.into());
    }
    match reply.body {
        Body::Failure { code } if status.code() == Some(2) => Err(match code.as_str() {
            "BLOB_SIZE_EXCEEDED" => Code::SizeExceeded,
            "BLOB_STORE_FAILED" => Code::StoreFailed,
            "BLOB_POLICY_INVALID" => Code::PolicyInvalid,
            "BLOB_CAPABILITY_MISSING" => Code::CapabilityMissing,
            _ => Code::FormatNotAllowed,
        }
        .into()),
        Body::Probe {
            ref formats,
            address_space,
            cpu,
            core,
            file_size,
            processes,
            files,
            stack,
            no_new_privileges,
            parent_death_signal,
            euid,
            effective_capabilities,
            dumpable,
        } if status.success() => {
            if formats.iter().map(String::as_str).ne(FORMATS)
                || address_space != resources.address_space_bytes
                || cpu != resources.cpu_seconds
                || core != 0
                || file_size != 0
                || processes != 0
                || files != 32
                || stack != 8388608
                || !no_new_privileges
                || parent_death_signal != libc::SIGKILL
                || euid == 0
                || effective_capabilities != 0
                || dumpable
            {
                return Err(Code::CapabilityMissing.into());
            }
            Ok(reply.body)
        }
        Body::Verified { .. } if status.success() => Ok(reply.body),
        _ => Err(Code::FormatNotAllowed.into()),
    }
}
fn hard(resource: Resource) -> BlobResult<u64> {
    let value = process::getrlimit(resource);
    match (value.current, value.maximum) {
        (Some(a), Some(b)) if a == b => Ok(a),
        _ => Err(Code::CapabilityMissing.into()),
    }
}
fn probe() -> BlobResult<Body> {
    let mut signal = 0;
    // SAFETY: fixed prctl operations and a valid stack output pointer; no shared state.
    let (privileges, dumpable) = unsafe {
        if libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) != 0
            || libc::prctl(libc::PR_GET_PDEATHSIG, &mut signal as *mut i32, 0, 0, 0) != 0
        {
            return Err(Code::CapabilityMissing.into());
        }
        (
            libc::prctl(libc::PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0),
            libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0),
        )
    };
    let status = std::fs::read_to_string("/proc/self/status").map_err(io_error)?;
    let mut real = 0;
    let mut effective = 0;
    let mut saved = 0;
    // SAFETY: all three outputs are distinct valid stack pointers. Reject a
    // retained privileged identity; RLIMIT_NPROC must apply to the real UID too.
    if unsafe { libc::getresuid(&mut real, &mut effective, &mut saved) } != 0
        || real == 0
        || real != effective
        || real != saved
    {
        return Err(Code::CapabilityMissing.into());
    }
    for name in ["CapPrm:", "CapAmb:"] {
        if status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
            != Some(0)
        {
            return Err(Code::CapabilityMissing.into());
        }
    }
    let capabilities = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
        .ok_or(Code::CapabilityMissing)?;
    let result = Body::Probe {
        formats: FORMATS.map(str::to_owned).into(),
        address_space: hard(Resource::As)?,
        cpu: hard(Resource::Cpu)?,
        core: hard(Resource::Core)?,
        file_size: hard(Resource::Fsize)?,
        processes: hard(Resource::Nproc)?,
        files: hard(Resource::Nofile)?,
        stack: hard(Resource::Stack)?,
        no_new_privileges: privileges == 1,
        parent_death_signal: signal,
        euid: process::geteuid().as_raw(),
        effective_capabilities: capabilities,
        dumpable: dumpable != 0,
    };
    if let Body::Probe {
        euid,
        effective_capabilities,
        no_new_privileges,
        parent_death_signal,
        core,
        file_size,
        processes,
        dumpable,
        ..
    } = &result
        && (*euid == 0
            || *effective_capabilities != 0
            || !*no_new_privileges
            || *parent_death_signal != libc::SIGKILL
            || *core != 0
            || *file_size != 0
            || *processes != 0
            || *dumpable)
    {
        return Err(Code::CapabilityMissing.into());
    }
    Ok(result)
}
fn operation() -> BlobResult<Body> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() == 1 && args[0] == "--probe-v1" {
        return probe();
    }
    if args.len() != 3
        || args[0] != "--verify-v1"
        || !FORMATS.contains(&args[1].as_str())
        || args[2].len() > 2048
    {
        return Err(Code::CapabilityMissing.into());
    }
    probe()?;
    let limits: LimitsWire =
        serde_json::from_str(&args[2]).map_err(|_| BlobError::from(Code::PolicyInvalid))?;
    let limits = limits.into_limits()?;
    let file = File::from(
        fs::open(
            "/proc/self/fd/0",
            OFlags::RDONLY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io_error)?,
    );
    let stat = fs::fstat(&file).map_err(io_error)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || stat.st_nlink != 0
        || stat.st_uid != process::geteuid().as_raw()
        || stat.st_mode & 0o077 != 0
    {
        return Err(Code::CapabilityMissing.into());
    }
    let size = file.metadata().map_err(io_error)?.len();
    if size == 0 || size > limits.maximum_input_bytes {
        return Err(Code::SizeExceeded.into());
    }
    let mut digest = Sha256::new();
    let mut input = &file;
    let mut buffer = [0; 65536];
    let mut read = 0;
    loop {
        let n = input.read(&mut buffer).map_err(io_error)?;
        if n == 0 {
            break;
        }
        read += n as u64;
        if read > size {
            return Err(Code::SizeExceeded.into());
        }
        digest.update(&buffer[..n]);
    }
    if read != size {
        return Err(Code::StoreFailed.into());
    }
    let facts = parse_completed(
        file,
        &args[1],
        &limits,
        size,
        format!("{:x}", digest.finalize()),
    )?;
    Ok(Body::Verified {
        content_type: facts.content_type().to_owned(),
        size_bytes: facts.size_bytes(),
        sha256: facts.sha256().to_owned(),
    })
}
/// Entrypoint for the packaged worker binary only. No fault/debug modes are accepted.
pub fn worker_main() -> ExitCode {
    let result = operation();
    let code = if result.is_ok() { 0 } else { 2 };
    let body = result.unwrap_or_else(|error| Body::Failure {
        code: error.code.code().to_owned(),
    });
    let reply = Envelope {
        schema: 1,
        version: env!("CARGO_PKG_VERSION").into(),
        body,
    };
    if let Ok(bytes) = serde_json::to_vec(&reply)
        && bytes.len() <= 4096
        && std::io::stdout().write_all(&bytes).is_ok()
    {
        ExitCode::from(code)
    } else {
        ExitCode::from(2)
    }
}
