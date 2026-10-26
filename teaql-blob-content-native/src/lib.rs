//! Optional native verifier. No provisioning, parser or worker starts on construction.
#![deny(unsafe_op_in_unsafe_fn)]
#[cfg(not(target_os = "linux"))]
compile_error!("teaql-blob-content-native currently requires Linux anonymous scratch files");
mod anonymous;
mod formats;
mod process;
mod wire;
use futures_util::StreamExt;
pub use process::{
    ProcessBlobContentVerifier, ProcessVerifierStats, ProcessWorkerOutcome, WorkerExecutable,
    WorkerResourceLimits,
};
use sha2::{Digest, Sha256};
use std::{fs::File, path::PathBuf, sync::Arc};
use teaql_blob_reference::{
    BlobContentStream, BlobContentVerifier, BlobError, BlobErrorCode as Code, BlobResult,
    VerifiedBlobContent,
};
use tokio::{io::AsyncWriteExt, sync::Semaphore};
#[doc(hidden)]
pub use wire::worker_main;

#[derive(Clone, Debug)]
pub struct VerificationLimits {
    pub maximum_input_bytes: u64,
    pub maximum_expanded_bytes: usize,
    pub maximum_package_entries: usize,
    pub maximum_text_characters: usize,
    pub maximum_spreadsheet_cells: usize,
    pub concurrent_verifications: usize,
}
impl Default for VerificationLimits {
    fn default() -> Self {
        Self {
            maximum_input_bytes: 104857600,
            maximum_expanded_bytes: 104857600,
            maximum_package_entries: 10000,
            maximum_text_characters: 1048576,
            maximum_spreadsheet_cells: 1048576,
            concurrent_verifications: 2,
        }
    }
}
impl VerificationLimits {
    fn validate(&self) -> BlobResult<()> {
        if self.maximum_input_bytes == 0
            || self.maximum_input_bytes > i64::MAX as u64
            || self.maximum_input_bytes > usize::MAX as u64
            || self.maximum_expanded_bytes == 0
            || self.maximum_package_entries == 0
            || self.maximum_package_entries > 1000000
            || self.maximum_text_characters == 0
            || self.maximum_spreadsheet_cells == 0
            || !(1..=32).contains(&self.concurrent_verifications)
        {
            return Err(Code::PolicyInvalid.into());
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct NativeBlobContentVerifier {
    scratch: anonymous::ScratchArea,
    limits: VerificationLimits,
    permits: Arc<Semaphore>,
}
impl NativeBlobContentVerifier {
    pub fn new(scratch_root: impl Into<PathBuf>, limits: VerificationLimits) -> BlobResult<Self> {
        limits.validate()?;
        Ok(Self {
            scratch: anonymous::ScratchArea::new(scratch_root.into())?,
            permits: Arc::new(Semaphore::new(limits.concurrent_verifications)),
            limits,
        })
    }
    /// Explicitly provision/check the private root and anonymous-file filesystem support.
    pub async fn initialize(&self) -> BlobResult<()> {
        self.scratch.initialize().await
    }
}
#[async_trait::async_trait]
impl BlobContentVerifier for NativeBlobContentVerifier {
    async fn validate_ready(&self) -> BlobResult<()> {
        self.scratch.validate_ready().await
    }
    async fn verify(
        &self,
        source: BlobContentStream,
        extension: &str,
        maximum_bytes: u64,
        allow_empty: bool,
    ) -> BlobResult<VerifiedBlobContent> {
        teaql_core::blob::content_type(extension).ok_or(Code::FormatNotAllowed)?;
        if maximum_bytes == 0 {
            return Err(Code::PolicyInvalid.into());
        }
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| BlobError::from(Code::VerificationBusy))?;
        let (file, size, hash) = stage_input(
            &self.scratch,
            source,
            maximum_bytes.min(self.limits.maximum_input_bytes),
            allow_empty,
        )
        .await?;
        let limits = self.limits.clone();
        let extension = extension.to_owned();
        tokio::task::spawn_blocking(move || {
            // Keep the permit and anonymous inode until parsing really terminates, even
            // when the caller cancels its future. No detached replacement parser is started.
            let _permit = permit;
            parse_completed(file, &extension, &limits, size, hash)
        })
        .await
        .map_err(|_| BlobError::from(Code::FormatNotAllowed))?
    }
}
async fn stage_input(
    area: &anonymous::ScratchArea,
    mut source: BlobContentStream,
    maximum: u64,
    allow_empty: bool,
) -> BlobResult<(File, u64, String)> {
    if maximum == 0 {
        return Err(Code::PolicyInvalid.into());
    }
    let mut output = tokio::fs::File::from_std(area.open().await?);
    let mut digest = Sha256::new();
    let mut size = 0;
    while let Some(chunk) = source.next().await {
        let chunk = chunk.map_err(|_| BlobError::from(Code::StoreFailed))?;
        if chunk.len() as u64 > maximum - size {
            return Err(Code::SizeExceeded.into());
        }
        if chunk.is_empty() {
            tokio::task::yield_now().await;
        }
        for part in chunk.chunks(65536) {
            output.write_all(part).await.map_err(io_error)?;
            digest.update(part);
        }
        size += chunk.len() as u64;
    }
    if size == 0 {
        return Err(if allow_empty {
            Code::FormatNotAllowed
        } else {
            Code::SizeExceeded
        }
        .into());
    }
    output.flush().await.map_err(io_error)?;
    Ok((
        output.into_std().await,
        size,
        format!("{:x}", digest.finalize()),
    ))
}
fn parse_completed(
    file: File,
    extension: &str,
    limits: &VerificationLimits,
    size: u64,
    hash: String,
) -> BlobResult<VerifiedBlobContent> {
    let expected = teaql_core::blob::content_type(extension).ok_or(Code::FormatNotAllowed)?;
    if file.metadata().map_err(io_error)?.len() != size
        || size == 0
        || size > limits.maximum_input_bytes
    {
        return Err(Code::SizeExceeded.into());
    }
    // SAFETY: the exclusively owned anonymous inode has no payload pathname or writer.
    // All writes ended before transfer. Parsers only read/seek, and the file outlives
    // this read-only mapping. The process worker additionally reopens stdin read-only.
    let mapped =
        unsafe { memmap2::MmapOptions::new().len(size as usize).map(&file) }.map_err(io_error)?;
    formats::verify(&mapped, &file, extension, limits)?;
    VerifiedBlobContent::new(expected, size, hash)
}
fn io_error(_: impl std::fmt::Debug) -> BlobError {
    Code::StoreFailed.into()
}
