//! Anonymous inodes have no crash leftovers or cleanup/publication race.
use crate::io_error;
use rustix::fs::{self, FileType, Mode, OFlags};
use std::{
    fs::File,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, OnceLock},
};
use teaql_blob_contracts::{BlobError, BlobErrorCode as Code, BlobResult};
const FORMAT: &[u8] = b"teaql.rust.blob-verifier-scratch/v1\n";
const SAFE: OFlags = OFlags::NOFOLLOW
    .union(OFlags::CLOEXEC)
    .union(OFlags::NONBLOCK);
#[derive(Clone)]
pub(crate) struct ScratchArea {
    configured: Arc<PathBuf>,
    root: Arc<OnceLock<File>>,
}
impl ScratchArea {
    pub fn new(configured: PathBuf) -> BlobResult<Self> {
        if !configured.is_absolute()
            || configured
                .components()
                .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(Code::PolicyInvalid.into());
        }
        Ok(Self {
            configured: Arc::new(configured),
            root: Arc::new(OnceLock::new()),
        })
    }
    pub async fn initialize(&self) -> BlobResult<()> {
        let area = self.clone();
        tokio::task::spawn_blocking(move || {
            let directory = open_path(&area.configured, true)?;
            check_directory(&directory)?;
            match fs::openat(
                &directory,
                "scratch.format",
                OFlags::RDONLY | SAFE,
                Mode::empty(),
            ) {
                Ok(_) => (),
                Err(rustix::io::Errno::NOENT) => {
                    for entry in fs::Dir::read_from(&directory).map_err(io_error)? {
                        let entry = entry.map_err(io_error)?;
                        if ![b".".as_slice(), b".."].contains(&entry.file_name().to_bytes()) {
                            return Err(Code::PolicyInvalid.into());
                        }
                    }
                    let mut marker = File::from(
                        fs::openat(
                            &directory,
                            "scratch.format",
                            OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | SAFE,
                            Mode::from_raw_mode(0o600),
                        )
                        .map_err(io_error)?,
                    );
                    marker.write_all(FORMAT).map_err(io_error)?;
                    marker.sync_all().map_err(io_error)?;
                    fs::fsync(&directory).map_err(io_error)?;
                }
                Err(error) => return Err(io_error(error)),
            }
            if let Some(old) = area.root.get() {
                if !same(old, &directory)? {
                    return Err(Code::PolicyInvalid.into());
                }
            } else {
                let _ = area.root.set(directory);
            }
            area.check()?;
            drop(area.anonymous()?);
            Ok(())
        })
        .await
        .map_err(|_| BlobError::from(Code::StoreFailed))?
    }
    pub async fn validate_ready(&self) -> BlobResult<()> {
        let area = self.clone();
        tokio::task::spawn_blocking(move || area.check())
            .await
            .map_err(|_| BlobError::from(Code::StoreFailed))?
    }
    pub async fn open(&self) -> BlobResult<File> {
        let area = self.clone();
        tokio::task::spawn_blocking(move || {
            area.check()?;
            area.anonymous()
        })
        .await
        .map_err(|_| BlobError::from(Code::StoreFailed))?
    }
    fn anonymous(&self) -> BlobResult<File> {
        let root = self.root.get().ok_or(Code::CapabilityMissing)?;
        let file = File::from(
            fs::openat(
                root,
                ".",
                OFlags::TMPFILE | OFlags::RDWR | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )
            .map_err(|_| BlobError::from(Code::CapabilityMissing))?,
        );
        let info = fs::fstat(&file).map_err(io_error)?;
        if info.st_nlink != 0
            || FileType::from_raw_mode(info.st_mode) != FileType::RegularFile
            || info.st_mode & 0o077 != 0
        {
            return Err(Code::StoreFailed.into());
        }
        Ok(file)
    }
    fn check(&self) -> BlobResult<()> {
        let root = self.root.get().ok_or(Code::CapabilityMissing)?;
        check_directory(root)?;
        let current = open_path(&self.configured, false)?;
        if !same(root, &current)? {
            return Err(Code::PolicyInvalid.into());
        }
        let mut marker = File::from(
            fs::openat(root, "scratch.format", OFlags::RDONLY | SAFE, Mode::empty())
                .map_err(io_error)?,
        );
        let stat = fs::fstat(&marker).map_err(io_error)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
            || stat.st_nlink != 1
            || stat.st_uid != rustix::process::geteuid().as_raw()
            || stat.st_mode & 0o077 != 0
            || stat.st_size != FORMAT.len() as i64
        {
            return Err(Code::PolicyInvalid.into());
        }
        let mut data = vec![0; FORMAT.len()];
        marker.read_exact(&mut data).map_err(io_error)?;
        if data != FORMAT {
            return Err(Code::PolicyInvalid.into());
        }
        Ok(())
    }
}
fn check_directory(file: &File) -> BlobResult<()> {
    let info = fs::fstat(file).map_err(io_error)?;
    if FileType::from_raw_mode(info.st_mode) != FileType::Directory
        || info.st_uid != rustix::process::geteuid().as_raw()
        || info.st_mode & 0o077 != 0
    {
        return Err(Code::PolicyInvalid.into());
    }
    Ok(())
}
fn same(a: &File, b: &File) -> BlobResult<bool> {
    let a = fs::fstat(a).map_err(io_error)?;
    let b = fs::fstat(b).map_err(io_error)?;
    Ok(a.st_dev == b.st_dev && a.st_ino == b.st_ino)
}
fn open_path(path: &Path, create: bool) -> BlobResult<File> {
    let mut parent = File::from(
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
                if create && index == parts.len() - 1 {
                    match fs::mkdirat(&parent, *name, Mode::from_raw_mode(0o700)) {
                        Ok(()) | Err(rustix::io::Errno::EXIST) => (),
                        Err(error) => return Err(io_error(error)),
                    }
                }
                parent = File::from(
                    fs::openat(
                        &parent,
                        *name,
                        OFlags::RDONLY | OFlags::DIRECTORY | SAFE,
                        Mode::empty(),
                    )
                    .map_err(io_error)?,
                );
            }
            _ => return Err(Code::PolicyInvalid.into()),
        }
    }
    Ok(parent)
}
