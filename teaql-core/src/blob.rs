//! Stable references and model restrictions, independent of storage and cryptography.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::BTreeSet, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobReferenceError;
impl fmt::Display for BlobReferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BLOB_REFERENCE_INVALID")
    }
}
impl std::error::Error for BlobReferenceError {}

/// Only a stable Key; knowing it conveys no authorization.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobRef(String);
impl BlobRef {
    pub fn new(key: impl Into<String>) -> Result<Self, BlobReferenceError> {
        let key = key.into();
        if !(6..=128).contains(&key.len())
            || !key.starts_with("blob_")
            || !key.as_bytes()[5..]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
        {
            return Err(BlobReferenceError);
        }
        Ok(Self(key))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for BlobRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobRef[masked]")
    }
}
impl Serialize for BlobRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de> Deserialize<'de> for BlobRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
impl From<BlobRef> for crate::Value {
    fn from(value: BlobRef) -> Self {
        Self::BlobRef(value)
    }
}
impl From<&BlobRef> for crate::Value {
    fn from(value: &BlobRef) -> Self {
        Self::BlobRef(value.clone())
    }
}

/// Canonical model restrictions. A trusted server policy may only narrow the formats.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobFieldSpec {
    entity: String,
    field: String,
    allowed_extensions: BTreeSet<String>,
    nullable: bool,
    policy_profile: String,
}
impl BlobFieldSpec {
    pub const CAPABILITY: &'static str = "blob.v1";
    pub fn new(
        entity: impl Into<String>,
        field: impl Into<String>,
        extensions: impl IntoIterator<Item = impl Into<String>>,
        nullable: bool,
        profile: impl Into<String>,
    ) -> Result<Self, &'static str> {
        let entity = entity.into();
        let field = field.into();
        let policy_profile = profile.into();
        if entity.trim().is_empty() || field.trim().is_empty() || policy_profile.trim().is_empty() {
            return Err("BLOB_POLICY_INVALID");
        }
        let allowed_extensions: BTreeSet<String> = extensions.into_iter().map(Into::into).collect();
        if allowed_extensions.is_empty()
            || allowed_extensions.iter().any(|e| content_type(e).is_none())
        {
            return Err("BLOB_FORMAT_NOT_ALLOWED");
        }
        Ok(Self {
            entity,
            field,
            allowed_extensions,
            nullable,
            policy_profile,
        })
    }
    pub fn entity(&self) -> &str {
        &self.entity
    }
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn allowed_extensions(&self) -> &BTreeSet<String> {
        &self.allowed_extensions
    }
    pub fn allowed_content_types(&self) -> BTreeSet<&'static str> {
        self.allowed_extensions
            .iter()
            .filter_map(|e| content_type(e))
            .collect()
    }
    pub fn nullable(&self) -> bool {
        self.nullable
    }
    pub fn policy_profile(&self) -> &str {
        &self.policy_profile
    }
    pub fn protocol_version(&self) -> &'static str {
        Self::CAPABILITY
    }
    pub fn max_files(&self) -> usize {
        1
    }
}
pub fn content_type(extension: &str) -> Option<&'static str> {
    Some(match extension {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "mp4" => "video/mp4",
        "mp3" => "audio/mpeg",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pdf" => "application/pdf",
        _ => return None,
    })
}
