use crate::{BlobErrorCode, BlobResult, add_seconds, name};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
pub use teaql_blob_contracts::{CachePolicy, Disposition, FORMATS};
use teaql_core::{BlobFieldSpec, blob::content_type};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UploadMode {
    ServerStream,
    Direct,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Sharing {
    AuthorizedSameScope,
    SingleBinding,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResponseShape {
    Url,
    Descriptor,
}
#[derive(Clone, Debug)]
pub struct PolicyLimits {
    pub max_upload_size_bytes: u64,
    pub read_url_maximum_seconds: u64,
    pub allowed_extensions: BTreeSet<String>,
    pub inline_content_types: BTreeSet<String>,
}
impl Default for PolicyLimits {
    fn default() -> Self {
        Self {
            max_upload_size_bytes: 104857600,
            read_url_maximum_seconds: 3600,
            allowed_extensions: FORMATS.into_iter().map(str::to_owned).collect(),
            inline_content_types: ["image/jpeg", "image/png", "audio/mpeg", "video/mp4"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct PolicyOverrides {
    pub max_size_bytes: Option<u64>,
    pub allow_empty: Option<bool>,
    pub allowed_extensions: Option<BTreeSet<String>>,
    pub upload_session_seconds: Option<u64>,
    pub upload_url_seconds: Option<u64>,
    pub unbound_seconds: Option<u64>,
    pub unreferenced_seconds: Option<u64>,
    pub minimum_seconds: Option<u64>,
    pub read_url_seconds: Option<u64>,
    pub upload_mode: Option<UploadMode>,
    pub sharing: Option<Sharing>,
    pub disposition: Option<Disposition>,
    pub response_shape: Option<ResponseShape>,
    pub cache_policy: Option<CachePolicy>,
}
#[derive(Clone, Debug)]
pub struct BlobPolicyProfile {
    pub name: String,
    pub version: u64,
    pub overrides: PolicyOverrides,
}
impl BlobPolicyProfile {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: 1,
            overrides: Default::default(),
        }
    }
}

macro_rules! snapshot {
    ($( $(#[$attribute:meta])* $visibility:vis $field:ident:$kind:ty ),* $(,)?)=>{
        #[derive(Clone,Debug,PartialEq,Eq,Serialize)]
        pub struct BlobPolicySnapshot { $( $(#[$attribute])* $visibility $field:$kind ),* }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct PolicyWire { $( $(#[$attribute])* $field:$kind ),* }
        impl<'de> Deserialize<'de> for BlobPolicySnapshot {
            fn deserialize<D:serde::Deserializer<'de>>(input:D)->Result<Self,D::Error> {
                let wire=PolicyWire::deserialize(input)?;
                let result=Self { $( $field:wire.$field ),* };
                result.validate().map_err(serde::de::Error::custom)?; Ok(result)
            }
        }
    }
}
// Both direct Serde and the bounded from_json path enforce the same closed invariants.
snapshot! {
    schema:u32,pub(crate) profile:String,pub(crate) version:u64,pub(crate) entity:String,pub(crate) field:String,
    #[serde(rename="fieldProfile")] field_profile:String,
    pub(crate) nullable:bool,
    #[serde(deserialize_with="unique_extensions")] pub(crate) extensions:BTreeSet<String>,
    #[serde(rename="maxSize")] pub(crate) max_size:u64,
    #[serde(rename="allowEmpty")] pub(crate) allow_empty:bool,
    #[serde(rename="uploadSession")] pub(crate) upload_session:u64,
    #[serde(rename="uploadUrl")] pub(crate) upload_url:u64,
    pub(crate) unbound:u64,pub(crate) unreferenced:u64,pub(crate) minimum:u64,
    #[serde(rename="readUrl")] pub(crate) read_url:u64,
    #[serde(rename="uploadMode")] pub(crate) upload_mode:UploadMode,
    pub(crate) sharing:Sharing,pub(crate) disposition:Disposition,
    #[serde(rename="responseShape")] pub(crate) response_shape:ResponseShape,pub(crate) cache:String,
}
fn unique_extensions<'de, D: serde::Deserializer<'de>>(
    input: D,
) -> Result<BTreeSet<String>, D::Error> {
    let values = Vec::<String>::deserialize(input)?;
    let expected = values.len();
    let unique: BTreeSet<_> = values.into_iter().collect();
    if unique.len() != expected {
        Err(serde::de::Error::custom("duplicate extension"))
    } else {
        Ok(unique)
    }
}
impl BlobPolicySnapshot {
    pub fn profile(&self) -> &str {
        &self.profile
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn field_spec(&self) -> BlobResult<BlobFieldSpec> {
        BlobFieldSpec::new(
            &self.entity,
            &self.field,
            self.extensions.iter().cloned(),
            self.nullable,
            &self.field_profile,
        )
        .map_err(|_| BlobErrorCode::PolicyInvalid.into())
    }
    pub fn max_size_bytes(&self) -> u64 {
        self.max_size
    }
    pub fn upload_session_seconds(&self) -> u64 {
        self.upload_session
    }
    pub fn unbound_seconds(&self) -> u64 {
        self.unbound
    }
    pub fn unreferenced_seconds(&self) -> u64 {
        self.unreferenced
    }
    pub fn minimum_seconds(&self) -> u64 {
        self.minimum
    }
    pub fn read_url_seconds(&self) -> u64 {
        self.read_url
    }
    pub fn allow_empty(&self) -> bool {
        self.allow_empty
    }
    pub fn sharing(&self) -> Sharing {
        self.sharing
    }
    pub fn upload_mode(&self) -> UploadMode {
        self.upload_mode
    }
    pub fn response_shape(&self) -> ResponseShape {
        self.response_shape
    }
    pub fn cache_policy(&self) -> CachePolicy {
        self.cache
            .strip_prefix("private, max-age=")
            .and_then(|value| value.parse().ok())
            .map_or(CachePolicy::NoStore, CachePolicy::PrivateMaxAge)
    }
    pub fn to_json(&self) -> BlobResult<String> {
        self.validate()?;
        serde_json::to_string(self).map_err(|_| BlobErrorCode::PolicyInvalid.into())
    }
    pub fn from_json(json: &str) -> BlobResult<Self> {
        if json.len() > 16384 {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        let value: Self = serde_json::from_str(json).map_err(|_| BlobErrorCode::PolicyInvalid)?;
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> BlobResult<()> {
        if self.schema != 1
            || self.version == 0
            || self.version > i64::MAX as u64
            || self.max_size == 0
            || self.max_size > i64::MAX as u64
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        for value in [
            &self.profile,
            &self.field_profile,
            &self.entity,
            &self.field,
        ] {
            name(value, 128).map_err(|_| BlobErrorCode::PolicyInvalid)?;
        }
        self.field_spec()?;
        for duration in [self.upload_session, self.upload_url, self.read_url] {
            if duration == 0 {
                return Err(BlobErrorCode::PolicyInvalid.into());
            }
        }
        for duration in [
            self.upload_session,
            self.upload_url,
            self.unbound,
            self.unreferenced,
            self.minimum,
            self.read_url,
        ] {
            add_seconds(0, duration)?;
        }
        if self.cache != "private, no-store" {
            let value = self
                .cache
                .strip_prefix("private, max-age=")
                .filter(|value| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(BlobErrorCode::PolicyInvalid)?;
            if value > self.read_url {
                return Err(BlobErrorCode::PolicyInvalid.into());
            }
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct BlobPolicies {
    limits: PolicyLimits,
    defaults: BlobPolicyProfile,
    profiles: BTreeMap<String, BlobPolicyProfile>,
    fields: BTreeMap<String, String>,
}
impl Default for BlobPolicies {
    fn default() -> Self {
        Self::new(
            PolicyLimits::default(),
            BlobPolicyProfile::named("default"),
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("valid built-ins")
    }
}
impl BlobPolicies {
    pub fn new(
        limits: PolicyLimits,
        defaults: BlobPolicyProfile,
        profiles: BTreeMap<String, BlobPolicyProfile>,
        fields: BTreeMap<String, String>,
    ) -> BlobResult<Self> {
        if limits.max_upload_size_bytes == 0
            || limits.max_upload_size_bytes > i64::MAX as u64
            || limits.read_url_maximum_seconds == 0
            || limits
                .allowed_extensions
                .iter()
                .any(|value| content_type(value).is_none())
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        add_seconds(0, limits.read_url_maximum_seconds)?;
        for (key, profile) in &profiles {
            if key != &profile.name || key == "default" {
                return Err(BlobErrorCode::PolicyInvalid.into());
            }
        }
        if fields
            .values()
            .any(|name| name != "default" && !profiles.contains_key(name))
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        let result = Self {
            limits,
            defaults,
            profiles,
            fields,
        };
        // Resolve all configured profiles against a format universe before host model readiness.
        for profile in std::iter::once(&result.defaults).chain(result.profiles.values()) {
            let field = BlobFieldSpec::new("Policy", "validation", FORMATS, true, &profile.name)
                .map_err(|_| BlobErrorCode::PolicyInvalid)?;
            result.resolve_for(&field, false)?;
        }
        Ok(result)
    }
    pub fn validate_model<'a>(
        &self,
        fields: impl IntoIterator<Item = &'a BlobFieldSpec>,
    ) -> BlobResult<()> {
        let mut found = BTreeSet::new();
        for field in fields {
            found.insert(format!("{}.{}", field.entity(), field.field()));
            self.resolve(field)?;
        }
        if self.fields.keys().any(|key| !found.contains(key)) {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        Ok(())
    }
    pub fn resolve(&self, model: &BlobFieldSpec) -> BlobResult<BlobPolicySnapshot> {
        self.resolve_for(model, true)
    }
    fn resolve_for(
        &self,
        model: &BlobFieldSpec,
        check_inline: bool,
    ) -> BlobResult<BlobPolicySnapshot> {
        let name = self
            .fields
            .get(&format!("{}.{}", model.entity(), model.field()))
            .map_or(model.policy_profile(), String::as_str);
        let profile = if name == "default" {
            &self.defaults
        } else {
            self.profiles
                .get(name)
                .ok_or(BlobErrorCode::PolicyInvalid)?
        };
        let mut result = BlobPolicySnapshot {
            schema: 1,
            profile: name.to_owned(),
            version: profile.version,
            entity: model.entity().into(),
            field: model.field().into(),
            field_profile: name.to_owned(),
            nullable: model.nullable(),
            extensions: FORMATS.into_iter().map(str::to_owned).collect(),
            max_size: 104857600,
            allow_empty: false,
            upload_session: 3600,
            upload_url: 900,
            unbound: 86400,
            unreferenced: 604800,
            minimum: 0,
            read_url: 600,
            upload_mode: UploadMode::ServerStream,
            sharing: Sharing::AuthorizedSameScope,
            disposition: Disposition::Auto,
            response_shape: ResponseShape::Url,
            cache: "private, no-store".into(),
        };
        for overrides in [&self.defaults.overrides, &profile.overrides] {
            macro_rules! apply {
                ($field:ident,$from:ident) => {
                    if let Some(value) = overrides.$from {
                        result.$field = value;
                    }
                };
            }
            apply!(max_size, max_size_bytes);
            apply!(allow_empty, allow_empty);
            apply!(upload_session, upload_session_seconds);
            apply!(upload_url, upload_url_seconds);
            apply!(unbound, unbound_seconds);
            apply!(unreferenced, unreferenced_seconds);
            apply!(minimum, minimum_seconds);
            apply!(read_url, read_url_seconds);
            apply!(upload_mode, upload_mode);
            apply!(sharing, sharing);
            apply!(disposition, disposition);
            apply!(response_shape, response_shape);
            if let Some(formats) = &overrides.allowed_extensions {
                result.extensions = formats.clone();
            }
            if let Some(cache) = overrides.cache_policy {
                result.cache = match cache {
                    CachePolicy::NoStore => "private, no-store".into(),
                    CachePolicy::PrivateMaxAge(seconds) => format!("private, max-age={seconds}"),
                };
            }
        }
        if result
            .extensions
            .iter()
            .any(|value| content_type(value).is_none())
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        result.extensions.retain(|extension| {
            model.allowed_extensions().contains(extension)
                && self.limits.allowed_extensions.contains(extension)
        });
        if result.max_size > self.limits.max_upload_size_bytes || result.extensions.is_empty() {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        if check_inline
            && result.disposition == Disposition::Inline
            && result.extensions.iter().any(|extension| {
                !self
                    .limits
                    .inline_content_types
                    .contains(content_type(extension).expect("validated format"))
            })
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        result.validate()?;
        Ok(result)
    }
    pub fn require_upload_format(&self, extension: &str) -> BlobResult<()> {
        if self.limits.allowed_extensions.contains(extension) {
            Ok(())
        } else {
            Err(BlobErrorCode::FormatNotAllowed.into())
        }
    }
    pub fn maximum_upload_size_bytes(&self) -> u64 {
        self.limits.max_upload_size_bytes
    }
    pub fn read_ttl(
        &self,
        policy: &BlobPolicySnapshot,
        requested: Option<u64>,
        store_maximum: u64,
        document_remaining: Option<u64>,
    ) -> BlobResult<u64> {
        if requested == Some(0) {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        if store_maximum == 0 {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        let mut ttl = policy
            .read_url
            .min(store_maximum)
            .min(self.limits.read_url_maximum_seconds);
        if let Some(value) = requested {
            ttl = ttl.min(value);
        }
        if let Some(value) = document_remaining {
            ttl = ttl.min(value);
        }
        if ttl == 0 {
            Err(BlobErrorCode::ReferenceExpired.into())
        } else {
            Ok(ttl)
        }
    }
    pub fn read_disposition(&self, policy: &BlobPolicySnapshot, mime: &str) -> Disposition {
        match policy.disposition {
            Disposition::Auto => {
                if self.limits.inline_content_types.contains(mime) {
                    Disposition::Inline
                } else {
                    Disposition::Attachment
                }
            }
            value => value,
        }
    }
    pub fn require_context_checked_read(
        required: bool,
        capabilities: &BTreeSet<String>,
    ) -> BlobResult<()> {
        if required && !capabilities.contains("context_checked_read") {
            Err(BlobErrorCode::CapabilityMissing.into())
        } else {
            Ok(())
        }
    }
}
