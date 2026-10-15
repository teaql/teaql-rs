//! Official AWS SDK adapter. Bucket creation is deployment administration, not a runtime operation.
use aws_sdk_s3::{Client, presigning::PresigningConfig, primitives::ByteStream};
use futures_util::StreamExt;
use std::{collections::BTreeMap, time::SystemTime};
use teaql_runtime::object_storage::*;
use tokio::io::AsyncWriteExt;

pub struct S3ObjectStorage {
    client: Client,
    bucket: String,
}
impl S3ObjectStorage {
    /// The caller configures region, endpoint, credential provider/refresh and request timeouts.
    /// A cloned SDK client shares its connection pool.
    pub fn new(client: Client, bucket: impl Into<String>) -> Result<Self, StorageError> {
        let bucket = bucket.into();
        if bucket.trim().is_empty() {
            return Err(StorageError(StorageErrorCode::InvalidInput));
        }
        Ok(Self { client, bucket })
    }
    async fn check_bucket(&self) -> Result<(), StorageError> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map(|_| ())
            .map_err(|error| {
                let mapped = translate(&error);
                if mapped.0 == StorageErrorCode::NotFound {
                    StorageError(StorageErrorCode::Unavailable)
                } else {
                    mapped
                }
            })
    }
}
fn io_error(_: std::io::Error) -> StorageError {
    StorageError(StorageErrorCode::Io)
}
fn translate<E: aws_sdk_s3::error::ProvideErrorMetadata>(
    error: &aws_sdk_s3::error::SdkError<E>,
) -> StorageError {
    use StorageErrorCode::*;
    if let Some(service) = error.as_service_error() {
        if service.code() == Some("NoSuchBucket") {
            return StorageError(Unavailable);
        }
    }
    let code = match error
        .raw_response()
        .map(|response| response.status().as_u16())
    {
        Some(401 | 403) => AccessDenied,
        Some(404) => NotFound,
        Some(409 | 412) => Conflict,
        Some(408 | 504) => Timeout,
        Some(413) => TooLarge,
        Some(501) => Unsupported,
        Some(400 | 422) => InvalidInput,
        _ if matches!(error, aws_sdk_s3::error::SdkError::TimeoutError(_)) => Timeout,
        _ if matches!(error, aws_sdk_s3::error::SdkError::DispatchFailure(failure) if failure.is_timeout()) => {
            Timeout
        }
        _ => Unavailable,
    };
    StorageError(code)
}
fn length(value: Option<i64>) -> Result<u64, StorageError> {
    value
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(StorageError(StorageErrorCode::Unavailable))
}
fn etag(value: Option<String>) -> Result<Option<String>, StorageError> {
    if value.as_ref().is_none_or(|value| value.trim().is_empty()) {
        return Err(StorageError(StorageErrorCode::Unavailable));
    }
    Ok(value)
}
#[async_trait::async_trait]
impl ObjectStorageProvider for S3ObjectStorage {
    async fn put(
        &self,
        _: &StorageInvocation<'_>,
        key: &ObjectKey,
        mut content: ContentStream,
        options: &PutOptions,
    ) -> Result<ObjectMetadata, StorageError> {
        let temporary = tempfile::NamedTempFile::new().map_err(io_error)?;
        let mut file = tokio::fs::File::from_std(temporary.reopen().map_err(io_error)?);
        let mut total = 0_u64;
        while let Some(bytes) = content.next().await {
            let bytes = bytes?;
            total = total
                .checked_add(bytes.len() as u64)
                .ok_or(StorageError(StorageErrorCode::TooLarge))?;
            if total > options.size() {
                return Err(StorageError(StorageErrorCode::InvalidInput));
            }
            file.write_all(&bytes).await.map_err(io_error)?;
        }
        if total != options.size() {
            return Err(StorageError(StorageErrorCode::InvalidInput));
        }
        file.flush().await.map_err(io_error)?;
        drop(file);
        let body = ByteStream::from_path(temporary.path())
            .await
            .map_err(|_| StorageError(StorageErrorCode::Io))?;
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key.as_str())
            .content_length(total as i64)
            .content_type(options.content_type())
            .set_metadata(Some(
                options
                    .metadata()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ))
            .body(body);
        if options.mode() == WriteMode::CreateOnly {
            request = request.if_none_match("*");
        }
        let response = request.send().await.map_err(|error| translate(&error))?;
        Ok(ObjectMetadata {
            key: key.clone(),
            size: total,
            content_type: Some(options.content_type().into()),
            etag: etag(response.e_tag)?,
            last_modified: None,
            custom_metadata: options.metadata().clone(),
        })
    }
    async fn get(
        &self,
        _: &StorageInvocation<'_>,
        key: &ObjectKey,
    ) -> Result<ObjectContent, StorageError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key.as_str())
            .send()
            .await
            .map_err(|error| translate(&error))?;
        let metadata = ObjectMetadata {
            key: key.clone(),
            size: length(response.content_length)?,
            content_type: response.content_type,
            etag: etag(response.e_tag)?,
            last_modified: response
                .last_modified
                .and_then(|t| SystemTime::try_from(t).ok()),
            custom_metadata: response.metadata.unwrap_or_default().into_iter().collect(),
        };
        let stream = futures_util::stream::unfold(response.body, |mut body| async {
            body.next()
                .await
                .map(|item| (item.map_err(|_| StorageError(StorageErrorCode::Io)), body))
        });
        Ok(ObjectContent {
            metadata,
            stream: Box::pin(stream),
        })
    }
    async fn head(
        &self,
        _: &StorageInvocation<'_>,
        key: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key.as_str())
            .send()
            .await
        {
            Ok(response) => Ok(Some(ObjectMetadata {
                key: key.clone(),
                size: length(response.content_length)?,
                content_type: response.content_type,
                etag: etag(response.e_tag)?,
                last_modified: response
                    .last_modified
                    .and_then(|t| SystemTime::try_from(t).ok()),
                custom_metadata: response
                    .metadata
                    .unwrap_or_default()
                    .into_iter()
                    .collect::<BTreeMap<_, _>>(),
            })),
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 404) =>
            {
                self.check_bucket().await?;
                Ok(None)
            }
            Err(error) => Err(translate(&error)),
        }
    }
    async fn delete(&self, _: &StorageInvocation<'_>, key: &ObjectKey) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key.as_str())
            .send()
            .await
            .map(|_| ())
            .map_err(|error| translate(&error))
    }
    async fn get_read_url(
        &self,
        _: &StorageInvocation<'_>,
        key: &ObjectKey,
        options: &ReadUrlOptions,
    ) -> Result<ObjectReadUrl, StorageError> {
        let config = PresigningConfig::expires_in(options.lifetime())
            .map_err(|_| StorageError(StorageErrorCode::InvalidInput))?;
        let expires_at = config.start_time() + config.expires();
        let signed = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key.as_str())
            .set_response_content_disposition(options.content_disposition().map(str::to_owned))
            .presigned(config)
            .await
            .map_err(|error| translate(&error))?;
        if signed
            .headers()
            .any(|(name, _)| !name.eq_ignore_ascii_case("host"))
        {
            return Err(StorageError(StorageErrorCode::Unsupported));
        }
        Ok(ObjectReadUrl::new(signed.uri().to_owned(), expires_at))
    }
}
