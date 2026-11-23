//! Context-owned storage capability. Providers are runtime-scoped, requests and streams operation-scoped.
use crate::{FailOpenRuntimeTelemetryScope, RuntimeOperation, UserContext};
use futures_core::Stream;
use futures_util::StreamExt;
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
pub use teaql_core::object_storage::*;

pub struct StorageInvocation<'a> {
    context: &'a UserContext,
    comment: &'a str,
}
impl StorageInvocation<'_> {
    pub fn context(&self) -> &UserContext {
        self.context
    }
    pub fn comment(&self) -> &str {
        self.comment
    }
}
#[async_trait::async_trait]
pub trait ObjectStorageProvider: Send + Sync {
    async fn put(
        &self,
        invocation: &StorageInvocation<'_>,
        key: &ObjectKey,
        content: ContentStream,
        options: &PutOptions,
    ) -> Result<ObjectMetadata, StorageError>;
    async fn get(
        &self,
        invocation: &StorageInvocation<'_>,
        key: &ObjectKey,
    ) -> Result<ObjectContent, StorageError>;
    async fn head(
        &self,
        invocation: &StorageInvocation<'_>,
        key: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>, StorageError>;
    async fn delete(
        &self,
        invocation: &StorageInvocation<'_>,
        key: &ObjectKey,
    ) -> Result<(), StorageError>;
    async fn get_read_url(
        &self,
        invocation: &StorageInvocation<'_>,
        key: &ObjectKey,
        options: &ReadUrlOptions,
    ) -> Result<ObjectReadUrl, StorageError>;
}
struct ProviderResource(Arc<dyn ObjectStorageProvider>);
/// Safe diagnostic event: no object key, URL, provider response, body or custom metadata.
#[derive(Debug, Clone)]
pub struct StorageEvent {
    pub operation: &'static str,
    pub comment: String,
    pub trace_id: String,
    pub outcome: String,
    pub elapsed: std::time::Duration,
}
pub trait ObjectStorageObserver: Send + Sync {
    fn record(&self, event: StorageEvent);
}
struct ObserverResource(Arc<dyn ObjectStorageObserver>);
struct ConsoleObserver;
impl ObjectStorageObserver for ConsoleObserver {
    fn record(&self, event: StorageEvent) {
        eprintln!(
            "[ObjectStorage] operation={} comment={} trace={} outcome={} elapsed_ms={}",
            event.operation,
            event.comment,
            event.trace_id,
            event.outcome,
            event.elapsed.as_millis()
        );
    }
}
pub struct ObjectStorage<'a> {
    invocation: StorageInvocation<'a>,
    provider: &'a dyn ObjectStorageProvider,
}
impl UserContext {
    pub fn with_object_storage_observer(
        mut self,
        observer: Arc<dyn ObjectStorageObserver>,
    ) -> Self {
        self.insert_resource(ObserverResource(observer));
        self
    }
    pub fn with_object_storage(mut self, provider: Arc<dyn ObjectStorageProvider>) -> Self {
        self.insert_resource(ProviderResource(provider));
        self
    }
    pub fn object_storage<'a>(
        &'a self,
        comment: &'a str,
    ) -> Result<ObjectStorage<'a>, StorageError> {
        if comment.trim().is_empty() {
            return Err(StorageError(StorageErrorCode::InvalidInput));
        }
        let provider = self
            .get_resource::<ProviderResource>()
            .ok_or(StorageError(StorageErrorCode::NotConfigured))?;
        Ok(ObjectStorage {
            invocation: StorageInvocation {
                context: self,
                comment,
            },
            provider: provider.0.as_ref(),
        })
    }
}
struct Receipt {
    operation: &'static str,
    comment: String,
    trace_id: String,
    started: Instant,
    scope: FailOpenRuntimeTelemetryScope,
    done: bool,
    observer: Arc<dyn ObjectStorageObserver>,
}
impl Receipt {
    fn finish(&mut self, result: Result<(), StorageError>) {
        if self.done {
            return;
        }
        self.done = true;
        match result {
            Ok(()) => self.scope.success(BTreeMap::new()),
            Err(error) => self.scope.failure(&format!("object_storage_{:?}", error.0)),
        }
        self.record(result.map_or_else(|e| format!("{:?}", e.0), |_| "success".into()));
    }
    fn record(&self, outcome: String) {
        let event = StorageEvent {
            operation: self.operation,
            comment: self.comment.clone(),
            trace_id: self.trace_id.clone(),
            outcome,
            elapsed: self.started.elapsed(),
        };
        let _ =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.observer.record(event)));
    }
}
impl Drop for Receipt {
    fn drop(&mut self) {
        if !self.done {
            self.scope.failure("cancelled");
            self.record("cancelled".into());
        }
    }
}
impl ObjectStorage<'_> {
    fn start(&self, operation: &'static str) -> Receipt {
        let safe = |text: &str| {
            text.chars()
                .map(|c| if c.is_control() { ' ' } else { c })
                .collect()
        };
        Receipt {
            operation,
            comment: safe(self.invocation.comment),
            trace_id: safe(self.invocation.context.trace_id()),
            started: Instant::now(),
            scope: self
                .invocation
                .context
                .start_runtime_operation(RuntimeOperation::new("object_storage", operation)),
            done: false,
            observer: self
                .invocation
                .context
                .get_resource::<ObserverResource>()
                .map(|r| r.0.clone())
                .unwrap_or_else(|| Arc::new(ConsoleObserver)),
        }
    }
    pub async fn put(
        &self,
        key: &ObjectKey,
        content: ContentStream,
        options: &PutOptions,
    ) -> Result<ObjectMetadata, StorageError> {
        let mut receipt = self.start("put");
        let result = receipt
            .scope
            .run(self.provider.put(&self.invocation, key, content, options))
            .await;
        receipt.finish(result.as_ref().map(|_| ()).map_err(|e| *e));
        result
    }
    pub async fn put_bytes(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
        options: &PutOptions,
    ) -> Result<ObjectMetadata, StorageError> {
        self.put(
            key,
            Box::pin(futures_util::stream::once(async { Ok(bytes) })),
            options,
        )
        .await
    }
    pub async fn get(&self, key: &ObjectKey) -> Result<ObjectContent, StorageError> {
        let mut receipt = self.start("get");
        match receipt
            .scope
            .run(self.provider.get(&self.invocation, key))
            .await
        {
            Ok(content) => Ok(ObjectContent {
                stream: Box::pin(ObservedStream {
                    inner: content.stream,
                    remaining: content.metadata.size,
                    receipt,
                    ended: false,
                }),
                metadata: content.metadata,
            }),
            Err(error) => {
                receipt.finish(Err(error));
                Err(error)
            }
        }
    }
    pub async fn get_bytes(
        &self,
        key: &ObjectKey,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let mut content = self.get(key).await?;
        if content.metadata.size > maximum_bytes as u64 {
            return Err(StorageError(StorageErrorCode::TooLarge));
        }
        let mut output = Vec::new();
        while let Some(bytes) = content.stream.next().await {
            let bytes = bytes?;
            if bytes.len() > maximum_bytes - output.len() {
                return Err(StorageError(StorageErrorCode::TooLarge));
            }
            output.extend_from_slice(&bytes);
        }
        Ok(output)
    }
    pub async fn head(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, StorageError> {
        let mut receipt = self.start("head");
        let result = receipt
            .scope
            .run(self.provider.head(&self.invocation, key))
            .await;
        receipt.finish(result.as_ref().map(|_| ()).map_err(|e| *e));
        result
    }
    pub async fn exists(&self, key: &ObjectKey) -> Result<bool, StorageError> {
        Ok(self.head(key).await?.is_some())
    }
    pub async fn delete(&self, key: &ObjectKey) -> Result<(), StorageError> {
        let mut receipt = self.start("delete");
        let result = receipt
            .scope
            .run(self.provider.delete(&self.invocation, key))
            .await;
        receipt.finish(result);
        result
    }
    pub async fn get_read_url(
        &self,
        key: &ObjectKey,
        options: &ReadUrlOptions,
    ) -> Result<ObjectReadUrl, StorageError> {
        let mut receipt = self.start("read_url");
        let result = receipt
            .scope
            .run(self.provider.get_read_url(&self.invocation, key, options))
            .await;
        receipt.finish(result.as_ref().map(|_| ()).map_err(|e| *e));
        result
    }
}
struct ObservedStream {
    inner: ContentStream,
    remaining: u64,
    receipt: Receipt,
    ended: bool,
}
impl Stream for ObservedStream {
    type Item = Result<Bytes, StorageError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) if bytes.len() as u64 <= self.remaining => {
                self.remaining -= bytes.len() as u64;
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(None) if self.remaining == 0 => {
                self.ended = true;
                self.inner = Box::pin(futures_util::stream::empty());
                self.receipt.finish(Ok(()));
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
            other => {
                let error = match other {
                    Poll::Ready(Some(Err(error))) => error,
                    _ => StorageError(StorageErrorCode::Io),
                };
                self.ended = true;
                self.inner = Box::pin(futures_util::stream::empty());
                self.receipt.finish(Err(error));
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

#[cfg(test)]
mod tests;
