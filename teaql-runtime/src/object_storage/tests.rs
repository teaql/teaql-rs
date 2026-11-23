use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
struct Stub {
    size: u64,
    error: Option<StorageErrorCode>,
    calls: Arc<AtomicUsize>,
}
impl Stub {
    fn metadata(&self, key: &ObjectKey) -> ObjectMetadata {
        ObjectMetadata {
            key: key.clone(),
            size: self.size,
            content_type: None,
            etag: None,
            last_modified: None,
            custom_metadata: BTreeMap::new(),
        }
    }
}
#[async_trait::async_trait]
impl ObjectStorageProvider for Stub {
    async fn put(
        &self,
        invocation: &StorageInvocation<'_>,
        key: &ObjectKey,
        _: ContentStream,
        _: &PutOptions,
    ) -> Result<ObjectMetadata, StorageError> {
        assert_eq!(invocation.context().trace_id(), "trace-a");
        assert!(!invocation.comment().is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.metadata(key))
    }
    async fn get(
        &self,
        _: &StorageInvocation<'_>,
        key: &ObjectKey,
    ) -> Result<ObjectContent, StorageError> {
        Ok(ObjectContent {
            metadata: self.metadata(key),
            stream: Box::pin(futures_util::stream::iter([Ok(Bytes::from_static(b"abc"))])),
        })
    }
    async fn head(
        &self,
        _: &StorageInvocation<'_>,
        _: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.error.map_or(Ok(None), |code| Err(StorageError(code)))
    }
    async fn delete(&self, _: &StorageInvocation<'_>, _: &ObjectKey) -> Result<(), StorageError> {
        Ok(())
    }
    async fn get_read_url(
        &self,
        _: &StorageInvocation<'_>,
        _: &ObjectKey,
        _: &ReadUrlOptions,
    ) -> Result<ObjectReadUrl, StorageError> {
        Err(StorageError(StorageErrorCode::Unsupported))
    }
}
#[derive(Default)]
struct Observer(Mutex<Vec<StorageEvent>>);
impl ObjectStorageObserver for Observer {
    fn record(&self, event: StorageEvent) {
        self.0.lock().unwrap().push(event);
    }
}
#[tokio::test]
async fn context_and_error_semantics() {
    assert!(matches!(
        UserContext::new().object_storage("read object"),
        Err(StorageError(StorageErrorCode::NotConfigured))
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let key = ObjectKey::new("PRIVATE-KEY-CANARY").unwrap();
    for error in [
        None,
        Some(StorageErrorCode::AccessDenied),
        Some(StorageErrorCode::Unavailable),
    ] {
        let context = UserContext::new()
            .with_trace_id("trace-a")
            .with_object_storage(Arc::new(Stub {
                size: 3,
                error,
                calls: calls.clone(),
            }));
        assert!(matches!(
            context.object_storage(" "),
            Err(StorageError(StorageErrorCode::InvalidInput))
        ));
        let storage = context.object_storage("check object").unwrap();
        match error {
            None => assert!(!storage.exists(&key).await.unwrap()),
            Some(code) => assert_eq!(storage.exists(&key).await, Err(StorageError(code))),
        }
        storage
            .put_bytes(
                &key,
                Bytes::new(),
                &PutOptions::new(0, WriteMode::CreateOnly).unwrap(),
            )
            .await
            .unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);
}
#[tokio::test]
async fn bounded_read_and_stream_completion_are_observed_without_keys() {
    let observer = Arc::new(Observer::default());
    let context = UserContext::new()
        .with_object_storage(Arc::new(Stub {
            size: 3,
            error: None,
            calls: Arc::new(AtomicUsize::new(0)),
        }))
        .with_object_storage_observer(observer.clone());
    let storage = context.object_storage("read attachment").unwrap();
    let key = ObjectKey::new("PRIVATE-KEY-CANARY").unwrap();
    drop(storage.get(&key).await.unwrap());
    assert_eq!(observer.0.lock().unwrap()[0].outcome, "cancelled");
    assert_eq!(storage.get_bytes(&key, 3).await.unwrap(), b"abc");
    assert_eq!(observer.0.lock().unwrap()[1].outcome, "success");
    assert_eq!(
        storage.get_bytes(&key, 2).await.unwrap_err().0,
        StorageErrorCode::TooLarge
    );
    assert!(!format!("{:?}", observer.0.lock().unwrap()).contains("PRIVATE-KEY-CANARY"));
    for size in [2, 4] {
        let context = UserContext::new().with_object_storage(Arc::new(Stub {
            size,
            error: None,
            calls: Arc::new(AtomicUsize::new(0)),
        }));
        assert_eq!(
            context
                .object_storage("verify size")
                .unwrap()
                .get_bytes(&key, 10)
                .await
                .unwrap_err()
                .0,
            StorageErrorCode::Io
        );
    }
}
