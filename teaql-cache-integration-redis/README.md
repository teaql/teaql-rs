# TeaQL Redis integration

This crate provides TeaQL's Redis-backed remote cache and distributed Remote
Lock provider.

```rust
use std::sync::Arc;
use teaql_cache_integration_redis::RedisRemoteLockProvider;
use teaql_runtime::{RemoteLockProvider, UserContext};

let provider: Arc<dyn RemoteLockProvider> = Arc::new(
    RedisRemoteLockProvider::new("redis://127.0.0.1:6379").await?,
);
let mut context = UserContext::new();
context.insert_resource(provider);

if context.try_remote_lock("invoice:42", 500, 5_000).await {
    // Perform the distributed critical section.
    context.unlock_remote("invoice:42").await;
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

The timeout and lease are milliseconds. A zero timeout performs one immediate
attempt; a zero lease disables automatic expiry. Unlock uses an atomic
owner-token comparison, so a previous holder cannot delete a lock acquired by
another process after lease expiry.
