use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::UserContext;

#[derive(Clone, Copy)]
struct LocalLockEntry {
    owner: u64,
    expires_at: Option<Instant>,
}

#[derive(Default)]
struct ProcessLocalLocks {
    entries: Mutex<HashMap<String, LocalLockEntry>>,
    changed: Condvar,
}

static PROCESS_LOCAL_LOCKS: OnceLock<ProcessLocalLocks> = OnceLock::new();
static NEXT_LOCAL_LOCK_OWNER: AtomicU64 = AtomicU64::new(1);

pub(super) fn next_local_lock_owner() -> u64 {
    NEXT_LOCAL_LOCK_OWNER.fetch_add(1, Ordering::Relaxed)
}

/// Provider-neutral distributed lock boundary.
///
/// Implementations must associate an acquired lock with `owner_token` and
/// release it only while that token still owns the key. A zero timeout is one
/// non-blocking attempt; a zero expiry means no automatic lease expiry.
#[async_trait::async_trait]
pub trait RemoteLockProvider: Send + Sync + 'static {
    async fn try_remote_lock(
        &self,
        key: &str,
        owner_token: &str,
        timeout_millis: u64,
        expire_millis: u64,
    ) -> bool;

    async fn unlock_remote(&self, key: &str, owner_token: &str) -> bool;
}

impl UserContext {
    pub fn try_local_lock(&self, key: &str, timeout_millis: u64, expire_millis: u64) -> bool {
        let locks = PROCESS_LOCAL_LOCKS.get_or_init(ProcessLocalLocks::default);
        let deadline = Instant::now() + Duration::from_millis(timeout_millis);
        let mut entries = locks.entries.lock().expect("local lock state poisoned");
        loop {
            let now = Instant::now();
            match entries.get(key).copied() {
                None => {
                    entries.insert(
                        key.to_owned(),
                        LocalLockEntry {
                            owner: self.local_lock_owner,
                            expires_at: (expire_millis > 0)
                                .then(|| now + Duration::from_millis(expire_millis)),
                        },
                    );
                    return true;
                }
                Some(current)
                    if current.owner == self.local_lock_owner
                        || current.expires_at.is_some_and(|expiry| now >= expiry) =>
                {
                    entries.insert(
                        key.to_owned(),
                        LocalLockEntry {
                            owner: self.local_lock_owner,
                            expires_at: (expire_millis > 0)
                                .then(|| now + Duration::from_millis(expire_millis)),
                        },
                    );
                    return true;
                }
                Some(current) => {
                    if timeout_millis == 0 || now >= deadline {
                        return false;
                    }
                    let wake_after = current
                        .expires_at
                        .map(|expiry| expiry.saturating_duration_since(now))
                        .unwrap_or_else(|| deadline.saturating_duration_since(now))
                        .min(deadline.saturating_duration_since(now));
                    let waited = locks
                        .changed
                        .wait_timeout(entries, wake_after)
                        .expect("local lock state poisoned");
                    entries = waited.0;
                }
            }
        }
    }

    pub fn unlock_local(&self, key: &str) {
        let locks = PROCESS_LOCAL_LOCKS.get_or_init(ProcessLocalLocks::default);
        let mut entries = locks.entries.lock().expect("local lock state poisoned");
        if entries
            .get(key)
            .is_some_and(|entry| entry.owner == self.local_lock_owner)
        {
            entries.remove(key);
            locks.changed.notify_all();
        }
    }

    /// Attempts to acquire a provider-backed distributed lock.
    ///
    /// A missing provider remains a no-op success, matching the optional
    /// Remote Lock boundary in the other TeaQL runtimes. Install an
    /// `Arc<dyn RemoteLockProvider>` resource to enable distributed exclusion.
    pub async fn try_remote_lock(
        &self,
        key: &str,
        timeout_millis: u64,
        expire_millis: u64,
    ) -> bool {
        match self.get_resource::<Arc<dyn RemoteLockProvider>>() {
            Some(provider) => {
                provider
                    .try_remote_lock(key, &self.remote_lock_owner, timeout_millis, expire_millis)
                    .await
            }
            None => true,
        }
    }

    /// Releases a distributed lock only when this context still owns it.
    pub async fn unlock_remote(&self, key: &str) -> bool {
        match self.get_resource::<Arc<dyn RemoteLockProvider>>() {
            Some(provider) => provider.unlock_remote(key, &self.remote_lock_owner).await,
            None => true,
        }
    }
}
