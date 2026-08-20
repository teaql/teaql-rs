use async_trait::async_trait;
use redis::AsyncCommands;
use std::time::{Duration, Instant};
use teaql_core::Value;
use teaql_runtime::{DataStore, RemoteLockProvider};

/// A Redis implementation of the DataStore trait for distributed caching
#[derive(Clone)]
pub struct RedisDataStore {
    client: redis::Client,
    conn: redis::aio::MultiplexedConnection,
}

impl RedisDataStore {
    /// Creates a new Redis data store by connecting to the specified URL.
    /// URL format: redis://[<username>][:<password>@]<hostname>[:port][/<db>]
    pub async fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self { client, conn })
    }

    /// Provides access to the underlying redis client if needed
    pub fn client(&self) -> &redis::Client {
        &self.client
    }
}

#[async_trait]
impl DataStore for RedisDataStore {
    async fn get(&self, key: &str) -> Option<Value> {
        let mut conn = self.conn.clone();
        let result: redis::RedisResult<Option<String>> = conn.get(key).await;

        if let Ok(Some(json_str)) = result
            && let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str)
        {
            return Some(Value::from(json_value));
        }
        None
    }

    async fn put(&self, key: &str, value: Value, timeout_seconds: Option<u64>) {
        let mut conn = self.conn.clone();
        let json_str = match serde_json::to_string(&value.to_json_value()) {
            Ok(s) => s,
            Err(_) => return,
        };

        match timeout_seconds {
            Some(secs) => {
                let _: redis::RedisResult<()> = conn.set_ex(key, json_str, secs).await;
            }
            None => {
                let _: redis::RedisResult<()> = conn.set(key, json_str).await;
            }
        }
    }

    async fn remove(&self, key: &str) {
        let mut conn = self.conn.clone();
        let _: redis::RedisResult<()> = conn.del(key).await;
    }
}

/// Redis-backed TeaQL Remote Lock provider.
///
/// Acquisition uses `SET NX` with an optional millisecond lease. Release is an
/// atomic compare-and-delete Lua script, so an expired lock that has already
/// been acquired by another owner cannot be deleted by the former owner.
#[derive(Clone)]
pub struct RedisRemoteLockProvider {
    conn: redis::aio::MultiplexedConnection,
}

impl RedisRemoteLockProvider {
    pub async fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self { conn })
    }

    pub fn from_connection(conn: redis::aio::MultiplexedConnection) -> Self {
        Self { conn }
    }

    async fn acquire_once(&self, key: &str, owner_token: &str, expire_millis: u64) -> bool {
        let mut conn = self.conn.clone();
        let mut command = redis::cmd("SET");
        command.arg(key).arg(owner_token).arg("NX");
        if expire_millis > 0 {
            command.arg("PX").arg(expire_millis);
        }
        command
            .query_async::<Option<String>>(&mut conn)
            .await
            .is_ok_and(|result| result.is_some())
    }
}

#[async_trait]
impl RemoteLockProvider for RedisRemoteLockProvider {
    async fn try_remote_lock(
        &self,
        key: &str,
        owner_token: &str,
        timeout_millis: u64,
        expire_millis: u64,
    ) -> bool {
        let deadline = Instant::now() + Duration::from_millis(timeout_millis);
        loop {
            if self.acquire_once(key, owner_token, expire_millis).await {
                return true;
            }
            if timeout_millis == 0 || Instant::now() >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
        }
    }

    async fn unlock_remote(&self, key: &str, owner_token: &str) -> bool {
        const COMPARE_AND_DELETE: &str = r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
        "#;
        let mut conn = self.conn.clone();
        redis::Script::new(COMPARE_AND_DELETE)
            .key(key)
            .arg(owner_token)
            .invoke_async::<i64>(&mut conn)
            .await
            .is_ok_and(|deleted| deleted == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires TEAQL_REDIS_URL"]
    async fn redis_remote_lock_enforces_contention_lease_and_owner_safe_release() {
        let redis_url = std::env::var("TEAQL_REDIS_URL")
            .expect("set TEAQL_REDIS_URL to run Redis Remote Lock conformance");
        let provider = RedisRemoteLockProvider::new(&redis_url)
            .await
            .expect("connect to Redis");
        let key = format!(
            "teaql:conformance:remote-lock:{}:{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );

        assert!(provider.try_remote_lock(&key, "owner-a", 0, 60).await);
        assert!(!provider.try_remote_lock(&key, "owner-b", 0, 60).await);
        assert!(!provider.unlock_remote(&key, "owner-b").await);

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(provider.try_remote_lock(&key, "owner-b", 0, 1_000).await);
        assert!(!provider.unlock_remote(&key, "owner-a").await);
        assert!(provider.unlock_remote(&key, "owner-b").await);

        assert!(provider.try_remote_lock(&key, "owner-a", 0, 0).await);
        let releasing_provider = provider.clone();
        let releasing_key = key.clone();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            releasing_provider
                .unlock_remote(&releasing_key, "owner-a")
                .await
        });
        assert!(provider.try_remote_lock(&key, "owner-b", 250, 1_000).await);
        assert!(release.await.expect("release task"));
        assert!(provider.unlock_remote(&key, "owner-b").await);
    }
}
