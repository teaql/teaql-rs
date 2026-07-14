use async_trait::async_trait;
use redis::AsyncCommands;
use teaql_core::Value;
use teaql_runtime::DataStore;

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
        let conn = client.get_multiplexed_tokio_connection().await?;
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
            && let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
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
