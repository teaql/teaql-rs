use tokio_postgres::NoTls;
use deadpool_postgres::{Config, Runtime};

#[tokio::main]
async fn main() {
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.dbname = Some("postgres".to_string());
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();
    let row = client.query_one("SELECT 1::int4", &[]).await.unwrap();
    let v: i32 = row.get(0);
    println!("Value: {}", v);
}
