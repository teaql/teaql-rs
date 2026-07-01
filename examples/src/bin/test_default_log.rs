use std::time::{Duration, SystemTime};
use teaql_runtime::log_formatter::{LogManager, SqlLogEntry, SqlLogOperation};

#[tokio::main]
async fn main() {
    println!("Starting test_default_log...");
    println!("This will trigger the default holographic trace log mechanism.");

    // The executable is named "test_default_log" (or similar depending on cargo).
    // The framework will auto-generate test_default_log.log

    let entry = SqlLogEntry {
        operation: SqlLogOperation::Select,
        sql: "SELECT * FROM orders WHERE id = $1".to_string(),
        params: vec![],
        debug_sql: "SELECT * FROM orders WHERE id = 1".to_string(),
        pretty_sql: "SELECT * FROM orders WHERE id = 1".to_string(),
        started_at: SystemTime::now(),
        ended_at: SystemTime::now(),
        elapsed: Duration::from_micros(152),
        result_count: Some(1),
        result_type: Some("Order".to_string()),
        affected_rows: Some(0),
        result_summary: "1 rows returned".to_string(),
    };

    LogManager::write_sql_log(&[], &entry);

    println!("Done! A .log file should have been generated.");
}
