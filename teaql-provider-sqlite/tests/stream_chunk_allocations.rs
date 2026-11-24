//! Cursor chunk-buffer costs, excluding fixture setup and result retention.
#[path = "../../teaql-runtime/tests/support/allocation_counter.rs"]
mod allocation_counter;
use futures_util::StreamExt;
use teaql_provider_sqlite::SqliteMutationExecutor;
use teaql_sql::{CompiledQuery, StreamingSqlTransport};

fn fixture(rows: usize) -> SqliteMutationExecutor {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch(&format!(
        "CREATE TABLE stream_probe(id INTEGER);\
         INSERT INTO stream_probe SELECT x FROM (WITH RECURSIVE seq(x) AS \
         (SELECT 1 WHERE {rows} > 0 UNION ALL SELECT x+1 FROM seq WHERE x < {rows}) SELECT x FROM seq);"
    )).unwrap();
    SqliteMutationExecutor::from_connection(connection)
}
fn query() -> CompiledQuery {
    CompiledQuery {
        log_context: Default::default(),
        sql: "SELECT id FROM stream_probe ORDER BY id".into(),
        params: vec![],
        comment: Some(
            "what: observe bounded cursor allocation; why: avoid chunk-buffer growth".into(),
        ),
    }
}
fn drain(executor: &SqliteMutationExecutor, size: usize) -> usize {
    let mut stream = executor.stream_sql(query(), size);
    futures_executor::block_on(async {
        let mut count = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert!(chunk.rows.len() <= size);
            for row in &chunk.rows {
                count += 1;
                assert_eq!(
                    row.get("id").and_then(teaql_core::Value::try_i64),
                    Some(count as i64)
                );
            }
        }
        count
    })
}
#[test]
fn report_real_cursor_chunk_allocations() {
    for (rows, size) in [
        (0, 73),
        (1, 73),
        (73, 73),
        (74, 73),
        (219, 73),
        (223, 73),
        (4096, 128),
    ] {
        let executor = fixture(rows);
        assert_eq!(drain(&executor, size), rows);
        let (count, calls, bytes, _) = allocation_counter::measured(|| drain(&executor, size));
        assert_eq!(count, rows);
        println!("STREAM_CHUNKS,{rows},{size},{calls},{bytes}");
    }
}
#[test]
fn every_chunk_buffer_has_bounded_capacity_and_releases_on_drop() {
    let executor = fixture(223);
    let mut stream = executor.stream_sql(query(), 73);
    futures_executor::block_on(async {
        let mut index = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert!(
                chunk.rows.capacity() <= 73,
                "chunk {index} must not geometrically overallocate"
            );
            assert_eq!(
                chunk.rows.capacity(),
                if chunk.rows.len() == 73 { 73 } else { 4 }
            );
            assert_eq!(chunk.chunk_index, index);
            index += 1;
        }
        assert_eq!(index, 4);
    });
    let mut early = executor.stream_sql(query(), 73);
    assert_eq!(
        futures_executor::block_on(early.next())
            .unwrap()
            .unwrap()
            .rows
            .len(),
        73
    );
    drop(early);
    // Re-entering the same provider proves the cursor and its lease were released.
    assert_eq!(drain(&executor, 73), 223);
}

#[test]
fn zero_chunk_size_preserves_the_existing_single_tail_behavior() {
    let executor = fixture(3);
    let mut stream = executor.stream_sql(query(), 0);
    let chunk = futures_executor::block_on(stream.next()).unwrap().unwrap();
    assert_eq!(chunk.rows.len(), 3);
    assert!(chunk.is_last);
    assert!(futures_executor::block_on(stream.next()).is_none());
}
