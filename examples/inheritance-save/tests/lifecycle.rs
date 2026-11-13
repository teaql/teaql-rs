//! Exercise only application-owned code; use SQL solely as a read-only oracle.
use std::process::Command;

#[test]
fn generated_lifecycle_twice_with_one_root_version_and_no_manual_parent_seed() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("inheritance.sqlite");
    let mut ids = Vec::new();
    for _ in 0..2 {
        let output = Command::new(env!("CARGO_BIN_EXE_inheritance-save-example"))
            .env("INHERITANCE_MATCHING_SERVICE_CORE_DATABASE_URL", &database)
            .env("TEAQL_TELEMETRY_ENABLED", "false")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        let lines: Vec<_> = text
            .lines()
            .filter_map(|line| line.strip_prefix("NATURAL_INHERITANCE_LIFECYCLE="))
            .collect();
        assert_eq!(lines.len(), 1);
        let proof: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        for field in [
            "passed",
            "generatedQAndE",
            "auditedMutation",
            "partialWriteRejected",
            "staleWriteRejected",
            "parentAndChildHidden",
            "retainedDeletedRow",
        ] {
            assert_eq!(proof[field], true, "{field}");
        }
        assert_eq!(proof["manualParentSeed"], false);
        assert_eq!(proof["caseCount"], 8);
        assert_eq!(proof["noopVersion"], 1);
        assert_eq!(proof["versions"], serde_json::json!([1, 2, 3, -4]));
        ids.push(proof["id"].as_i64().unwrap());
    }
    assert_ne!(ids[0], ids[1]);
    let connection = rusqlite::Connection::open_with_flags(
        &database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut query = connection.prepare("SELECT s.id,s.version,a.\"Legacy Campus Code\" FROM school_data s JOIN academy_data a ON a.id=s.id ORDER BY s.id").unwrap();
    let rows: Vec<(i64, i64, String)> = query
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    // The model maps campus_code to a legacy column, so the oracle below binds
    // the physical spelling rather than accepting a duplicated logical column.
    assert_eq!(rows.len(), 2);
    for (id, version, campus) in rows {
        assert!(ids.contains(&id));
        assert_eq!(version, -4);
        assert_eq!(campus, "SOUTH");
    }
    let count: i64 = connection
        .query_row("SELECT count(*) FROM school_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2);
    let count: i64 = connection
        .query_row("SELECT count(*) FROM platform_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let count: i64 = connection
        .query_row(
            "SELECT count(*) FROM school_type_data WHERE id IN (1001,1002)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
    let mut query = connection
        .prepare("PRAGMA table_info(academy_data)")
        .unwrap();
    let columns: Vec<String> = query
        .query_map([], |row| row.get(1))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(columns, vec!["id", "Legacy Campus Code"]);
}
