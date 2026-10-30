use teaql_core::{
    BlobRef, DataType, Entity, EntityDescriptor, InsertCommand, PropertyDescriptor, Value,
};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_sql::{CompiledQuery, SqlDialect};

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "BlobAsset")]
struct BlobAsset {
    #[teaql(id)]
    id: u64,
    document: Option<BlobRef>,
}

#[test]
fn real_sqlite_stores_and_queries_keys_as_text_then_decodes_typed_rows() {
    let executor =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    let descriptor = EntityDescriptor::new("BlobAsset")
        .table_name("blob_asset")
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(PropertyDescriptor::new("document", DataType::BlobRef));
    let ddl = SqliteDialect.compile_create_table(&descriptor).unwrap();
    executor
        .connection()
        .lock()
        .unwrap()
        .execute(&ddl, [])
        .unwrap();
    let reference = BlobRef::new("blob_sql_native").unwrap();
    let insert = SqliteDialect
        .compile_insert(
            &descriptor,
            &InsertCommand::new("BlobAsset")
                .value("id", 1_u64)
                .value("document", reference.clone()),
        )
        .unwrap();
    executor.execute(&insert).unwrap();
    let select = CompiledQuery {
        log_context: Default::default(),
        sql: "SELECT id, document, typeof(document) AS physical FROM blob_asset WHERE document = ?"
            .into(),
        params: vec![reference.clone().into()],
        comment: Some("verify stable Blob SQL text codec".into()),
    };
    let mut rows = executor.fetch_all_compact(&select).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("physical"), Some(&Value::Text("text".into())));
    assert_eq!(
        rows[0].get("document"),
        Some(&Value::Text(reference.as_str().into()))
    );
    let entity = BlobAsset::from_compact_row(rows.remove(0)).unwrap();
    assert_eq!(entity.document, Some(reference));
    executor
        .connection()
        .lock()
        .unwrap()
        .execute(
            "UPDATE blob_asset SET document = 'https://example.invalid/file'",
            [],
        )
        .unwrap();
    let query = CompiledQuery {
        sql: "SELECT id, document FROM blob_asset".into(),
        params: vec![],
        ..select
    };
    let row = executor.fetch_all_compact(&query).unwrap().remove(0);
    assert!(BlobAsset::from_compact_row(row).is_err());
}
