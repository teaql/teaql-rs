//! Generated Q/E acceptance plus a trusted, test-only native-driver ordinal probe.
use school_management_service_core::{School, E, Q};
use std::sync::Arc;
use teaql_core::{Entity, TeaqlEntity, Value};
use teaql_provider_sqlite::SqliteMutationExecutor;

pub async fn verify(
    context: &teaql_runtime::UserContext,
    id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let left = Q::schools_minimal()
        .with_id_is(id)
        .select_name()
        .select_address()
        .limit(1)
        .comment("what: select name then address")
        .purpose("why: verify fixed indexes ignore Q selection order")
        .execute_for_one(context)
        .await?
        .unwrap();
    let right = Q::schools_minimal()
        .with_id_is(id)
        .select_address()
        .select_name()
        .limit(1)
        .comment("what: select address then name")
        .purpose("why: compare the same projected values in reverse selection order")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert_eq!(
        E::school(&left).get_name().eval(),
        E::school(&right).get_name().eval()
    );
    assert_eq!(
        E::school(&left).get_address().eval(),
        E::school(&right).get_address().eval()
    );
    assert!(Arc::ptr_eq(
        &left.loaded_state_snapshot().unwrap(),
        &right.loaded_state_snapshot().unwrap()
    ));
    assert!(!left.is_field_loaded("active"));
    assert!(left.dirty_fields().is_none() && right.dirty_fields().is_none());

    // Read only fixture rows written by the generated audited mutation API.
    // Direct SQL is confined to the provider-ordinal control, not an application query recipe.
    let database = std::env::var("TEAQL_LOAD_STATE_DATABASE")?;
    let driver = SqliteMutationExecutor::from_connection(rusqlite::Connection::open(database)?);
    let expected = School::field_layout()?.unwrap();
    let mut rows = Vec::new();
    for columns in ["name,address,id,version", "address,version,name,id"] {
        let query = teaql_sql::CompiledQuery {
            log_context: Default::default(),
            sql: format!("SELECT {columns} FROM school_data WHERE id = ? LIMIT 1"),
            params: vec![Value::U64(id)],
            comment: Some(
                "what: permute native result columns; why: qualify typed fixed slots".into(),
            ),
        };
        let mut result = driver.fetch_all_compact(&query)?;
        assert_eq!(result.len(), 1);
        let row = result.pop().unwrap();
        assert_eq!(
            row.shared_columns()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            columns.split(',').collect::<Vec<_>>()
        );
        let entity = School::from_compact_row(row)?;
        let state = entity.loaded_state_snapshot().unwrap();
        assert!(Arc::ptr_eq(state.layout(), &expected));
        for (canonical, slot) in School::__TEAQL_FIXED_FIELD_INDEXES {
            assert_eq!(state.layout().index(canonical), Some(*slot));
        }
        assert_eq!(
            E::school(&entity).get_name().eval(),
            E::school(&left).get_name().eval()
        );
        assert_eq!(
            E::school(&entity).get_address().eval(),
            E::school(&left).get_address().eval()
        );
        assert_eq!(entity.id(), left.id());
        assert_eq!(entity.version(), left.version());
        assert!(!entity.is_field_loaded("active"));
        assert!(entity.dirty_fields().is_none());
        rows.push(entity);
    }
    assert!(Arc::ptr_eq(
        &rows[0].loaded_state_snapshot().unwrap(),
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    println!("PASS generated Rust Q selection and real SQLite column-order invariance");
    Ok(())
}
