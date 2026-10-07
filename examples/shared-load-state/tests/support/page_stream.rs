//! Bounded generated Q/E page and chunked-stream loaded-state acceptance.
use futures_util::StreamExt;
use school_management_service_core::{School, E, Q};
use std::sync::Arc;
use teaql_core::Entity;

pub async fn verify(
    context: &teaql_runtime::UserContext,
    first: &str,
    second: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let first_page = Q::schools_minimal()
        .with_name_in([first, second])
        .select_name()
        .order_by_id_asc()
        .comment("what: load the first matching School page")
        .purpose("why: preserve bounded projection state in pagination")
        .execute_for_page(context, 0, 1)
        .await?;
    let second_page = Q::schools_minimal()
        .with_name_in([first, second])
        .select_name()
        .order_by_id_asc()
        .comment("what: load the second matching School page")
        .purpose("why: qualify adjacent pages and filtered totals")
        .execute_for_page(context, 1, 1)
        .await?;
    assert_eq!(first_page.total_count, Some(2));
    assert_eq!(second_page.total_count, Some(2));
    assert_eq!(first_page.len(), 1);
    assert_eq!(second_page.len(), 1);
    assert_ne!(first_page[0].id(), second_page[0].id());
    assert_eq!(
        E::school(&first_page[0]).get_name().eval().as_deref(),
        Some(first)
    );
    assert_eq!(
        E::school(&second_page[0]).get_name().eval().as_deref(),
        Some(second)
    );
    assert!(!first_page[0].is_field_loaded("address"));
    assert!(first_page[0].dirty_fields().is_none());
    assert!(Arc::ptr_eq(
        &first_page[0].loaded_state_snapshot().unwrap(),
        &second_page[0].loaded_state_snapshot().unwrap()
    ));
    for full in [false, true] {
        let mut stream = if full {
            Q::schools()
                .with_name_in([first, second])
                .select_self_fields()
                .order_by_id_asc()
                .limit(2)
                .stream(1)
                .comment("what: stream two complete Schools one row per chunk")
                .purpose("why: retain loaded NULL and overflow across cursor chunks")
                .execute_for_stream(context)
                .await?
        } else {
            Q::schools_minimal()
                .with_name_in([first, second])
                .select_name()
                .order_by_id_asc()
                .limit(2)
                .stream(1)
                .comment("what: stream two sparse Schools one row per chunk")
                .purpose("why: retain NotLoaded boundaries across cursor chunks")
                .execute_for_stream(context)
                .await?
        };
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row?);
        }
        drop(stream);
        assert_eq!(rows.len(), 2);
        let snapshot = rows[0].loaded_state_snapshot().unwrap();
        for (index, row) in rows.iter().enumerate() {
            assert!(Arc::ptr_eq(
                &snapshot,
                &row.loaded_state_snapshot().unwrap()
            ));
            assert_eq!(
                E::school(row).get_name().eval().as_deref(),
                Some(if index == 0 { first } else { second })
            );
            assert_eq!(row.is_field_loaded("address"), full);
            assert!(row.is_field_loaded("id") && row.is_field_loaded("version"));
            assert!(row.dirty_fields().is_none());
            if full && std::env::var("TEAQL_LOAD_STATE_WIDE").as_deref() == Ok("true") {
                let json = row.clone().into_json();
                for slot in [63, 64, 65, 129] {
                    let (name, _) = School::__TEAQL_FIXED_FIELD_INDEXES
                        .iter()
                        .find(|(_, index)| *index == slot)
                        .unwrap();
                    assert!(row.is_field_loaded(name));
                    assert!(json.get(name).unwrap().is_null());
                }
            }
        }
    }
    println!("PASS generated Rust page and chunked stream shared load state");
    Ok(())
}
