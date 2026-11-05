//! Ordinary generated Q and existing Entity presentation, without source repair.
use school_management_service_core::{service_runtime, ServiceRuntimeConfig, Q};
use teaql_core::{Entity, WebResponse};

#[tokio::test]
async fn generated_query_response_omits_unselected_defaults_and_editing_state() {
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_FACET_DATABASE").unwrap(),
    })
    .await
    .unwrap();
    context.ensure_schema().await.unwrap();
    context.ensure_schema().await.unwrap();
    context.clear_sql_logs();
    let rows = Q::school_types_minimal()
        .select_code()
        .order_by_id_asc()
        .limit(1)
        .facet_by_platform_as_with_options(
            "platforms",
            Q::platforms_minimal()
                .select_name()
                .limit(10)
                .count_school_types_with("typeCount", Q::school_types_minimal().limit(10)),
            true,
        )
        .comment("inspect bounded generated entity response")
        .purpose("keep projection and editing metadata separate")
        .execute_for_list(&context)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let expected = rows[0].clone().into_json();
    assert_eq!(expected["code"], "PRIMARY");
    assert!(expected.get("name").is_none());
    let count = context.sql_logs().len();
    let one = WebResponse::from_entity(&rows[0]).to_json_value();
    let entities = WebResponse::from_entities(rows.data.clone()).to_json_value();
    let list = WebResponse::from_smart_list(rows.with_total_count(73)).to_json_value();
    for response in [&one, &entities, &list] {
        assert_eq!(response["data"], serde_json::json!([expected]));
        for field in [
            "name",
            "display_order",
            "platform_id",
            "_original_values",
            "_comment",
            "_dirty_fields",
        ] {
            assert!(
                response["data"][0].get(field).is_none(),
                "unselected/internal field leaked: {field}"
            );
        }
    }
    assert_eq!(list["recordCount"], 73);
    assert_eq!(list["facets"]["platforms"][0]["typeCount"], 2);
    assert_eq!(
        context.sql_logs().len(),
        count,
        "wrapping must not issue a query"
    );
    eprintln!(
        "WEB_RESPONSE_PROJECTION={}",
        serde_json::json!({
            "passed":true,"constructors":3,"generatedLibraryChanged":false,"wrapperDiagnosticStatements":0,
            "selected":expected,"response":list,"suppliedCountCalibration":73,"generatedPageCountAccepted":false
        })
    );
}
