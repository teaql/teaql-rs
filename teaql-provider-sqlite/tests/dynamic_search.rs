use std::{collections::BTreeMap, sync::Arc};
use teaql_core::{
    DataType, EntityDescriptor, Expr, InsertCommand, OrderBy, PropertyDescriptor, SelectQuery,
    TraceKind, TraceNode, Value, dynamic_search::*,
};
use teaql_data_service::{QueryExecutor, QueryRequest, SchemaProvider};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_sql::{SqlDataServiceExecutor, SqlDialect};

#[derive(Clone)]
struct Schema(Vec<Arc<EntityDescriptor>>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.iter().find(|e| e.name == name).cloned()
    }
}

#[test]
fn scoped_dynamic_search_retains_outer_and_related_tenant_filters() {
    futures_executor::block_on(async {
        let platform = EntityDescriptor::new("Platform")
            .table_name("search_platform")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("tenant_id", DataType::I64))
            .property(PropertyDescriptor::new("name", DataType::Text));
        let school = EntityDescriptor::new("School")
            .table_name("search_school")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("tenant_id", DataType::I64))
            .property(PropertyDescriptor::new("platform_id", DataType::I64))
            .property(PropertyDescriptor::new("name", DataType::Text));
        let transport = SqliteMutationExecutor::from_connection(
            rusqlite::Connection::open_in_memory().unwrap(),
        );
        // Provider boundary fixture, separate from generated bootstrap lifecycle tests.
        transport
            .ensure_schema(&SqliteDialect, &[&platform, &school])
            .unwrap();
        for (id, tenant) in [(1i64, 7i64), (2, 8)] {
            let command = InsertCommand::new("Platform")
                .value("id", id)
                .value("tenant_id", tenant)
                .value("name", "Campus");
            transport
                .execute(&SqliteDialect.compile_insert(&platform, &command).unwrap())
                .unwrap();
        }
        for (id, tenant, parent, name) in [
            (1i64, 7i64, 1i64, "School"),
            (2, 7, 1, "School"),
            (3, 7, 1, "School"),
            (4, 8, 1, "School"),
            (5, 7, 2, "School"),
            (6, 7, 1, "Other"),
        ] {
            let command = InsertCommand::new("School")
                .value("id", id)
                .value("tenant_id", tenant)
                .value("platform_id", parent)
                .value("name", name);
            transport
                .execute(&SqliteDialect.compile_insert(&school, &command).unwrap())
                .unwrap();
        }
        let executor = SqlDataServiceExecutor::new(
            SqliteDialect,
            transport,
            Schema(vec![Arc::new(platform.clone()), Arc::new(school)]),
        );
        let models = BTreeMap::from([
            (
                "School".into(),
                SearchModel {
                    fields: BTreeMap::from([
                        ("id".into(), "integer".into()),
                        ("name".into(), "string".into()),
                    ]),
                    relations: BTreeMap::from([("platform".into(), "Platform".into())]),
                },
            ),
            (
                "Platform".into(),
                SearchModel {
                    fields: BTreeMap::from([("name".into(), "string".into())]),
                    ..Default::default()
                },
            ),
        ]);
        let mut base = SelectQuery::new("School")
            .project("id")
            .project("name")
            .filter(Expr::eq("tenant_id", 7i64))
            .order_by(OrderBy::desc("id"))
            .limit(2)
            .comment("what: search tenant schools");
        base.hard_limit = 2;
        base.trace_chain.push(TraceNode::typed(
            TraceKind::Purpose,
            "School",
            None,
            "why: verify schema drift isolation",
        ));
        let before = base.clone();
        let mut warnings = vec![];
        let result = merge_dynamic_search(&base,
            r#"{"filter":{"name":"School","platform.name":"Campus","old_name":"secret","old_relation.name":"secret","platform.old_name":"secret"},"orderBy":[{"field":"removed","direction":"asc"},{"field":"name","direction":"asc"}]}"#,
            &models, |filter| match filter.field_path.as_str() {
                "name" => Ok(Expr::eq("name", filter.value.as_str().unwrap())),
                "platform.name" => Ok(Expr::in_subquery("platform_id", platform.clone(),
                    SelectQuery::new("Platform").filter(Expr::eq("tenant_id", 7i64))
                        .and_filter(Expr::eq("name", filter.value.as_str().unwrap())), "id")),
                _ => Err(DynamicSearchError("Unbound trusted field")),
            }, |order| Ok(OrderBy::asc(&order.field_path)), Some(&mut |w| warnings.push(w.clone()))).unwrap();
        assert_eq!(base, before);
        assert_eq!(result.query.hard_limit, 2);
        assert_eq!(result.query.trace_chain, base.trace_chain);
        let response = executor
            .query(QueryRequest {
                query: result.query,
                comment: base.comment,
                trace_chain: base.trace_chain,
                capture_debug_query: true,
                capture_execution_metadata: true,
            })
            .await
            .unwrap();
        assert_eq!(
            response
                .rows
                .iter()
                .map(|row| row.get("id").cloned())
                .collect::<Vec<_>>(),
            vec![Some(Value::I64(3)), Some(Value::I64(2))]
        );
        assert_eq!(warnings.len(), 4);
    });
}
