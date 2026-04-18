extern crate self as teaql_core;

mod entity;
mod expr;
mod list;
mod meta;
mod mutation;
mod naming;
mod query;
mod value;

pub use entity::{
    BaseEntity, BaseEntityData, Entity, EntityDescriptorStore, EntityError, IdentifiableEntity,
    TeaqlEntity, VersionedEntity,
};
pub use expr::{BinaryOp, Expr, ExprFunction};
pub use list::SmartList;
pub use meta::{EntityDescriptor, PropertyDescriptor, RelationDescriptor};
pub use mutation::{DeleteCommand, InsertCommand, MutationKind, RecoverCommand, UpdateCommand};
pub use naming::default_table_name;
pub use query::{
    Aggregate, AggregateFunction, NamedExpr, OrderBy, Record, RelationLoad, SelectQuery, Slice,
    SortDirection, record_to_json_value,
};
pub use value::{DataType, Decimal, Value};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use teaql_macros::TeaqlEntity;

    #[derive(Default)]
    struct TestStore {
        descriptors: Vec<EntityDescriptor>,
    }

    impl EntityDescriptorStore for TestStore {
        fn register_descriptor(&mut self, descriptor: EntityDescriptor) {
            self.descriptors.push(descriptor);
        }
    }

    #[allow(dead_code)]
    #[derive(Clone, TeaqlEntity)]
    #[teaql(entity = "Order", table = "orders")]
    struct OrderRow {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        #[teaql(column = "display_name")]
        name: String,
    }

    #[test]
    fn derive_entity_descriptor() {
        let descriptor = OrderRow::entity_descriptor();
        assert_eq!(descriptor.name, "Order");
        assert_eq!(descriptor.table_name, "orders");
        assert_eq!(
            descriptor.id_property().map(|p| p.name.as_str()),
            Some("id")
        );
        assert_eq!(
            descriptor
                .property_by_name("name")
                .map(|p| p.column_name.as_str()),
            Some("display_name")
        );
    }

    #[allow(dead_code)]
    #[derive(TeaqlEntity)]
    #[teaql(entity = "Product", table = "product")]
    struct ProductRow {
        #[teaql(id)]
        id: u64,
        name: String,
    }

    #[allow(dead_code)]
    #[derive(TeaqlEntity)]
    #[teaql(entity = "OrderLine", table = "orderline")]
    struct OrderLineRow {
        #[teaql(id)]
        id: u64,
        #[teaql(column = "order_id")]
        order_id: u64,
        #[teaql(relation(
            target = "Product",
            local_key = "product_id",
            foreign_key = "id",
            attach = false,
            delete_missing = false
        ))]
        product: Option<ProductRow>,
    }

    #[test]
    fn derive_relation_descriptor_and_register() {
        let descriptor = OrderLineRow::entity_descriptor();
        let relation = descriptor.relation_by_name("product").unwrap();
        assert_eq!(relation.target_entity, "Product");
        assert_eq!(relation.local_key, "product_id");
        assert_eq!(relation.foreign_key, "id");
        assert!(!relation.attach);
        assert!(!relation.delete_missing);

        let mut store = TestStore::default();
        OrderLineRow::register_into(&mut store);
        assert_eq!(store.descriptors.len(), 1);
        assert_eq!(store.descriptors[0].name, "OrderLine");
    }

    #[test]
    fn register_entities_macro_registers_multiple_descriptors() {
        let mut store = TestStore::default();
        crate::register_entities!(&mut store, OrderRow, OrderLineRow);

        assert_eq!(store.descriptors.len(), 2);
        assert_eq!(store.descriptors[0].name, "Order");
        assert_eq!(store.descriptors[1].name, "OrderLine");
    }

    #[allow(dead_code)]
    #[derive(TeaqlEntity)]
    struct DefaultTableNameRow {
        #[teaql(id)]
        id: u64,
    }

    #[allow(dead_code)]
    #[derive(TeaqlEntity)]
    struct TypedValueRow {
        #[teaql(id)]
        id: u64,
        payload: serde_json::Value,
        birthday: NaiveDate,
        happened_at: chrono::DateTime<Utc>,
    }

    #[allow(dead_code)]
    #[derive(TeaqlEntity)]
    #[teaql(entity = "OrderAggregate", table = "order_aggregate")]
    struct OrderAggregateRow {
        #[teaql(id)]
        id: u64,
        #[teaql(dynamic)]
        dynamic: BTreeMap<String, serde_json::Value>,
    }

    #[test]
    fn default_table_name_matches_java_sql_repository_rule() {
        assert_eq!(default_table_name("Order"), "order_data");
        assert_eq!(default_table_name("OrderLine"), "order_line_data");
        assert_eq!(EntityDescriptor::new("Order").table_name, "order_data");
        assert_eq!(
            EntityDescriptor::new("OrderLine").table_name,
            "order_line_data"
        );
        assert_eq!(
            DefaultTableNameRow::entity_descriptor().table_name,
            "default_table_name_row_data"
        );
    }

    #[test]
    fn derive_maps_json_date_and_timestamp_types() {
        let descriptor = TypedValueRow::entity_descriptor();
        assert_eq!(
            descriptor.property_by_name("payload").map(|p| p.data_type),
            Some(DataType::Json)
        );
        assert_eq!(
            descriptor.property_by_name("birthday").map(|p| p.data_type),
            Some(DataType::Date)
        );
        assert_eq!(
            descriptor
                .property_by_name("happened_at")
                .map(|p| p.data_type),
            Some(DataType::Timestamp)
        );

        let birthday = NaiveDate::from_ymd_opt(2024, 2, 3).unwrap();
        let happened_at = Utc.with_ymd_and_hms(2024, 2, 3, 4, 5, 6).unwrap();
        assert_eq!(
            Value::from(serde_json::json!({"a": 1})),
            Value::Json(serde_json::json!({"a": 1}))
        );
        assert_eq!(Value::from(birthday), Value::Date(birthday));
        assert_eq!(Value::from(happened_at), Value::Timestamp(happened_at));
    }

    #[test]
    fn query_builders_cover_filters_sort_aggregates_and_relations() {
        let query = SelectQuery::new("Order")
            .projects(["id", "name"])
            .filter(Expr::gte("version", 1_i64))
            .and_filter(Expr::not_in_list(
                "name",
                vec![Value::from("archived"), Value::from("deleted")],
            ))
            .and_filter(Expr::in_large(
                "id",
                vec![Value::from(1_u64), Value::from(2_u64)],
            ))
            .and_filter(Expr::contain("name", "rob"))
            .and_filter(Expr::sound_like("name", "Robert"))
            .or_filter(Expr::is_null("name"))
            .project_expr("nameSound", Expr::soundex(Expr::column("name")))
            .order_desc("id")
            .order_gbk_asc("name")
            .group_by("name")
            .count("total")
            .sum("version", "versionSum")
            .stddev("version", "versionStddev")
            .having(Expr::gt("total", 1_i64))
            .relation("lines")
            .page(20, 10);

        assert_eq!(query.projection, vec!["id", "name"]);
        assert_eq!(
            query.expr_projection,
            vec![NamedExpr::new(
                "nameSound",
                Expr::soundex(Expr::column("name"))
            )]
        );
        assert_eq!(
            query.order_by,
            vec![OrderBy::desc("id"), OrderBy::asc_gbk("name")]
        );
        assert_eq!(query.group_by, vec!["name"]);
        assert_eq!(
            query.aggregates,
            vec![
                Aggregate::count("total"),
                Aggregate::sum("version", "versionSum"),
                Aggregate::stddev("version", "versionStddev")
            ]
        );
        assert_eq!(query.having, Some(Expr::gt("total", 1_i64)));
        assert_eq!(query.relations, vec![RelationLoad::new("lines")]);
        assert_eq!(
            query.slice,
            Some(Slice {
                limit: Some(10),
                offset: 20
            })
        );
        assert!(matches!(query.filter, Some(Expr::Or(_))));
    }

    #[test]
    fn sound_like_builds_soundex_equality() {
        assert_eq!(
            Expr::sound_like("name", "Robert"),
            Expr::binary(
                Expr::soundex(Expr::column("name")),
                BinaryOp::Eq,
                Expr::soundex(Expr::value("Robert"))
            )
        );
    }

    #[test]
    fn java_style_string_match_builders_expand_like_patterns() {
        assert_eq!(Expr::contain("name", "tea"), Expr::like("name", "%tea%"));
        assert_eq!(
            Expr::not_contain("name", "tea"),
            Expr::not_like("name", "%tea%")
        );
        assert_eq!(Expr::begin_with("name", "tea"), Expr::like("name", "tea%"));
        assert_eq!(
            Expr::not_begin_with("name", "tea"),
            Expr::not_like("name", "tea%")
        );
        assert_eq!(Expr::end_with("name", "tea"), Expr::like("name", "%tea"));
        assert_eq!(
            Expr::not_end_with("name", "tea"),
            Expr::not_like("name", "%tea")
        );
    }

    #[test]
    fn large_in_builders_use_large_binary_ops() {
        assert_eq!(
            Expr::in_large("id", vec![Value::from(1_u64)]),
            Expr::binary(
                Expr::column("id"),
                BinaryOp::InLarge,
                Expr::value(Value::List(vec![Value::from(1_u64)]))
            )
        );
        assert_eq!(
            Expr::not_in_large("id", vec![Value::from(1_u64)]),
            Expr::binary(
                Expr::column("id"),
                BinaryOp::NotInLarge,
                Expr::value(Value::List(vec![Value::from(1_u64)]))
            )
        );
    }

    #[test]
    fn subquery_builder_projects_requested_field() {
        let query = SelectQuery::new("OrderLine").filter(Expr::eq("name", "line-1"));
        let expr = Expr::in_subquery("id", OrderLineRow::entity_descriptor(), query, "order_id");

        let Expr::SubQuery {
            left,
            op,
            entity,
            query,
        } = expr
        else {
            panic!("expected subquery expression");
        };
        assert_eq!(*left, Expr::column("id"));
        assert_eq!(op, BinaryOp::In);
        assert_eq!(entity.name, "OrderLine");
        assert_eq!(query.projection, vec!["order_id"]);
    }

    #[test]
    fn smart_list_supports_entity_ids_versions_and_records() {
        let rows = SmartList::from(vec![
            OrderRow {
                id: 1,
                version: 2,
                name: String::from("a"),
            },
            OrderRow {
                id: 3,
                version: 4,
                name: String::from("b"),
            },
        ]);

        assert_eq!(rows.ids(), vec![Value::U64(1), Value::U64(3)]);
        assert_eq!(rows.versions(), vec![2, 4]);

        let records = rows.into_records();
        assert_eq!(records.len(), 2);
        assert_eq!(records.data[0].get("id"), Some(&Value::U64(1)));
        assert_eq!(records.data[1].get("version"), Some(&Value::I64(4)));
    }

    #[test]
    fn smart_list_supports_java_style_collection_helpers() {
        let mut rows = SmartList::empty()
            .with_total_count(10)
            .with_aggregation("count", 2_u64)
            .with_summary("label", "orders");
        rows.push(OrderRow {
            id: 1,
            version: 2,
            name: String::from("a"),
        });
        rows.extend(vec![OrderRow {
            id: 3,
            version: 4,
            name: String::from("b"),
        }]);

        assert_eq!(rows.total_count_or_len(), 10);
        assert_eq!(rows.get(1).map(|row| row.name.as_str()), Some("b"));
        assert_eq!(rows.last().map(|row| row.id), Some(3));
        assert_eq!(rows.aggregation("count"), Some(&Value::U64(2)));
        assert_eq!(
            rows.summary("label"),
            Some(&Value::Text(String::from("orders")))
        );
        assert_eq!(rows.aggregation_json(), serde_json::json!({"count": 2}));
        assert_eq!(rows.summary_json(), serde_json::json!({"label": "orders"}));

        let names = rows.to_list(|row| row.name.clone());
        assert_eq!(names, vec![String::from("a"), String::from("b")]);
        let ids = rows.to_set(|row| row.id);
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![1, 3]);

        let by_id = rows.map_by_id();
        assert_eq!(by_id.get("u:1").map(|row| row.name.as_str()), Some("a"));
        assert_eq!(by_id.get("u:3").map(|row| row.name.as_str()), Some("b"));

        let identity = rows.identity_map(|row| row.name.clone());
        assert_eq!(identity.get("a").map(|row| row.id), Some(1));
        let grouped = rows.group_by(|row| row.version % 2);
        assert_eq!(grouped.get(&0).map(Vec::len), Some(2));

        rows.merge_by(
            vec![
                OrderRow {
                    id: 3,
                    version: 5,
                    name: String::from("b2"),
                },
                OrderRow {
                    id: 4,
                    version: 1,
                    name: String::from("c"),
                },
            ],
            |row| row.id,
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.map_by_id().get("u:3").map(|row| row.version), Some(5));

        rows.retain(|row| row.id != 1);
        assert_eq!(rows.ids(), vec![Value::U64(3), Value::U64(4)]);
        assert_eq!((&rows).into_iter().count(), 2);
        assert_eq!(rows[0].name, "b2");
    }

    #[test]
    fn dynamic_properties_roundtrip_into_json() {
        let aggregate = OrderAggregateRow::from_record(Record::from([
            (String::from("id"), Value::U64(7)),
            (String::from("lineCount"), Value::I64(3)),
            (String::from("amount"), Value::F64(18.5)),
            (
                String::from("detail"),
                Value::Object(Record::from([(String::from("status"), Value::from("ok"))])),
            ),
        ]))
        .unwrap();

        assert_eq!(
            aggregate.dynamic.get("lineCount"),
            Some(&serde_json::json!(3))
        );
        assert_eq!(
            aggregate.dynamic.get("amount"),
            Some(&serde_json::json!(18.5))
        );
        assert_eq!(
            aggregate.dynamic.get("detail"),
            Some(&serde_json::json!({"status": "ok"}))
        );

        let json = aggregate.into_json();
        assert_eq!(json["id"], serde_json::json!(7));
        assert_eq!(json["lineCount"], serde_json::json!(3));
        assert_eq!(json["amount"], serde_json::json!(18.5));
        assert_eq!(json["detail"], serde_json::json!({"status": "ok"}));
    }

    #[test]
    fn base_entity_data_roundtrips_record_and_dynamic_properties() {
        let mut base = BaseEntityData::new()
            .with_id(11)
            .with_version(3)
            .with_dynamic("lineCount", 5)
            .with_dynamic("detail", serde_json::json!({"status": "ok"}));
        assert_eq!(base.dynamic("lineCount"), Some(&serde_json::json!(5)));
        base.put_dynamic("amount", 18.5);

        let record = base.to_record();
        assert_eq!(record.get("id"), Some(&Value::U64(11)));
        assert_eq!(record.get("version"), Some(&Value::I64(3)));
        assert_eq!(
            record.get("lineCount"),
            Some(&Value::Json(serde_json::json!(5)))
        );
        assert_eq!(
            record.get("detail"),
            Some(&Value::Json(serde_json::json!({"status": "ok"})))
        );

        let restored = BaseEntityData::from_record(&record).unwrap();
        assert_eq!(restored.id, Some(11));
        assert_eq!(restored.version, 3);
        assert_eq!(restored.dynamic("amount"), Some(&serde_json::json!(18.5)));
    }
}
