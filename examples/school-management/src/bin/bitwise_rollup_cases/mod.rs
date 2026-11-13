//! Application-owned generated Q acceptance; no SQL or generated-source edits.
use school_management_service_core::{Q, SchoolRequest, TeaqlRuntime};
use std::collections::BTreeMap;
use teaql_core::{CompactRow, Expr, Value};
type Error = Box<dyn std::error::Error>;

fn snapshot(rows: &[CompactRow]) -> Vec<serde_json::Value> {
    rows.iter()
        .map(|row| {
            serde_json::Value::Object(
                row.iter()
                    .map(|(key, value)| (key.clone(), value.to_json_value()))
                    .collect(),
            )
        })
        .collect()
}

fn source() -> SchoolRequest {
    Q::schools_minimal()
        .count_as("schools")
        .aggregate_bit_and("student_capacity", "bitsAnd")
        .aggregate_bit_or("student_capacity", "bitsOr")
        .aggregate_bit_xor("student_capacity", "bitsXor")
}
async fn rows(
    request: SchoolRequest,
    context: &impl TeaqlRuntime,
) -> Result<Vec<CompactRow>, Error> {
    Ok(request
        .limit(20)
        .comment("Compute integer bitwise results at each declared observation grain")
        .purpose("Verify generated ordinary and layered bitwise statistics")
        .execute_for_rows(context)
        .await?
        .into_iter()
        .collect())
}
fn object<'a>(row: &'a CompactRow, key: &str) -> &'a BTreeMap<String, Value> {
    let Some(Value::Object(value)) = row.get(key) else {
        panic!("missing object {key}");
    };
    value
}
fn bits<'a>(get: impl Fn(&str) -> Option<&'a Value>, expected: Option<[i64; 3]>) {
    for (index, name) in ["bitsAnd", "bitsOr", "bitsXor"].into_iter().enumerate() {
        assert_eq!(
            get(name),
            Some(&expected.map_or(Value::Null, |values| Value::I64(values[index])))
        );
    }
}
pub async fn verify(context: &impl TeaqlRuntime) -> Result<(), Error> {
    let mut observations = Vec::new();
    let result = rows(source(), context).await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools"), Some(&Value::I64(5)));
    bits(|key| result[0].get(key), Some([32, 868, 100]));
    observations.push(serde_json::json!({"case":"bitwise-ordinary","rows":snapshot(&result)}));

    let result = rows(
        source().group_by_school_type().order_by_school_type_asc(),
        context,
    )
    .await?;
    assert_eq!(result.len(), 2);
    for (index, id, count, expected) in [(0, 1001, 4, [800, 800, 0]), (1, 1002, 1, [100, 100, 100])]
    {
        assert_eq!(result[index].get("school_type_id"), Some(&Value::I64(id)));
        assert_eq!(result[index].get("schools"), Some(&Value::I64(count)));
        bits(|key| result[index].get(key), Some(expected));
    }
    observations.push(serde_json::json!({"case":"bitwise-grouped","rows":snapshot(&result)}));

    let mut query = source();
    query
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types")
        .aggregate_bit_and("id", "bitsAnd")
        .aggregate_bit_or("id", "bitsOr")
        .aggregate_bit_xor("id", "bitsXor");
    let result = rows(query, context).await?;
    assert_eq!(result.len(), 1);
    bits(|key| result[0].get(key), Some([32, 868, 100]));
    let parent = object(&result[0], "school_type");
    assert_eq!(parent.get("types"), Some(&Value::I64(2)));
    bits(|key| parent.get(key), Some([1000, 1003, 3]));
    observations.push(serde_json::json!({"case":"bitwise-parent","rows":snapshot(&result)}));

    let mut query = source();
    let mut parent = query
        .roll_up_to_school_type()
        .count_as("types")
        .aggregate_bit_and("id", "bitsAnd")
        .aggregate_bit_or("id", "bitsOr")
        .aggregate_bit_xor("id", "bitsXor");
    parent
        .roll_up_to_platform()
        .group_by_name()
        .count_as("platforms")
        .aggregate_bit_and("id", "bitsAnd")
        .aggregate_bit_or("id", "bitsOr")
        .aggregate_bit_xor("id", "bitsXor");
    let result = rows(query, context).await?;
    assert_eq!(result.len(), 1);
    bits(|key| result[0].get(key), Some([32, 868, 100]));
    let parent = object(&result[0], "school_type");
    assert_eq!(parent.get("types"), Some(&Value::I64(2)));
    bits(|key| parent.get(key), Some([1000, 1003, 3]));
    let Some(Value::Object(platform)) = parent.get("platform") else {
        panic!("missing platform");
    };
    assert_eq!(platform.get("platforms"), Some(&Value::I64(1)));
    bits(|key| platform.get(key), Some([1, 1, 1]));
    observations.push(serde_json::json!({"case":"bitwise-two-hop","rows":snapshot(&result)}));

    let mut query = source();
    query
        .roll_up_to_school_type()
        .group_by_platform()
        .aggregate_bit_xor("id", "parentXor")
        .having(Expr::eq("parentXor", 3))
        .add_order_by_descending("parentXor")
        .limit(1);
    let query = query
        .having(Expr::eq("bitsXor", 100))
        .add_order_by_descending("school_type.parentXor");
    let rejected = query.clone().having(Expr::eq("bitsXor", 101));
    let result = rows(query, context).await?;
    assert_eq!(result.len(), 1);
    bits(|key| result[0].get(key), Some([32, 868, 100]));
    assert!(rows(rejected, context).await?.is_empty());
    observations.push(serde_json::json!({"case":"bitwise-having-page","rows":snapshot(&result)}));

    let empty = || source().with_student_capacity_less_than(0);
    let result = rows(empty(), context).await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools"), Some(&Value::I64(0)));
    bits(|key| result[0].get(key), None);
    observations.push(serde_json::json!({"case":"bitwise-empty-scalar","rows":snapshot(&result)}));
    let mut query = empty();
    query
        .roll_up_to_school_type()
        .count_as("types")
        .aggregate_bit_and("id", "bitsAnd")
        .aggregate_bit_or("id", "bitsOr")
        .aggregate_bit_xor("id", "bitsXor");
    let result = rows(query, context).await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools"), Some(&Value::I64(0)));
    bits(|key| result[0].get(key), None);
    let parent = object(&result[0], "school_type");
    assert_eq!(parent.get("types"), Some(&Value::I64(0)));
    bits(|key| parent.get(key), None);
    observations.push(serde_json::json!({"case":"bitwise-empty-layered","rows":snapshot(&result)}));
    let result = rows(empty().group_by_school_type(), context).await?;
    assert!(result.is_empty());
    observations.push(serde_json::json!({"case":"bitwise-empty-grouped","rows":snapshot(&result)}));
    println!(
        "GENERATED_BITWISE_ROLLUP={}",
        serde_json::json!({"passed":true,"caseCount":observations.len(),
        "generatedQ":true,"integerResults":true,"observations":observations})
    );
    Ok(())
}
