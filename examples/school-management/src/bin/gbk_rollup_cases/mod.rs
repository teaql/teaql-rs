//! Application-owned generated Q/E/Mutation acceptance, without SQL.
use school_management_service_core::{
    E, Q, SchoolRequest, SchoolTypeRequest, TeaqlRuntime, request_support::AuditedSave as _,
};
use teaql_core::{CompactRow, Entity as _, Value};
type Error = Box<dyn std::error::Error>;

async fn rename(context: &impl TeaqlRuntime, id: u64, name: &str) -> Result<String, Error> {
    let mut entity = Q::school_types()
        .with_id_is(id)
        .limit(1)
        .comment("Load every scalar before changing a constant presentation name")
        .purpose("Prepare audited GBK ordering observations")
        .execute_for_one(context)
        .await?
        .ok_or("missing type")?;
    let before = E::school_type(&entity)
        .get_name()
        .eval()
        .ok_or("missing name")?;
    let code = E::school_type(&entity).get_code().eval();
    let version = E::school_type(&entity)
        .get_version()
        .eval()
        .ok_or("missing version")?;
    entity.update_name(name.to_owned());
    entity
        .audit_as("Temporarily set a Chinese presentation name for GBK acceptance")
        .save(context)
        .await?;
    let saved = Q::school_types()
        .with_id_is(id)
        .limit(1)
        .comment("Read back the audited presentation name")
        .purpose("Verify persisted GBK fixture through generated E")
        .execute_for_one(context)
        .await?
        .ok_or("missing saved type")?;
    assert_eq!(E::school_type(&saved).get_id().eval(), Some(id));
    assert_eq!(
        E::school_type(&saved).get_name().eval().as_deref(),
        Some(name)
    );
    assert_eq!(E::school_type(&saved).get_code().eval(), code);
    assert_eq!(
        E::school_type(&saved).get_version().eval(),
        Some(version + i64::from(before != name))
    );
    Ok(before)
}
async fn kind_rows(
    request: SchoolTypeRequest,
    context: &impl TeaqlRuntime,
) -> Result<Vec<CompactRow>, Error> {
    Ok(request
        .limit(1)
        .comment("Order completed Chinese text cells")
        .purpose("Verify generated GBK ordering rather than UTF8 fallback")
        .execute_for_rows(context)
        .await?
        .into_iter()
        .collect())
}
async fn school_rows(
    request: SchoolRequest,
    context: &impl TeaqlRuntime,
) -> Result<Vec<CompactRow>, Error> {
    Ok(request
        .limit(1)
        .comment("Order completed cells at the selected parent grain")
        .purpose("Verify generated parent and nested GBK result ordering")
        .execute_for_rows(context)
        .await?
        .into_iter()
        .collect())
}
fn check(
    result: Result<Vec<CompactRow>, Error>,
    supported: bool,
    label: &str,
    caption: &str,
    nested: bool,
) -> Result<serde_json::Value, Error> {
    if !supported {
        let error = result
            .expect_err("An unsupported backend must not silently return UTF8-ordered rows")
            .to_string();
        assert!(
            error.contains("GBK") && error.contains("unsupported"),
            "{error}"
        );
        return Ok(serde_json::json!({"case":label,"supported":false}));
    }
    let rows = result?;
    assert_eq!(rows.len(), 1);
    if nested {
        assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(1));
        assert_eq!(rows[0].get("capacity").and_then(Value::try_i64), Some(100));
        let Some(Value::Object(kind)) = rows[0].get("school_type") else {
            panic!("missing type");
        };
        assert_eq!(kind.get("caption"), Some(&Value::from(caption)));
    } else {
        assert_eq!(rows[0].get("caption"), Some(&Value::from(caption)));
    }
    let row = rows[0]
        .iter()
        .map(|(k, v)| (k.clone(), v.to_json_value()))
        .collect::<std::collections::BTreeMap<_, _>>();
    Ok(serde_json::json!({"case":label,"supported":true,"row":row}))
}
fn source() -> SchoolRequest {
    Q::schools_minimal()
        .count_as("schools")
        .sum_student_capacity_as("capacity")
}
pub async fn verify(context: &impl TeaqlRuntime, supported: bool) -> Result<(), Error> {
    let first = rename(context, 1001, "中类").await?;
    let second = rename(context, 1002, "阿类").await?;
    let kinds = Q::school_types_minimal()
        .group_by_name_as("caption")
        .count_as("types");
    let mut observations = vec![
        check(
            kind_rows(
                kinds.clone().add_order_by_ascending_using_gbk("caption"),
                context,
            )
            .await,
            supported,
            "gbk-ordinary-asc",
            "阿类",
            false,
        )?,
        check(
            kind_rows(kinds.add_order_by_descending_using_gbk("caption"), context).await,
            supported,
            "gbk-ordinary-desc",
            "中类",
            false,
        )?,
    ];
    let mut parent = source();
    parent
        .roll_up_to_school_type()
        .group_by_name_as("caption")
        .count_as("types")
        .add_order_by_ascending_using_gbk("caption")
        .limit(1);
    observations.push(check(
        school_rows(parent, context).await,
        supported,
        "gbk-parent",
        "阿类",
        true,
    )?);
    let mut nested = source();
    nested
        .roll_up_to_school_type()
        .group_by_name_as("caption")
        .count_as("types");
    observations.push(check(
        school_rows(
            nested.add_order_by_ascending_using_gbk("school_type.caption"),
            context,
        )
        .await,
        supported,
        "gbk-nested",
        "阿类",
        true,
    )?);
    if supported {
        let request = Q::school_types_minimal()
            .group_by_name_with_function("encoded", teaql_core::ExprFunction::Gbk)
            .count_as("types").add_order_by_ascending("encoded");
        let rows = kind_rows(request, context).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("encoded").and_then(Value::try_binary), Some([0xb0, 0xa2, 0xc0, 0xe0].as_slice()));
        observations.push(serde_json::json!({"case":"gbk-binary-ordinary","supported":true,
            "row":rows[0].iter().map(|(key,value)|(key.clone(),value.to_json_value())).collect::<std::collections::BTreeMap<_,_>>()}));
        let mut request = source();
        request.roll_up_to_school_type()
            .group_by_name_with_function("encoded", teaql_core::ExprFunction::Gbk)
            .count_as("types").add_order_by_ascending("encoded").limit(1);
        let rows = school_rows(request, context).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(1));
        assert_eq!(rows[0].get("capacity").and_then(Value::try_i64), Some(100));
        let Some(Value::Object(parent)) = rows[0].get("school_type") else { panic!("missing parent"); };
        assert_eq!(parent.get("encoded").and_then(Value::try_binary), Some([0xb0, 0xa2, 0xc0, 0xe0].as_slice()));
        observations.push(serde_json::json!({"case":"gbk-binary-parent","supported":true,
            "row":rows[0].iter().map(|(key,value)|(key.clone(),value.to_json_value())).collect::<std::collections::BTreeMap<_,_>>()}));
    }
    rename(context, 1001, &first).await?;
    rename(context, 1002, &second).await?;
    println!(
        "GENERATED_GBK_ROLLUP={}",
        serde_json::json!({"passed":true,"supported":supported,"caseCount":observations.len(),
        "observations":observations,"auditedMutation":true,"generatedQAndE":true,"restoredNames":[first,second]})
    );
    Ok(())
}
