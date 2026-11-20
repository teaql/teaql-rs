//! Application-owned school browsing through the current model-aware Q/Create APIs.
use school_management_service_core::{service_runtime, School, ServiceRuntimeConfig, Q};
use std::{collections::BTreeMap, error::Error, path::PathBuf};
use teaql_core::{SmartList, Value};

const PREFIX: &str = "Facet Showcase ";

fn counts(rows: &SmartList<School>) -> Result<BTreeMap<String, i64>, Box<dyn Error>> {
    let facets = rows
        .facet("schoolTypes")
        .ok_or("missing school type facets")?;
    facets
        .data
        .iter()
        .map(|row| {
            let code = match row.get("code") {
                Some(Value::Text(value)) => value.clone(),
                _ => return Err("requested school type code was not returned".into()),
            };
            let count = row
                .get("matchingSchools")
                .and_then(Value::try_i64)
                .ok_or("requested membership count was not returned")?;
            Ok((code, count))
        })
        .collect()
}

pub async fn run(database: PathBuf) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = database
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: database.to_string_lossy().into_owned(),
    })
    .await?;
    context.ensure_schema().await?;
    let mut created = 0;
    for (name, primary) in [
        ("Facet Showcase Harbor Primary", true),
        ("Facet Showcase Riverside Primary", true),
        ("Facet Showcase Hillview Secondary", false),
    ] {
        let existing = Q::schools_minimal()
            .with_name_is(name)
            .limit(2)
            .comment("look up one demonstration school")
            .purpose("repeat the school directory without duplicate records")
            .execute_for_list(&context)
            .await?;
        assert!(
            existing.len() <= 1,
            "demonstration school identity is unique"
        );
        if existing.is_empty() {
            let mut school = Q::schools()
                .comment("prepare a school directory record")
                .purpose("make the school search demonstration reproducible")
                .new_entity(&context)?;
            school
                .update_platform_id(1)
                .update_name(name)
                .update_address("Campus demonstration district")
                .update_student_capacity(800)
                .update_active(true);
            if primary {
                school.update_school_type_to_primary();
            } else {
                school.update_school_type_to_secondary();
            }
            school
                .audit_as("publish one demonstration school")
                .save(&context)
                .await?;
            created += 1;
        }
    }

    let all = Q::schools_minimal()
        .select_name()
        .with_name_starting_with(PREFIX)
        .order_by_name_asc()
        .limit(10)
        .comment("read the school directory dataset")
        .purpose("verify three real schools before browsing")
        .execute_for_list(&context)
        .await?;
    assert_eq!(all.len(), 3);

    let mut rejected = Q::schools()
        .comment("prepare an unaudited publication attempt")
        .purpose("show that directory changes require an audit reason")
        .new_entity(&context)?;
    rejected
        .update_platform_id(1)
        .update_name("Facet Showcase Rejected");
    rejected.update_school_type_to_primary();
    assert!(rejected.audit_as("").save(&context).await.is_err());
    let rejected_rows = Q::schools_minimal()
        .with_name_is("Facet Showcase Rejected")
        .limit(2)
        .comment("check the rejected publication")
        .purpose("verify a missing audit reason cannot publish a school")
        .execute_for_list(&context)
        .await?;
    assert!(rejected_rows.is_empty());

    let mut observations = Vec::new();
    for (primary_only, include_all) in [(false, true), (true, false), (true, true)] {
        let request = Q::schools_minimal()
            .select_name()
            .with_name_starting_with(PREFIX)
            .order_by_name_asc()
            .limit(1)
            .facet_by_school_type_as_with_options(
                "schoolTypes",
                Q::school_types_minimal()
                    .select_code()
                    .order_by_id_asc()
                    .limit(10)
                    .count_schools_with(
                        "matchingSchools",
                        Q::schools_minimal()
                            .with_name_starting_with(PREFIX)
                            .limit(10),
                    ),
                include_all,
            );
        let request = if primary_only {
            request.with_school_type_is_primary()
        } else {
            request
        };
        let page = request
            .comment("browse one school and the school type choices")
            .purpose("render a filtered school directory with full membership counts")
            .execute_for_list(&context)
            .await?;
        assert_eq!(page.len(), 1);
        let actual = counts(&page)?;
        let mut expected = BTreeMap::from([("PRIMARY".to_owned(), 2)]);
        if !primary_only || include_all {
            expected.insert("SECONDARY".to_owned(), if primary_only { 0 } else { 1 });
        }
        assert_eq!(
            actual, expected,
            "counts describe all matching schools, not the one-row page"
        );
        observations.push(serde_json::json!({
            "primaryOnly":primary_only,"includeAll":include_all,
            "visibleSchools":page.len(),"membershipCounts":actual
        }));
    }
    println!("School directory: 3 schools; a one-row page retains full school-type counts.");
    println!(
        "SHOWCASE_RESULT {}",
        serde_json::json!({
            "scenario":"school-directory-facets","schools":all.len(),"created":created,
            "cases":observations,"missingAuditRejectedWithoutWrite":true,
            "generatedLibraryScope":"rust-lib-core",
            "applicationScope":"rust-app-console"
        })
    );
    Ok(())
}
