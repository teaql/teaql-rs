use std::{collections::BTreeMap, error::Error, fs, io, sync::Arc};

use serde::Deserialize;
use serde_json::{json, Value};
use supplier_onboarding_service_core::teaql_core::Entity;
use supplier_onboarding_service_core::{service_runtime_from_env, AuditedSave, Q};
use teaql_runtime::{CheckRule, I18nCatalog, JsonFieldNamingProfile, RuntimeError, UserContext};

#[derive(Deserialize)]
struct Registration {
    name: Option<String>,
    minimum_order_quantity: i64,
}

#[derive(Deserialize)]
struct Warehouse {
    id: u64,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case")]
enum Operation {
    Select {
        locale: String,
    },
    Validate {
        #[serde(rename = "case")]
        id: String,
        input: Registration,
    },
    InstallCatalog {
        path: String,
    },
    VerifyUnchanged,
}

#[derive(Deserialize)]
struct Step {
    context: String,
    #[serde(flatten)]
    operation: Operation,
}

#[derive(Deserialize)]
struct Scenario {
    database: String,
    warehouse: Warehouse,
    steps: Vec<Step>,
}

fn emit(name: &str, context: &UserContext, kind: &str, mut details: Value) {
    details["type"] = json!(kind);
    details["context"] = json!(name);
    details["effectiveLocale"] = json!(context.language().code());
    println!("TEAQL_I18N_SHOWCASE {details}");
}

async fn verify_failure(
    name: &str,
    context: &UserContext,
    warehouse_id: u64,
    id: &str,
    input: Registration,
) -> Result<(), Box<dyn Error>> {
    let mut supplier = Q::suppliers()
        .comment("Prepare supplier registration")
        .purpose("Register a supplier for warehouse purchasing")
        .new_entity(context);
    supplier.update_warehouse_id(warehouse_id);
    if let Some(name) = input.name {
        supplier.update_name(name);
    }
    supplier.update_minimum_order_quantity(input.minimum_order_quantity);
    let outcome = supplier
        .audit_as("Register the supplier for purchasing")
        .save(context)
        .await;
    let failure = match outcome {
        Ok(_) => return Err(format!("invalid supplier registration was stored: {id}").into()),
        Err(RuntimeError::Check(failures)) if failures.len() == 1 => failures,
        Err(error) => return Err(error.into()),
    };
    let violation = &failure[0];
    let wire = violation.to_wire(JsonFieldNamingProfile::CamelCase);
    let (key, arguments) = match violation.rule {
        CheckRule::Required => ("checker.required", json!({})),
        CheckRule::Min => {
            let bound = wire
                .system_value
                .as_ref()
                .and_then(Value::as_i64)
                .ok_or("missing numeric minimum in the Checker result")?;
            ("checker.min", json!({"minimum": bound}))
        }
        _ => return Err(RuntimeError::Check(failure).into()),
    };
    emit(
        name,
        context,
        "validation",
        json!({
            "case": id, "messageKey": key, "nativeRule": wire.rule_id,
            "location": violation.location.model_path(), "message": wire.message,
            "arguments": arguments,
            "nativeArguments": {"inputValue": wire.input_value, "systemValue": wire.system_value},
        }),
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let plan: Scenario = serde_json::from_reader(io::stdin().lock())?;
    std::env::set_var(
        "SUPPLIER_ONBOARDING_SERVICE_CORE_DATABASE_URL",
        format!("sqlite://{}", plan.database),
    );
    let mut contexts = BTreeMap::new();
    for name in ["main", "independent"] {
        contexts.insert(name, service_runtime_from_env().await?);
    }
    contexts["main"].ensure_schema().await?;
    for step in plan.steps {
        let name = step.context.as_str();
        let context = contexts.get_mut(name).ok_or("unknown example context")?;
        match step.operation {
            Operation::Select { locale } => {
                let mut result = json!({"requestedLocale": locale, "accepted": true});
                match context.set_locale_code(&locale) {
                    Ok(()) => {}
                    Err(RuntimeError::UnsupportedLocale(_)) => {
                        result["accepted"] = json!(false);
                        result["nativeError"] = json!("UnsupportedLocale");
                    }
                    Err(error) => return Err(error.into()),
                }
                emit(name, context, "locale", result);
            }
            Operation::Validate { id, input } => {
                verify_failure(name, context, plan.warehouse.id, &id, input).await?;
            }
            Operation::InstallCatalog { path } => {
                let catalog = I18nCatalog::from_json(&fs::read_to_string(path)?)?;
                context.set_i18n_catalog(Arc::new(catalog));
                emit(name, context, "catalog-installed", json!({}));
            }
            Operation::VerifyUnchanged => {
                let rows = Q::suppliers_minimal()
                    .limit(1)
                    .comment("Check the rejected registrations")
                    .purpose("Verify no invalid supplier was stored")
                    .execute_for_list(context)
                    .await?;
                emit(
                    name,
                    context,
                    "storage",
                    json!({"supplierCount": rows.len()}),
                );
            }
        }
    }
    Ok(())
}
