use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{Value as Json, json};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, Value};
use teaql_runtime::checker_messages::*;
use teaql_runtime::{
    CheckResult, CheckResults, Checker, EntityValues, I18nCatalog, InMemoryCheckerRegistry,
    InMemoryMetadataStore, JsonFieldNamingProfile, Language, ObjectLocation, RuntimeError,
    RuntimeModule, UserContext,
};

fn fixture() -> Json {
    serde_json::from_str(include_str!("fixtures/natural-messages/cases-v1.json")).unwrap()
}
fn model() -> Arc<CheckerPresentationModel> {
    let fixture = fixture();
    Arc::new(CheckerPresentationModel::from_json(&json!({"schema":"teaql.checker-model/v1", "revision":fixture["revision"], "entities":fixture["entities"], "currentObject":fixture["currentObject"]}).to_string()).unwrap())
}
fn path(segments: &Json) -> ObjectLocation {
    segments.as_array().unwrap().iter().fold(
        ObjectLocation::root(),
        |path, segment| match segment {
            Json::String(name) => path.member(name),
            _ => path.element(segment.as_u64().unwrap() as usize),
        },
    )
}
fn context() -> UserContext {
    let mut context = UserContext::new();
    context.set_checker_presentation_model(model());
    context
}

#[test]
fn shared_twenty_four_golden_cases() {
    let fixture = fixture();
    let roots: Json =
        serde_json::from_str(include_str!("fixtures/natural-messages/root-cases-v1.json")).unwrap();
    let cases: Vec<_> = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .chain(roots["cases"].as_array().unwrap())
        .collect();
    assert_eq!(cases.len(), 24);
    for case in cases {
        let origin = if case.as_object().unwrap().contains_key("origin") {
            case["origin"].as_str()
        } else {
            Some("Catalog")
        };
        let location = path(&case["path"]);
        let presentation = CheckerPresentation::capture(model(), origin, &location);
        assert_eq!(
            format!("{:?}", presentation.shape()).to_uppercase(),
            case["shape"].as_str().unwrap(),
            "{}",
            case["name"]
        );
        let language = Language::from_code(case["locale"].as_str().unwrap()).unwrap();
        let rule = case["rule"].as_str().unwrap();
        let selection =
            CheckerMessageCatalog::builtin().select(language, rule, presentation.shape());
        let arguments = case
            .get("arguments")
            .and_then(Json::as_object)
            .map(|args| {
                args.iter()
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            value
                                .as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| value.to_string()),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let output = presentation.render(CheckerMessageCatalog::builtin(), &selection, &arguments);
        assert_eq!(
            output.message,
            case["expected"].as_str().unwrap(),
            "{}",
            case["name"]
        );
        if let Some(pointer) = case["instancePath"].as_str() {
            assert_eq!(location.instance_path(), pointer);
        }
        println!("golden {} PASS", case["name"]);
    }
}

#[test]
fn all_fifteen_locales_have_270_complete_templates() {
    for language in Language::ALL {
        for rule in [
            "checker.required",
            "checker.min",
            "checker.max",
            "checker.minLength",
            "checker.maxLength",
            "checker.invalid",
        ] {
            for shape in [MessageShape::One, MessageShape::Two, MessageShape::Deep] {
                let selection = CheckerMessageCatalog::builtin()
                    .lookup(language, rule, shape)
                    .unwrap();
                let text = selection.render(&BTreeMap::from([
                    ("field".into(), "F".into()),
                    ("object".into(), "O".into()),
                    ("objectPath".into(), "P".into()),
                    ("system".into(), "8".into()),
                    ("input_len".into(), "9".into()),
                    ("unit".into(), "U".into()),
                ]));
                assert!(text.contains('F') && !text.contains('{'));
                if shape == MessageShape::Two {
                    assert!(text.contains('O'));
                }
                if shape == MessageShape::Deep {
                    assert!(text.contains('P'));
                }
            }
        }
    }
}

fn catalog(locales: Json) -> String {
    json!({"schema":"teaql.checker-messages/v1", "defaultLocale":"en", "locales": locales})
        .to_string()
}

#[test]
fn malformed_catalogs_reject_without_replacing_active_data() {
    let good = catalog(json!({"en":{"rules":{"checker.required":{"one":"Need {field}."}}}}));
    let mut context = context();
    context.install_checker_message_catalog(&good).unwrap();
    let mut negatives = vec![
        catalog(json!({"en":{"rules":{"checker.required":{"one":"Need {unknown}."}}}})),
        catalog(json!({"en":{"rules":{"checker.required":{"one":"Need {field} {input}."}}}})),
        catalog(json!({"en":{"rules":{"checker.min":{"one":"Need {field}."}}}})),
        catalog(
            json!({"en":{"rules":{"checker.maxLength":{"one":"{field} {system} {input_len} {unit} {input}"}}}}),
        ),
        catalog(
            json!({"en":{"locationPatterns":{"reference":"{relation}","item":"{ordinal} {relation}","separator":"{input}"}}}),
        ),
        catalog(json!({"en":{"rules":{"checker.required":{"one":"no field"}}}})),
        catalog(json!({"en":{"rules":{"checker.required":{"two":"Need {field}."}}}})),
        catalog(json!({"en":{"rules":{"checker.required":{"deep":"Need {field}."}}}})),
        catalog(json!({"en":{"rules":{"checker.required":{"four":"{field}"}}}})),
        catalog(json!({"en":{"rules":{"checker.required":{"one":""}}}})),
        catalog(json!({"en":{"rules":{"checker.required":{"one":"{field"}}}})),
        catalog(json!({"en":{"rules":{"checker.required":{"one":"{field}}"}}}})),
        catalog(json!({"en":{},"en-US":{}})),
        catalog(
            json!({"en":{"locationPatterns":{"reference":"{relation}","item":"{relation}","separator":" → "}}}),
        ),
        good.replace("teaql.checker-messages/v1", "teaql.checker-messages/v2"),
        format!("{good} {{}}"),
    ];
    negatives.push(good.replace("\"one\":", "\"one\":\"duplicate {field}\",\"one\":"));
    for bad in negatives {
        assert!(context.install_checker_message_catalog(&bad).is_err());
    }
    let mut errors = vec![CheckResult::required(ObjectLocation::root().member("name"))];
    context.translate_check_results_from("Catalog", &mut errors);
    assert_eq!(errors[0].message.as_deref(), Some("Need name."));
}

#[test]
fn model_identity_conflicts_reject_and_shared_models_compose_without_casing_guesses() {
    let original = model();
    assert_eq!(
        original.merge(&original).unwrap().revision(),
        original.revision()
    );
    let mut fixture = fixture();
    fixture["entities"]["Catalog"]["fields"][0]["aliases"] = json!(["entryList"]);
    let bad = json!({"schema":"teaql.checker-model/v1", "revision":"bad", "entities":fixture["entities"]}).to_string();
    assert!(CheckerPresentationModel::from_json(&bad).is_err());
    let mut fixture = self::fixture();
    fixture["entities"]["Catalog"]["labels"]["en"] = json!("different catalog");
    let conflict = CheckerPresentationModel::from_json(&json!({"schema":"teaql.checker-model/v1", "revision":"conflict", "entities":fixture["entities"]}).to_string()).unwrap();
    assert!(original.merge(&conflict).is_err());
    let shared = RuntimeModule::new()
        .checker_presentation_model(original)
        .into_context();
    let mut errors = vec![CheckResult::required(
        ObjectLocation::root()
            .member("entryList")
            .element(1)
            .member("name"),
    )];
    shared.translate_check_results_from("Catalog", &mut errors);
    assert_eq!(
        errors[0].message.as_deref(),
        Some("The name of the 2nd entry is required.")
    );
}

#[test]
fn english_ordinals_include_teens_and_maximum_machine_index() {
    for (n, word) in [
        (1, "1st"),
        (2, "2nd"),
        (3, "3rd"),
        (11, "11th"),
        (12, "12th"),
        (13, "13th"),
        (21, "21st"),
    ] {
        assert_eq!(ordinal(n), word);
    }
    let location = ObjectLocation::root()
        .member("entry_list")
        .element(usize::MAX)
        .member("name");
    let snapshot = CheckerPresentation::capture(model(), Some("Catalog"), &location);
    let output = snapshot.render(
        CheckerMessageCatalog::builtin(),
        &CheckerMessageCatalog::builtin().select(
            Language::English,
            "checker.required",
            snapshot.shape(),
        ),
        &BTreeMap::new(),
    );
    assert!(
        output
            .message
            .contains(&(usize::MAX as u128 + 1).to_string())
    );
    assert!(location.instance_path().contains(&usize::MAX.to_string()));
}

struct Rules;
impl Checker for Rules {
    fn entity(&self) -> &str {
        "Catalog"
    }
    fn check_and_fix(
        &self,
        _: &UserContext,
        _: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        let owner = location.clone().member("entry_list").element(1);
        results.push(CheckResult::required(owner.clone().member("name")).with_entity_type("Entry"));
        results.push(
            CheckResult::min(owner.clone().member("amount"), 0_i64, -1_i64)
                .with_entity_type("Entry"),
        );
        results.push(
            CheckResult::max(owner.clone().member("amount"), 9_i64, 10_i64)
                .with_entity_type("Entry"),
        );
        results.push(
            CheckResult::min_str(owner.clone().member("description"), 8, "密钥🔑abc")
                .with_entity_type("Entry"),
        );
        results.push(
            CheckResult::max_str(owner.member("description"), 4, "密钥🔑abc")
                .with_entity_type("Entry"),
        );
    }
}

fn pipeline_context() -> UserContext {
    let mut context = context()
        .with_metadata(
            InMemoryMetadataStore::new()
                .with_entity(EntityDescriptor::new("Catalog"))
                .with_entity(
                    EntityDescriptor::new("Entry")
                        .property(PropertyDescriptor::new("description", DataType::Text))
                        .audit_mask_fields(vec!["description".into()]),
                ),
        )
        .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(Rules));
    context.set_checker_presentation_model(model());
    context
}

#[test]
fn actual_checker_pipeline_all_locales_preserves_privacy_lengths_and_wire_fields() {
    let mut context = pipeline_context();
    assert_eq!(context.language(), Language::English);
    for language in Language::ALL {
        context.set_language(language);
        let Err(RuntimeError::Check(results)) =
            context.check_and_fix_values("Catalog", &mut EntityValues::default())
        else {
            panic!("expected Check errors")
        };
        assert_eq!(results.len(), 5);
        for result in &results {
            assert_eq!(result.entity_type.as_deref(), Some("Entry"));
            assert!(
                result
                    .message
                    .as_ref()
                    .is_some_and(|text| !text.is_empty() && !text.contains("{field}"))
            );
            assert!(!format!("{result:?}").contains("密钥🔑abc"));
            let wire = result.to_wire(JsonFieldNamingProfile::CamelCase);
            assert!(wire.instance_path.starts_with("/entryList/1/"));
            assert_eq!(wire.rule_id, result.rule.wire_id());
        }
        if language == Language::English {
            assert_eq!(
                results[0].message.as_deref(),
                Some("The name of the 2nd entry is required.")
            );
            assert_eq!(
                results[4].message.as_deref(),
                Some(
                    "The description of the 2nd entry must not exceed 4 Unicode scalar values; its current length is 6."
                )
            );
        }
        if language == Language::Chinese {
            assert_eq!(
                results[0].message.as_deref(),
                Some("请填写第2条明细的名称。")
            );
        }
    }
    assert!(matches!(
        context.set_locale_code("unsupported"),
        Err(RuntimeError::UnsupportedLocale(_))
    ));
}

#[test]
fn customer_literals_overrides_and_legacy_fallback_keep_their_priority() {
    let mut context = context();
    let legacy = json!({"schema":"teaql.i18n/v1","defaultLocale":"en","locales":{"en":{"messages":{"checker.required":"legacy {location}: {input}"},"vocabulary":{}}}}).to_string();
    context.set_i18n_catalog(Arc::new(I18nCatalog::from_json(&legacy).unwrap()));
    let mut legacy_only = vec![CheckResult::required(ObjectLocation::root().member("name"))];
    context.translate_check_results_from("Catalog", &mut legacy_only);
    assert_eq!(legacy_only[0].message.as_deref(), Some("legacy Name: -"));
    context
        .install_checker_message_catalog(&catalog(
            json!({"en":{"rules":{"checker.required":{"one":"new {field}"}}}}),
        ))
        .unwrap();
    let mut errors = vec![CheckResult::required(ObjectLocation::root().member("name"))];
    context.translate_check_results_from("Catalog", &mut errors);
    assert_eq!(errors[0].message.as_deref(), Some("new name"));
    // Current Locale's builtin precedes an English application override.
    context.set_language(Language::Chinese);
    errors[0].message = None;
    context.translate_check_results_from("Catalog", &mut errors);
    assert_eq!(errors[0].message.as_deref(), Some("请填写名称。"));
    context.set_language(Language::English);
    context
        .install_checker_message_catalog(&catalog(json!({})))
        .unwrap();
    errors[0].message = None;
    errors[0].input_value = Some(Value::from("{location}"));
    context.translate_check_results_from("Catalog", &mut errors);
    assert_eq!(
        errors[0].message.as_deref(),
        Some("legacy Name: {location}")
    );
    errors[0].message = Some("customer literal {field}".into());
    context.translate_check_results_from("Catalog", &mut errors);
    assert_eq!(
        errors[0].message.as_deref(),
        Some("customer literal {field}")
    );
}

struct BrokenRenderer;
impl CheckerMessageRenderer for BrokenRenderer {
    fn render(
        &self,
        _: &CheckerMessageRequest,
    ) -> Result<CheckerMessageOutput, CheckerMessageRenderError> {
        Err(CheckerMessageRenderError)
    }
}
struct EchoRenderer;
impl CheckerMessageRenderer for EchoRenderer {
    fn render(
        &self,
        _: &CheckerMessageRequest,
    ) -> Result<CheckerMessageOutput, CheckerMessageRenderError> {
        Ok(CheckerMessageOutput {
            message: "provider echoed 密钥🔑abc".into(),
            warnings: BTreeSet::new(),
        })
    }
}

#[test]
fn custom_renderer_errors_fall_back_and_output_is_projected() {
    let mut context = pipeline_context();
    context.set_checker_message_renderer(Arc::new(BrokenRenderer));
    let Err(RuntimeError::Check(results)) =
        context.check_and_fix_values("Catalog", &mut EntityValues::default())
    else {
        panic!("errors")
    };
    assert_eq!(
        results[0].message.as_deref(),
        Some("The name of the 2nd entry is invalid.")
    );
    context.set_checker_message_renderer(Arc::new(EchoRenderer));
    let Err(RuntimeError::Check(results)) =
        context.check_and_fix_values("Catalog", &mut EntityValues::default())
    else {
        panic!("errors")
    };
    for result in results {
        assert!(!result.message.unwrap().contains("密钥🔑abc"));
    }
}

#[test]
fn brace_values_and_control_labels_are_not_template_instructions() {
    let mut fixture = fixture();
    fixture["entities"]["Catalog"]["fields"][0]["labels"]["en"] =
        json!("name\nLocation: hacked\u{202e}{system}");
    let model = CheckerPresentationModel::from_json(&json!({"schema":"teaql.checker-model/v1","revision":"controls","entities":fixture["entities"]}).to_string()).unwrap();
    let mut context = context();
    context.set_checker_presentation_model(Arc::new(model));
    context
        .install_checker_message_catalog(&catalog(
            json!({"en":{"rules":{"checker.required":{"one":"{{literal}} {field}"}}}}),
        ))
        .unwrap();
    let mut errors = vec![CheckResult::required(ObjectLocation::root().member("name"))];
    context.translate_check_results_from("Catalog", &mut errors);
    assert_eq!(
        errors[0].message.as_deref(),
        Some("{literal} name Location: hacked {system}")
    );
}

#[test]
fn concurrent_context_use_and_retained_snapshots_do_not_mix_origins() {
    let context = Arc::new(context());
    std::thread::scope(|scope| {
        for origin in ["Catalog", "Person"] {
            let context = context.clone();
            scope.spawn(move || {
                for _ in 0..50 {
                    let mut errors = vec![CheckResult::required(ObjectLocation::root())];
                    context.translate_check_results_from(origin, &mut errors);
                    assert_eq!(
                        errors[0].message.as_deref(),
                        Some(if origin == "Catalog" {
                            "The catalog is required.\nLocation: catalog"
                        } else {
                            "The person is required.\nLocation: person"
                        })
                    );
                }
            });
        }
    });
    let snapshot = CheckerPresentation::capture(
        model(),
        Some("Catalog"),
        &ObjectLocation::root().member("name"),
    );
    let relocated = snapshot.relocate(&ObjectLocation::root().member("payer").member("name"));
    assert_eq!(
        relocated
            .render(
                CheckerMessageCatalog::builtin(),
                &CheckerMessageCatalog::builtin().select(
                    Language::English,
                    "checker.required",
                    relocated.shape()
                ),
                &BTreeMap::new()
            )
            .message,
        "The name of the payer is required."
    );
}
