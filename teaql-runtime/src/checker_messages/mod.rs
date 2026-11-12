//! Model-aware Checker presentation. No I/O, clock, entity graph or Context in renderer inputs.
mod catalog;
mod presentation;

use sha2::{Digest, Sha256};
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

use crate::{CheckResult, CheckRule, Language, ObjectLocation, UserContext};
use teaql_core::Value;

pub use catalog::{CheckerMessageCatalog, MessageSelection, MessageShape, ordinal};
pub use presentation::{CheckerMessageWarning, CheckerPresentation, CheckerPresentationModel};

pub struct CheckerMessageRequest {
    pub presentation: CheckerPresentation,
    pub catalog: Arc<CheckerMessageCatalog>,
    pub selection: MessageSelection,
    pub safe_arguments: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct CheckerMessageOutput {
    pub message: String,
    pub warnings: BTreeSet<CheckerMessageWarning>,
}

#[derive(Debug)]
pub struct CheckerMessageRenderError;

pub trait CheckerMessageRenderer: Send + Sync + Any {
    fn render(
        &self,
        request: &CheckerMessageRequest,
    ) -> Result<CheckerMessageOutput, CheckerMessageRenderError>;
}

pub struct StandardCheckerMessageRenderer;
impl CheckerMessageRenderer for StandardCheckerMessageRenderer {
    fn render(
        &self,
        request: &CheckerMessageRequest,
    ) -> Result<CheckerMessageOutput, CheckerMessageRenderError> {
        Ok(request.presentation.render(
            &request.catalog,
            &request.selection,
            &request.safe_arguments,
        ))
    }
}

#[derive(Clone)]
struct Origin {
    context: usize,
    model: Arc<CheckerPresentationModel>,
    root: String,
}

tokio::task_local! { static ORIGIN: Origin; }

/// Always establish a new graph origin, including nested independent graph saves.
pub(crate) fn with_origin<T>(
    context: &UserContext,
    root: &str,
    operation: impl FnOnce() -> T,
) -> T {
    ORIGIN.sync_scope(
        Origin {
            context: std::ptr::from_ref(context) as usize,
            model: context.checker_presentation_model.clone(),
            root: root.into(),
        },
        operation,
    )
}

pub(crate) fn with_check_origin<T>(
    context: &UserContext,
    entity: &str,
    location: &ObjectLocation,
    operation: impl FnOnce() -> T,
) -> T {
    let scoped = ORIGIN
        .try_with(|origin| origin.context == std::ptr::from_ref(context) as usize)
        .unwrap_or(false);
    if scoped || !location.is_root() {
        operation()
    } else {
        with_origin(context, entity, operation)
    }
}

#[derive(Clone)]
enum Plan {
    Natural {
        selection: MessageSelection,
        custom: bool,
        generic: bool,
    },
    Legacy {
        template: catalog::Template,
        language: Language,
    },
}

impl Plan {
    fn project(&self, projection: &impl Fn(&str) -> String) -> Self {
        match self {
            Self::Natural {
                selection,
                custom,
                generic,
            } => Self::Natural {
                selection: if *custom {
                    selection.project(projection)
                } else {
                    selection.clone()
                },
                custom: *custom,
                generic: *generic,
            },
            Self::Legacy { template, language } => Self::Legacy {
                template: template.project(projection),
                language: *language,
            },
        }
    }
    fn language(&self) -> Language {
        match self {
            Self::Natural { selection, .. } => selection.language,
            Self::Legacy { language, .. } => *language,
        }
    }
}

struct Plans {
    rule: &'static str,
    catalog: Arc<CheckerMessageCatalog>,
    entries: BTreeMap<(Language, MessageShape), Plan>,
}

fn project_text(text: &str, secrets: &[String]) -> String {
    let mut safe = text.to_owned();
    crate::log_privacy::scrub(&mut safe, secrets);
    safe
}

impl Plans {
    fn prepare(context: &UserContext, rule: &'static str, secrets: &[String]) -> Arc<Self> {
        let projection = |text: &str| project_text(text, secrets);
        let catalog = context
            .checker_message_catalog
            .project_rule(rule, &projection);
        let mut entries = BTreeMap::new();
        for language in Language::ALL {
            for shape in [MessageShape::One, MessageShape::Two, MessageShape::Deep] {
                let mut plan = None;
                for candidate in [language, Language::English] {
                    if let Some(selection) = catalog.application_override(candidate, rule, shape) {
                        plan = Some(Plan::Natural {
                            selection,
                            custom: true,
                            generic: false,
                        });
                        break;
                    }
                    if let Some(source) = context.i18n_catalog.application_message(candidate, rule)
                    {
                        plan = Some(Plan::Legacy {
                            template: catalog::Template::legacy(source).project(&projection),
                            language: candidate,
                        });
                        break;
                    }
                    if let Some(selection) =
                        CheckerMessageCatalog::builtin().lookup(candidate, rule, shape)
                    {
                        plan = Some(Plan::Natural {
                            selection,
                            custom: false,
                            generic: false,
                        });
                        break;
                    }
                }
                entries.insert(
                    (language, shape),
                    plan.unwrap_or_else(|| Plan::Natural {
                        selection: CheckerMessageCatalog::builtin().select(
                            language,
                            "checker.invalid",
                            shape,
                        ),
                        custom: false,
                        generic: true,
                    }),
                );
            }
        }
        Arc::new(Self {
            rule,
            catalog,
            entries,
        })
    }

    fn fallback(rule: &'static str) -> Arc<Self> {
        let entries = Language::ALL
            .into_iter()
            .flat_map(|language| {
                [MessageShape::One, MessageShape::Two, MessageShape::Deep]
                    .into_iter()
                    .map(move |shape| {
                        (
                            (language, shape),
                            Plan::Natural {
                                selection: CheckerMessageCatalog::builtin().select(
                                    language,
                                    "checker.invalid",
                                    shape,
                                ),
                                custom: false,
                                generic: false,
                            },
                        )
                    })
            })
            .collect();
        Arc::new(Self {
            rule,
            catalog: CheckerMessageCatalog::builtin().clone(),
            entries,
        })
    }

    fn project(self: &Arc<Self>, secrets: &[String]) -> Arc<Self> {
        if secrets.is_empty() {
            return self.clone();
        }
        let projection = |text: &str| project_text(text, secrets);
        Arc::new(Self {
            rule: self.rule,
            catalog: self.catalog.project_rule(self.rule, &projection),
            entries: self
                .entries
                .iter()
                .map(|(key, plan)| (*key, plan.project(&projection)))
                .collect(),
        })
    }
}

#[derive(Clone)]
enum Prepared {
    Templates(Arc<Plans>),
    Literal(String),
}

/// Error-owned safe data only. Weak resource identities cannot retain raw catalogs.
pub(crate) struct RenderedState {
    rule: CheckRule,
    presentation: CheckerPresentation,
    prepared: Prepared,
    input_len: Option<usize>,
    input_digest: Option<[u8; 32]>,
    message_digest: [u8; 32],
    projected: bool,
    catalog_identity: Weak<CheckerMessageCatalog>,
    legacy_identity: Weak<crate::I18nCatalog>,
}

impl std::fmt::Debug for RenderedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckerMessageState")
            .field("original_length", &self.input_len)
            .finish_non_exhaustive()
    }
}

fn input_digest(value: &Option<Value>) -> Option<[u8; 32]> {
    match value {
        Some(Value::Text(text)) => Some(Sha256::digest(text.as_bytes()).into()),
        _ => None,
    }
}

pub(crate) fn original_input_length(result: &CheckResult) -> Option<usize> {
    if let Some(previous) = &result.presentation {
        if previous.input_digest == input_digest(&result.input_value) {
            return previous.input_len;
        }
    }
    result.input_value.as_ref().and_then(|value| match value {
        Value::Text(text) => Some(text.chars().count()),
        _ => None,
    })
}

fn arguments(
    result: &CheckResult,
    input_len: Option<usize>,
    language: Language,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("system".into(), scalar(result.system_value.as_ref())),
        ("input".into(), scalar(result.input_value.as_ref())),
        ("input_len".into(), input_len.unwrap_or(0).to_string()),
        // Existing Rust validators count Unicode scalar values, not bytes or UTF-16.
        (
            "unit".into(),
            presentation::defaults()[&language]["scalar"].clone(),
        ),
    ])
}

pub(crate) fn render(
    context: &UserContext,
    result: &mut CheckResult,
    input_len: Option<usize>,
    secrets: &[String],
    warnings: &std::cell::RefCell<BTreeSet<CheckerMessageWarning>>,
) {
    let previous = result.presentation.as_ref();
    let origin = ORIGIN
        .try_with(Clone::clone)
        .ok()
        .filter(|origin| origin.context == std::ptr::from_ref(context) as usize);
    let presentation = previous
        .map(|state| state.presentation.relocate(&result.location))
        .unwrap_or_else(|| {
            CheckerPresentation::capture(
                origin
                    .as_ref()
                    .map(|o| o.model.clone())
                    .unwrap_or_else(|| context.checker_presentation_model.clone()),
                origin.as_ref().map(|o| o.root.as_str()),
                &result.location,
            )
        });
    let explicit = result.message.as_ref().filter(|message| {
        previous.is_none_or(|state| {
            state.message_digest != <[u8; 32]>::from(Sha256::digest(message.as_bytes()))
        })
    });
    let resources_changed = previous.is_some_and(|state| {
        !Weak::ptr_eq(
            &state.catalog_identity,
            &Arc::downgrade(&context.checker_message_catalog),
        ) || !Weak::ptr_eq(
            &state.legacy_identity,
            &Arc::downgrade(&context.i18n_catalog),
        )
    });
    let fresh = previous.is_none_or(|state| state.rule != result.rule)
        || (result.message.is_none()
            && resources_changed
            && previous.is_some_and(|state| !state.projected));
    let mut prepared = if let Some(message) = explicit {
        Prepared::Literal(project_text(message, secrets))
    } else if fresh {
        Prepared::Templates(if previous.is_some_and(|state| state.projected) {
            // A changed rule cannot reopen raw resources from an earlier secret's scope.
            Plans::fallback(rule_key(result.rule))
        } else {
            Plans::prepare(context, rule_key(result.rule), secrets)
        })
    } else {
        previous.expect("retained state").prepared.clone()
    };
    let message = match prepared.clone() {
        Prepared::Literal(text) => {
            let safe = project_text(&text, secrets);
            prepared = Prepared::Literal(safe.clone());
            safe
        }
        Prepared::Templates(plans) => {
            let plans = plans.project(secrets);
            prepared = Prepared::Templates(plans.clone());
            let plan = &plans.entries[&(context.language(), presentation.shape())];
            let safe_arguments = arguments(result, input_len, plan.language());
            let output = match plan {
                Plan::Legacy { template, .. } => {
                    let mut args = safe_arguments;
                    args.insert(
                        "location".into(),
                        crate::i18n::translate_location(&result.location),
                    );
                    if result.input_value.is_none() {
                        args.insert("input".into(), "-".into());
                    }
                    if result.system_value.is_none() {
                        args.insert("system".into(), "-".into());
                    }
                    CheckerMessageOutput {
                        message: template.render(&args),
                        warnings: BTreeSet::new(),
                    }
                }
                Plan::Natural {
                    selection, generic, ..
                } => {
                    if *generic {
                        warn(warnings, CheckerMessageWarning::GenericRule);
                    }
                    if fresh
                        && context.checker_message_renderer.as_ref().type_id()
                            != TypeId::of::<StandardCheckerMessageRenderer>()
                    {
                        let request = CheckerMessageRequest {
                            presentation: presentation.clone(),
                            catalog: plans.catalog.clone(),
                            selection: selection.clone(),
                            safe_arguments,
                        };
                        match context.checker_message_renderer.render(&request) {
                            Ok(mut output) if !output.message.trim().is_empty() => {
                                output.message = project_text(&output.message, secrets);
                                prepared = Prepared::Literal(output.message.clone());
                                output
                            }
                            _ => {
                                warn(warnings, CheckerMessageWarning::RenderFallback);
                                let fallback = Plans::fallback(rule_key(result.rule));
                                let Plan::Natural { selection, .. } =
                                    &fallback.entries[&(context.language(), presentation.shape())]
                                else {
                                    unreachable!()
                                };
                                let output = presentation.render(
                                    &fallback.catalog,
                                    selection,
                                    &BTreeMap::new(),
                                );
                                prepared = Prepared::Templates(fallback);
                                output
                            }
                        }
                    } else {
                        presentation.render(&plans.catalog, selection, &safe_arguments)
                    }
                }
            };
            for warning in output.warnings {
                warn(warnings, warning);
            }
            output.message
        }
    };
    let projected = !secrets.is_empty() || previous.is_some_and(|state| state.projected);
    let (catalog_identity, legacy_identity) = if fresh {
        (
            Arc::downgrade(&context.checker_message_catalog),
            Arc::downgrade(&context.i18n_catalog),
        )
    } else {
        previous
            .map(|state| {
                (
                    state.catalog_identity.clone(),
                    state.legacy_identity.clone(),
                )
            })
            .unwrap_or_else(|| {
                (
                    Arc::downgrade(&context.checker_message_catalog),
                    Arc::downgrade(&context.i18n_catalog),
                )
            })
    };
    result.presentation = Some(Arc::new(RenderedState {
        rule: result.rule,
        presentation,
        prepared,
        input_len,
        input_digest: input_digest(&result.input_value),
        message_digest: Sha256::digest(message.as_bytes()).into(),
        projected,
        catalog_identity,
        legacy_identity,
    }));
    result.message = Some(message);
}
fn warn(
    seen: &std::cell::RefCell<BTreeSet<CheckerMessageWarning>>,
    warning: CheckerMessageWarning,
) {
    if seen.borrow_mut().insert(warning) {
        eprintln!("{}", warning.code());
    }
}

fn rule_key(rule: CheckRule) -> &'static str {
    match rule {
        CheckRule::Required => "checker.required",
        CheckRule::Min => "checker.min",
        CheckRule::Max => "checker.max",
        CheckRule::MinStringLength => "checker.minLength",
        CheckRule::MaxStringLength => "checker.maxLength",
        CheckRule::InvalidType => "checker.invalidType",
        CheckRule::InvalidDeletionState => "checker.invalidDeletionState",
        CheckRule::InvalidVersionState => "checker.invalidVersionState",
        CheckRule::VersionExhausted => "checker.versionExhausted",
        CheckRule::ContextRootMissing => "checker.contextRootMissing",
        CheckRule::ContextRootMismatch => "checker.contextRootMismatch",
    }
}

fn scalar(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null | Value::TypedNull(_)) => "null".into(),
        Some(Value::Text(text)) => presentation::sanitize(text),
        Some(Value::I64(n)) => n.to_string(),
        Some(Value::U64(n)) => n.to_string(),
        Some(Value::F64(n)) => n.to_string(),
        Some(Value::Decimal(n)) => n.to_string(),
        Some(Value::Bool(n)) => n.to_string(),
        Some(Value::Date(n)) => n.to_string(),
        Some(Value::Timestamp(n)) => n.0.to_string(),
        _ => "[value omitted]".into(),
    }
}

pub(crate) fn prepare() {
    let _ = CheckerMessageCatalog::builtin();
    let _ = CheckerPresentationModel::empty();
    presentation::prepare_defaults();
}
