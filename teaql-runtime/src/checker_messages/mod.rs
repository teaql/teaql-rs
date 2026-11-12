//! Model-aware Checker presentation. No I/O, clock, entity graph or Context in renderer inputs.
mod catalog;
mod presentation;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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

pub trait CheckerMessageRenderer: Send + Sync {
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

pub(crate) fn render(
    context: &UserContext,
    result: &CheckResult,
    input_len: Option<usize>,
    warnings: &std::cell::RefCell<BTreeSet<CheckerMessageWarning>>,
) -> String {
    let origin = ORIGIN
        .try_with(Clone::clone)
        .ok()
        .filter(|origin| origin.context == std::ptr::from_ref(context) as usize);
    let model = origin
        .as_ref()
        .map(|origin| origin.model.clone())
        .unwrap_or_else(|| context.checker_presentation_model.clone());
    let presentation = CheckerPresentation::capture(
        model,
        origin.as_ref().map(|origin| origin.root.as_str()),
        &result.location,
    );
    let rule = rule_key(result.rule);
    let catalog = context.checker_message_catalog.clone();
    let mut selection = None;
    for language in [context.language(), Language::English] {
        if let Some(selected) = catalog.application_override(language, rule, presentation.shape()) {
            selection = Some(selected);
            break;
        }
        if let Some(template) = context.i18n_catalog.application_message(language, rule) {
            let args = BTreeMap::from([
                (
                    "location".into(),
                    crate::i18n::translate_location(&result.location),
                ),
                (
                    "system".into(),
                    result
                        .system_value
                        .as_ref()
                        .map(|v| scalar(Some(v)))
                        .unwrap_or_else(|| "-".into()),
                ),
                (
                    "input".into(),
                    result
                        .input_value
                        .as_ref()
                        .map(|v| scalar(Some(v)))
                        .unwrap_or_else(|| "-".into()),
                ),
                ("input_len".into(), input_len.unwrap_or(0).to_string()),
            ]);
            return render_legacy(template, &args);
        }
        if let Some(selected) =
            CheckerMessageCatalog::builtin().lookup(language, rule, presentation.shape())
        {
            selection = Some(selected);
            break;
        }
    }
    let selection = selection.unwrap_or_else(|| {
        warn(warnings, CheckerMessageWarning::GenericRule);
        CheckerMessageCatalog::builtin().select(
            context.language(),
            "checker.invalid",
            presentation.shape(),
        )
    });
    let safe_arguments = BTreeMap::from([
        ("system".into(), scalar(result.system_value.as_ref())),
        ("input".into(), scalar(result.input_value.as_ref())),
        ("input_len".into(), input_len.unwrap_or(0).to_string()),
        // Existing Rust validators count Unicode scalar values, not bytes or UTF-16.
        (
            "unit".into(),
            presentation::defaults()[&selection.language]["scalar"].clone(),
        ),
    ]);
    let request = CheckerMessageRequest {
        presentation,
        catalog,
        selection,
        safe_arguments,
    };
    let output = context.checker_message_renderer.render(&request);
    let output = match output {
        Ok(output) if !output.message.trim().is_empty() => output,
        _ => {
            warn(warnings, CheckerMessageWarning::RenderFallback);
            request.presentation.render(
                CheckerMessageCatalog::builtin(),
                &CheckerMessageCatalog::builtin().select(
                    context.language(),
                    "checker.invalid",
                    request.presentation.shape(),
                ),
                &BTreeMap::new(),
            )
        }
    };
    for warning in output.warnings {
        warn(warnings, warning);
    }
    output.message
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

/// Only four trusted legacy slots; inserted values are never parsed again.
fn render_legacy(template: &str, args: &BTreeMap<String, String>) -> String {
    let mut output = String::new();
    let mut offset = 0;
    while offset < template.len() {
        let rest = &template[offset..];
        let found = ["location", "system", "input", "input_len"]
            .into_iter()
            .find(|key| rest.starts_with(&format!("{{{key}}}")));
        if let Some(key) = found {
            output.push_str(&args[key]);
            offset += key.len() + 2;
        } else {
            let c = rest.chars().next().expect("remaining character");
            output.push(c);
            offset += c.len_utf8();
        }
    }
    output
}

pub(crate) fn prepare() {
    let _ = CheckerMessageCatalog::builtin();
    let _ = CheckerPresentationModel::empty();
    presentation::prepare_defaults();
}
