use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use super::{CheckerMessageCatalog, MessageSelection, MessageShape};
use crate::{Language, LocationSegment, ObjectLocation, RuntimeError};

type Labels = BTreeMap<Language, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentationField {
    name: String,
    target_type: Option<String>,
    labels: Labels,
    classifiers: Labels,
    aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PresentationEntity {
    labels: Labels,
    fields: BTreeMap<String, Arc<PresentationField>>,
}

/// Passive model metadata, shared by revision. Never stores domain objects or Context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckerPresentationModel {
    revision: String,
    entities: BTreeMap<String, Arc<PresentationEntity>>,
    current_object: Labels,
}

impl CheckerPresentationModel {
    pub fn empty() -> &'static Arc<Self> {
        static EMPTY: OnceLock<Arc<CheckerPresentationModel>> = OnceLock::new();
        EMPTY.get_or_init(|| {
            Arc::new(Self {
                revision: "unconfigured".into(),
                entities: BTreeMap::new(),
                current_object: defaults()
                    .iter()
                    .map(|(language, words)| (*language, words["currentObject"].clone()))
                    .collect(),
            })
        })
    }

    /// Parse once during composition. Aliases must be explicit and unambiguous.
    pub fn from_json(source: &str) -> Result<Self, RuntimeError> {
        let value = super::catalog::strict_json(source)?;
        let root = map(&value)?;
        if root.get("schema").and_then(serde_json::Value::as_str) != Some("teaql.checker-model/v1")
            || root.keys().any(|key| {
                !["schema", "revision", "entities", "currentObject"].contains(&key.as_str())
            })
        {
            return Err(invalid());
        }
        let revision = text(root.get("revision"))?.to_owned();
        let mut current_object = Self::empty().current_object.clone();
        if let Some(labels) = root.get("currentObject") {
            current_object.extend(parse_labels(labels)?);
        }
        let mut entities = BTreeMap::new();
        for (identity, value) in map(root.get("entities").ok_or_else(invalid)?)? {
            if identity.trim().is_empty() {
                return Err(invalid());
            }
            let entity = map(value)?;
            if entity
                .keys()
                .any(|key| !["labels", "fields"].contains(&key.as_str()))
            {
                return Err(invalid());
            }
            let labels = entity
                .get("labels")
                .map(parse_labels)
                .transpose()?
                .unwrap_or_default();
            let mut fields = BTreeMap::new();
            for value in entity
                .get("fields")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(invalid)?
            {
                let field = map(value)?;
                if field.keys().any(|key| {
                    !["name", "targetType", "labels", "classifiers", "aliases"]
                        .contains(&key.as_str())
                }) {
                    return Err(invalid());
                }
                let name = text(field.get("name"))?.to_owned();
                let target_type = field
                    .get("targetType")
                    .map(|v| text(Some(v)).map(str::to_owned))
                    .transpose()?;
                let labels = field
                    .get("labels")
                    .map(parse_labels)
                    .transpose()?
                    .unwrap_or_default();
                let classifiers = field
                    .get("classifiers")
                    .map(parse_labels)
                    .transpose()?
                    .unwrap_or_default();
                let aliases: Vec<String> = field
                    .get("aliases")
                    .map(|v| {
                        v.as_array()
                            .ok_or_else(invalid)?
                            .iter()
                            .map(|v| text(Some(v)).map(str::to_owned))
                            .collect()
                    })
                    .transpose()?
                    .unwrap_or_default();
                let descriptor = Arc::new(PresentationField {
                    name: name.clone(),
                    target_type,
                    labels,
                    classifiers,
                    aliases: aliases.clone(),
                });
                for key in std::iter::once(name).chain(aliases) {
                    if fields.insert(key, descriptor.clone()).is_some() {
                        return Err(invalid());
                    }
                }
            }
            entities.insert(
                identity.clone(),
                Arc::new(PresentationEntity { labels, fields }),
            );
        }
        Ok(Self {
            revision,
            entities,
            current_object,
        })
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Merge module metadata before installation; conflicting identities reject atomically.
    pub fn merge(&self, other: &Self) -> Result<Self, RuntimeError> {
        if self.entities.is_empty() && self.revision == "unconfigured" {
            return Ok(other.clone());
        }
        if other.entities.is_empty() && other.revision == "unconfigured" {
            return Ok(self.clone());
        }
        let mut merged = self.clone();
        for (key, value) in &other.entities {
            match merged.entities.get(key) {
                Some(previous) if previous != value => return Err(invalid()),
                Some(_) => (),
                None => {
                    merged.entities.insert(key.clone(), value.clone());
                }
            }
        }
        for (language, label) in &other.current_object {
            if merged
                .current_object
                .get(language)
                .is_some_and(|previous| previous != label)
            {
                return Err(invalid());
            }
            merged.current_object.insert(*language, label.clone());
        }
        if merged.entities != self.entities {
            merged.revision = format!("{}+{}", self.revision, other.revision);
        }
        Ok(merged)
    }
}

#[derive(Debug, Clone)]
struct Step {
    identity: Option<String>,
    field: Option<Arc<PresentationField>>,
    indices: Vec<usize>,
}

/// A finite resolved path and shared metadata, not a copy of an entity graph.
#[derive(Debug, Clone)]
pub struct CheckerPresentation {
    model: Arc<CheckerPresentationModel>,
    root_type: Option<String>,
    steps: Vec<Step>,
    shape: MessageShape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CheckerMessageWarning {
    LabelFallback,
    GenericRule,
    RenderFallback,
}

impl CheckerMessageWarning {
    pub fn code(self) -> &'static str {
        match self {
            Self::LabelFallback => "CHECKER_MESSAGE_LABEL_FALLBACK",
            Self::GenericRule => "CHECKER_MESSAGE_GENERIC_RULE",
            Self::RenderFallback => "CHECKER_MESSAGE_RENDER_FALLBACK",
        }
    }
}

impl CheckerPresentation {
    pub fn capture(
        model: Arc<CheckerPresentationModel>,
        root_type: Option<&str>,
        location: &ObjectLocation,
    ) -> Self {
        let mut entity = root_type.and_then(|root| model.entities.get(root));
        let mut unusual = entity.is_none();
        let mut steps: Vec<Step> = Vec::new();
        for segment in location.segments() {
            match segment {
                LocationSegment::Member(name) => {
                    let field = entity.and_then(|e| e.fields.get(name)).cloned();
                    entity = field
                        .as_ref()
                        .and_then(|f| f.target_type.as_ref())
                        .and_then(|target| model.entities.get(target));
                    steps.push(Step {
                        identity: Some(name.clone()),
                        field,
                        indices: Vec::new(),
                    });
                }
                LocationSegment::Index(index) => {
                    if steps.is_empty() {
                        steps.push(Step {
                            identity: None,
                            field: None,
                            indices: Vec::new(),
                        });
                        unusual = true;
                    }
                    let step = steps.last_mut().expect("selector step");
                    step.indices.push(*index);
                    if step.indices.len() > 1 {
                        unusual = true;
                    }
                }
            }
        }
        if steps.is_empty() || steps.last().is_some_and(|s| !s.indices.is_empty()) {
            unusual = true;
        }
        let shape = if unusual || steps.len() > 2 {
            MessageShape::Deep
        } else if steps.len() == 1 {
            MessageShape::One
        } else {
            MessageShape::Two
        };
        Self {
            model,
            root_type: root_type.map(str::to_owned),
            steps,
            shape,
        }
    }

    pub fn shape(&self) -> MessageShape {
        self.shape
    }
    pub fn model_revision(&self) -> &str {
        self.model.revision()
    }
    pub fn relocate(&self, location: &ObjectLocation) -> Self {
        Self::capture(self.model.clone(), self.root_type.as_deref(), location)
    }

    pub fn render(
        &self,
        catalog: &CheckerMessageCatalog,
        selection: &MessageSelection,
        safe_arguments: &BTreeMap<String, String>,
    ) -> super::CheckerMessageOutput {
        let language = selection.language;
        let mut warnings = BTreeSet::new();
        let root = self.root_label(language, &mut warnings);
        let leaf = self.steps.last();
        let mut args: BTreeMap<_, _> = safe_arguments
            .iter()
            .filter(|(key, _)| ["system", "input", "input_len", "unit"].contains(&key.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let field = leaf
            .filter(|s| s.identity.is_some())
            .map(|s| label(s, language, &mut warnings))
            .unwrap_or_else(|| root.clone());
        args.insert("field".into(), field);
        let owner_count = self.steps.len()
            - usize::from(leaf.is_some_and(|s| s.identity.is_some() && s.indices.is_empty()));
        let mut owners = Vec::new();
        for step in self.steps.iter().take(owner_count) {
            let relation = if step.identity.is_none() {
                root.clone()
            } else {
                label(step, language, &mut warnings)
            };
            if step.indices.is_empty() {
                owners.push(catalog.reference(language, &relation));
            } else {
                for (offset, index) in step.indices.iter().enumerate() {
                    let current = self.current_label(language, &mut warnings);
                    let item_label = if offset == 0 { &relation } else { &current };
                    let classifier = step
                        .field
                        .as_ref()
                        .and_then(|field| field.classifiers.get(&language))
                        .map(String::as_str)
                        .unwrap_or(
                            if matches!(language, Language::Chinese | Language::TraditionalChinese)
                            {
                                "项"
                            } else {
                                ""
                            },
                        );
                    owners.push(catalog.item(language, item_label, *index, &sanitize(classifier)));
                }
            }
        }
        args.insert("object".into(), owners.first().unwrap_or(&root).clone());
        args.insert(
            "objectPath".into(),
            if owners.is_empty() {
                root
            } else {
                owners.join(catalog.separator(language))
            },
        );
        super::CheckerMessageOutput {
            message: selection.render(&args),
            warnings,
        }
    }

    fn root_label(
        &self,
        language: Language,
        warnings: &mut BTreeSet<CheckerMessageWarning>,
    ) -> String {
        self.root_type
            .as_ref()
            .and_then(|root| {
                self.model
                    .entities
                    .get(root)
                    .map(|entity| vocabulary(&entity.labels, language, root, warnings))
            })
            .unwrap_or_else(|| self.current_label(language, warnings))
    }
    fn current_label(
        &self,
        language: Language,
        warnings: &mut BTreeSet<CheckerMessageWarning>,
    ) -> String {
        vocabulary(
            &self.model.current_object,
            language,
            "current object",
            warnings,
        )
    }
}

fn label(
    step: &Step,
    language: Language,
    warnings: &mut BTreeSet<CheckerMessageWarning>,
) -> String {
    match &step.field {
        Some(field) => vocabulary(&field.labels, language, &field.name, warnings),
        None => vocabulary(
            &BTreeMap::new(),
            language,
            step.identity.as_deref().unwrap_or("current object"),
            warnings,
        ),
    }
}
fn vocabulary(
    labels: &Labels,
    language: Language,
    identity: &str,
    warnings: &mut BTreeSet<CheckerMessageWarning>,
) -> String {
    let text = labels
        .get(&language)
        .filter(|text| !text.trim().is_empty())
        .or_else(|| {
            warnings.insert(CheckerMessageWarning::LabelFallback);
            labels
                .get(&Language::English)
                .filter(|text| !text.trim().is_empty())
        });
    let result = text
        .map(|text| sanitize(text))
        .unwrap_or_else(|| sanitize(&identity.replace('_', " ")));
    if result.is_empty() {
        warnings.insert(CheckerMessageWarning::LabelFallback);
        "current object".into()
    } else {
        result
    }
}
pub(crate) fn sanitize(text: &str) -> String {
    text.chars().map(|c| if c.is_control() || matches!(c, '\u{00ad}' | '\u{061c}' | '\u{200b}'..='\u{200f}' | '\u{2028}'..='\u{202e}' | '\u{2060}'..='\u{206f}' | '\u{feff}') { ' ' } else { c }).collect::<String>().trim().to_owned()
}

pub(crate) fn defaults() -> &'static BTreeMap<Language, BTreeMap<String, String>> {
    static DEFAULTS: OnceLock<BTreeMap<Language, BTreeMap<String, String>>> = OnceLock::new();
    DEFAULTS.get_or_init(|| {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("presentation-defaults-v1.json"))
                .expect("embedded Checker vocabulary");
        value["locales"]
            .as_object()
            .expect("locales")
            .iter()
            .map(|(code, words)| {
                (
                    Language::from_code(code).expect("embedded Locale"),
                    words
                        .as_object()
                        .expect("vocabulary")
                        .iter()
                        .map(|(key, value)| (key.clone(), value.as_str().expect("label").into()))
                        .collect(),
                )
            })
            .collect()
    })
}

pub(crate) fn prepare_defaults() {
    let _ = defaults();
}

fn parse_labels(value: &serde_json::Value) -> Result<Labels, RuntimeError> {
    let mut labels = BTreeMap::new();
    for (locale, text) in map(value)? {
        let language = Language::from_code(locale).ok_or_else(invalid)?;
        if labels
            .insert(language, self::text(Some(text))?.into())
            .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(labels)
}
fn invalid() -> RuntimeError {
    RuntimeError::Language("invalid Checker presentation model".into())
}
fn map(
    value: &serde_json::Value,
) -> Result<&serde_json::Map<String, serde_json::Value>, RuntimeError> {
    value.as_object().ok_or_else(invalid)
}
fn text(value: Option<&serde_json::Value>) -> Result<&str, RuntimeError> {
    value
        .and_then(serde_json::Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(invalid)
}
