use super::{DocumentView, ViewAuthorization, ViewErrorCode, ViewFieldDecision, require_text};
use crate::UserContext;
use serde_json::{Map, Value as Json};
use std::collections::BTreeSet;
use teaql_core::{BlobRef, Value};

const MAX_NODES: usize = 10_000;
const MAX_DEPTH: usize = 64;
const MAX_TEXT: usize = 8_388_608;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewPathSegment {
    Field(String),
    Index(usize),
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewPath(pub(crate) Vec<ViewPathSegment>);
impl ViewPath {
    pub fn segments(&self) -> &[ViewPathSegment] {
        &self.0
    }
    pub fn field(&self, name: &str) -> Self {
        let mut path = self.clone();
        path.0.push(ViewPathSegment::Field(name.to_owned()));
        path
    }
    pub fn index(&self, index: usize) -> Self {
        let mut path = self.clone();
        path.0.push(ViewPathSegment::Index(index));
        path
    }
    pub(crate) fn value_mut<'a>(&self, value: &'a mut Json) -> Option<&'a mut Json> {
        let mut current = value;
        for segment in &self.0 {
            current = match segment {
                ViewPathSegment::Field(name) => current.as_object_mut()?.get_mut(name)?,
                ViewPathSegment::Index(index) => current.as_array_mut()?.get_mut(*index)?,
            };
        }
        Some(current)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewEntityTarget {
    pub entity: String,
    pub id: Option<u64>,
    pub revision: Option<i64>,
}
impl ViewEntityTarget {
    fn validate(&self) -> Result<(), ViewErrorCode> {
        require_text(&self.entity)?;
        if self.id == Some(0) || self.revision == Some(i64::MIN) {
            return Err(ViewErrorCode::ResultUnsupported);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewField {
    pub canonical: String,
    pub member: String,
}
impl ViewField {
    pub fn new(canonical: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            canonical: canonical.into(),
            member: member.into(),
        }
    }
}

/// A borrowed host/generated adapter. Metadata access must not read business values.
/// Return only already loaded fields; read_field is invoked after field authorization.
pub trait ViewEntity: Send + Sync {
    fn target(&self) -> ViewEntityTarget;
    fn loaded_fields(&self) -> Vec<ViewField>;
    fn read_field(&self, canonical: &str) -> Result<ViewInput<'_>, ViewErrorCode>;
    fn presentation_ready(&self) -> bool {
        true
    }
}

pub enum ViewInput<'a> {
    Json(&'a Json),
    Value(&'a Value),
    Entity(&'a dyn ViewEntity),
    Array(Vec<ViewInput<'a>>),
    Object(Vec<(&'a str, ViewInput<'a>)>),
    Document(&'a DocumentView),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewBlobTarget {
    pub target: ViewEntityTarget,
    pub canonical: String,
    pub path: ViewPath,
    pub reference: BlobRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Check {
    Entity(ViewEntityTarget, ViewPath),
    Field(ViewEntityTarget, String, ViewPath, ViewFieldDecision),
}

/// Owned immutable values, paths and authority facts; no entities, actor or loaders.
#[derive(Clone, PartialEq)]
pub struct AuthorizedResponseGraph {
    value: Json,
    checks: Vec<Check>,
    blobs: Vec<ViewBlobTarget>,
}
impl AuthorizedResponseGraph {
    pub fn value(&self) -> &Json {
        &self.value
    }
    pub fn blobs(&self) -> &[ViewBlobTarget] {
        &self.blobs
    }
    /// Document stages may replace scalar references, while preserving the frozen surface.
    pub fn replace_scalars(
        &self,
        replacements: impl IntoIterator<Item = (ViewPath, Json)>,
    ) -> Result<Self, ViewErrorCode> {
        let mut graph = self.clone();
        for (path, replacement) in replacements {
            if replacement.is_array()
                || replacement.is_object()
                || self.blobs.iter().any(|b| b.path == path)
            {
                return Err(ViewErrorCode::AccessDenied);
            }
            let slot = path
                .value_mut(&mut graph.value)
                .ok_or(ViewErrorCode::AccessDenied)?;
            if slot.is_array() || slot.is_object() {
                return Err(ViewErrorCode::AccessDenied);
            }
            *slot = replacement;
        }
        validate_json(&graph.value)?;
        Ok(graph)
    }
    pub(crate) fn same_surface(&self, other: &Self) -> bool {
        fn same(left: &Json, right: &Json) -> bool {
            match (left, right) {
                (Json::Object(a), Json::Object(b)) => {
                    a.len() == b.len()
                        && a.iter()
                            .all(|(key, value)| b.get(key).is_some_and(|other| same(value, other)))
                }
                (Json::Array(a), Json::Array(b)) => {
                    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b))
                }
                (Json::Array(_) | Json::Object(_), _) | (_, Json::Array(_) | Json::Object(_)) => {
                    false
                }
                _ => true,
            }
        }
        self.checks == other.checks && self.blobs == other.blobs && same(&self.value, &other.value)
    }
    pub(crate) fn revalidate(
        &self,
        context: &UserContext,
        auth: &dyn ViewAuthorization,
    ) -> Result<(), ViewErrorCode> {
        for check in &self.checks {
            match check {
                Check::Entity(target, path) if !auth.can_read_entity(context, target, path)? => {
                    return Err(ViewErrorCode::AccessDenied);
                }
                Check::Field(target, field, path, decision)
                    if auth.field(context, target, field, path)? != *decision =>
                {
                    return Err(ViewErrorCode::AccessDenied);
                }
                _ => (),
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Budget {
    nodes: usize,
    text: usize,
}
impl Budget {
    fn node(&mut self, depth: usize) -> Result<(), ViewErrorCode> {
        self.nodes += 1;
        if self.nodes > MAX_NODES || depth > MAX_DEPTH {
            Err(ViewErrorCode::LimitExceeded)
        } else {
            Ok(())
        }
    }
    fn text(&mut self, text: &str) -> Result<(), ViewErrorCode> {
        self.text = self
            .text
            .checked_add(text.len())
            .ok_or(ViewErrorCode::LimitExceeded)?;
        if self.text > MAX_TEXT {
            Err(ViewErrorCode::LimitExceeded)
        } else {
            Ok(())
        }
    }
    fn json(&mut self, value: &Json, depth: usize) -> Result<Json, ViewErrorCode> {
        self.node(depth)?;
        match value {
            Json::String(text) => {
                self.text(text)?;
                Ok(value.clone())
            }
            Json::Array(values) => values
                .iter()
                .map(|value| self.json(value, depth + 1))
                .collect::<Result<Vec<_>, _>>()
                .map(Json::Array),
            Json::Object(values) => values
                .iter()
                .map(|(key, value)| {
                    self.text(key)?;
                    Ok((key.clone(), self.json(value, depth + 1)?))
                })
                .collect::<Result<Map<_, _>, _>>()
                .map(Json::Object),
            _ => Ok(value.clone()),
        }
    }
}
pub(crate) fn validate_json(value: &Json) -> Result<(), ViewErrorCode> {
    Budget::default().json(value, 0).map(|_| ())
}
pub(crate) fn validate_output(
    value: &Json,
    credentials: Option<&Json>,
) -> Result<(), ViewErrorCode> {
    let mut budget = Budget::default();
    budget.json(value, 0)?;
    if let Some(credentials) = credentials {
        budget.json(credentials, 0)?;
    }
    Ok(())
}

pub(crate) fn freeze(
    context: &UserContext,
    auth: &dyn ViewAuthorization,
    input: ViewInput<'_>,
) -> Result<AuthorizedResponseGraph, ViewErrorCode> {
    let mut state = Freeze {
        context,
        auth,
        budget: Budget::default(),
        checks: Vec::new(),
        blobs: Vec::new(),
        entities: Vec::new(),
    };
    let value = state.input(input, &ViewPath::default(), 0, None)?;
    Ok(AuthorizedResponseGraph {
        value,
        checks: state.checks,
        blobs: state.blobs,
    })
}
struct Freeze<'a> {
    context: &'a UserContext,
    auth: &'a dyn ViewAuthorization,
    budget: Budget,
    checks: Vec<Check>,
    blobs: Vec<ViewBlobTarget>,
    entities: Vec<usize>,
}
impl Freeze<'_> {
    fn input(
        &mut self,
        input: ViewInput<'_>,
        path: &ViewPath,
        depth: usize,
        field: Option<(&ViewEntityTarget, &str)>,
    ) -> Result<Json, ViewErrorCode> {
        match input {
            ViewInput::Json(value) => self.budget.json(value, depth),
            ViewInput::Value(value) => self.core_value(value, path, depth, field),
            ViewInput::Entity(entity) => self.entity(entity, path, depth),
            ViewInput::Array(values) => {
                self.budget.node(depth)?;
                values
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| self.input(value, &path.index(i), depth + 1, None))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Json::Array)
            }
            ViewInput::Object(values) => {
                self.budget.node(depth)?;
                let mut result = Map::new();
                for (key, value) in values {
                    self.budget.text(key)?;
                    if result.contains_key(key) {
                        return Err(ViewErrorCode::ResultUnsupported);
                    }
                    result.insert(
                        key.to_owned(),
                        self.input(value, &path.field(key), depth + 1, None)?,
                    );
                }
                Ok(Json::Object(result))
            }
            ViewInput::Document(_) => Err(ViewErrorCode::ResultUnsupported),
        }
    }
    fn core_value(
        &mut self,
        value: &Value,
        path: &ViewPath,
        depth: usize,
        field: Option<(&ViewEntityTarget, &str)>,
    ) -> Result<Json, ViewErrorCode> {
        match value {
            Value::BlobRef(reference) => {
                self.budget.node(depth)?;
                let (target, canonical) = field.ok_or(ViewErrorCode::ResultUnsupported)?;
                self.blobs.push(ViewBlobTarget {
                    target: target.clone(),
                    canonical: canonical.to_owned(),
                    path: path.clone(),
                    reference: reference.clone(),
                });
                Ok(Json::Null)
            }
            Value::F64(value) if !value.is_finite() => Err(ViewErrorCode::ResultUnsupported),
            Value::Json(value) => self.budget.json(value, depth),
            Value::Text(value) => {
                self.budget.node(depth)?;
                self.budget.text(value)?;
                Ok(Json::String(value.clone()))
            }
            Value::Object(values) if values.len() > MAX_NODES => Err(ViewErrorCode::LimitExceeded),
            Value::List(values) if values.len() > MAX_NODES => Err(ViewErrorCode::LimitExceeded),
            Value::Object(values) => self.input(
                ViewInput::Object(
                    values
                        .iter()
                        .map(|(key, value)| (key.as_str(), ViewInput::Value(value)))
                        .collect(),
                ),
                path,
                depth,
                None,
            ),
            Value::List(values) => self.input(
                ViewInput::Array(values.iter().map(ViewInput::Value).collect()),
                path,
                depth,
                None,
            ),
            _ => self.budget.json(&value.to_json_value(), depth),
        }
    }
    fn entity(
        &mut self,
        entity: &dyn ViewEntity,
        path: &ViewPath,
        depth: usize,
    ) -> Result<Json, ViewErrorCode> {
        self.budget.node(depth)?;
        if !entity.presentation_ready() {
            return Err(ViewErrorCode::TransactionUnfinished);
        }
        let target = entity.target();
        target.validate()?;
        if !self.auth.can_read_entity(self.context, &target, path)? {
            return Err(ViewErrorCode::AccessDenied);
        }
        self.checks
            .push(Check::Entity(target.clone(), path.clone()));
        let address = entity as *const dyn ViewEntity as *const () as usize;
        let cycle = self.entities.contains(&address);
        self.entities.push(address);
        let fields = entity.loaded_fields();
        if fields.len() > MAX_NODES {
            return Err(ViewErrorCode::LimitExceeded);
        }
        let mut canonical_names = BTreeSet::new();
        let mut members = BTreeSet::new();
        let mut result = Map::new();
        for field in fields {
            require_text(&field.canonical)?;
            require_text(&field.member)?;
            if !canonical_names.insert(field.canonical.clone())
                || !members.insert(field.member.clone())
            {
                return Err(ViewErrorCode::ResultUnsupported);
            }
            if cycle && field.canonical != "id" && field.canonical != "version" {
                continue;
            }
            let field_path = path.field(&field.member);
            let decision = self
                .auth
                .field(self.context, &target, &field.canonical, &field_path)?;
            let value = match &decision {
                ViewFieldDecision::Hidden => continue,
                ViewFieldDecision::Mask(mask) => self.budget.json(
                    &mask
                        .as_ref()
                        .map_or(Json::Null, |value| Json::String(value.clone())),
                    depth + 1,
                )?,
                ViewFieldDecision::Value { .. } => self.input(
                    entity.read_field(&field.canonical)?,
                    &field_path,
                    depth + 1,
                    Some((&target, &field.canonical)),
                )?,
            };
            self.budget.text(&field.member)?;
            self.checks.push(Check::Field(
                target.clone(),
                field.canonical,
                field_path,
                decision,
            ));
            result.insert(field.member, value);
        }
        self.entities.pop();
        Ok(Json::Object(result))
    }
}
