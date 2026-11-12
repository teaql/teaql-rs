//! Data-only templates. Compilation happens at installation, never during a check.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

use crate::{Language, RuntimeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessageShape {
    One,
    Two,
    Deep,
}

impl MessageShape {
    fn parse(name: &str) -> Result<Self, RuntimeError> {
        match name {
            "one" => Ok(Self::One),
            "two" => Ok(Self::Two),
            "deep" => Ok(Self::Deep),
            _ => Err(invalid()),
        }
    }
}

#[derive(Debug, Clone)]
enum Node {
    Literal(String),
    Argument(String),
}

#[derive(Debug, Clone)]
struct Template(Arc<[Node]>);

impl Template {
    fn compile(text: &str, allowed: &[&str], required: &[&str]) -> Result<Self, RuntimeError> {
        if text.trim().is_empty() {
            return Err(invalid());
        }
        let mut nodes = Vec::new();
        let mut literal = String::new();
        let mut names = BTreeSet::new();
        let mut chars = text.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '{' if chars.peek() == Some(&'{') => {
                    chars.next();
                    literal.push('{');
                }
                '}' if chars.peek() == Some(&'}') => {
                    chars.next();
                    literal.push('}');
                }
                '{' => {
                    if !literal.is_empty() {
                        nodes.push(Node::Literal(std::mem::take(&mut literal)));
                    }
                    let mut name = String::new();
                    loop {
                        match chars.next() {
                            Some('}') => break,
                            Some('{') | None => return Err(invalid()),
                            Some(c) => name.push(c),
                        }
                    }
                    if !allowed.contains(&name.as_str()) {
                        return Err(invalid());
                    }
                    names.insert(name.clone());
                    nodes.push(Node::Argument(name));
                }
                '}' => return Err(invalid()),
                _ => literal.push(c),
            }
        }
        if !required.iter().all(|name| names.contains(*name)) {
            return Err(invalid());
        }
        if !literal.is_empty() {
            nodes.push(Node::Literal(literal));
        }
        Ok(Self(nodes.into()))
    }

    fn render(&self, args: &BTreeMap<String, String>) -> String {
        let mut out = String::new();
        for node in self.0.iter() {
            match node {
                Node::Literal(text) => out.push_str(text),
                Node::Argument(name) => {
                    out.push_str(args.get(name).map(String::as_str).unwrap_or("-"))
                }
            }
        }
        out
    }
}

#[derive(Debug, Clone)]
struct Patterns {
    reference: Template,
    item: Template,
    separator: String,
}

#[derive(Debug, Clone, Default)]
struct Locale {
    rules: BTreeMap<(String, MessageShape), Template>,
    patterns: Option<Patterns>,
}

/// Immutable application overrides or the complete embedded shared catalog.
#[derive(Debug, Clone)]
pub struct CheckerMessageCatalog {
    locales: BTreeMap<Language, Locale>,
    application: bool,
}

#[derive(Debug, Clone)]
pub struct MessageSelection {
    pub language: Language,
    template: Template,
}

impl MessageSelection {
    pub fn render(&self, safe_arguments: &BTreeMap<String, String>) -> String {
        self.template.render(safe_arguments)
    }
}

impl CheckerMessageCatalog {
    pub fn from_json(source: &str) -> Result<Self, RuntimeError> {
        Self::parse(source, false)
    }

    fn parse(source: &str, complete: bool) -> Result<Self, RuntimeError> {
        let value = strict_json(source)?;
        let root = object(&value)?;
        if root.len() != 3
            || root.get("schema").and_then(Value::as_str) != Some("teaql.checker-messages/v1")
            || root.get("defaultLocale").and_then(Value::as_str) != Some("en")
        {
            return Err(invalid());
        }
        let mut locales = BTreeMap::new();
        for (code, value) in object(root.get("locales").ok_or_else(invalid)?)? {
            let language = Language::from_code(code).ok_or_else(invalid)?;
            let entry = object(value)?;
            if entry
                .keys()
                .any(|key| !["rules", "locationPatterns"].contains(&key.as_str()))
            {
                return Err(invalid());
            }
            let mut locale = Locale::default();
            if let Some(rules) = entry.get("rules") {
                for (rule, shapes) in object(rules)? {
                    if !rule.starts_with("checker.") || rule.len() == 8 {
                        return Err(invalid());
                    }
                    for (shape, text) in object(shapes)? {
                        let shape = MessageShape::parse(shape)?;
                        let mut allowed = match rule.as_str() {
                            "checker.required" | "checker.invalid" => vec!["field"],
                            "checker.min" | "checker.max" => vec!["field", "system", "input"],
                            "checker.minLength" | "checker.maxLength" => {
                                vec!["field", "system", "input_len", "unit"]
                            }
                            _ => vec!["field", "system", "input", "input_len", "unit"],
                        };
                        let mut required = vec!["field"];
                        match rule.as_str() {
                            "checker.min" | "checker.max" => required.push("system"),
                            "checker.minLength" | "checker.maxLength" => {
                                required.extend(["system", "input_len", "unit"])
                            }
                            _ => (),
                        }
                        match shape {
                            MessageShape::One => (),
                            MessageShape::Two => {
                                allowed.push("object");
                                required.push("object");
                            }
                            MessageShape::Deep => {
                                allowed.push("objectPath");
                                required.push("objectPath");
                            }
                        }
                        let template = Template::compile(
                            text.as_str().ok_or_else(invalid)?,
                            &allowed,
                            &required,
                        )?;
                        locale.rules.insert((rule.clone(), shape), template);
                    }
                }
            }
            if let Some(patterns) = entry.get("locationPatterns") {
                let patterns = object(patterns)?;
                if patterns.len() != 3 {
                    return Err(invalid());
                }
                let reference = Template::compile(
                    string(patterns, "reference")?,
                    &["relation"],
                    &["relation"],
                )?;
                let item = Template::compile(
                    string(patterns, "item")?,
                    &["relation", "position", "ordinal", "classifier"],
                    &["relation"],
                )?;
                // An item must expose a position; a relation label alone loses the selector.
                if !item.0.iter().any(|node| matches!(node, Node::Argument(name) if name == "position" || name == "ordinal")) { return Err(invalid()); }
                let separator = string(patterns, "separator")?.to_owned();
                if separator.is_empty()
                    || separator
                        .chars()
                        .any(|c| matches!(c, '{' | '}') || c.is_control())
                {
                    return Err(invalid());
                }
                locale.patterns = Some(Patterns {
                    reference,
                    item,
                    separator,
                });
            }
            if locales.insert(language, locale).is_some() {
                return Err(invalid());
            }
        }
        if complete {
            for language in Language::ALL {
                let locale = locales.get(&language).ok_or_else(invalid)?;
                if locale.patterns.is_none() {
                    return Err(invalid());
                }
                for rule in [
                    "checker.required",
                    "checker.min",
                    "checker.max",
                    "checker.minLength",
                    "checker.maxLength",
                    "checker.invalid",
                ] {
                    for shape in [MessageShape::One, MessageShape::Two, MessageShape::Deep] {
                        if !locale.rules.contains_key(&(rule.to_owned(), shape)) {
                            return Err(invalid());
                        }
                    }
                }
            }
        }
        Ok(Self {
            locales,
            application: !complete,
        })
    }

    pub fn builtin() -> &'static Arc<Self> {
        static BUILTIN: OnceLock<Arc<CheckerMessageCatalog>> = OnceLock::new();
        BUILTIN.get_or_init(|| {
            Arc::new(
                Self::parse(include_str!("builtin-v1.json"), true)
                    .expect("embedded natural messages"),
            )
        })
    }

    pub fn lookup(
        &self,
        language: Language,
        rule: &str,
        shape: MessageShape,
    ) -> Option<MessageSelection> {
        self.locales
            .get(&language)?
            .rules
            .get(&(rule.to_owned(), shape))
            .cloned()
            .map(|template| MessageSelection { language, template })
    }

    pub(crate) fn application_override(
        &self,
        language: Language,
        rule: &str,
        shape: MessageShape,
    ) -> Option<MessageSelection> {
        if self.application {
            self.lookup(language, rule, shape)
        } else {
            None
        }
    }

    pub fn select(&self, language: Language, rule: &str, shape: MessageShape) -> MessageSelection {
        for locale in [language, Language::English] {
            if let Some(selected) = self
                .lookup(locale, rule, shape)
                .or_else(|| Self::builtin().lookup(locale, rule, shape))
            {
                return selected;
            }
        }
        Self::builtin()
            .lookup(language, "checker.invalid", shape)
            .expect("complete generic messages")
    }

    fn patterns(&self, language: Language) -> &Patterns {
        self.locales
            .get(&language)
            .and_then(|l| l.patterns.as_ref())
            .unwrap_or_else(|| {
                Self::builtin()
                    .locales
                    .get(&language)
                    .and_then(|l| l.patterns.as_ref())
                    .expect("complete patterns")
            })
    }

    pub(crate) fn reference(&self, language: Language, relation: &str) -> String {
        self.patterns(language)
            .reference
            .render(&BTreeMap::from([("relation".into(), relation.into())]))
    }

    pub(crate) fn item(
        &self,
        language: Language,
        relation: &str,
        index: usize,
        classifier: &str,
    ) -> String {
        // u128 also represents usize::MAX + 1, so human positions never wrap.
        let position = index as u128 + 1;
        self.patterns(language).item.render(&BTreeMap::from([
            ("relation".into(), relation.into()),
            ("position".into(), position.to_string()),
            ("ordinal".into(), ordinal(position)),
            ("classifier".into(), classifier.into()),
        ]))
    }

    pub(crate) fn separator(&self, language: Language) -> &str {
        &self.patterns(language).separator
    }
}

pub fn ordinal(position: u128) -> String {
    let suffix = if (11..=13).contains(&(position % 100)) {
        "th"
    } else {
        match position % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    format!("{position}{suffix}")
}

fn invalid() -> RuntimeError {
    RuntimeError::Language("invalid Checker message catalog".into())
}
fn object(value: &Value) -> Result<&serde_json::Map<String, Value>, RuntimeError> {
    value.as_object().ok_or_else(invalid)
}
fn string<'a>(map: &'a serde_json::Map<String, Value>, key: &str) -> Result<&'a str, RuntimeError> {
    map.get(key).and_then(Value::as_str).ok_or_else(invalid)
}

/// Reject duplicate keys at every depth, before a JSON map can overwrite them.
pub(crate) fn strict_json(source: &str) -> Result<Value, RuntimeError> {
    struct Strict(Value);
    impl<'de> Deserialize<'de> for Strict {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct StrictVisitor;
            impl<'de> Visitor<'de> for StrictVisitor {
                type Value = Strict;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("JSON without duplicate keys")
                }
                fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Strict, M::Error> {
                    let mut values = serde_json::Map::new();
                    while let Some((key, Strict(value))) = map.next_entry::<String, Strict>()? {
                        if values.insert(key, value).is_some() {
                            return Err(serde::de::Error::custom("duplicate JSON key"));
                        }
                    }
                    Ok(Strict(Value::Object(values)))
                }
                fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Strict, S::Error> {
                    let mut values = Vec::new();
                    while let Some(Strict(value)) = seq.next_element()? {
                        values.push(value);
                    }
                    Ok(Strict(Value::Array(values)))
                }
                fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Strict, E> {
                    Ok(Strict(Value::String(s.into())))
                }
                fn visit_bool<E: serde::de::Error>(self, b: bool) -> Result<Strict, E> {
                    Ok(Strict(Value::Bool(b)))
                }
                fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<Strict, E> {
                    Ok(Strict(Value::Number(n.into())))
                }
                fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<Strict, E> {
                    Ok(Strict(Value::Number(n.into())))
                }
                fn visit_f64<E: serde::de::Error>(self, n: f64) -> Result<Strict, E> {
                    serde_json::Number::from_f64(n)
                        .map(|n| Strict(Value::Number(n)))
                        .ok_or_else(|| E::custom("invalid number"))
                }
                fn visit_unit<E: serde::de::Error>(self) -> Result<Strict, E> {
                    Ok(Strict(Value::Null))
                }
            }
            d.deserialize_any(StrictVisitor)
        }
    }
    let mut parser = serde_json::Deserializer::from_str(source);
    let Strict(value) = Strict::deserialize(&mut parser).map_err(|_| invalid())?;
    parser.end().map_err(|_| invalid())?;
    Ok(value)
}
