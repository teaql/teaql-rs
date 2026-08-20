use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use teaql_core::Value;

use crate::{CheckResult, CheckRule, Language, ObjectLocation, RuntimeError};

const BUILTIN_CATALOG_JSON: &str = include_str!("builtin-i18n-v1.json");
const ALLOWED_ARGUMENTS: [&str; 4] = ["location", "system", "input", "input_len"];

#[derive(Debug, Clone, Default)]
struct LocaleCatalog {
    messages: BTreeMap<String, String>,
    vocabulary: BTreeMap<String, String>,
}

/// Immutable TeaQL internationalization catalog.
#[derive(Debug, Clone)]
pub struct I18nCatalog {
    default_locale: String,
    locales: BTreeMap<String, LocaleCatalog>,
}

impl I18nCatalog {
    pub fn from_json(source: &str) -> Result<Self, RuntimeError> {
        let root: serde_json::Value = serde_json::from_str(source)
            .map_err(|error| RuntimeError::Language(format!("invalid i18n catalog: {error}")))?;
        let object = root.as_object().ok_or_else(|| {
            RuntimeError::Language("i18n catalog root must be an object".to_owned())
        })?;
        if object.len() != 3
            || !["schema", "defaultLocale", "locales"]
                .iter()
                .all(|key| object.contains_key(*key))
        {
            return Err(RuntimeError::Language(
                "i18n catalog root must contain only schema, defaultLocale and locales".to_owned(),
            ));
        }
        if object.get("schema").and_then(serde_json::Value::as_str) != Some("teaql.i18n/v1") {
            return Err(RuntimeError::Language(
                "unsupported i18n catalog schema".to_owned(),
            ));
        }
        let default_locale = object
            .get("defaultLocale")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| RuntimeError::Language("missing i18n defaultLocale".to_owned()))?;
        let default_language = Language::from_code(default_locale).ok_or_else(|| {
            RuntimeError::Language(format!("unsupported i18n defaultLocale: {default_locale}"))
        })?;
        let locale_values = object
            .get("locales")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| RuntimeError::Language("missing i18n locales".to_owned()))?;
        let mut locales = BTreeMap::new();
        for (locale_code, value) in locale_values {
            let language = Language::from_code(locale_code).ok_or_else(|| {
                RuntimeError::Language(format!("unsupported i18n locale: {locale_code}"))
            })?;
            let canonical = language.code().to_owned();
            if canonical != *locale_code {
                return Err(RuntimeError::Language(format!(
                    "i18n locale keys must be canonical: {locale_code}"
                )));
            }
            let entry = value.as_object().ok_or_else(|| {
                RuntimeError::Language(format!("i18n locale {locale_code} must be an object"))
            })?;
            if entry.len() != 2
                || !["messages", "vocabulary"]
                    .iter()
                    .all(|key| entry.contains_key(*key))
            {
                return Err(RuntimeError::Language(format!(
                    "i18n locale {locale_code} must contain only messages and vocabulary"
                )));
            }
            let messages = parse_entries(entry.get("messages"), locale_code, "messages", true)?;
            let vocabulary =
                parse_entries(entry.get("vocabulary"), locale_code, "vocabulary", false)?;
            if locales
                .insert(
                    canonical,
                    LocaleCatalog {
                        messages,
                        vocabulary,
                    },
                )
                .is_some()
            {
                return Err(RuntimeError::Language(format!(
                    "duplicate canonical i18n locale: {locale_code}"
                )));
            }
        }
        if !locales.contains_key(default_language.code()) {
            return Err(RuntimeError::Language(
                "i18n catalog does not contain its default locale".to_owned(),
            ));
        }
        Ok(Self {
            default_locale: default_language.code().to_owned(),
            locales,
        })
    }

    pub fn builtin() -> &'static Arc<Self> {
        static BUILTIN: OnceLock<Arc<I18nCatalog>> = OnceLock::new();
        BUILTIN.get_or_init(|| {
            Arc::new(
                I18nCatalog::from_json(BUILTIN_CATALOG_JSON)
                    .expect("embedded TeaQL i18n catalog must be valid"),
            )
        })
    }

    pub fn message(&self, language: Language, key: &str) -> String {
        self.lookup(language.code(), "messages", key)
            .unwrap_or_else(|| key.to_owned())
    }

    pub fn vocabulary(&self, language: Language, key: &str) -> String {
        self.lookup(language.code(), "vocabulary", key)
            .unwrap_or_else(|| key.to_owned())
    }

    pub fn translate_check_result(&self, language: Language, result: &CheckResult) -> String {
        let key = match result.rule {
            CheckRule::Required => "checker.required",
            CheckRule::Min => "checker.min",
            CheckRule::Max => "checker.max",
            CheckRule::MinStringLength => "checker.minLength",
            CheckRule::MaxStringLength => "checker.maxLength",
        };
        let location = translate_location(&result.location);
        let system = result
            .system_value
            .as_ref()
            .map(format_value)
            .unwrap_or_else(|| "-".to_owned());
        let input = result
            .input_value
            .as_ref()
            .map(format_value)
            .unwrap_or_else(|| "-".to_owned());
        let input_len = result
            .input_value
            .as_ref()
            .and_then(|value| match value {
                Value::Text(value) => Some(value.chars().count()),
                _ => None,
            })
            .unwrap_or(0)
            .to_string();
        render_template(
            &self.message(language, key),
            &[
                ("location", location.as_str()),
                ("system", system.as_str()),
                ("input", input.as_str()),
                ("input_len", input_len.as_str()),
            ],
        )
    }

    fn lookup(&self, locale: &str, namespace: &str, key: &str) -> Option<String> {
        [locale, self.default_locale.as_str()]
            .into_iter()
            .find_map(|code| {
                let catalog = self.locales.get(code)?;
                match namespace {
                    "messages" => catalog.messages.get(key).cloned(),
                    "vocabulary" => catalog.vocabulary.get(key).cloned(),
                    _ => None,
                }
            })
    }
}

fn parse_entries(
    value: Option<&serde_json::Value>,
    locale: &str,
    namespace: &str,
    validate_placeholders: bool,
) -> Result<BTreeMap<String, String>, RuntimeError> {
    let entries = value
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            RuntimeError::Language(format!("i18n {locale}.{namespace} must be an object"))
        })?;
    let mut parsed = BTreeMap::new();
    for (key, value) in entries {
        let text = value
            .as_str()
            .filter(|text| !text.is_empty())
            .ok_or_else(|| {
                RuntimeError::Language(format!(
                    "empty i18n translation: {locale}.{namespace}.{key}"
                ))
            })?;
        if validate_placeholders {
            validate_template(text).map_err(|message| {
                RuntimeError::Language(format!("{locale}.{namespace}.{key}: {message}"))
            })?;
        }
        parsed.insert(key.clone(), text.to_owned());
    }
    Ok(parsed)
}

fn validate_template(template: &str) -> Result<(), String> {
    let mut characters = template.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '{' => {
                let mut name = String::new();
                loop {
                    match characters.next() {
                        Some('}') => break,
                        Some('{') => return Err("nested template opening brace".to_owned()),
                        Some(character) => name.push(character),
                        None => return Err("unclosed template placeholder".to_owned()),
                    }
                }
                if !ALLOWED_ARGUMENTS.contains(&name.as_str()) {
                    return Err(format!("unknown template placeholder: {name}"));
                }
            }
            '}' => return Err("unmatched template closing brace".to_owned()),
            _ => {}
        }
    }
    Ok(())
}

fn render_template(template: &str, arguments: &[(&str, &str)]) -> String {
    arguments
        .iter()
        .fold(template.to_owned(), |rendered, (key, value)| {
            rendered.replace(&format!("{{{key}}}"), value)
        })
}

fn translate_location(location: &ObjectLocation) -> String {
    title_case_path(&location.to_string())
}

fn title_case_path(path: &str) -> String {
    path.split('.')
        .map(|part| {
            part.split_once('[')
                .map(|(name, index)| format!("{}[{}", title_case_identifier(name), index))
                .unwrap_or_else(|| title_case_identifier(part))
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn title_case_identifier(value: &str) -> String {
    let mut output = String::new();
    for (index, ch) in value.chars().enumerate() {
        if index > 0 && ch.is_uppercase() {
            output.push(' ');
        }
        match index {
            0 => output.extend(ch.to_uppercase()),
            _ => output.extend(ch.to_lowercase()),
        }
    }
    output
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F64(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Json(value) => value.to_string(),
        Value::Date(value) => value.to_string(),
        Value::Timestamp(value) => value.0.to_string(),
        Value::Object(_) => "<object>".to_owned(),
        Value::List(_) => "<list>".to_owned(),
        Value::TypedNull(_) => "null".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_placeholders_before_installation() {
        let invalid = BUILTIN_CATALOG_JSON.replace("{location}", "{unsafe}");
        let error = I18nCatalog::from_json(&invalid).unwrap_err();
        assert!(error.to_string().contains("unknown template placeholder"));
    }

    #[test]
    fn falls_back_to_english_and_then_stable_key() {
        let catalog = I18nCatalog::from_json(
            r#"{
                "schema":"teaql.i18n/v1",
                "defaultLocale":"en",
                "locales":{"en":{"messages":{"known":"English"},"vocabulary":{}}}
            }"#,
        )
        .unwrap();
        assert_eq!(catalog.message(Language::French, "known"), "English");
        assert_eq!(catalog.message(Language::French, "missing"), "missing");
    }
}
