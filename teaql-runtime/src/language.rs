use crate::{CheckResult, ObjectLocation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Language {
    English,
    Chinese,
    TraditionalChinese,
    Japanese,
    Korean,
    German,
    French,
    Spanish,
    Portuguese,
    Arabic,
    Thai,
    Indonesian,
    Filipino,
    Ukrainian,
    Vietnamese,
}

impl Default for Language {
    fn default() -> Self {
        Self::English
    }
}

impl Language {
    pub const ALL: [Language; 15] = [
        Language::English,
        Language::Chinese,
        Language::TraditionalChinese,
        Language::Japanese,
        Language::Korean,
        Language::German,
        Language::French,
        Language::Spanish,
        Language::Portuguese,
        Language::Arabic,
        Language::Thai,
        Language::Indonesian,
        Language::Filipino,
        Language::Ukrainian,
        Language::Vietnamese,
    ];

    pub fn code(self) -> &'static str {
        match self {
            Self::English => "en",
            Self::Chinese => "zh-CN",
            Self::TraditionalChinese => "zh-TW",
            Self::Japanese => "ja",
            Self::Korean => "ko",
            Self::German => "de",
            Self::French => "fr",
            Self::Spanish => "es",
            Self::Portuguese => "pt",
            Self::Arabic => "ar",
            Self::Thai => "th",
            Self::Indonesian => "id",
            Self::Filipino => "fil",
            Self::Ukrainian => "uk",
            Self::Vietnamese => "vi",
        }
    }

    pub fn from_code(code: &str) -> Option<Self> {
        let normalized = code.trim().replace('_', "-").to_ascii_lowercase();
        match normalized.as_str() {
            "zh" | "zh-cn" | "zh-hans" | "zh-sg" | "cn" => Some(Self::Chinese),
            "zh-tw" | "zh-hant" | "zh-hk" | "zh-mo" | "tw" => Some(Self::TraditionalChinese),
            "tl" => Some(Self::Filipino),
            "zh-latn" | "" => None,
            _ => match normalized.split('-').next()? {
                "en" => Some(Self::English),
                "ja" => Some(Self::Japanese),
                "ko" => Some(Self::Korean),
                "de" => Some(Self::German),
                "fr" => Some(Self::French),
                "es" => Some(Self::Spanish),
                "pt" => Some(Self::Portuguese),
                "ar" => Some(Self::Arabic),
                "th" => Some(Self::Thai),
                "id" => Some(Self::Indonesian),
                "fil" => Some(Self::Filipino),
                "uk" => Some(Self::Ukrainian),
                "vi" => Some(Self::Vietnamese),
                _ => None,
            },
        }
    }
}

pub type Locale = Language;

pub trait MessageTranslator: Send + Sync {
    fn language(&self) -> Language;
    fn translate_check_result(&self, result: &CheckResult) -> String;
}

#[derive(Debug, Clone, Copy)]
pub struct BuiltinTranslator {
    language: Language,
}

impl BuiltinTranslator {
    pub fn new(language: Language) -> Self {
        Self { language }
    }
}

impl MessageTranslator for BuiltinTranslator {
    fn language(&self) -> Language {
        self.language
    }

    fn translate_check_result(&self, result: &CheckResult) -> String {
        translate_check_result(self.language, result)
    }
}

pub fn translate_check_result(language: Language, result: &CheckResult) -> String {
    crate::I18nCatalog::builtin().translate_check_result(language, result)
}

pub fn translate_location(_language: Language, location: &ObjectLocation) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_code_aliases_and_canonical_codes() {
        // Test canonical codes
        assert_eq!(Language::English.code(), "en");
        assert_eq!(Language::Chinese.code(), "zh-CN");
        assert_eq!(Language::TraditionalChinese.code(), "zh-TW");
        assert_eq!(Language::Filipino.code(), "fil");

        // Test from_code with canonical codes
        assert_eq!(Language::from_code("en"), Some(Language::English));
        assert_eq!(Language::from_code("zh-CN"), Some(Language::Chinese));

        // Test from_code with aliases
        assert_eq!(Language::from_code("en-US"), Some(Language::English));
        assert_eq!(Language::from_code("en-GB"), Some(Language::English));
        assert_eq!(Language::from_code("zh"), Some(Language::Chinese));
        assert_eq!(Language::from_code("cn"), Some(Language::Chinese));
        assert_eq!(
            Language::from_code("zh-HK"),
            Some(Language::TraditionalChinese)
        );
        assert_eq!(
            Language::from_code("tw"),
            Some(Language::TraditionalChinese)
        );
        assert_eq!(Language::from_code("tl"), Some(Language::Filipino));
        assert_eq!(Language::from_code("pt-BR"), Some(Language::Portuguese));
        assert_eq!(Language::from_code("EN_us"), Some(Language::English));
        assert_eq!(Language::from_code("zh-Hans"), Some(Language::Chinese));
        assert_eq!(
            Language::from_code("zh-Hant"),
            Some(Language::TraditionalChinese)
        );
        assert_eq!(Language::from_code("es-MX"), Some(Language::Spanish));

        // Test invalid code
        assert_eq!(Language::from_code("invalid-code"), None);
        assert_eq!(Language::from_code("zh-Latn"), None);
    }

    #[test]
    fn shared_locale_fixture_matches_rust_normalization() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("locale-cases-v1.json")).unwrap();
        let canonical = cases["canonical"].as_array().unwrap();
        assert_eq!(canonical.len(), Language::ALL.len());
        for code in canonical {
            let code = code.as_str().unwrap();
            assert_eq!(Language::from_code(code).unwrap().code(), code);
        }
        for (alias, expected) in cases["aliases"].as_object().unwrap() {
            assert_eq!(
                Language::from_code(alias).map(Language::code),
                expected.as_str(),
                "alias {alias}"
            );
        }
        for unsupported in cases["unsupported"].as_array().unwrap() {
            assert_eq!(Language::from_code(unsupported.as_str().unwrap()), None);
        }
    }
}
