use teaql_core::Value;

use crate::{CheckResult, CheckRule, ObjectLocation};

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
        match code {
            "en" | "en-US" | "en-GB" => Some(Self::English),
            "zh" | "zh-CN" | "cn" => Some(Self::Chinese),
            "zh-TW" | "zh-HK" | "tw" => Some(Self::TraditionalChinese),
            "ja" | "ja-JP" => Some(Self::Japanese),
            "ko" | "ko-KR" => Some(Self::Korean),
            "de" | "de-DE" => Some(Self::German),
            "fr" | "fr-FR" => Some(Self::French),
            "es" | "es-ES" => Some(Self::Spanish),
            "pt" | "pt-BR" | "pt-PT" => Some(Self::Portuguese),
            "ar" => Some(Self::Arabic),
            "th" | "th-TH" => Some(Self::Thai),
            "id" | "id-ID" => Some(Self::Indonesian),
            "fil" | "tl" => Some(Self::Filipino),
            "uk" | "uk-UA" => Some(Self::Ukrainian),
            "vi" | "vi-VN" => Some(Self::Vietnamese),
            _ => None,
        }
    }
}

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
    let location = translate_location(language, &result.location);
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
        .unwrap_or(0);

    match (language, result.rule) {
        (Language::English, CheckRule::Required) => format!("The {location} is required"),
        (Language::English, CheckRule::Min) => {
            format!("The {location} should be equal or greater than {system}, but input is {input}")
        }
        (Language::English, CheckRule::Max) => {
            format!("The {location} should be equal or less than {system}, but input is {input}")
        }
        (Language::English, CheckRule::MinStringLength) => format!(
            "The length of {location} should be equal or greater than {system}, but the length of {input} is {input_len}"
        ),
        (Language::English, CheckRule::MaxStringLength) => format!(
            "The length of {location} should be equal or less than {system}, but the length of {input} is {input_len}"
        ),

        (Language::Chinese, CheckRule::Required) => format!("{location} 是必填项"),
        (Language::Chinese, CheckRule::Min) => {
            format!("{location} 应该大于等于 {system}，但输入值为 {input}")
        }
        (Language::Chinese, CheckRule::Max) => {
            format!("{location} 应该小于等于 {system}，但输入值为 {input}")
        }
        (Language::Chinese, CheckRule::MinStringLength) => {
            format!("{location} 的长度应大于等于 {system}，但实际长度为 {input_len}")
        }
        (Language::Chinese, CheckRule::MaxStringLength) => {
            format!("{location} 的长度应小于等于 {system}，但实际长度为 {input_len}")
        }

        (Language::TraditionalChinese, CheckRule::Required) => format!("{location} 是必填的"),
        (Language::TraditionalChinese, CheckRule::Min) => {
            format!("{location} 應該等於或大於 {system}，但輸入為 {input}")
        }
        (Language::TraditionalChinese, CheckRule::Max) => {
            format!("{location} 應該等於或小於 {system}，但輸入為 {input}")
        }
        (Language::TraditionalChinese, CheckRule::MinStringLength) => {
            format!("{location} 的長度應該等於或大於 {system}，但 {input} 的長度是 {input_len}")
        }
        (Language::TraditionalChinese, CheckRule::MaxStringLength) => {
            format!("{location} 的長度應該等於或小於 {system}，但 {input} 的長度是 {input_len}")
        }

        (Language::Japanese, CheckRule::Required) => format!("{location} は必須です"),
        (Language::Japanese, CheckRule::Min) => {
            format!("{location} は {system} 以上である必要があります。入力値は {input} です")
        }
        (Language::Japanese, CheckRule::Max) => {
            format!("{location} は {system} 以下である必要があります。入力値は {input} です")
        }
        (Language::Japanese, CheckRule::MinStringLength) => {
            format!(
                "{location} の長さは {system} 以上である必要があります。実際の長さは {input_len} です"
            )
        }
        (Language::Japanese, CheckRule::MaxStringLength) => {
            format!(
                "{location} の長さは {system} 以下である必要があります。実際の長さは {input_len} です"
            )
        }

        (Language::Korean, CheckRule::Required) => format!("{location}은(는) 필수입니다"),
        (Language::Korean, CheckRule::Min) => {
            format!("{location}은(는) {system} 이상이어야 하지만 입력값은 {input}입니다")
        }
        (Language::Korean, CheckRule::Max) => {
            format!("{location}은(는) {system} 이하여야 하지만 입력값은 {input}입니다")
        }
        (Language::Korean, CheckRule::MinStringLength) => {
            format!("{location}의 길이는 {system} 이상이어야 하지만 실제 길이는 {input_len}입니다")
        }
        (Language::Korean, CheckRule::MaxStringLength) => {
            format!("{location}의 길이는 {system} 이하여야 하지만 실제 길이는 {input_len}입니다")
        }

        (Language::German, CheckRule::Required) => format!("{location} ist erforderlich"),
        (Language::German, CheckRule::Min) => {
            format!("{location} muss mindestens {system} sein, aber die Eingabe ist {input}")
        }
        (Language::German, CheckRule::Max) => {
            format!("{location} darf höchstens {system} sein, aber die Eingabe ist {input}")
        }
        (Language::German, CheckRule::MinStringLength) => {
            format!("Die Länge von {location} muss mindestens {system} sein, ist aber {input_len}")
        }
        (Language::German, CheckRule::MaxStringLength) => {
            format!("Die Länge von {location} darf höchstens {system} sein, ist aber {input_len}")
        }

        (Language::French, CheckRule::Required) => format!("{location} est obligatoire"),
        (Language::French, CheckRule::Min) => {
            format!(
                "{location} doit être supérieur ou égal à {system}, mais la valeur saisie est {input}"
            )
        }
        (Language::French, CheckRule::Max) => {
            format!(
                "{location} doit être inférieur ou égal à {system}, mais la valeur saisie est {input}"
            )
        }
        (Language::French, CheckRule::MinStringLength) => {
            format!(
                "La longueur de {location} doit être supérieure ou égale à {system}, mais elle est {input_len}"
            )
        }
        (Language::French, CheckRule::MaxStringLength) => {
            format!(
                "La longueur de {location} doit être inférieure ou égale à {system}, mais elle est {input_len}"
            )
        }

        (Language::Spanish, CheckRule::Required) => format!("{location} es requerido/a"),
        (Language::Spanish, CheckRule::Min) => {
            format!(
                "{location} debe ser mayor o igual que {system}, pero el valor ingresado es {input}"
            )
        }
        (Language::Spanish, CheckRule::Max) => {
            format!(
                "{location} debe ser menor o igual que {system}, pero el valor ingresado es {input}"
            )
        }
        (Language::Spanish, CheckRule::MinStringLength) => {
            format!(
                "La longitud de {location} debe ser mayor o igual que {system}, pero es {input_len}"
            )
        }
        (Language::Spanish, CheckRule::MaxStringLength) => {
            format!(
                "La longitud de {location} debe ser menor o igual que {system}, pero es {input_len}"
            )
        }

        (Language::Portuguese, CheckRule::Required) => format!("{location} é obrigatório"),
        (Language::Portuguese, CheckRule::Min) => {
            format!("{location} deve ser maior ou igual a {system}, mas a entrada é {input}")
        }
        (Language::Portuguese, CheckRule::Max) => {
            format!("{location} deve ser menor ou igual a {system}, mas a entrada é {input}")
        }
        (Language::Portuguese, CheckRule::MinStringLength) => {
            format!(
                "O comprimento de {location} deve ser maior ou igual a {system}, mas é {input_len}"
            )
        }
        (Language::Portuguese, CheckRule::MaxStringLength) => {
            format!(
                "O comprimento de {location} deve ser menor ou igual a {system}, mas é {input_len}"
            )
        }

        (Language::Arabic, CheckRule::Required) => format!("{location} مطلوب"),
        (Language::Arabic, CheckRule::Min) => {
            format!("يجب أن يكون {location} مساويًا أو أكبر من {system}، لكن المُدخل هو {input}")
        }
        (Language::Arabic, CheckRule::Max) => {
            format!("يجب أن يكون {location} مساويًا أو أصغر من {system}، لكن المُدخل هو {input}")
        }
        (Language::Arabic, CheckRule::MinStringLength) => {
            format!(
                "يجب أن يكون طول {location} مساويًا أو أكبر من {system}، لكن الطول هو {input_len}"
            )
        }
        (Language::Arabic, CheckRule::MaxStringLength) => {
            format!(
                "يجب أن يكون طول {location} مساويًا أو أصغر من {system}، لكن الطول هو {input_len}"
            )
        }

        (Language::Thai, CheckRule::Required) => format!("{location} เป็นสิ่งจำเป็น"),
        (Language::Thai, CheckRule::Min) => {
            format!("{location} ควรจะเท่ากับหรือมากกว่า {system} แต่ข้อมูลที่ป้อนคือ {input}")
        }
        (Language::Thai, CheckRule::Max) => {
            format!("{location} ควรจะเท่ากับหรือน้อยกว่า {system} แต่ข้อมูลที่ป้อนคือ {input}")
        }
        (Language::Thai, CheckRule::MinStringLength) => {
            format!("ความยาวของ {location} ควรจะเท่ากับหรือมากกว่า {system} แต่ความยาวคือ {input_len}")
        }
        (Language::Thai, CheckRule::MaxStringLength) => {
            format!("ความยาวของ {location} ควรจะเท่ากับหรือน้อยกว่า {system} แต่ความยาวคือ {input_len}")
        }

        (Language::Indonesian, CheckRule::Required) => format!("{location} wajib diisi"),
        (Language::Indonesian, CheckRule::Min) => {
            format!(
                "{location} harus sama dengan atau lebih besar dari {system}, tetapi input adalah {input}"
            )
        }
        (Language::Indonesian, CheckRule::Max) => {
            format!(
                "{location} harus sama dengan atau lebih kecil dari {system}, tetapi input adalah {input}"
            )
        }
        (Language::Indonesian, CheckRule::MinStringLength) => {
            format!(
                "Panjang {location} harus sama dengan atau lebih besar dari {system}, tetapi panjangnya {input_len}"
            )
        }
        (Language::Indonesian, CheckRule::MaxStringLength) => {
            format!(
                "Panjang {location} harus sama dengan atau lebih kecil dari {system}, tetapi panjangnya {input_len}"
            )
        }

        (Language::Filipino, CheckRule::Required) => format!("Ang {location} ay kinakailangan"),
        (Language::Filipino, CheckRule::Min) => {
            format!(
                "Ang {location} ay dapat katumbas o mas malaki kaysa {system}, ngunit ang input ay {input}"
            )
        }
        (Language::Filipino, CheckRule::Max) => {
            format!(
                "Ang {location} ay dapat katumbas o mas maliit kaysa {system}, ngunit ang input ay {input}"
            )
        }
        (Language::Filipino, CheckRule::MinStringLength) => {
            format!(
                "Ang haba ng {location} ay dapat katumbas o mas malaki kaysa {system}, ngunit ang haba ay {input_len}"
            )
        }
        (Language::Filipino, CheckRule::MaxStringLength) => {
            format!(
                "Ang haba ng {location} ay dapat katumbas o mas maliit kaysa {system}, ngunit ang haba ay {input_len}"
            )
        }

        (Language::Ukrainian, CheckRule::Required) => format!("{location} є обов'язковим"),
        (Language::Ukrainian, CheckRule::Min) => {
            format!(
                "{location} повинен бути рівним або більшим за {system}, але ввідне значення {input}"
            )
        }
        (Language::Ukrainian, CheckRule::Max) => {
            format!(
                "{location} повинен бути рівним або меншим за {system}, але ввідне значення {input}"
            )
        }
        (Language::Ukrainian, CheckRule::MinStringLength) => {
            format!(
                "Довжина {location} повинна бути рівною або більшою за {system}, але довжина становить {input_len}"
            )
        }
        (Language::Ukrainian, CheckRule::MaxStringLength) => {
            format!(
                "Довжина {location} повинна бути рівною або меншою за {system}, але довжина становить {input_len}"
            )
        }

        (Language::Vietnamese, CheckRule::Required) => format!("{location} là bắt buộc"),
        (Language::Vietnamese, CheckRule::Min) => {
            format!("{location} phải lớn hơn hoặc bằng {system}, nhưng giá trị nhập là {input}")
        }
        (Language::Vietnamese, CheckRule::Max) => {
            format!("{location} phải nhỏ hơn hoặc bằng {system}, nhưng giá trị nhập là {input}")
        }
        (Language::Vietnamese, CheckRule::MinStringLength) => {
            format!(
                "Độ dài của {location} phải lớn hơn hoặc bằng {system}, nhưng độ dài là {input_len}"
            )
        }
        (Language::Vietnamese, CheckRule::MaxStringLength) => {
            format!(
                "Độ dài của {location} phải nhỏ hơn hoặc bằng {system}, nhưng độ dài là {input_len}"
            )
        }
    }
}

pub fn translate_location(_language: Language, location: &ObjectLocation) -> String {
    title_case_path(&location.to_string())
}

fn title_case_path(path: &str) -> String {
    path.split('.')
        .map(|part| {
            if let Some((name, index)) = part.split_once('[') {
                format!("{}[{}", title_case_identifier(name), index)
            } else {
                title_case_identifier(part)
            }
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
        if index == 0 {
            output.extend(ch.to_uppercase());
        } else {
            output.extend(ch.to_lowercase());
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
        Value::Timestamp(value) => value.to_rfc3339(),
        Value::Object(_) => "<object>".to_owned(),
        Value::List(_) => "<list>".to_owned(),
    }
}
