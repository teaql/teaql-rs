use serde::{Deserialize, Deserializer, Serializer};

pub mod trimmed_string {
    use super::*;

    pub fn serialize<S>(value: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(value.trim())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(s.trim().to_owned())
    }
}

pub mod trimmed_opt_string {
    use super::*;

    pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(v) => serializer.serialize_str(v.trim()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<String>::deserialize(deserializer)?;
        Ok(opt.map(|s| s.trim().to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::{trimmed_opt_string, trimmed_string};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TrimmedFields {
        #[serde(with = "trimmed_string")]
        required: String,
        #[serde(default, with = "trimmed_opt_string")]
        optional: Option<String>,
    }

    #[test]
    fn trimmed_string_helpers_trim_during_serialization() {
        let fields = TrimmedFields {
            required: "  required value\n".to_owned(),
            optional: Some("\toptional value  ".to_owned()),
        };

        assert_eq!(
            serde_json::to_value(fields).expect("trimmed fields should serialize"),
            serde_json::json!({
                "required": "required value",
                "optional": "optional value"
            })
        );
    }

    #[test]
    fn trimmed_optional_string_preserves_none_during_serialization() {
        let fields = TrimmedFields {
            required: " value ".to_owned(),
            optional: None,
        };

        assert_eq!(
            serde_json::to_value(fields).expect("trimmed fields should serialize"),
            serde_json::json!({
                "required": "value",
                "optional": null
            })
        );
    }

    #[test]
    fn trimmed_string_helpers_trim_during_deserialization() {
        let fields: TrimmedFields = serde_json::from_value(serde_json::json!({
            "required": "  required value\n",
            "optional": "\toptional value  "
        }))
        .expect("trimmed fields should deserialize");

        assert_eq!(
            fields,
            TrimmedFields {
                required: "required value".to_owned(),
                optional: Some("optional value".to_owned()),
            }
        );
    }

    #[test]
    fn trimmed_optional_string_keeps_whitespace_only_input_as_some_empty() {
        let fields: TrimmedFields = serde_json::from_value(serde_json::json!({
            "required": " value ",
            "optional": " \t\n "
        }))
        .expect("trimmed fields should deserialize");

        assert_eq!(fields.required, "value");
        assert_eq!(fields.optional, Some(String::new()));
    }
}
