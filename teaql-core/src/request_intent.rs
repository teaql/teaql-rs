//! Request-owned intent. No Context, trace frame or logging option can supply it.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Query,
    Mutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestIntentError {
    pub request_kind: RequestKind,
    pub field: &'static str,
}

impl RequestIntentError {
    pub fn code(&self) -> &'static str {
        match self.field {
            "purpose" => "QUERY_PURPOSE_REQUIRED",
            _ => "REQUEST_COMMENT_REQUIRED",
        }
    }
}

impl std::fmt::Display for RequestIntentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {:?} Request requires a non-blank {}; supply it at the request entry point",
            self.code(),
            self.request_kind,
            self.field,
        )
    }
}

impl std::error::Error for RequestIntentError {}

fn required<'a>(
    value: Option<&'a str>,
    request_kind: RequestKind,
    field: &'static str,
) -> Result<&'a str, RequestIntentError> {
    value
        .filter(|text| !text.trim().is_empty())
        .ok_or(RequestIntentError {
            request_kind,
            field,
        })
}

/// Validated query intent. Private fields and no Default prevent blank envelopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryIntent {
    comment: String,
    purpose: String,
}

impl QueryIntent {
    pub fn new(
        comment: impl Into<String>,
        purpose: impl Into<String>,
    ) -> Result<Self, RequestIntentError> {
        let comment = comment.into();
        let purpose = purpose.into();
        required(Some(&comment), RequestKind::Query, "comment")?;
        required(Some(&purpose), RequestKind::Query, "purpose")?;
        Ok(Self { comment, purpose })
    }

    pub fn from_optional(
        comment: Option<&str>,
        purpose: Option<&str>,
    ) -> Result<Self, RequestIntentError> {
        Self::new(
            required(comment, RequestKind::Query, "comment")?,
            required(purpose, RequestKind::Query, "purpose")?,
        )
    }

    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn purpose(&self) -> &str {
        &self.purpose
    }
}

/// Validated root mutation reason. Local descendant reasons remain optional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationIntent {
    comment: String,
}

impl MutationIntent {
    pub fn new(comment: impl Into<String>) -> Result<Self, RequestIntentError> {
        let comment = comment.into();
        required(Some(&comment), RequestKind::Mutation, "comment")?;
        Ok(Self { comment })
    }

    pub fn from_optional(comment: Option<&str>) -> Result<Self, RequestIntentError> {
        Self::new(required(comment, RequestKind::Mutation, "comment")?)
    }

    pub fn comment(&self) -> &str {
        &self.comment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_empty_ascii_and_unicode_whitespace_are_rejected() {
        for value in [None, Some(""), Some(" \t\r\n"), Some("\u{2003}")] {
            let query = QueryIntent::from_optional(value, Some("render details")).unwrap_err();
            let mutation = MutationIntent::from_optional(value).unwrap_err();
            for error in [query, mutation] {
                assert_eq!(error.code(), "REQUEST_COMMENT_REQUIRED");
                assert_eq!(error.field, "comment");
            }
            let purpose = QueryIntent::from_optional(Some("load details"), value).unwrap_err();
            assert_eq!(purpose.code(), "QUERY_PURPOSE_REQUIRED");
            assert_eq!(purpose.field, "purpose");
        }
    }

    #[test]
    fn intent_is_preserved_without_trimming_or_duplicating_fields() {
        let query = QueryIntent::new(" what: load details ", "why: render details").unwrap();
        assert_eq!(query.comment(), " what: load details ");
        assert_eq!(query.purpose(), "why: render details");
        assert_eq!(
            MutationIntent::new("submit order").unwrap().comment(),
            "submit order"
        );
    }
}
