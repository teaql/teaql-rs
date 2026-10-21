//! Canonical graph locations shared by Checker and E diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocationSegment {
    Member(String),
    Index(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JsonFieldNamingProfile {
    #[default]
    CamelCase,
    SnakeCase,
    PascalCase,
}

impl JsonFieldNamingProfile {
    pub fn from_model_value(value: &str) -> Result<Self, String> {
        match value {
            "" | "camelCase" => Ok(Self::CamelCase),
            "snake_case" => Ok(Self::SnakeCase),
            "PascalCase" => Ok(Self::PascalCase),
            other => Err(format!("unsupported json_field_naming: {other}")),
        }
    }

    fn render(self, name: &str) -> String {
        match self {
            Self::SnakeCase => name.to_owned(),
            Self::CamelCase => lower_camel(name),
            Self::PascalCase => {
                let camel = lower_camel(name);
                let mut chars = camel.chars();
                chars
                    .next()
                    .map(|first| first.to_uppercase().chain(chars).collect())
                    .unwrap_or_default()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectLocation {
    segments: Vec<LocationSegment>,
}

impl ObjectLocation {
    /// Read-only canonical segments; never derive identity by parsing a display path.
    pub fn segments(&self) -> &[LocationSegment] {
        &self.segments
    }

    pub fn wire_segments(&self) -> Vec<WireLocationSegment> {
        self.segments
            .iter()
            .map(|segment| match segment {
                LocationSegment::Member(name) => {
                    WireLocationSegment::Property { name: name.clone() }
                }
                LocationSegment::Index(index) => WireLocationSegment::Index { index: *index },
            })
            .collect()
    }

    pub fn root() -> Self {
        Self::default()
    }

    pub fn hash_root(member: impl Into<String>) -> Self {
        Self::root().member(member)
    }

    pub fn array_root(index: usize) -> Self {
        Self::root().element(index)
    }

    pub fn member(mut self, member: impl Into<String>) -> Self {
        self.segments.push(LocationSegment::Member(member.into()));
        self
    }

    pub fn element(mut self, index: usize) -> Self {
        self.segments.push(LocationSegment::Index(index));
        self
    }

    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn level(&self) -> usize {
        self.segments.len()
    }

    /// Canonical casing-neutral path using KSML property names.
    pub fn model_path(&self) -> String {
        self.render_path(|name| name.to_owned())
    }

    /// Rust diagnostic path. KSML snake_case is already idiomatic Rust.
    pub fn native_path(&self) -> String {
        self.render_path(|name| name.to_owned())
    }

    /// RFC 6901 JSON pointer using TeaQL's default lower-camel wire policy.
    pub fn instance_path(&self) -> String {
        self.instance_path_with(JsonFieldNamingProfile::CamelCase)
    }

    pub fn instance_path_with(&self, profile: JsonFieldNamingProfile) -> String {
        self.segments
            .iter()
            .map(|segment| match segment {
                LocationSegment::Member(member) => {
                    format!("/{}", escape_json_pointer(&profile.render(member)))
                }
                LocationSegment::Index(index) => format!("/{index}"),
            })
            .collect()
    }

    fn render_path(&self, property_name: impl Fn(&str) -> String) -> String {
        let mut result = String::new();
        for segment in &self.segments {
            match segment {
                LocationSegment::Member(member) => {
                    if !result.is_empty() {
                        result.push('.');
                    }
                    result.push_str(&property_name(member));
                }
                LocationSegment::Index(index) => result.push_str(&format!("[{index}]")),
            }
        }
        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum WireLocationSegment {
    Property { name: String },
    Index { index: usize },
}

fn lower_camel(name: &str) -> String {
    let mut parts = name.split('_');
    let mut result = parts.next().unwrap_or_default().to_owned();
    for part in parts {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            result.extend(first.to_uppercase());
            result.extend(chars);
        }
    }
    result
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

impl std::fmt::Display for ObjectLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.segments.is_empty() {
            return write!(f, "$");
        }
        let mut first = true;
        for segment in &self.segments {
            match segment {
                LocationSegment::Member(member) => {
                    if !first {
                        write!(f, ".")?;
                    }
                    write!(f, "{member}")?;
                }
                LocationSegment::Index(index) => {
                    write!(f, "[{index}]")?;
                }
            }
            first = false;
        }
        Ok(())
    }
}
