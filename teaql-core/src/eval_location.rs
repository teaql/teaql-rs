//! E traversal tracks canonical borrowed segments, not a parsed display string.
//! Up to eight segments stay inline; owned strings are built only on an error.
use crate::object_location::{JsonFieldNamingProfile, ObjectLocation, WireLocationSegment};

#[derive(Debug, Clone, Copy)]
enum Segment {
    Property {
        name: &'static str,
        method: Option<&'static str>,
    },
    Index {
        index: usize,
        first: bool,
    },
    Operation(&'static str),
}

#[derive(Debug, Clone, Default)]
pub struct ExpressionPath {
    segments: smallvec::SmallVec<[Segment; 8]>,
    missing: Option<usize>,
    root: Option<&'static str>,
}

impl ExpressionPath {
    pub fn root(expression: &'static str) -> Self {
        Self {
            root: Some(expression),
            ..Self::default()
        }
    }
    pub fn member(&self, name: &'static str) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Property { name, method: None });
        next
    }
    pub fn member_as(&self, name: &'static str, method: &'static str) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Property {
            name,
            method: Some(method),
        });
        next
    }
    pub fn element(&self, index: usize) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Index {
            index,
            first: false,
        });
        next
    }
    pub fn first(&self) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Index {
            index: 0,
            first: true,
        });
        next
    }
    pub fn operation(&self, call: &'static str) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Operation(call));
        next
    }
    pub fn location(&self) -> ObjectLocation {
        self.segments
            .iter()
            .fold(ObjectLocation::root(), |location, segment| match segment {
                Segment::Property { name, .. } => location.member(*name),
                Segment::Index { index, .. } => location.element(*index),
                Segment::Operation(_) => location,
            })
    }

    /// Freeze the first missing node. Later E access must not pretend that an
    /// unavailable relation or list was loaded merely to append a child path.
    pub fn mark_missing(&mut self) {
        if self.missing.is_none() {
            self.missing = self.segments.len().checked_sub(1);
        }
    }

    pub fn missing_member(&self) -> Option<&'static str> {
        match self.segments.get(self.missing?)? {
            Segment::Property { name, .. } => Some(*name),
            _ => None,
        }
    }

    pub fn failure_location(&self) -> ObjectLocation {
        let mut path = self.clone();
        if let Some(index) = path.missing {
            path.segments.truncate(index + 1);
        }
        path.location()
    }

    /// The complete generated native chain, with no entity IDs or argument values.
    pub fn render(&self, broken: bool) -> String {
        let mut result = self.root.unwrap_or("E::entity(<root>)").to_owned();
        for (index, segment) in self.segments.iter().enumerate() {
            match segment {
                Segment::Property { name, method } => {
                    result.push('.');
                    if let Some(method) = method {
                        result.push_str(method);
                    } else {
                        result.push_str("get_");
                        result.push_str(name);
                    }
                    result.push_str("()");
                }
                Segment::Index { index, first } => {
                    if *first {
                        result.push_str(".first()");
                    } else {
                        result.push_str(&format!(".get({index})"));
                    }
                }
                Segment::Operation(call) => {
                    result.push('.');
                    result.push_str(call);
                }
            }
            if broken && self.missing == Some(index) {
                result.push_str("<broken>");
            }
        }
        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLoadedDiagnostic {
    pub location: ObjectLocation,
    pub break_point: String,
    profile: JsonFieldNamingProfile,
    attempted_location: ObjectLocation,
    original_expression: Option<String>,
    expression_with_broken_point: Option<String>,
}

impl NotLoadedDiagnostic {
    pub fn new(location: ObjectLocation, break_point: impl Into<String>) -> Self {
        Self {
            attempted_location: location.clone(),
            location,
            break_point: break_point.into(),
            profile: JsonFieldNamingProfile::default(),
            original_expression: None,
            expression_with_broken_point: None,
        }
    }
    pub fn from_path(path: &ExpressionPath, fallback_member: &str, terminal: &'static str) -> Self {
        let complete = path.operation(terminal);
        Self {
            location: path.failure_location(),
            attempted_location: path.location(),
            break_point: path.missing_member().unwrap_or(fallback_member).to_owned(),
            profile: JsonFieldNamingProfile::default(),
            original_expression: Some(complete.render(false)),
            expression_with_broken_point: Some(complete.render(true)),
        }
    }
    pub fn with_profile(mut self, profile: JsonFieldNamingProfile) -> Self {
        self.profile = profile;
        self
    }
    pub fn to_wire(&self) -> WireNotLoadedDiagnostic {
        self.to_wire_with(self.profile)
    }
    pub fn to_wire_with(&self, profile: JsonFieldNamingProfile) -> WireNotLoadedDiagnostic {
        WireNotLoadedDiagnostic {
            code: "NOT_LOADED",
            location: self.location.wire_segments(),
            instance_path: self.location.instance_path_with(profile),
            break_point: self.break_point.clone(),
            attempted_location: self.attempted_location.wire_segments(),
            attempted_instance_path: self.attempted_location.instance_path_with(profile),
            break_point_index: self.location.wire_segments().len().saturating_sub(1),
            original_expression: self.original_expression.clone(),
            expression_with_broken_point: self.expression_with_broken_point.clone(),
            suggested_fix: PreloadGuidance {
                action: "preload",
                location: self.location.wire_segments(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireNotLoadedDiagnostic {
    pub code: &'static str,
    pub location: Vec<WireLocationSegment>,
    pub instance_path: String,
    pub break_point: String,
    pub attempted_location: Vec<WireLocationSegment>,
    pub attempted_instance_path: String,
    pub break_point_index: usize,
    pub original_expression: Option<String>,
    pub expression_with_broken_point: Option<String>,
    pub suggested_fix: PreloadGuidance,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PreloadGuidance {
    pub action: &'static str,
    pub location: Vec<WireLocationSegment>,
}

impl std::fmt::Display for NotLoadedDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TeaQLNotLoadedError: access_path={} break_point={} — select the missing field or relation",
            self.location.native_path(),
            self.break_point
        )?;
        if let Some(expression) = &self.expression_with_broken_point {
            write!(f, " expression={expression}")?;
        }
        Ok(())
    }
}
impl std::error::Error for NotLoadedDiagnostic {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_location_has_separate_model_native_and_wire_views() {
        let path = ExpressionPath::default()
            .member("school_type")
            .member("user_url");
        assert!(!path.segments.spilled());
        let diagnostic = NotLoadedDiagnostic::new(path.location(), "user_url");
        assert_eq!(diagnostic.location.model_path(), "school_type.user_url");
        assert_eq!(diagnostic.location.native_path(), "school_type.user_url");
        assert_eq!(diagnostic.to_wire().instance_path, "/schoolType/userUrl");
        assert_eq!(
            diagnostic
                .to_wire_with(JsonFieldNamingProfile::SnakeCase)
                .instance_path,
            "/school_type/user_url"
        );
        assert_eq!(
            diagnostic
                .to_wire_with(JsonFieldNamingProfile::PascalCase)
                .instance_path,
            "/SchoolType/UserUrl"
        );
    }

    #[test]
    fn list_indices_are_not_fake_property_names() {
        let path = ExpressionPath::default()
            .member("order_items")
            .element(12)
            .member("unit_price");
        let wire = NotLoadedDiagnostic::new(path.location(), "unit_price").to_wire();
        assert_eq!(wire.instance_path, "/orderItems/12/unitPrice");
        assert_eq!(wire.location[1], WireLocationSegment::Index { index: 12 });
    }

    #[test]
    fn literal_punctuation_is_not_split_or_reparsed() {
        let path = ExpressionPath::default().member("a~/b.c");
        assert_eq!(path.location().segments().len(), 1);
        assert_eq!(path.location().instance_path(), "/a~0~1b.c");
    }

    #[test]
    fn deep_paths_preserve_all_segments_and_do_not_change_siblings() {
        let original = ExpressionPath::default().member("root_field");
        let mut deep = original.clone();
        for index in 0..12 {
            deep = deep.element(index);
        }
        assert_eq!(deep.location().level(), 13);
        assert_eq!(original.location().instance_path(), "/rootField");
        assert_eq!(
            deep.location().segments().last(),
            Some(&crate::object_location::LocationSegment::Index(11))
        );
    }

    #[test]
    fn wire_projection_omits_internal_root_and_entity_values() {
        let wire =
            NotLoadedDiagnostic::new(ObjectLocation::hash_root("user_url"), "user_url").to_wire();
        let value = serde_json::to_value(wire).unwrap();
        assert_eq!(value["code"], "NOT_LOADED");
        assert_eq!(value["instancePath"], "/userUrl");
        assert!(value.get("root").is_none());
        assert!(value.get("accessPath").is_none());
        assert!(value.get("inputValue").is_none());
    }

    #[test]
    fn first_missing_node_survives_later_properties_and_list_indices() {
        let mut path = ExpressionPath::default().member("order_items");
        path.mark_missing();
        let mut child = path.element(2).member("unit_price");
        child.mark_missing();
        assert_eq!(child.location().model_path(), "order_items[2].unit_price");
        assert_eq!(child.failure_location().instance_path(), "/orderItems");
        assert_eq!(child.missing_member(), Some("order_items"));
    }

    #[test]
    fn early_failure_retains_suffix_and_marks_only_the_first_repeated_member() {
        let mut parent = ExpressionPath::root("E::node(<root>)").member("parent");
        parent.mark_missing();
        let mut leaf = parent.member("parent").member("user_url");
        leaf.mark_missing();
        let wire = NotLoadedDiagnostic::from_path(&leaf, "parent", "eval()").to_wire();
        assert_eq!(wire.instance_path, "/parent");
        assert_eq!(wire.attempted_instance_path, "/parent/parent/userUrl");
        assert_eq!(
            wire.original_expression.as_deref(),
            Some("E::node(<root>).get_parent().get_parent().get_user_url().eval()")
        );
        assert_eq!(
            wire.expression_with_broken_point.as_deref(),
            Some("E::node(<root>).get_parent()<broken>.get_parent().get_user_url().eval()")
        );
        assert_eq!(
            wire.expression_with_broken_point
                .unwrap()
                .matches("<broken>")
                .count(),
            1
        );
        assert_eq!(wire.break_point_index, 0);
        assert_eq!(wire.suggested_fix.location, wire.location);
    }

    #[test]
    fn native_calls_indices_and_operations_do_not_change_canonical_identity() {
        let mut items = ExpressionPath::root("E::order(<root>)").member("order_items");
        items.mark_missing();
        let leaf = items.first().member_as("school_type", "get_school_type_id");
        let wire = NotLoadedDiagnostic::from_path(&leaf, "order_items", "or_if_null(<fallback>)")
            .with_profile(JsonFieldNamingProfile::SnakeCase)
            .to_wire();
        assert_eq!(wire.attempted_instance_path, "/order_items/0/school_type");
        assert_eq!(
            wire.expression_with_broken_point.as_deref(),
            Some(
                "E::order(<root>).get_order_items()<broken>.first().get_school_type_id().or_if_null(<fallback>)"
            )
        );
        let size =
            NotLoadedDiagnostic::from_path(&items.operation("size()"), "order_items", "eval()")
                .to_wire();
        assert_eq!(size.attempted_instance_path, "/orderItems");
        assert_eq!(
            size.expression_with_broken_point.as_deref(),
            Some("E::order(<root>).get_order_items()<broken>.size().eval()")
        );
        assert_eq!(
            items.element(12).location().instance_path(),
            "/orderItems/12"
        );
    }
}
