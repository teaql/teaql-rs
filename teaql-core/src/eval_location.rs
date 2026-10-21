//! E traversal tracks canonical borrowed segments, not a parsed display string.
//! Up to eight segments stay inline; owned strings are built only on an error.
use crate::object_location::{JsonFieldNamingProfile, ObjectLocation, WireLocationSegment};

#[derive(Debug, Clone, Copy)]
enum Segment {
    Property(&'static str),
    Index(usize),
}

#[derive(Debug, Clone, Default)]
pub struct ExpressionPath {
    segments: smallvec::SmallVec<[Segment; 8]>,
    missing: Option<usize>,
}

impl ExpressionPath {
    pub fn member(&self, name: &'static str) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Property(name));
        next
    }
    pub fn element(&self, index: usize) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Index(index));
        next
    }
    pub fn location(&self) -> ObjectLocation {
        self.segments
            .iter()
            .fold(ObjectLocation::root(), |location, segment| match segment {
                Segment::Property(name) => location.member(*name),
                Segment::Index(index) => location.element(*index),
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
            Segment::Property(name) => Some(*name),
            Segment::Index(_) => None,
        }
    }

    pub fn failure_location(&self) -> ObjectLocation {
        let mut path = self.clone();
        if let Some(index) = path.missing {
            path.segments.truncate(index + 1);
        }
        path.location()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLoadedDiagnostic {
    pub location: ObjectLocation,
    pub break_point: String,
    profile: JsonFieldNamingProfile,
}

impl NotLoadedDiagnostic {
    pub fn new(location: ObjectLocation, break_point: impl Into<String>) -> Self {
        Self {
            location,
            break_point: break_point.into(),
            profile: JsonFieldNamingProfile::default(),
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
}

impl std::fmt::Display for NotLoadedDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TeaQLNotLoadedError: access_path={} break_point={} — select the missing field or relation",
            self.location.native_path(),
            self.break_point
        )
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
}
