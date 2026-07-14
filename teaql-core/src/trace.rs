#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceNode {
    pub entity_type: String,
    pub entity_id: Option<u64>,
    pub comment: String,
}

impl TraceNode {
    pub fn new(
        entity_type: impl Into<String>,
        entity_id: Option<u64>,
        comment: impl Into<String>,
    ) -> Self {
        Self {
            entity_type: entity_type.into(),
            entity_id,
            comment: comment.into(),
        }
    }
}
