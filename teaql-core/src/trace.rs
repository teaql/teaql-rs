#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceNode {
    pub entity_type: String,
    pub entity_id: Option<u64>,
    pub comment: String,
}
