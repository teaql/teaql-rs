#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceKind {
    Operation,
    Request,
    Relation,
    Entity,
    Provider,
    Sql,
    Comment,
    Purpose,
    AuditReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceNode {
    pub kind: TraceKind,
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
            kind: TraceKind::Entity,
            entity_type: entity_type.into(),
            entity_id,
            comment: comment.into(),
        }
    }

    pub fn typed(
        kind: TraceKind,
        name: impl Into<String>,
        entity_id: Option<u64>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            entity_type: name.into(),
            entity_id,
            comment: value.into(),
        }
    }
}
