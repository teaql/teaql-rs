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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_node_new() {
        let node = TraceNode::new("User", Some(123), "created user");
        assert_eq!(node.entity_type, "User");
        assert_eq!(node.entity_id, Some(123));
        assert_eq!(node.comment, "created user");
        
        let node2 = TraceNode::new("System", None, "system startup");
        assert_eq!(node2.entity_type, "System");
        assert_eq!(node2.entity_id, None);
        assert_eq!(node2.comment, "system startup");
    }
}
