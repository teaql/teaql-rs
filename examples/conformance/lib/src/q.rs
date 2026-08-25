use teaql_core::Expr;

use crate::*;

pub struct PurposedQuery<T> {
    pub inner: T,
    pub purpose: String,
}

impl<T> PurposedQuery<T> {
    pub fn new(inner: T, purpose: impl Into<String>) -> Self {
        let purpose = purpose.into();
        assert!(
            !purpose.trim().is_empty(),
            "query purpose must not be empty"
        );
        Self { inner, purpose }
    }
}

pub struct Q;

impl Q {
    pub fn platforms() -> PlatformRequest {
        PlatformRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn platforms_minimal() -> PlatformRequest {
        PlatformRequest::new().and_filter(Expr::gt("version", 0_i64))
    }

    pub fn platforms_with_children() -> PlatformRequest {
        PlatformRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }

    pub fn work_items() -> WorkItemRequest {
        WorkItemRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn work_items_minimal() -> WorkItemRequest {
        WorkItemRequest::new().and_filter(Expr::gt("version", 0_i64))
    }

    pub fn work_items_with_children() -> WorkItemRequest {
        WorkItemRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }
}
