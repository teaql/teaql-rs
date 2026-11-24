use teaql_core::Expr;

use crate::*;

pub struct PurposedQuery<T> {
    pub inner: T,
    pub purpose: String,
}

impl<T> PurposedQuery<T> {
    pub fn new(inner: T, purpose: impl Into<String>) -> Self {
        let purpose = purpose.into();
        assert!(!purpose.trim().is_empty(), "{}", teaql_core::RequestIntentError {
            request_kind: teaql_core::RequestKind::Query,
            field: "purpose",
        });
        Self { inner, purpose }
    }
}

pub struct Q;

impl Q {
    pub fn commerce_platforms() -> CommercePlatformRequest {
        CommercePlatformRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn commerce_platforms_minimal() -> CommercePlatformRequest {
        CommercePlatformRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn commerce_platforms_with_children() -> CommercePlatformRequest {
        CommercePlatformRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn customer_orders() -> CustomerOrderRequest {
        CustomerOrderRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn customer_orders_minimal() -> CustomerOrderRequest {
        CustomerOrderRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn customer_orders_with_children() -> CustomerOrderRequest {
        CustomerOrderRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn order_attachments() -> OrderAttachmentRequest {
        OrderAttachmentRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_attachments_minimal() -> OrderAttachmentRequest {
        OrderAttachmentRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_attachments_with_children() -> OrderAttachmentRequest {
        OrderAttachmentRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}