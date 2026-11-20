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
    pub fn dispatch_desks() -> DispatchDeskRequest {
        DispatchDeskRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn dispatch_desks_minimal() -> DispatchDeskRequest {
        DispatchDeskRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn dispatch_desks_with_children() -> DispatchDeskRequest {
        DispatchDeskRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn shipping_orders() -> ShippingOrderRequest {
        ShippingOrderRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipping_orders_minimal() -> ShippingOrderRequest {
        ShippingOrderRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipping_orders_with_children() -> ShippingOrderRequest {
        ShippingOrderRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn shipping_lines() -> ShippingLineRequest {
        ShippingLineRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipping_lines_minimal() -> ShippingLineRequest {
        ShippingLineRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipping_lines_with_children() -> ShippingLineRequest {
        ShippingLineRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}