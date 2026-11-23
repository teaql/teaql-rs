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
    pub fn tenants() -> TenantRequest {
        TenantRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn tenants_minimal() -> TenantRequest {
        TenantRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn tenants_with_children() -> TenantRequest {
        TenantRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn purchase_orders() -> PurchaseOrderRequest {
        PurchaseOrderRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn purchase_orders_minimal() -> PurchaseOrderRequest {
        PurchaseOrderRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn purchase_orders_with_children() -> PurchaseOrderRequest {
        PurchaseOrderRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn order_items() -> OrderItemRequest {
        OrderItemRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_items_minimal() -> OrderItemRequest {
        OrderItemRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_items_with_children() -> OrderItemRequest {
        OrderItemRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}