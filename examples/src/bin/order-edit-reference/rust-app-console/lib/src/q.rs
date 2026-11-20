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
    pub fn merchants() -> MerchantRequest {
        MerchantRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn merchants_minimal() -> MerchantRequest {
        MerchantRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn merchants_with_children() -> MerchantRequest {
        MerchantRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn sales_orders() -> SalesOrderRequest {
        SalesOrderRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn sales_orders_minimal() -> SalesOrderRequest {
        SalesOrderRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn sales_orders_with_children() -> SalesOrderRequest {
        SalesOrderRequest::new()
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