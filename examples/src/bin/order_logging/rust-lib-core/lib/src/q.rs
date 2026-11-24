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
    pub fn warehouses() -> WarehouseRequest {
        WarehouseRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn warehouses_minimal() -> WarehouseRequest {
        WarehouseRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn warehouses_with_children() -> WarehouseRequest {
        WarehouseRequest::new()
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



    pub fn shipping_items() -> ShippingItemRequest {
        ShippingItemRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipping_items_minimal() -> ShippingItemRequest {
        ShippingItemRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipping_items_with_children() -> ShippingItemRequest {
        ShippingItemRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}