use teaql_core::Expr;

use crate::*;

pub struct PurposedQuery<T> {
    pub inner: T,
    pub purpose: String,
}

impl<T> PurposedQuery<T> {
    pub fn new(inner: T, purpose: impl Into<String>) -> Self {
        Self { inner, purpose: purpose.into() }
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

    pub fn customers() -> CustomerRequest {
        CustomerRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn customers_minimal() -> CustomerRequest {
        CustomerRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn customers_with_children() -> CustomerRequest {
        CustomerRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }

    pub fn order_statuses() -> OrderStatusRequest {
        OrderStatusRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_statuses_minimal() -> OrderStatusRequest {
        OrderStatusRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_statuses_with_children() -> OrderStatusRequest {
        OrderStatusRequest::new()
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

    pub fn products() -> ProductRequest {
        ProductRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn products_minimal() -> ProductRequest {
        ProductRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn products_with_children() -> ProductRequest {
        ProductRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }

    pub fn order_lines() -> OrderLineRequest {
        OrderLineRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_lines_minimal() -> OrderLineRequest {
        OrderLineRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_lines_with_children() -> OrderLineRequest {
        OrderLineRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }

    pub fn order_search_presets() -> OrderSearchPresetRequest {
        OrderSearchPresetRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_search_presets_minimal() -> OrderSearchPresetRequest {
        OrderSearchPresetRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn order_search_presets_with_children() -> OrderSearchPresetRequest {
        OrderSearchPresetRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }
}