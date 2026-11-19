use teaql_core::Expr;

use crate::*;

pub struct PurposedQuery<T> {
    pub inner: T,
    pub purpose: String,
}

impl<T> PurposedQuery<T> {
    pub fn new(inner: T, purpose: impl Into<String>) -> Self {
        let purpose = purpose.into();
        assert!(!purpose.trim().is_empty(), "query purpose must not be empty");
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
        PlatformRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn platforms_with_children() -> PlatformRequest {
        PlatformRequest::new()
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



    pub fn payments() -> PaymentRequest {
        PaymentRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn payments_minimal() -> PaymentRequest {
        PaymentRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn payments_with_children() -> PaymentRequest {
        PaymentRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn payment_attempts() -> PaymentAttemptRequest {
        PaymentAttemptRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn payment_attempts_minimal() -> PaymentAttemptRequest {
        PaymentAttemptRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn payment_attempts_with_children() -> PaymentAttemptRequest {
        PaymentAttemptRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn shipments() -> ShipmentRequest {
        ShipmentRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipments_minimal() -> ShipmentRequest {
        ShipmentRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn shipments_with_children() -> ShipmentRequest {
        ShipmentRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}