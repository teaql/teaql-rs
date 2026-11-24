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
    pub fn distribution_centers() -> DistributionCenterRequest {
        DistributionCenterRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn distribution_centers_minimal() -> DistributionCenterRequest {
        DistributionCenterRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn distribution_centers_with_children() -> DistributionCenterRequest {
        DistributionCenterRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn dispatch_orders() -> DispatchOrderRequest {
        DispatchOrderRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn dispatch_orders_minimal() -> DispatchOrderRequest {
        DispatchOrderRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn dispatch_orders_with_children() -> DispatchOrderRequest {
        DispatchOrderRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}