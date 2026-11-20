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
    pub fn suppliers() -> SupplierRequest {
        SupplierRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn suppliers_minimal() -> SupplierRequest {
        SupplierRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn suppliers_with_children() -> SupplierRequest {
        SupplierRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn supplier_orders() -> SupplierOrderRequest {
        SupplierOrderRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn supplier_orders_minimal() -> SupplierOrderRequest {
        SupplierOrderRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn supplier_orders_with_children() -> SupplierOrderRequest {
        SupplierOrderRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}