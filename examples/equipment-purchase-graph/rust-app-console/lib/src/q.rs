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
    pub fn procurement_desks() -> ProcurementDeskRequest {
        ProcurementDeskRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn procurement_desks_minimal() -> ProcurementDeskRequest {
        ProcurementDeskRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn procurement_desks_with_children() -> ProcurementDeskRequest {
        ProcurementDeskRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn catalog_products() -> CatalogProductRequest {
        CatalogProductRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn catalog_products_minimal() -> CatalogProductRequest {
        CatalogProductRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn catalog_products_with_children() -> CatalogProductRequest {
        CatalogProductRequest::new()
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



    pub fn purchase_lines() -> PurchaseLineRequest {
        PurchaseLineRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn purchase_lines_minimal() -> PurchaseLineRequest {
        PurchaseLineRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn purchase_lines_with_children() -> PurchaseLineRequest {
        PurchaseLineRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}