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
    pub fn catalogs() -> CatalogRequest {
        CatalogRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn catalogs_minimal() -> CatalogRequest {
        CatalogRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn catalogs_with_children() -> CatalogRequest {
        CatalogRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn entries() -> EntryRequest {
        EntryRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn entries_minimal() -> EntryRequest {
        EntryRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn entries_with_children() -> EntryRequest {
        EntryRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}