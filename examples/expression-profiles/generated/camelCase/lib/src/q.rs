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
    pub fn type_platforms() -> TypePlatformRequest {
        TypePlatformRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn type_platforms_minimal() -> TypePlatformRequest {
        TypePlatformRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn type_platforms_with_children() -> TypePlatformRequest {
        TypePlatformRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }



    pub fn scalar_type_probes() -> ScalarTypeProbeRequest {
        ScalarTypeProbeRequest::new()
            .select_self()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn scalar_type_probes_minimal() -> ScalarTypeProbeRequest {
        ScalarTypeProbeRequest::new()
            .and_filter(Expr::gt("version", 0_i64))
    }

    pub fn scalar_type_probes_with_children() -> ScalarTypeProbeRequest {
        ScalarTypeProbeRequest::new()
            .unlimited()
            .select_self_fields()
            .enhance_children_if_needed()
    }


}