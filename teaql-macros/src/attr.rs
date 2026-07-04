use quote::quote;
use syn::{Expr, Lit};

pub struct ContainerAttrs {
    pub entity_name: String,
    pub table_name: String,
    pub data_service: Option<String>,
    pub audit_mask_fields: Vec<String>,
    pub audit_value_max_len: Option<usize>,
}

pub fn parse_container_attrs(attrs: &[syn::Attribute], default_name: &str) -> ContainerAttrs {
    let mut attrs_out = ContainerAttrs {
        entity_name: default_name.to_owned(),
        table_name: default_table_name(default_name),
        data_service: None,
        audit_mask_fields: Vec::new(),
        audit_value_max_len: None,
    };

    for attr in attrs {
        if !attr.path().is_ident("teaql") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("entity") {
                let value = meta.value()?;
                attrs_out.entity_name = parse_string_expr(&value.parse::<Expr>()?);
            } else if meta.path.is_ident("table") {
                let value = meta.value()?;
                attrs_out.table_name = parse_string_expr(&value.parse::<Expr>()?);
            } else if meta.path.is_ident("data_service") {
                let value = meta.value()?;
                attrs_out.data_service = Some(parse_string_expr(&value.parse::<Expr>()?));
            } else if meta.path.is_ident("audit_mask_fields") {
                let value = meta.value()?;
                let fields_str = parse_string_expr(&value.parse::<Expr>()?);
                attrs_out.audit_mask_fields = fields_str.split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect();
            } else if meta.path.is_ident("audit_value_max_len") {
                let value = meta.value()?;
                let expr = value.parse::<Expr>()?;
                if let Expr::Lit(syn::ExprLit { lit: Lit::Int(lit_int), .. }) = &expr {
                    attrs_out.audit_value_max_len = lit_int.base10_parse().ok();
                } else if let Expr::Lit(syn::ExprLit { lit: Lit::Str(lit_str), .. }) = &expr {
                    attrs_out.audit_value_max_len = lit_str.value().parse().ok();
                }
            } else if meta.path.is_ident("audit_value_max_len_int") {
                // In case it's passed as an integer literal instead of string
                let value = meta.value()?;
                if let Expr::Lit(syn::ExprLit { lit: Lit::Int(lit_int), .. }) = value.parse::<Expr>()? {
                    attrs_out.audit_value_max_len = lit_int.base10_parse().ok();
                }
            }
            Ok(())
        });
    }

    attrs_out
}

fn default_table_name(entity_name: &str) -> String {
    let mut out = String::with_capacity(entity_name.len() + 5);
    for (index, ch) in entity_name.chars().enumerate() {
        match ch.is_ascii_uppercase() {
            true => {
                if index > 0 {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
            }
            false => out.push(ch),
        }
    }
    out.push_str("_data");
    out
}

pub struct ParsedFieldAttrs {
    pub id: bool,
    pub version: bool,
    pub dynamic: bool,
    pub skip: bool,
    pub boxed_relations: bool,
    pub column: Option<String>,
    pub relation: Option<ParsedRelation>,
}

impl Default for ParsedFieldAttrs {
    fn default() -> Self {
        Self {
            id: false,
            version: false,
            dynamic: false,
            skip: false,
            boxed_relations: false,
            column: None,
            relation: None,
        }
    }
}

#[derive(Default)]
pub struct ParsedRelation {
    pub target: String,
    pub local_key: Option<String>,
    pub foreign_key: Option<String>,
    pub many: proc_macro2::TokenStream,
    pub attach: proc_macro2::TokenStream,
    pub delete_missing: proc_macro2::TokenStream,
}

pub fn parse_field_attrs(attrs: &[syn::Attribute]) -> ParsedFieldAttrs {
    let mut parsed = ParsedFieldAttrs::default();

    for attr in attrs {
        if !attr.path().is_ident("teaql") {
            continue;
        }

        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                parsed.id = true;
            } else if meta.path.is_ident("version") {
                parsed.version = true;
            } else if meta.path.is_ident("dynamic") {
                parsed.dynamic = true;
            } else if meta.path.is_ident("skip") {
                parsed.skip = true;
            } else if meta.path.is_ident("boxed_relations") {
                parsed.boxed_relations = true;
            } else if meta.path.is_ident("column") {
                let value = meta.value()?;
                parsed.column = Some(parse_string_expr(&value.parse::<Expr>()?));
            } else if meta.path.is_ident("relation") {
                let mut relation = ParsedRelation::default();
                meta.parse_nested_meta(|nested| {
                    if nested.path.is_ident("target") {
                        let value = nested.value()?;
                        relation.target = parse_string_expr(&value.parse::<Expr>()?);
                    } else if nested.path.is_ident("local_key") {
                        let value = nested.value()?;
                        relation.local_key = Some(parse_string_expr(&value.parse::<Expr>()?));
                    } else if nested.path.is_ident("foreign_key") {
                        let value = nested.value()?;
                        relation.foreign_key = Some(parse_string_expr(&value.parse::<Expr>()?));
                    } else if nested.path.is_ident("many") {
                        relation.many = quote! { .many() };
                    } else if nested.path.is_ident("attach") {
                        let value = nested.value()?;
                        if !parse_bool_expr(&value.parse::<Expr>()?) {
                            relation.attach = quote! { .detached() };
                        }
                    } else if nested.path.is_ident("delete_missing") {
                        let value = nested.value()?;
                        if !parse_bool_expr(&value.parse::<Expr>()?) {
                            relation.delete_missing = quote! { .keep_missing() };
                        }
                    }
                    Ok(())
                })?;
                parsed.relation = Some(relation);
            }
            Ok(())
        });
    }

    parsed
}

fn parse_string_expr(expr: &Expr) -> String {
    match expr {
        Expr::Lit(expr_lit) => match &expr_lit.lit {
            Lit::Str(value) => value.value(),
            _ => panic!("expected string literal"),
        },
        _ => panic!("expected string literal"),
    }
}

fn parse_bool_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Lit(expr_lit) => match &expr_lit.lit {
            Lit::Bool(value) => value.value,
            _ => panic!("expected bool literal"),
        },
        _ => panic!("expected bool literal"),
    }
}
