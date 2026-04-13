use quote::quote;
use syn::{Expr, Lit};

pub fn parse_container_attrs(attrs: &[syn::Attribute], default_name: &str) -> (String, String) {
    let mut entity_name = default_name.to_owned();
    let mut table_name = default_table_name(default_name);

    for attr in attrs {
        if !attr.path().is_ident("teaql") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("entity") {
                let value = meta.value()?;
                entity_name = parse_string_expr(&value.parse::<Expr>()?);
            } else if meta.path.is_ident("table") {
                let value = meta.value()?;
                table_name = parse_string_expr(&value.parse::<Expr>()?);
            }
            Ok(())
        });
    }

    (entity_name, table_name)
}

fn default_table_name(entity_name: &str) -> String {
    let mut out = String::with_capacity(entity_name.len() + 5);
    for (index, ch) in entity_name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out.push_str("_data");
    out
}

#[derive(Default)]
pub struct ParsedFieldAttrs {
    pub column: Option<String>,
    pub id: bool,
    pub version: bool,
    pub dynamic: bool,
    pub relation: Option<ParsedRelation>,
}

#[derive(Default)]
pub struct ParsedRelation {
    pub target: String,
    pub local_key: Option<String>,
    pub foreign_key: Option<String>,
    pub many: proc_macro2::TokenStream,
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
