use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Expr, Fields, Lit, Type, parse_macro_input,
};

#[proc_macro_derive(TeaqlEntity, attributes(teaql))]
pub fn derive_teaql_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_teaql_entity(input).into()
}

fn expand_teaql_entity(input: DeriveInput) -> proc_macro2::TokenStream {
    let struct_name = input.ident;
    let (entity_name, table_name) = parse_container_attrs(&input.attrs, &struct_name.to_string());

    let fields = match input.data {
        Data::Struct(data) => data.fields,
        _ => panic!("TeaqlEntity only supports structs"),
    };

    let named_fields = match fields {
        Fields::Named(fields) => fields.named,
        _ => panic!("TeaqlEntity only supports structs with named fields"),
    };

    let mut property_tokens = Vec::new();
    let mut relation_tokens = Vec::new();

    for field in named_fields {
        let field_ident = field.ident.expect("named field");
        let field_name = field_ident.to_string();
        let parsed = parse_field_attrs(&field.attrs);

        if let Some(relation) = parsed.relation {
            let local_key = relation.local_key.unwrap_or_else(|| "id".to_owned());
            let foreign_key = relation.foreign_key.unwrap_or_else(|| "id".to_owned());
            let target = relation.target;
            let many = relation.many;
            relation_tokens.push(quote! {
                descriptor = descriptor.relation(
                    ::teaql_core::RelationDescriptor::new(#field_name, #target)
                        .local_key(#local_key)
                        .foreign_key(#foreign_key)
                        #many
                );
            });
            continue;
        }

        let data_type = rust_type_to_data_type(&field.ty);
        let column_name = parsed.column.unwrap_or_else(|| field_name.clone());
        let nullable = is_option(&field.ty);
        let id = parsed.id;
        let version = parsed.version;

        let nullable_tokens = if nullable {
            quote! {}
        } else {
            quote! { .not_null() }
        };
        let id_tokens = if id {
            quote! { .id() }
        } else {
            quote! {}
        };
        let version_tokens = if version {
            quote! { .version() }
        } else {
            quote! {}
        };

        property_tokens.push(quote! {
            descriptor = descriptor.property(
                ::teaql_core::PropertyDescriptor::new(#field_name, #data_type)
                    .column_name(#column_name)
                    #nullable_tokens
                    #id_tokens
                    #version_tokens
            );
        });
    }

    quote! {
        impl ::teaql_core::TeaqlEntity for #struct_name {
            fn entity_descriptor() -> ::teaql_core::EntityDescriptor {
                let mut descriptor = ::teaql_core::EntityDescriptor::new(#entity_name)
                    .table_name(#table_name);
                #(#property_tokens)*
                #(#relation_tokens)*
                descriptor
            }
        }
    }
}

fn parse_container_attrs(attrs: &[syn::Attribute], default_name: &str) -> (String, String) {
    let mut entity_name = default_name.to_owned();
    let mut table_name = default_name.to_lowercase();

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

#[derive(Default)]
struct ParsedFieldAttrs {
    column: Option<String>,
    id: bool,
    version: bool,
    relation: Option<ParsedRelation>,
}

#[derive(Default)]
struct ParsedRelation {
    target: String,
    local_key: Option<String>,
    foreign_key: Option<String>,
    many: proc_macro2::TokenStream,
}

fn parse_field_attrs(attrs: &[syn::Attribute]) -> ParsedFieldAttrs {
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

fn is_option(ty: &Type) -> bool {
    match ty {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident == "Option")
            .unwrap_or(false),
        _ => false,
    }
}

fn rust_type_to_data_type(ty: &Type) -> proc_macro2::TokenStream {
    let type_name = base_type_name(ty);
    match type_name.as_deref() {
        Some("bool") => quote! { ::teaql_core::DataType::Bool },
        Some("i64") | Some("i32") | Some("i16") | Some("u64") | Some("u32") => {
            quote! { ::teaql_core::DataType::I64 }
        }
        Some("f64") | Some("f32") => quote! { ::teaql_core::DataType::F64 },
        Some("String") | Some("str") => quote! { ::teaql_core::DataType::Text },
        _ => quote! { ::teaql_core::DataType::Json },
    }
}

fn base_type_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(path) => {
            let last = path.path.segments.last()?;
            if last.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return base_type_name(inner);
                    }
                }
                None
            } else {
                Some(last.ident.to_string())
            }
        }
        Type::Reference(reference) => base_type_name(&reference.elem),
        _ => None,
    }
}
