use quote::quote;
use syn::Type;

use crate::types::{
    base_type_name, is_option, option_inner_type, smart_list_inner_type, vec_inner_type,
};

pub fn from_record_value_tokens(
    ty: &Type,
    field_name: &str,
    entity_name: &str,
) -> proc_macro2::TokenStream {
    if is_option(ty) {
        let inner = option_inner_type(ty).expect("Option must have inner type");
        let inner_decode = decode_value_tokens(inner, quote! { value }, field_name, entity_name);
        return quote! {
            match record.get(#field_name) {
                Some(::teaql_core::Value::Null) | None => None,
                Some(value) => Some(#inner_decode),
            }
        };
    }

    let value_expr = quote! {
        record.get(#field_name).ok_or_else(|| {
            ::teaql_core::EntityError::new(#entity_name, format!("missing field: {}", #field_name))
        })?
    };
    decode_value_tokens(ty, value_expr, field_name, entity_name)
}

pub fn decode_value_tokens(
    ty: &Type,
    value_expr: proc_macro2::TokenStream,
    field_name: &str,
    entity_name: &str,
) -> proc_macro2::TokenStream {
    let type_name = base_type_name(ty);
    match type_name.as_deref() {
        Some("bool") => quote! {
            match #value_expr {
                ::teaql_core::Value::Bool(v) => *v,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("i64") => quote! {
            match #value_expr {
                ::teaql_core::Value::I64(v) => *v,
                ::teaql_core::Value::U64(v) => i64::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: u64 out of i64 range", #field_name)))?,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("i16") => quote! {
            match #value_expr {
                ::teaql_core::Value::I64(v) => i16::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: i64 out of i16 range", #field_name)))?,
                ::teaql_core::Value::U64(v) => i16::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: u64 out of i16 range", #field_name)))?,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("i32") => quote! {
            match #value_expr {
                ::teaql_core::Value::I64(v) => i32::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: i64 out of i32 range", #field_name)))?,
                ::teaql_core::Value::U64(v) => i32::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: u64 out of i32 range", #field_name)))?,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("u64") => quote! {
            match #value_expr {
                ::teaql_core::Value::U64(v) => *v,
                ::teaql_core::Value::I64(v) => u64::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: negative i64 cannot map to u64", #field_name)))?,
                ::teaql_core::Value::Decimal(v) => ::teaql_core::Value::Decimal(*v).try_u64()
                    .ok_or_else(|| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: decimal cannot map exactly to u64", #field_name)))?,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("u16") => quote! {
            match #value_expr {
                ::teaql_core::Value::U64(v) => u16::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: u64 out of u16 range", #field_name)))?,
                ::teaql_core::Value::I64(v) => u16::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: i64 out of u16 range", #field_name)))?,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("u32") => quote! {
            match #value_expr {
                ::teaql_core::Value::U64(v) => u32::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: u64 out of u32 range", #field_name)))?,
                ::teaql_core::Value::I64(v) => u32::try_from(*v)
                    .map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: i64 out of u32 range", #field_name)))?,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("f64") => quote! {
            match #value_expr {
                ::teaql_core::Value::F64(v) => *v,
                ::teaql_core::Value::I64(v) => *v as f64,
                ::teaql_core::Value::Decimal(v) => {
                    v.to_string().parse::<f64>().map_err(|_| ::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: decimal out of f64 range", #field_name)))?
                },
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("f32") => quote! {
            match #value_expr {
                ::teaql_core::Value::F64(v) => {
                    if *v < f32::MIN as f64 || *v > f32::MAX as f64 {
                        return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: f64 out of f32 range", #field_name)));
                    }
                    *v as f32
                },
                ::teaql_core::Value::I64(v) => *v as f32,
                ::teaql_core::Value::U64(v) => *v as f32,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("Decimal") => quote! {
            match #value_expr {
                ::teaql_core::Value::Decimal(v) => *v,
                ::teaql_core::Value::I64(v) => ::teaql_core::Decimal::from(*v),
                ::teaql_core::Value::U64(v) => ::teaql_core::Decimal::from(*v),
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("String") => quote! {
            match #value_expr {
                ::teaql_core::Value::Text(v) => v.clone(),
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("NaiveDate") => quote! {
            match #value_expr {
                ::teaql_core::Value::Date(v) => *v,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        Some("DateTime") => quote! {
            match #value_expr {
                ::teaql_core::Value::Timestamp(v) => *v,
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
        _ => quote! {
            match #value_expr {
                ::teaql_core::Value::Json(v) => v.clone(),
                other => return Err(::teaql_core::EntityError::new(#entity_name, format!("invalid field {}: {:?}", #field_name, other))),
            }
        },
    }
}

pub fn into_record_value_tokens(
    ty: &Type,
    value_expr: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    if is_option(ty) {
        return quote! {
            match #value_expr {
                Some(v) => v.into(),
                None => ::teaql_core::Value::Null,
            }
        };
    }
    quote! { (#value_expr).into() }
}

pub fn from_relation_value_tokens(
    ty: &Type,
    field_name: &str,
    entity_name: &str,
) -> proc_macro2::TokenStream {
    if let Some(inner) = option_inner_type(ty) {
        return quote! {
            match record.get(#field_name) {
                Some(::teaql_core::Value::Object(record)) => {
                    Some(<#inner as ::teaql_core::Entity>::from_record(record.clone())?)
                }
                Some(::teaql_core::Value::Null) | None => None,
                other => {
                    return Err(::teaql_core::EntityError::new(
                        #entity_name,
                        format!("invalid relation field {}: {:?}", #field_name, other),
                    ))
                }
            }
        };
    }

    if let Some(inner) = vec_inner_type(ty) {
        return quote! {
            match record.get(#field_name) {
                Some(::teaql_core::Value::List(values)) => values
                    .iter()
                    .map(|value| match value {
                        ::teaql_core::Value::Object(record) => {
                            <#inner as ::teaql_core::Entity>::from_record(record.clone())
                        }
                        other => Err(::teaql_core::EntityError::new(
                            #entity_name,
                            format!("invalid relation list item {}: {:?}", #field_name, other),
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Some(::teaql_core::Value::Null) | None => Vec::new(),
                other => {
                    return Err(::teaql_core::EntityError::new(
                        #entity_name,
                        format!("invalid relation field {}: {:?}", #field_name, other),
                    ))
                }
            }
        };
    }

    if let Some(inner) = smart_list_inner_type(ty) {
        return quote! {
            match record.get(#field_name) {
                Some(::teaql_core::Value::List(values)) => ::teaql_core::SmartList::from(
                    values
                        .iter()
                        .map(|value| match value {
                            ::teaql_core::Value::Object(record) => {
                                <#inner as ::teaql_core::Entity>::from_record(record.clone())
                            }
                            other => Err(::teaql_core::EntityError::new(
                                #entity_name,
                                format!("invalid relation list item {}: {:?}", #field_name, other),
                            )),
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                Some(::teaql_core::Value::Null) | None => ::teaql_core::SmartList::default(),
                other => {
                    return Err(::teaql_core::EntityError::new(
                        #entity_name,
                        format!("invalid relation field {}: {:?}", #field_name, other),
                    ))
                }
            }
        };
    }

    quote! { ::core::default::Default::default() }
}

pub fn into_relation_value_tokens(
    ty: &Type,
    value_expr: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    if option_inner_type(ty).is_some() {
        return quote! {
            match #value_expr {
                Some(entity) => ::teaql_core::Value::object(entity.into_record()),
                None => ::teaql_core::Value::Null,
            }
        };
    }

    if vec_inner_type(ty).is_some() {
        return quote! {
            ::teaql_core::Value::List(
                (#value_expr)
                    .into_iter()
                    .map(|entity| ::teaql_core::Value::object(entity.into_record()))
                    .collect(),
            )
        };
    }

    if smart_list_inner_type(ty).is_some() {
        return quote! {
            ::teaql_core::Value::List(
                (#value_expr)
                    .data
                    .into_iter()
                    .map(|entity| ::teaql_core::Value::object(entity.into_record()))
                    .collect(),
            )
        };
    }

    quote! { ::teaql_core::Value::Null }
}

pub fn identifiable_value_tokens(
    ty: &Type,
    value_expr: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let type_name = base_type_name(ty);
    if is_option(ty) {
        return match type_name.as_deref() {
            Some("u64") | Some("u32") | Some("u16") => {
                quote! {
                    match #value_expr {
                        Some(value) => ::teaql_core::Value::U64((*value).into()),
                        None => ::teaql_core::Value::Null,
                    }
                }
            }
            Some("i64") | Some("i32") | Some("i16") => {
                quote! {
                    match #value_expr {
                        Some(value) => ::teaql_core::Value::I64((*value).into()),
                        None => ::teaql_core::Value::Null,
                    }
                }
            }
            Some("String") => {
                quote! {
                    match #value_expr {
                        Some(value) => ::teaql_core::Value::Text(value.clone()),
                        None => ::teaql_core::Value::Null,
                    }
                }
            }
            _ => quote! {
                match #value_expr {
                    Some(value) => value.clone().into(),
                    None => ::teaql_core::Value::Null,
                }
            },
        };
    }

    match type_name.as_deref() {
        Some("u64") | Some("u32") | Some("u16") => {
            quote! { ::teaql_core::Value::U64((*#value_expr).into()) }
        }
        Some("i64") | Some("i32") | Some("i16") => {
            quote! { ::teaql_core::Value::I64((*#value_expr).into()) }
        }
        Some("String") => quote! { ::teaql_core::Value::Text((#value_expr).clone()) },
        _ => quote! { (#value_expr).clone().into() },
    }
}
