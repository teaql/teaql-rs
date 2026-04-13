use quote::quote;
use syn::Type;

pub fn is_option(ty: &Type) -> bool {
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

pub fn rust_type_to_data_type(ty: &Type) -> proc_macro2::TokenStream {
    let type_name = base_type_name(ty);
    match type_name.as_deref() {
        Some("bool") => quote! { ::teaql_core::DataType::Bool },
        Some("i64") | Some("i32") | Some("i16") => quote! { ::teaql_core::DataType::I64 },
        Some("u64") | Some("u32") | Some("u16") => quote! { ::teaql_core::DataType::U64 },
        Some("f64") | Some("f32") => quote! { ::teaql_core::DataType::F64 },
        Some("String") | Some("str") => quote! { ::teaql_core::DataType::Text },
        Some("NaiveDate") => quote! { ::teaql_core::DataType::Date },
        Some("DateTime") => quote! { ::teaql_core::DataType::Timestamp },
        _ => quote! { ::teaql_core::DataType::Json },
    }
}

pub fn base_type_name(ty: &Type) -> Option<String> {
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

pub fn option_inner_type(ty: &Type) -> Option<&Type> {
    match ty {
        Type::Path(path) => {
            let last = path.path.segments.last()?;
            if last.ident != "Option" {
                return None;
            }
            if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                    return Some(inner);
                }
            }
            None
        }
        _ => None,
    }
}

pub fn vec_inner_type(ty: &Type) -> Option<&Type> {
    generic_inner_type(ty, "Vec")
}

pub fn smart_list_inner_type(ty: &Type) -> Option<&Type> {
    generic_inner_type(ty, "SmartList")
}

fn generic_inner_type<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    match ty {
        Type::Path(path) => {
            let last = path.path.segments.last()?;
            if last.ident != name {
                return None;
            }
            if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                    return Some(inner);
                }
            }
            None
        }
        _ => None,
    }
}
