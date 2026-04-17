use quote::quote;
use syn::{Data, DeriveInput, Fields};

use crate::attr::{parse_container_attrs, parse_field_attrs};
use crate::mapping::{
    from_record_value_tokens, from_relation_value_tokens, identifiable_value_tokens,
    into_record_value_tokens, into_relation_value_tokens,
};
use crate::types::{is_option, rust_type_to_data_type};

pub fn expand_teaql_entity(input: DeriveInput) -> proc_macro2::TokenStream {
    let struct_name = input.ident;
    let (entity_name, table_name) = parse_container_attrs(&input.attrs, &struct_name.to_string());

    let fields = match input.data {
        Data::Struct(data) => data.fields,
        _ => panic!("TeaqlEntity only supports structs"),
    };

    let named_fields: Vec<_> = match fields {
        Fields::Named(fields) => fields.named.into_iter().collect(),
        _ => panic!("TeaqlEntity only supports structs with named fields"),
    };

    let mut property_tokens = Vec::new();
    let mut relation_tokens = Vec::new();
    let mut from_record_fields = Vec::new();
    let mut into_record_fields = Vec::new();
    let mut id_impl = None;
    let mut version_impl = None;
    let explicit_field_names: Vec<String> = named_fields
        .iter()
        .filter_map(|field| {
            let field_ident = field.ident.as_ref()?;
            let parsed = parse_field_attrs(&field.attrs);
            if parsed.dynamic {
                None
            } else {
                Some(field_ident.to_string())
            }
        })
        .collect();

    for field in named_fields {
        let field_ident = field.ident.expect("named field");
        let field_name = field_ident.to_string();
        let parsed = parse_field_attrs(&field.attrs);

        if parsed.dynamic {
            let known_field_names = explicit_field_names.clone();
            from_record_fields.push(quote! {
                #field_ident: {
                    let known_fields = [#(#known_field_names),*];
                    record
                        .iter()
                        .filter(|(key, _)| !known_fields.contains(&key.as_str()))
                        .map(|(key, value)| (key.clone(), value.to_json_value()))
                        .collect()
                }
            });
            into_record_fields.push(quote! {
                for (key, value) in self.#field_ident {
                    record.insert(key, ::teaql_core::Value::Json(value));
                }
            });
            continue;
        }

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
            let from_relation = from_relation_value_tokens(&field.ty, &field_name, &entity_name);
            let into_relation = into_relation_value_tokens(&field.ty, quote! { self.#field_ident });
            from_record_fields.push(quote! {
                #field_ident: #from_relation
            });
            into_record_fields.push(quote! {
                record.insert(#field_name.to_owned(), #into_relation);
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
            id_impl = Some(identifiable_value_tokens(
                &field.ty,
                quote! { &self.#field_ident },
            ));
            quote! { .id() }
        } else {
            quote! {}
        };
        let version_tokens = if version {
            version_impl = Some(quote! { self.#field_ident });
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

        let from_value = from_record_value_tokens(&field.ty, &field_name, &entity_name);
        let into_value = into_record_value_tokens(&field.ty, quote! { self.#field_ident });
        from_record_fields.push(quote! {
            #field_ident: #from_value
        });
        into_record_fields.push(quote! {
            record.insert(#field_name.to_owned(), #into_value);
        });
    }

    let identifiable_impl_tokens = id_impl.map(|id_value| {
        quote! {
            impl ::teaql_core::IdentifiableEntity for #struct_name {
                fn id_value(&self) -> ::teaql_core::Value {
                    #id_value
                }
            }
        }
    });

    let versioned_impl_tokens = version_impl.map(|version| {
        quote! {
            impl ::teaql_core::VersionedEntity for #struct_name {
                fn version(&self) -> i64 {
                    #version
                }
            }
        }
    });

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

        impl ::teaql_core::Entity for #struct_name {
            fn from_record(record: ::teaql_core::Record) -> Result<Self, ::teaql_core::EntityError> {
                Ok(Self {
                    #(#from_record_fields),*
                })
            }

            fn into_record(self) -> ::teaql_core::Record {
                let mut record = ::teaql_core::Record::new();
                #(#into_record_fields)*
                record
            }
        }

        #identifiable_impl_tokens
        #versioned_impl_tokens
    }
}
