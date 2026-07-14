use quote::quote;
use syn::{Data, DeriveInput, Fields};

use crate::attr::{parse_container_attrs, parse_field_attrs};
use crate::mapping::{
    from_record_value_tokens, from_relation_value_tokens, identifiable_value_tokens,
    into_record_value_tokens, into_relation_value_tokens,
};
use crate::types::{is_option, rust_type_to_data_type};

pub fn expand_teaql_entity(input: DeriveInput) -> proc_macro2::TokenStream {
    let struct_name = input.ident.clone();
    let attrs = parse_container_attrs(&input.attrs, &struct_name.to_string());
    let entity_name = attrs.entity_name;
    let table_name = attrs.table_name;
    let data_service = attrs.data_service;

    let data_service_token = data_service
        .map(|ds| {
            quote! {
                descriptor = descriptor.data_service(#ds);
            }
        })
        .unwrap_or_default();

    let audit_mask_fields = attrs.audit_mask_fields;
    let audit_mask_fields_token = (!audit_mask_fields.is_empty())
        .then(|| {
            let fields = audit_mask_fields.iter().map(|f| quote! { #f.to_owned() });
            quote! {
                descriptor = descriptor.audit_mask_fields(vec![#(#fields),*]);
            }
        })
        .unwrap_or_default();

    let audit_value_max_len = attrs.audit_value_max_len;
    let audit_value_max_len_token = audit_value_max_len
        .map(|len| {
            quote! {
                descriptor = descriptor.audit_value_max_len(Some(#len));
            }
        })
        .unwrap_or_default();

    let fields = match input.data {
        Data::Struct(data) => data.fields,
        _ => {
            return syn::Error::new(input.ident.span(), "TeaqlEntity only supports structs")
                .to_compile_error();
        }
    };

    let named_fields: Vec<_> = match fields {
        Fields::Named(fields) => fields.named.into_iter().collect(),
        _ => {
            return syn::Error::new(
                struct_name.span(),
                "TeaqlEntity only supports structs with named fields",
            )
            .to_compile_error();
        }
    };

    let has_load_state_field = named_fields.iter().any(|field| {
        field
            .ident
            .as_ref()
            .map(|ident| ident == "__load_state")
            .unwrap_or(false)
    });

    let mut property_tokens = Vec::new();
    let mut relation_tokens = Vec::new();
    let mut from_record_fields = Vec::new();
    let mut into_record_fields = Vec::new();
    let mut id_impl = None;
    let mut version_impl = None;
    let mut has_root_field = false;
    let mut id_field_ident: Option<syn::Ident> = None;
    let explicit_field_names: Vec<String> = named_fields
        .iter()
        .filter_map(|field| {
            let field_ident = field.ident.as_ref()?;
            let parsed = parse_field_attrs(&field.attrs);
            (!parsed.dynamic && !parsed.skip).then(|| field_ident.to_string())
        })
        .collect();

    for field in named_fields {
        let field_ident = field.ident.expect("named field");
        let field_name = field_ident.to_string();
        let parsed = parse_field_attrs(&field.attrs);

        if parsed.skip {
            if field_name == "root" {
                has_root_field = true;
            }
            from_record_fields.push(quote! {
                #field_ident: Default::default()
            });
            continue;
        }

        if parsed.dynamic {
            let known_field_names = explicit_field_names.clone();
            from_record_fields.push(quote! {
                #field_ident: {
                    let known_fields = [#(#known_field_names),*];
                    record
                        .iter()
                        .filter(|(key, _)| !known_fields.contains(&key.as_str()))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                }
            });
            into_record_fields.push(quote! {
                for (key, value) in self.#field_ident {
                    record.insert(key, value);
                }
            });
            continue;
        }

        if let Some(relation) = parsed.relation {
            let local_key = relation.local_key.unwrap_or_else(|| "id".to_owned());
            let foreign_key = relation.foreign_key.unwrap_or_else(|| "id".to_owned());
            let target = relation.target;
            let many = relation.many;
            let attach = relation.attach;
            let delete_missing = relation.delete_missing;
            relation_tokens.push(quote! {
                descriptor = descriptor.relation(
                    ::teaql_core::RelationDescriptor::new(#field_name, #target)
                        .local_key(#local_key)
                        .foreign_key(#foreign_key)
                        #many
                        #attach
                        #delete_missing
                );
            });
            let from_relation = from_relation_value_tokens(
                &field.ty,
                &field_name,
                &entity_name,
                &local_key,
                &foreign_key,
            );
            let into_relation = into_relation_value_tokens(&field.ty, quote! { self.#field_ident });
            from_record_fields.push(quote! {
                #field_ident: #from_relation
            });
            into_record_fields.push(quote! {
                if let Some(val) = #into_relation {
                    record.insert(#field_name.to_owned(), val);
                }
            });
            continue;
        }

        let data_type = rust_type_to_data_type(&field.ty);
        let column_name = parsed.column.unwrap_or_else(|| field_name.clone());
        let nullable = is_option(&field.ty);
        let id = parsed.id;
        let version = parsed.version;

        let nullable_tokens = (!nullable)
            .then(|| quote! { .not_null() })
            .unwrap_or_default();
        let id_tokens = id
            .then(|| {
                id_field_ident = Some(field_ident.clone());
                id_impl = Some(identifiable_value_tokens(
                    &field.ty,
                    quote! { &self.#field_ident },
                ));
                quote! { .id() }
            })
            .unwrap_or_default();
        let version_tokens = version
            .then(|| {
                version_impl = Some(quote! { self.#field_ident });
                quote! { .version() }
            })
            .unwrap_or_default();

        if parsed.boxed_relations {
            let boxed_type = &field.ty;
            relation_tokens.push(quote! {
                <#boxed_type as ::teaql_core::TeaqlBoxedRelations>::extend_descriptor(&mut descriptor);
            });
            from_record_fields.push(quote! {
                #field_ident: <#boxed_type as ::teaql_core::TeaqlBoxedRelations>::extract_from_record(&record)?
            });
            into_record_fields.push(quote! {
                ::teaql_core::TeaqlBoxedRelations::inject_into_record(self.#field_ident, &mut record);
            });
            continue;
        }

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

    let ledger_entity_impl_tokens = has_root_field
        .then(|| {
            quote! {
                impl ::teaql_runtime::LedgerEntity for #struct_name {
                    fn entity_root(&self) -> Option<::teaql_runtime::EntityRoot> {
                        Some(self.root.clone())
                    }
                }
            }
        })
        .unwrap_or_default();

    // Generate dirty_fields() if entity has a 'root' field (EntityRoot) and an id field.
    // This is the Rust equivalent of Java's entity.getUpdatedProperties().
    let (dirty_fields_impl, is_marked_as_delete_impl) = match (has_root_field, &id_field_ident) {
        (true, Some(id_ident)) => (
            quote! {
                fn dirty_fields(&self) -> Option<std::collections::BTreeSet<String>> {
                    let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                    let fields = self.root.changed_field_names(&key);
                    (!fields.is_empty()).then_some(fields)
                }
            },
            quote! {
                fn is_marked_as_delete(&self) -> bool {
                    let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                    self.root.is_marked_as_delete(&key)
                }

                fn is_new(&self) -> bool {
                    let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                    self.root.is_new(&key)
                }

                fn mark_as_new(&mut self) {
                    let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                    self.root.mark_as_new(key)
                }
            },
        ),
        _ => (quote! {}, quote! {}),
    };

    let set_original_record_impl = has_root_field
        .then(|| quote! { entity.root.set_original_record(record); })
        .unwrap_or_default();

    let root_methods_impl = has_root_field
        .then(|| {
            quote! {
                fn get_comment(&self) -> Option<String> {
                    self.root.get_comment()
                }

                fn set_comment(&mut self, comment: String) {
                    self.root.set_comment(comment);
                }

                fn original_values(&self) -> Option<::teaql_core::Record> {
                    self.root.original_record()
                }
            }
        })
        .unwrap_or_default();

    let set_load_state_impl = has_load_state_field.then(|| quote! {
        entity.__load_state = ::teaql_core::eval::LoadState::Partial(record.keys().cloned().collect());
    }).unwrap_or_default();

    quote! {
        impl ::teaql_core::TeaqlEntity for #struct_name {
            fn entity_descriptor() -> ::teaql_core::EntityDescriptor {
                let mut descriptor = ::teaql_core::EntityDescriptor::new(#entity_name)
                    .table_name(#table_name);

                #data_service_token
                #audit_mask_fields_token
                #audit_value_max_len_token

                #(#property_tokens)*
                #(#relation_tokens)*
                descriptor
            }
        }

        impl ::teaql_core::Entity for #struct_name {
            fn from_record(record: ::teaql_core::Record) -> Result<Self, ::teaql_core::EntityError> {
                let mut entity = Self {
                    #(#from_record_fields),*
                };
                #set_load_state_impl
                #set_original_record_impl
                Ok(entity)
            }

            fn into_record(self) -> ::teaql_core::Record {
                use ::teaql_core::Entity;
                let mut record = ::teaql_core::Record::new();
                if let Some(comment) = self.get_comment() {
                    record.insert("_comment".to_owned(), ::teaql_core::Value::Text(comment));
                }
                if let Some(dirty_fields) = self.dirty_fields() {
                    let fields: Vec<::teaql_core::Value> = dirty_fields.into_iter().map(::teaql_core::Value::Text).collect();
                    record.insert("_dirty_fields".to_owned(), ::teaql_core::Value::List(fields));
                }
                if let Some(original_values) = self.original_values() {
                    record.insert("_original_values".to_owned(), ::teaql_core::Value::Object(original_values));
                }
                #(#into_record_fields)*
                record
            }

            fn on_loaded(&mut self, _context: &dyn std::any::Any) {
            }

            #dirty_fields_impl
            #is_marked_as_delete_impl
            #root_methods_impl
        }

        #identifiable_impl_tokens
        #versioned_impl_tokens
        #ledger_entity_impl_tokens
    }
}

pub fn expand_teaql_reverse_relations(input: DeriveInput) -> proc_macro2::TokenStream {
    let struct_name = input.ident;
    let fields = match input.data {
        Data::Struct(data) => data.fields,
        _ => {
            return syn::Error::new(
                struct_name.span(),
                "TeaqlReverseRelations only supports structs",
            )
            .to_compile_error();
        }
    };

    let named_fields: Vec<_> = match fields {
        Fields::Named(fields) => fields.named.into_iter().collect(),
        _ => {
            return syn::Error::new(
                struct_name.span(),
                "TeaqlReverseRelations only supports structs with named fields",
            )
            .to_compile_error();
        }
    };

    let mut from_record_fields = Vec::new();
    let mut into_record_fields = Vec::new();
    let mut relation_tokens = Vec::new();
    let entity_name = struct_name.to_string();

    for field in named_fields {
        let field_ident = field.ident.expect("named field");
        let field_name = field_ident.to_string();

        let parsed = crate::attr::parse_field_attrs(&field.attrs);
        let mut rel_local_key = "id".to_owned();
        let mut rel_foreign_key = "id".to_owned();
        if let Some(relation) = parsed.relation {
            let local_key = relation.local_key.unwrap_or_else(|| "id".to_owned());
            let foreign_key = relation.foreign_key.unwrap_or_else(|| "id".to_owned());
            rel_local_key = local_key.clone();
            rel_foreign_key = foreign_key.clone();
            let target = relation.target;
            let many = relation.many;
            let attach = relation.attach;
            let delete_missing = relation.delete_missing;
            relation_tokens.push(quote! {
                *descriptor = descriptor.clone().relation(
                    ::teaql_core::RelationDescriptor::new(#field_name, #target)
                        .local_key(#local_key)
                        .foreign_key(#foreign_key)
                        #many
                        #attach
                        #delete_missing
                );
            });
        }

        let from_value = crate::mapping::from_relation_value_tokens(
            &field.ty,
            &field_name,
            &entity_name,
            &rel_local_key,
            &rel_foreign_key,
        );
        let into_value =
            crate::mapping::into_relation_value_tokens(&field.ty, quote! { self.#field_ident });

        from_record_fields.push(quote! {
            #field_ident: #from_value
        });
        into_record_fields.push(quote! {
            if let Some(val) = #into_value {
                record.insert(#field_name.to_owned(), val);
            }
        });
    }

    quote! {
        impl ::teaql_core::TeaqlBoxedRelations for #struct_name {
            fn extend_descriptor(descriptor: &mut ::teaql_core::EntityDescriptor) {
                #(#relation_tokens)*
            }

            fn extract_from_record(record: &::teaql_core::Record) -> Result<Self, ::teaql_core::EntityError> {
                Ok(Self {
                    #(#from_record_fields,)*
                })
            }

            fn inject_into_record(self, record: &mut ::teaql_core::Record) {
                #(#into_record_fields)*
            }
        }
    }
}
