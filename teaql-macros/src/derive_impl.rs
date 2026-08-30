use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, ItemStruct, parse_quote};

use crate::attr::{parse_container_attrs, parse_field_attrs};
use crate::mapping::{
    from_record_value_tokens_with_lookup, from_relation_value_tokens, identifiable_value_tokens,
    into_record_value_tokens, into_relation_value_tokens,
};
use crate::types::{is_option, rust_type_to_data_type};

pub fn expand_teaql_entity_attribute(mut input: ItemStruct) -> proc_macro2::TokenStream {
    let struct_name = input.ident.clone();
    let attrs = parse_container_attrs(&input.attrs, &struct_name.to_string());
    let entity_name = attrs.entity_name;
    let Fields::Named(fields) = &mut input.fields else {
        return syn::Error::new(
            struct_name.span(),
            "teaql_entity only supports structs with named fields",
        )
        .to_compile_error();
    };
    if fields.named.iter().any(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|ident| ident == "__teaql_runtime_state")
    }) {
        return syn::Error::new(
            struct_name.span(),
            "__teaql_runtime_state is reserved for TeaQL runtime state",
        )
        .to_compile_error();
    }
    let id_field = fields.named.iter().find_map(|field| {
        parse_field_attrs(&field.attrs)
            .id
            .then(|| field.ident.clone())
            .flatten()
    });
    let Some(id_field) = id_field else {
        return syn::Error::new(
            struct_name.span(),
            "teaql_entity requires one #[teaql(id)] field",
        )
        .to_compile_error();
    };
    fields.named.push(parse_quote! {
        #[teaql(skip)]
        #[doc(hidden)]
        __teaql_runtime_state: ::teaql_runtime::EntityRuntimeState
    });

    quote! {
        #input

        impl #struct_name {
            #[doc(hidden)]
            pub(crate) fn __teaql_runtime_state(&self) -> &::teaql_runtime::EntityRuntimeState {
                &self.__teaql_runtime_state
            }

            #[doc(hidden)]
            pub(crate) fn __teaql_replace_runtime_state(
                &mut self,
                state: ::teaql_runtime::EntityRuntimeState,
            ) {
                self.__teaql_runtime_state = state;
            }

            pub fn entity_key(&self) -> ::teaql_runtime::EntityKey {
                ::teaql_runtime::EntityKey::new(#entity_name, self.#id_field)
            }

            pub fn mark_for_deletion(&mut self) -> &mut Self {
                self.__teaql_runtime_state.mark_as_delete(self.entity_key());
                self
            }

            pub fn set_comment(&mut self, comment: impl Into<String>) -> &mut Self {
                self.__teaql_runtime_state.set_comment(comment);
                self
            }
        }
    }
}

pub fn expand_teaql_entity(input: DeriveInput) -> proc_macro2::TokenStream {
    let struct_name = input.ident.clone();
    let attrs = parse_container_attrs(&input.attrs, &struct_name.to_string());
    let entity_name = attrs.entity_name;
    let table_name = attrs.table_name;
    let data_service = attrs.data_service;
    let container_relation_tokens = attrs.reverse_relations.into_iter().map(|relation| {
        let name = relation.name;
        let target = relation.target;
        let local_key = relation.local_key.unwrap_or_else(|| "id".to_owned());
        let foreign_key = relation.foreign_key.unwrap_or_else(|| "id".to_owned());
        let many = relation.many.then(|| quote! { .many() });
        quote! {
            descriptor = descriptor.relation(
                ::teaql_core::RelationDescriptor::new(#name, #target)
                    .local_key(#local_key)
                    .foreign_key(#foreign_key)
                    #many
            );
        }
    });

    let data_service_token = data_service
        .map(|ds| {
            quote! {
                descriptor = descriptor.data_service(#ds);
            }
        })
        .unwrap_or_default();

    let audit_mask_fields = attrs.audit_mask_fields;
    let audit_mask_fields_token = if !audit_mask_fields.is_empty() {
        {
            let fields = audit_mask_fields.iter().map(|f| quote! { #f.to_owned() });
            quote! {
                descriptor = descriptor.audit_mask_fields(vec![#(#fields),*]);
            }
        }
    } else {
        Default::default()
    };

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
    let mut record_value_slots = Vec::new();
    let mut record_value_match_arms = Vec::new();
    let mut into_record_fields = Vec::new();
    let mut id_impl = None;
    let mut version_impl = None;
    let mut runtime_state_field_ident: Option<syn::Ident> = None;
    let mut id_field_ident: Option<syn::Ident> = None;
    let mut unknown_record_field_arm = quote! { _ => {} };

    for field in named_fields {
        let field_ident = field.ident.expect("named field");
        let field_name = field_ident.to_string();
        let parsed = parse_field_attrs(&field.attrs);

        if parsed.skip {
            if field_name == "__teaql_runtime_state" || field_name == "root" {
                runtime_state_field_ident = Some(field_ident.clone());
                from_record_fields.push(quote! {
                    #field_ident: load_context
                        .and_then(|context| context.downcast_ref::<::teaql_runtime::EntityRuntimeState>())
                        .map(::teaql_runtime::EntityRuntimeState::fresh_with_shared_graph)
                        .unwrap_or_default()
                });
                continue;
            }
            from_record_fields.push(quote! {
                #field_ident: Default::default()
            });
            continue;
        }

        if parsed.dynamic {
            record_value_slots.push(quote! {
                let mut __teaql_dynamic_values = ::std::collections::BTreeMap::new();
            });
            unknown_record_field_arm = quote! {
                _ => {
                    __teaql_dynamic_values.insert(key.clone(), value.clone());
                }
            };
            from_record_fields.push(quote! {
                #field_ident: __teaql_dynamic_values
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
            let from_relation = from_relation_value_tokens(&field.ty, &field_name, &entity_name);
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

        let mut data_type = rust_type_to_data_type(&field.ty);
        if parsed.large_text {
            data_type = quote! { ::teaql_core::DataType::LargeText };
        }
        let column_name = parsed.column.unwrap_or_else(|| field_name.clone());
        let nullable = is_option(&field.ty);
        let id = parsed.id;
        let version = parsed.version;

        let nullable_tokens = if !nullable {
            quote! { .not_null() }
        } else {
            Default::default()
        };
        let id_tokens = if id {
            {
                id_field_ident = Some(field_ident.clone());
                id_impl = Some(identifiable_value_tokens(
                    &field.ty,
                    quote! { &self.#field_ident },
                ));
                quote! { .id() }
            }
        } else {
            Default::default()
        };
        let version_tokens = if version {
            {
                version_impl = Some(quote! { self.#field_ident });
                quote! { .version() }
            }
        } else {
            Default::default()
        };

        if parsed.boxed_relations {
            let boxed_type = &field.ty;
            relation_tokens.push(quote! {
                <#boxed_type as ::teaql_core::TeaqlBoxedRelations>::extend_descriptor(&mut descriptor);
            });
            from_record_fields.push(quote! {
                #field_ident: <#boxed_type as ::teaql_core::TeaqlBoxedRelations>::extract_from_values(&record)?
            });
            into_record_fields.push(quote! {
                ::teaql_core::TeaqlBoxedRelations::inject_into_values(self.#field_ident, &mut record);
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

        let value_slot = format_ident!("__teaql_value_{}", field_ident);
        record_value_slots.push(quote! {
            let mut #value_slot: Option<&::teaql_core::Value> = None;
        });
        record_value_match_arms.push(quote! {
            #field_name => #value_slot = Some(value),
        });
        let from_value = from_record_value_tokens_with_lookup(
            &field.ty,
            quote! { #value_slot },
            &field_name,
            &entity_name,
        );
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

    let ledger_entity_impl_tokens = if let Some(state_ident) = &runtime_state_field_ident {
        {
            quote! {
                impl ::teaql_runtime::LedgerEntity for #struct_name {
                    fn entity_runtime_state(&self) -> Option<::teaql_runtime::EntityRuntimeState> {
                        Some(self.#state_ident.clone())
                    }
                }
            }
        }
    } else {
        Default::default()
    };

    // Generate dirty_fields() when the attribute macro injected EntityRuntimeState.
    // This is the Rust equivalent of Java's entity.getUpdatedProperties().
    let (dirty_fields_impl, is_marked_as_delete_impl) =
        match (&runtime_state_field_ident, &id_field_ident) {
            (Some(state_ident), Some(id_ident)) => (
                quote! {
                    fn dirty_fields(&self) -> Option<std::collections::BTreeSet<String>> {
                        let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                        let fields = self.#state_ident.changed_field_names(&key);
                        (!fields.is_empty()).then_some(fields)
                    }
                },
                quote! {
                    fn is_marked_as_delete(&self) -> bool {
                        let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                        self.#state_ident.is_marked_as_delete(&key)
                    }

                    fn is_new(&self) -> bool {
                        let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                        self.#state_ident.is_new(&key)
                    }

                    fn mark_as_new(&mut self) {
                        let key = teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                        self.#state_ident.mark_as_new(key)
                    }
                },
            ),
            _ => (quote! {}, quote! {}),
        };

    let set_original_compact_impl = if let Some(state_ident) = &runtime_state_field_ident {
        quote! {
            entity.#state_ident.set_original_compact_row(record);
        }
    } else {
        Default::default()
    };

    let root_methods_impl = if let Some(state_ident) = &runtime_state_field_ident {
        {
            quote! {
                fn get_comment(&self) -> Option<String> {
                    self.#state_ident.get_comment()
                }

                fn set_comment(&mut self, comment: String) {
                    self.#state_ident.set_comment(comment);
                }

                fn original_values(&self) -> Option<::teaql_core::EntitySnapshot> {
                    self.#state_ident.original_snapshot()
                }
            }
        }
    } else {
        Default::default()
    };

    let on_loaded_impl = if let Some(state_ident) = &runtime_state_field_ident {
        quote! {
            if let Some(root) = context.downcast_ref::<::teaql_runtime::EntityRuntimeState>() {
                self.#state_ident = self.#state_ident.with_shared_graph(root);
            }
        }
    } else {
        Default::default()
    };

    let set_load_state_impl = if has_load_state_field {
        quote! {
            entity.__load_state =
                ::teaql_core::eval::LoadState::SharedColumns(record.shared_columns());
        }
    } else {
        Default::default()
    };

    let from_compact_body = quote! {
            #(#record_value_slots)*
            for (key, value) in record.iter() {
                match key.as_str() {
                    #(#record_value_match_arms)*
                    #unknown_record_field_arm
                }
            }
            let mut entity = Self {
                #(#from_record_fields),*
            };
            #set_load_state_impl
            #set_original_compact_impl
            Ok(entity)
    };

    let from_compact_with_context_impl = runtime_state_field_ident.as_ref().map(|_| {
        quote! {
            fn from_compact_row_with_context(
                record: ::teaql_core::CompactRow,
                load_context: &dyn std::any::Any,
            ) -> Result<Self, ::teaql_core::EntityError> {
                let load_context = Some(load_context);
                #from_compact_body
            }
        }
    });

    let from_compact_impl = quote! {
        fn from_compact_row(record: ::teaql_core::CompactRow) -> Result<Self, ::teaql_core::EntityError> {
            let load_context: Option<&dyn std::any::Any> = None;
            #from_compact_body
        }

        #from_compact_with_context_impl
    };

    quote! {
        impl ::teaql_core::TeaqlEntity for #struct_name {
            const ENTITY_NAME: &'static str = #entity_name;

            fn entity_descriptor() -> ::teaql_core::EntityDescriptor {
                let mut descriptor = ::teaql_core::EntityDescriptor::new(#entity_name)
                    .table_name(#table_name);

                #data_service_token
                #audit_mask_fields_token
                #audit_value_max_len_token

                #(#property_tokens)*
                #(#relation_tokens)*
                #(#container_relation_tokens)*
                descriptor
            }
        }

        impl ::teaql_core::Entity for #struct_name {
            #from_compact_impl

            fn into_values(self) -> ::teaql_core::MutationValues {
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
                    record.insert("_original_values".to_owned(), ::teaql_core::Value::Object(original_values.into()));
                }
                if self.is_new() {
                    record.insert("_is_new".to_owned(), ::teaql_core::Value::Bool(true));
                }
                if self.is_marked_as_delete() {
                    record.insert("_is_deleted".to_owned(), ::teaql_core::Value::Bool(true));
                }
                #(#into_record_fields)*
                record.into()
            }

            fn on_loaded(&mut self, context: &dyn std::any::Any) {
                #on_loaded_impl
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
        if let Some(relation) = parsed.relation {
            let local_key = relation.local_key.unwrap_or_else(|| "id".to_owned());
            let foreign_key = relation.foreign_key.unwrap_or_else(|| "id".to_owned());
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

        let from_value =
            crate::mapping::from_relation_value_tokens(&field.ty, &field_name, &entity_name);
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

            fn extract_from_values(record: &::teaql_core::CompactRow) -> Result<Self, ::teaql_core::EntityError> {
                Ok(Self {
                    #(#from_record_fields,)*
                })
            }

            fn inject_into_values(self, record: &mut ::std::collections::BTreeMap<String, ::teaql_core::Value>) {
                #(#into_record_fields)*
            }
        }
    }
}
