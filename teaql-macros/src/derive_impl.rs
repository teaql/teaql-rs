use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, ItemStruct, parse_quote};

use crate::attr::{parse_container_attrs, parse_field_attrs};
use crate::mapping::{
    from_record_value_tokens_with_lookup, from_relation_value_tokens, identifiable_value_tokens,
    into_record_value_tokens, into_relation_json_value_tokens, into_relation_value_tokens,
};
use crate::types::{box_inner_type, is_option, option_inner_type, rust_type_to_data_type};

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
                self.__teaql_runtime_state = state.with_loaded_view_from(&self.__teaql_runtime_state);
            }

            pub fn entity_key(&self) -> ::teaql_runtime::EntityKey {
                ::teaql_runtime::EntityKey::new(#entity_name, self.#id_field)
            }

            pub fn mark_for_deletion(&mut self) -> &mut Self {
                self.__teaql_runtime_state.mark_as_delete(self.entity_key());
                self
            }

            pub fn set_comment(&mut self, comment: impl Into<String>) -> &mut Self {
                self.__teaql_runtime_state.set_entity_comment(self.entity_key(), comment);
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
    let indexed_layout = attrs.indexed_layout;
    let mut materialized_relation_names: Vec<String> = attrs
        .reverse_relations
        .iter()
        .map(|relation| relation.name.clone())
        .collect();
    let container_relation_tokens = attrs.reverse_relations.iter().map(|relation| {
        let name = &relation.name;
        let target = &relation.target;
        let local_key = relation
            .local_key
            .clone()
            .unwrap_or_else(|| "id".to_owned());
        let foreign_key = relation
            .foreign_key
            .clone()
            .unwrap_or_else(|| "id".to_owned());
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
    let audit_mask_fields_token = if attrs.audit_mask_fields_declared {
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
    let mut into_json_fields = Vec::new();
    let mut property_member_names = Vec::new();
    let forward_relation_names: std::collections::HashSet<String> = named_fields
        .iter()
        .filter(|field| parse_field_attrs(&field.attrs).relation.is_some())
        .filter_map(|field| field.ident.as_ref().map(ToString::to_string))
        .collect();
    let mut alias_state_tokens = Vec::new();
    let mut relation_tokens = Vec::new();
    let mut from_record_fields = Vec::new();
    let mut record_value_slots = Vec::new();
    let mut record_value_match_arms = Vec::new();
    let mut record_member_fallback_arms = Vec::new();
    let mut into_record_fields = Vec::new();
    let mut id_impl = None;
    let mut version_impl = None;
    let mut runtime_state_field_ident: Option<syn::Ident> = None;
    let mut id_field_ident: Option<syn::Ident> = None;
    let mut unknown_record_field_arm = quote! { _ => {} };
    let mut has_dynamic_properties = false;
    let mut dynamic_property_impl = quote! {};

    for field in named_fields.iter().cloned() {
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
            has_dynamic_properties = true;
            dynamic_property_impl = quote! {
                fn dynamic_property(&self, key: &str) -> Option<&::teaql_core::Value> {
                    if !key.starts_with('_') { return None; }
                    self.#field_ident.get(key).filter(|value| match value {
                        ::teaql_core::Value::Null | ::teaql_core::Value::TypedNull(_) => false,
                        ::teaql_core::Value::Json(value) => !value.is_null(),
                        _ => true,
                    })
                }

                fn has_dynamic_property(&self, key: &str) -> bool {
                    key.starts_with('_') && self.#field_ident.contains_key(key)
                }
            };
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
            into_json_fields.push(into_record_fields.last().unwrap().clone());
            continue;
        }

        if let Some(relation) = parsed.relation {
            // Relation payloads are not readonly dynamic properties. In a flat
            // view this may be only the selected-edge marker.
            if !named_fields.iter().any(|candidate| {
                let attrs = parse_field_attrs(&candidate.attrs);
                attrs.relation.is_none() && attrs.column.as_deref() == Some(&field_name)
            }) {
                record_value_match_arms.push(quote! { #field_name => {}, });
            }
            materialized_relation_names.push(field_name.clone());
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
            let loaded = if has_load_state_field {
                quote! { self.__load_state.is_loaded(#field_name) }
            } else {
                quote! { false }
            };
            let json_relation =
                into_relation_json_value_tokens(&field.ty, quote! { self.#field_ident }, loaded);
            from_record_fields.push(quote! {
                #field_ident: #from_relation
            });
            into_record_fields.push(quote! {
                if let Some(val) = #into_relation {
                    record.insert(#field_name.to_owned(), val);
                }
            });
            into_json_fields.push(quote! {
                if let Some(val) = #json_relation { record.insert(#field_name.to_owned(), val); }
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
        let max_length = parsed
            .max_length
            .map(|value| quote! { .max_length(#value) });
        let numeric_precision = parsed
            .numeric_precision
            .map(|value| quote! { .numeric_precision(#value) });
        let numeric_scale = parsed
            .numeric_scale
            .map(|value| quote! { .numeric_scale(#value) });

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
            let loaded = if has_load_state_field {
                quote! { Some(&self.__load_state) }
            } else {
                quote! { None }
            };
            into_json_fields.push(quote! {
                ::teaql_core::TeaqlBoxedRelations::inject_into_json_values(self.#field_ident, &mut record, #loaded);
            });
            continue;
        }

        property_member_names.push(field_name.clone());
        property_tokens.push(quote! {
            descriptor = descriptor.property(
                ::teaql_core::PropertyDescriptor::new(#field_name, #data_type)
                    .column_name(#column_name)
                    #nullable_tokens
                    #id_tokens
                    #version_tokens
                    #max_length
                    #numeric_precision
                    #numeric_scale
            );
        });

        let value_slot = format_ident!("__teaql_value_{}", field_ident);
        record_member_fallback_arms.push(quote! {
            #field_name => { #value_slot = Some(value); true },
        });
        record_value_slots.push(quote! {
            let mut #value_slot: Option<&::teaql_core::Value> = None;
        });
        record_value_match_arms.push(quote! {
            #field_name => #value_slot = Some(value),
        });
        if indexed_layout && column_name != field_name {
            if forward_relation_names.contains(&column_name) {
                let alias_kind = format_ident!("__teaql_alias_kind_{}", field_ident);
                record_value_slots.push(quote! { let mut #alias_kind: Option<bool> = None; });
                record_value_match_arms.push(quote! {
                    #column_name => {
                        if let ::teaql_core::Value::Object(object) = value {
                            #value_slot = object.get("id");
                            if #value_slot.is_none() {
                                return Err(::teaql_core::EntityError::new(#entity_name, concat!("missing reference id: ", #column_name)));
                            }
                            #alias_kind = Some(true);
                        } else {
                            #value_slot = Some(value);
                            #alias_kind = Some(false);
                        }
                    },
                });
                alias_state_tokens.push(quote! {
                    if let Some(details) = #alias_kind {
                        if !details && !record.is_loaded_relation(#column_name) { entity.__load_state.mark_unloaded(#column_name).expect("known relation alias"); }
                        entity.__load_state.mark_loaded(#field_name).expect("known native FK field");
                    }
                });
            } else {
                record_value_match_arms.push(quote! { #column_name => #value_slot = Some(value), });
            }
        }
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
        into_record_fields.push(if indexed_layout && has_load_state_field {
            quote! {
                if self.__load_state.is_loaded(#field_name) {
                    record.insert(#field_name.to_owned(), #into_value);
                }
            }
        } else {
            quote! { record.insert(#field_name.to_owned(), #into_value); }
        });
        into_json_fields.push(into_record_fields.last().unwrap().clone());
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

    // Only runtime-owned indexed carriers can borrow the immutable identity graph.
    // No Clone bound on entities and no per-row serialization sidecar are needed.
    let borrowed_json_impl = if indexed_layout
        && has_load_state_field
        && let Some(state_ident) = &runtime_state_field_ident
    {
        let mut values = Vec::new();
        let mut relations = Vec::new();
        for field in &named_fields {
            let ident = field.ident.as_ref().expect("named field");
            let parsed = parse_field_attrs(&field.attrs);
            let name = ident.to_string();
            if parsed.skip {
                continue;
            }
            if parsed.boxed_relations {
                // Preserve the consuming SPI until this carrier supports borrowing.
                values.push(quote! { return None; });
                continue;
            }
            if parsed.dynamic {
                values.push(quote! { if expand { record.extend(self.#ident.iter().map(|(key, value)| (key.clone(), value.clone()))); } });
                continue;
            }
            if let Some(relation) = parsed.relation {
                let Some(inner) = option_inner_type(&field.ty) else {
                    values.push(quote! { return None; });
                    continue;
                };
                let target = box_inner_type(inner).unwrap_or(inner);
                let local_key = relation.local_key.unwrap_or_else(|| "id".to_owned());
                let local = named_fields.iter().find(|candidate| {
                    candidate
                        .ident
                        .as_ref()
                        .is_some_and(|key| key == local_key.as_str())
                        || parse_field_attrs(&candidate.attrs).column.as_deref() == Some(&local_key)
                });
                let null_value = if let Some(local) = local {
                    let local_ident = local.ident.as_ref().unwrap();
                    let value =
                        into_record_value_tokens(&local.ty, quote! { self.#local_ident.clone() });
                    quote! {{ let value: ::teaql_core::Value = #value; matches!(value, ::teaql_core::Value::Null) }}
                } else {
                    quote! { false }
                };
                let resolve = if let Some(local) = local {
                    let local_ident = local.ident.as_ref().unwrap();
                    let id =
                        into_record_value_tokens(&local.ty, quote! { self.#local_ident.clone() });
                    quote! {{
                        let id: ::teaql_core::Value = #id;
                        id.try_u64().and_then(|id| self.#state_ident.resolve_entity::<#target>(id))
                    }}
                } else {
                    quote! { None::<&#target> }
                };
                let resolve = if let Some(id_ident) = &id_field_ident {
                    quote! {
                        match self.#state_ident.resolve_relation_option::<#target>(#entity_name, self.#id_ident, #name) {
                            Some(edge) => edge.as_ref(),
                            None => #resolve,
                        }
                    }
                } else {
                    resolve
                };
                let embedded = if box_inner_type(inner).is_some() {
                    quote! { self.#ident.as_deref() }
                } else {
                    quote! { self.#ident.as_ref() }
                };
                relations.push(quote! {
                    if let Some(entity) = #embedded {
                        record.insert(#name.to_owned(), ::teaql_core::Value::Json(::teaql_core::Entity::borrowed_json(entity, traversal)?));
                    } else if self.__load_state.is_loaded(#name) {
                        if let Some(entity) = #resolve {
                            record.insert(#name.to_owned(), ::teaql_core::Value::Json(::teaql_core::Entity::borrowed_json(entity, traversal)?));
                        } else if #null_value {
                            record.insert(#name.to_owned(), ::teaql_core::Value::Null);
                        }
                        // A missing filtered target is NotLoaded, not a synthetic NULL.
                    }
                });
                continue;
            }
            let value = into_record_value_tokens(&field.ty, quote! { self.#ident.clone() });
            let identity = parsed.id || parsed.version;
            values.push(quote! {
                if (expand || #identity) && self.__load_state.is_loaded(#name) { record.insert(#name.to_owned(), #value); }
            });
        }
        if let Some(id_ident) = &id_field_ident {
            for relation in &attrs.reverse_relations {
                let Some(target) = &relation.json_type else {
                    continue;
                };
                let name = &relation.name;
                if relation.many {
                    relations.push(quote! {
                        if self.__load_state.is_loaded(#name) && let Some(list) = self.#state_ident.resolve_relation_list::<#target>(#entity_name, self.#id_ident, #name) {
                            let mut items = Vec::with_capacity(list.len());
                            for entity in &list.data { items.push(::teaql_core::Value::Json(::teaql_core::Entity::borrowed_json(entity, traversal)?)); }
                            record.insert(#name.to_owned(), ::teaql_core::Value::List(items));
                        }
                    });
                } else {
                    relations.push(quote! {
                        if self.__load_state.is_loaded(#name) && let Some(edge) = self.#state_ident.resolve_relation_option::<#target>(#entity_name, self.#id_ident, #name) {
                            let value = match edge {
                                Some(entity) => ::teaql_core::Value::Json(::teaql_core::Entity::borrowed_json(entity, traversal)?),
                                None => ::teaql_core::Value::Null,
                            };
                            record.insert(#name.to_owned(), value);
                        }
                    });
                }
            }
        }
        quote! {
            fn borrowed_json(&self, traversal: &mut ::teaql_core::EntityJsonTraversal) -> Option<::teaql_core::serde_json::Value> {
                let expand = traversal.enter(#entity_name, self as *const Self as usize);
                let result = (|| {
                    let mut record = ::std::collections::BTreeMap::new();
                    #(#values)*
                    if expand { #(#relations)* }
                    for key in ["_comment", "_dirty_fields", "_original_values", "_is_new", "_is_deleted"] { record.remove(key); }
                    let mut json = ::teaql_core::record_to_json_value(&record);
                    if expand && let Some(fields) = ::teaql_core::Entity::dynamic_field_values(self) {
                        json.as_object_mut().expect("entity JSON object").extend(fields.values().iter().map(|(code, value)| (format!("#{code}"), value.to_json_value())));
                    }
                    Some(json)
                })();
                traversal.leave();
                result
            }
        }
    } else {
        Default::default()
    };

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
        if indexed_layout && has_load_state_field {
            quote! {
                let snapshot_type = match &entity.__load_state {
                    ::teaql_core::eval::LoadState::Indexed(snapshot) => snapshot.layout().shared_entity_name(),
                    _ => unreachable!("indexed hydration installs the generated layout"),
                };
                entity.#state_ident.set_original_compact_row(snapshot_type, record);
            }
        } else {
            quote! { entity.#state_ident.set_original_compact_row(#entity_name, record); }
        }
    } else {
        Default::default()
    };

    let root_methods_impl = if let (Some(state_ident), Some(id_ident)) =
        (&runtime_state_field_ident, &id_field_ident)
    {
        {
            quote! {
                fn get_comment(&self) -> Option<String> {
                    let key = ::teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                    self.#state_ident.get_entity_comment(&key)
                }

                fn set_comment(&mut self, comment: String) {
                    let key = ::teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                    self.#state_ident.set_entity_comment(key, comment);
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

    let field_layout_impl = if indexed_layout {
        quote! {
            fn field_layout() -> Result<Option<::std::sync::Arc<::teaql_core::FieldLayout>>, ::teaql_core::EntityError> {
                static LAYOUT: ::std::sync::OnceLock<Result<::std::sync::Arc<::teaql_core::FieldLayout>, String>> = ::std::sync::OnceLock::new();
                LAYOUT.get_or_init(|| ::teaql_core::FieldLayout::from_generated(
                    #entity_name, Self::__TEAQL_FIELD_LAYOUT_REVISION,
                    Self::__TEAQL_FIXED_FIELD_INDEXES, Self::__TEAQL_FIXED_FIELD_MAPPINGS,
                    &[#(#materialized_relation_names),*], &[#(#property_member_names),*],
                )).clone().map(Some).map_err(|message| ::teaql_core::EntityError::new(#entity_name, message))
            }
        }
    } else {
        Default::default()
    };

    let validate_layout = if indexed_layout {
        quote! { <Self as ::teaql_core::TeaqlEntity>::field_layout().expect("invalid generated field layout"); }
    } else {
        Default::default()
    };

    let set_load_state_impl = if has_load_state_field && indexed_layout {
        quote! {
            entity.__load_state = record.indexed_load_state(
                <Self as ::teaql_core::TeaqlEntity>::field_layout()?.expect("indexed entity layout"));
        }
    } else if has_load_state_field {
        quote! {
            entity.__load_state =
                ::teaql_core::eval::LoadState::SharedColumns(record.shared_columns());
        }
    } else {
        Default::default()
    };

    let checker_load_state_impl = if has_load_state_field && indexed_layout {
        quote! {
            fn is_field_loaded(&self, field: &str) -> bool { self.__load_state.is_loaded(field) }
            fn set_checker_loaded_fields(&mut self, fields: ::std::collections::BTreeSet<String>) {
                let layout = <Self as ::teaql_core::TeaqlEntity>::field_layout()
                    .expect("validated generated field layout").expect("indexed entity layout");
                self.__load_state = ::teaql_core::eval::LoadState::Indexed(
                    ::teaql_core::LoadedSnapshot::projection(layout, fields.iter().map(String::as_str)).into_shared());
            }
        }
    } else if has_load_state_field {
        quote! {
            fn is_field_loaded(&self, field: &str) -> bool {
                self.__load_state.is_loaded(field)
            }

            fn set_checker_loaded_fields(&mut self, fields: ::std::collections::BTreeSet<String>) {
                self.__load_state = ::teaql_core::eval::LoadState::Partial(fields.into_iter().collect());
            }
        }
    } else {
        Default::default()
    };

    let checker_dirty_state_impl = match (&runtime_state_field_ident, &id_field_ident) {
        (Some(state_ident), Some(id_ident)) => quote! {
            fn set_checker_dirty_fields(
                &mut self,
                fields: ::std::collections::BTreeSet<String>,
                values: &::teaql_core::MutationValues,
            ) {
                let key = ::teaql_runtime::EntityKey::new(#entity_name, self.#id_ident);
                for field in fields {
                    if let Some(value) = values.get(&field) {
                        self.#state_ident.set(key.clone(), field, value.clone());
                    }
                }
            }
        },
        _ => Default::default(),
    };

    let dynamic_field_impl = if let Some(state_ident) = &runtime_state_field_ident {
        let mutate = if has_load_state_field && indexed_layout {
            id_field_ident.as_ref().map(|id_ident| quote! {
                fn update_dynamic_field(&mut self, code: &str, value: ::teaql_core::Value) -> Result<(), ::teaql_core::dynamic_fields::DynamicFieldError> {
                    let state = <Self as ::teaql_core::Entity>::loaded_state_snapshot(self).ok_or_else(|| ::teaql_core::dynamic_fields::DynamicFieldError {
                        code: "DYNAMIC_FIELD_INDEXED_STATE_MISSING", field: code.to_owned(),
                    })?;
                    if <Self as ::teaql_core::TeaqlEntity>::field_layout().ok().flatten().is_none_or(|layout| !::std::sync::Arc::ptr_eq(state.layout(), &layout)) {
                        return Err(::teaql_core::dynamic_fields::DynamicFieldError { code: "DYNAMIC_FIELD_LAYOUT_MISMATCH", field: code.to_owned() });
                    }
                    if self.#state_ident.update_dynamic_field(::teaql_runtime::EntityKey::new(#entity_name, self.#id_ident), code, value)? {
                        self.__load_state = ::teaql_core::eval::LoadState::Indexed(::teaql_core::LoadedSnapshot::with_dynamic_fields(
                            &state, self.#state_ident.loaded_dynamic_fields().expect("validated dynamic definitions")
                        ).expect("validated dynamic owner and layout"));
                    }
                    Ok(())
                }
                fn delete_dynamic_field(&mut self, code: &str) -> Result<(), ::teaql_core::dynamic_fields::DynamicFieldError> {
                    let state = <Self as ::teaql_core::Entity>::loaded_state_snapshot(self).ok_or_else(|| ::teaql_core::dynamic_fields::DynamicFieldError {
                        code: "DYNAMIC_FIELD_INDEXED_STATE_MISSING", field: code.to_owned(),
                    })?;
                    if <Self as ::teaql_core::TeaqlEntity>::field_layout().ok().flatten().is_none_or(|layout| !::std::sync::Arc::ptr_eq(state.layout(), &layout)) {
                        return Err(::teaql_core::dynamic_fields::DynamicFieldError { code: "DYNAMIC_FIELD_LAYOUT_MISMATCH", field: code.to_owned() });
                    }
                    if self.#state_ident.delete_dynamic_field(::teaql_runtime::EntityKey::new(#entity_name, self.#id_ident), code)? {
                        self.__load_state = ::teaql_core::eval::LoadState::Indexed(::teaql_core::LoadedSnapshot::with_dynamic_fields(
                            &state, self.#state_ident.loaded_dynamic_fields().expect("validated dynamic definitions")
                        ).expect("validated dynamic owner and layout"));
                    }
                    Ok(())
                }
            })
        } else {
            None
        };
        let install = if has_load_state_field && indexed_layout {
            quote! {
                fn supports_dynamic_field_load() -> bool { true }
                fn loaded_state_snapshot(&self) -> Option<::std::sync::Arc<::teaql_core::LoadedSnapshot>> {
                    match &self.__load_state { ::teaql_core::eval::LoadState::Indexed(state) => Some(state.clone()), _ => None }
                }
                fn install_loaded_dynamic_fields(
                    &mut self,
                    values: ::teaql_core::dynamic_fields::DynamicFieldValues,
                    state: ::std::sync::Arc<::teaql_core::LoadedSnapshot>,
                ) -> Result<(), ::teaql_core::EntityError> {
                    let layout = <Self as ::teaql_core::TeaqlEntity>::field_layout()?.expect("indexed entity layout");
                    if !::std::sync::Arc::ptr_eq(state.layout(), &layout) || values.definitions().owner_type() != #entity_name {
                        return Err(::teaql_core::EntityError::new(#entity_name, "incompatible dynamic-field owner or loaded layout"));
                    }
                    self.#state_ident.install_loaded_dynamic_fields(values);
                    self.__load_state = ::teaql_core::eval::LoadState::Indexed(state);
                    Ok(())
                }
            }
        } else {
            quote! {}
        };
        quote! {
            fn __teaql_runtime_state_any(&self) -> Option<&dyn ::std::any::Any> { Some(&self.#state_ident) }
            fn dynamic_field_values(&self) -> Option<&::teaql_core::dynamic_fields::DynamicFieldValues> {
                self.#state_ident.loaded_dynamic_fields()
            }
            fn has_pending_dynamic_mutations(&self) -> bool { self.#state_ident.has_pending_dynamic_mutations() }
            #install
            #mutate
        }
    } else {
        quote! {}
    };

    let unknown_record_field_arm = if indexed_layout {
        quote! {
            _ => {
                // Known native/physical names take the match arms above. Only
                // an unmatched spelling consults the shared generated table;
                // never build or clone an alias dictionary per entity row.
                let known = Self::__TEAQL_FIXED_FIELD_MAPPINGS.iter()
                    .find(|(canonical, _, column)| *canonical == key.as_str() || *column == key.as_str())
                    .is_some_and(|(_, member, _)| match *member {
                        #(#record_member_fallback_arms)*
                        _ => false,
                    });
                if !known {
                    match key.as_str() { #unknown_record_field_arm }
                }
            }
        }
    } else {
        unknown_record_field_arm
    };

    let from_compact_body = quote! {
            record.validate_dynamic_properties()
                .map_err(|message| ::teaql_core::EntityError::new(#entity_name, message))?;
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
            #(#alias_state_tokens)*
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

            #field_layout_impl

            fn entity_descriptor() -> ::teaql_core::EntityDescriptor {
                #validate_layout
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
            fn supports_dynamic_property_load() -> bool { #has_dynamic_properties }
            #dynamic_property_impl
            #from_compact_impl
            #borrowed_json_impl

            fn into_json_values(self) -> ::std::collections::BTreeMap<String, ::teaql_core::Value> {
                let mut record = ::std::collections::BTreeMap::new();
                #(#into_json_fields)*
                record
            }

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

            #checker_load_state_impl
            #checker_dirty_state_impl
            #dynamic_field_impl

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
    let mut into_json_fields = Vec::new();
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
        let json_value = crate::mapping::into_relation_json_value_tokens(
            &field.ty,
            quote! { self.#field_ident },
            quote! { loaded.is_some_and(|state| state.is_loaded(#field_name)) },
        );

        from_record_fields.push(quote! {
            #field_ident: #from_value
        });
        into_record_fields.push(quote! {
            if let Some(val) = #into_value {
                record.insert(#field_name.to_owned(), val);
            }
        });
        into_json_fields.push(quote! {
            if let Some(val) = #json_value { record.insert(#field_name.to_owned(), val); }
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
            fn inject_into_json_values(self, record: &mut ::std::collections::BTreeMap<String, ::teaql_core::Value>, loaded: Option<&::teaql_core::eval::LoadState>) {
                let _ = loaded;
                #(#into_json_fields)*
            }
        }
    }
}
