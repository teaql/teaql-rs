mod attr;
mod derive_impl;
mod mapping;
mod types;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(TeaqlEntity, attributes(teaql))]
pub fn derive_teaql_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_impl::expand_teaql_entity(input).into()
}
