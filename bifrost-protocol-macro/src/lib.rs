use proc_macro::TokenStream;
use quote::quote;
use syn::{self, parse_macro_input, Data, DeriveInput, Fields, Meta, NestedMeta};

#[proc_macro_derive(KafkaProtocol, attributes(kafka))]
pub fn kafka_protocol_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(data) => {
            if let Fields::Named(fields) = data.fields {
                fields.named
            } else {
                panic!("KafkaProtocol derive macro only supports structs with named fields");
            }
        }
        _ => panic!("KafkaProtocol derive macro can only be used on structs"),
    };

    let decode_fields = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let _field_type = &f.ty;

        // Simplified version parsing for now, assumes "X+" format
        let mut versions = "0+".to_string(); // Default
        for attr in &f.attrs {
            if attr.path.is_ident("kafka") {
                if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                    for nested in meta_list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(name_value)) = nested {
                            if name_value.path.is_ident("versions") {
                                if let syn::Lit::Str(lit_str) = name_value.lit {
                                    versions = lit_str.value();
                                }
                            }
                        }
                    }
                }
            }
        }

        let min_version = versions.trim_end_matches('+').parse::<i16>().unwrap_or(0);

        quote! {
            #field_name: {
                if api_version >= #min_version {
                    Decode::decode(buf, api_version)?
                } else {
                    // This assumes that the struct has a Default impl
                    Default::default()
                }
            }
        }
    });

    let encode_fields = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();

        let mut versions = "0+".to_string(); // Default
        for attr in &f.attrs {
            if attr.path.is_ident("kafka") {
                if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                    for nested in meta_list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(name_value)) = nested {
                            if name_value.path.is_ident("versions") {
                                if let syn::Lit::Str(lit_str) = name_value.lit {
                                    versions = lit_str.value();
                                }
                            }
                        }
                    }
                }
            }
        }
        
        let min_version = versions.trim_end_matches('+').parse::<i16>().unwrap_or(0);

        quote! {
            if api_version >= #min_version {
                self.#field_name.encode(buf, api_version)?;
            }
        }
    });

    let expanded = quote! {
        impl Decode for #name {
            fn decode(buf: &mut impl bytes::Buf, api_version: i16) -> crate::error::protocol::Result<Self> {
                Ok(Self {
                    #(#decode_fields),*
                })
            }
        }
        
        // Placeholder for Encode
        impl Encode for #name {
            fn encode(&self, buf: &mut impl bytes::BufMut, api_version: i16) -> crate::error::protocol::Result<()> {
                #(#encode_fields)*
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}
