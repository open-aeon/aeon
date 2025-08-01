use proc_macro::TokenStream;
use quote::quote;
use syn::{self, parse_macro_input, Data, DeriveInput, Fields, Meta, NestedMeta};

// Helper function to parse a version string like "X+" or "X-Y" into a min version number.
fn parse_min_version(version_str: &str) -> i16 {
    version_str
        .split('-')
        .next()
        .unwrap_or("0")
        .trim_end_matches('+')
        .parse::<i16>()
        .unwrap_or(0)
}

#[proc_macro_derive(KafkaProtocol, attributes(kafka, kafka_flexible_versions))]
pub fn kafka_protocol_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            &fields.named
        } else {
            panic!("Only structs with named fields are supported");
        }
    } else {
        panic!("Only structs are supported");
    };

    // --- Generate Legacy Logic ---
    let legacy_decode_fields = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let mut versions = "0+".to_string();
        for attr in &f.attrs {
            if attr.path.is_ident("kafka") {
                if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                    for nested in &meta_list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("versions") {
                                if let syn::Lit::Str(lit) = &nv.lit {
                                    versions = lit.value();
                                }
                            }
                        }
                    }
                }
            }
        }
        let min_version = parse_min_version(&versions);
        quote! {
            #field_name: {
                if api_version >= #min_version {
                    Decode::decode(buf, api_version)?
                } else {
                    Default::default()
                }
            }
        }
    });

    let legacy_encode_fields = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let mut versions = "0+".to_string();
        for attr in &f.attrs {
            if attr.path.is_ident("kafka") {
                if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                    for nested in &meta_list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("versions") {
                                if let syn::Lit::Str(lit) = &nv.lit {
                                    versions = lit.value();
                                }
                            }
                        }
                    }
                }
            }
        }
        let min_version = parse_min_version(&versions);
        quote! {
            if api_version >= #min_version {
                self.#field_name.encode(buf, api_version)?;
            }
        }
    });

    let legacy_decode_impl = quote! {
        Ok(Self {
            #(#legacy_decode_fields),*
        })
    };
    let legacy_encode_impl = quote! {
        #(#legacy_encode_fields)*
        Ok(())
    };

    // --- Generate Flexible Logic ---
    let (tagged_fields, untagged_fields): (Vec<_>, Vec<_>) =
        fields.iter().partition(|f| {
            f.attrs.iter().any(|attr| {
                if attr.path.is_ident("kafka") {
                    if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                        return meta_list.nested.iter().any(|nested| {
                            if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                                return nv.path.is_ident("tag");
                            }
                            false
                        });
                    }
                }
                false
            })
        });

    let flexible_decode_untagged_fields = untagged_fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        quote! {
            #field_name: Decode::decode(buf, api_version)?
        }
    });

    // TODO: Implement tagged field decoding
    let flexible_decode_tagged_fields_init = tagged_fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        quote! {
            #field_name: Default::default()
        }
    });

    let mut tagged_field_match_arms = Vec::new();
    for f in &tagged_fields {
        let field_name = f.ident.as_ref().unwrap();
        let mut tag = None;
        for attr in &f.attrs {
            if attr.path.is_ident("kafka") {
                if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                    for nested in &meta_list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("tag") {
                                if let syn::Lit::Int(lit_int) = &nv.lit {
                                    tag = Some(lit_int.base10_parse::<u32>().unwrap());
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(tag_val) = tag {
            tagged_field_match_arms.push(quote! {
                #tag_val => {
                    obj.#field_name = Decode::decode(buf, api_version)?;
                }
            });
        }
    }

    let flexible_decode_impl = quote! {
        let mut obj = Self {
            #(#flexible_decode_untagged_fields,)*
            #(#flexible_decode_tagged_fields_init,)*
        };
        
        let num_tagged_fields = u32::decode_varint(buf)?;
        for _ in 0..num_tagged_fields {
            let tag = u32::decode_varint(buf)?;
            let size = u32::decode_varint(buf)?;
            match tag {
                #(#tagged_field_match_arms)*
                _ => { 
                    // Skip unknown tagged field
                    if buf.remaining() < size as usize {
                        // Handle error: not enough bytes to skip
                        return Err(crate::error::protocol::ProtocolError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "Not enough bytes to skip unknown tagged field",
                        )));
                    }
                    buf.advance(size as usize);
                }
            }
        }

        Ok(obj)
    };

    let flexible_encode_untagged_fields = untagged_fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        quote! {
            self.#field_name.encode(buf, api_version)?;
        }
    });

    let mut tagged_field_count_arms = vec![];
    let mut tagged_field_encode_arms = vec![];

    for f in &tagged_fields {
        let field_name = f.ident.as_ref().unwrap();
        let field_type = &f.ty;
        
        let mut tag = None;
         for attr in &f.attrs {
            if attr.path.is_ident("kafka") {
                if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                    for nested in &meta_list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("tag") {
                                if let syn::Lit::Int(lit_int) = &nv.lit {
                                    tag = Some(lit_int.base10_parse::<u32>().unwrap());
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(tag_val) = tag {
            tagged_field_count_arms.push(quote! {
                if self.#field_name != <#field_type>::default() {
                    count += 1;
                }
            });

            tagged_field_encode_arms.push(quote! {
                if self.#field_name != <#field_type>::default() {
                    let mut tagged_buf = vec![];
                    self.#field_name.encode(&mut tagged_buf, api_version)?;
                    
                    (#tag_val as u32).encode_varint(buf);
                    (tagged_buf.len() as u32).encode_varint(buf);
                    buf.put_slice(&tagged_buf);
                }
            });
        }
    }

    let flexible_encode_impl = quote! {
        #(#flexible_encode_untagged_fields)*

        let mut count = 0u32;
        #(#tagged_field_count_arms)*
        {
            use crate::kafka::codec::Varint;
            count.encode_varint(buf);
        }

        #(#tagged_field_encode_arms)*

        Ok(())
    };

    // --- Parse top-level flexible_versions attribute ---
    let mut min_flexible_version = i16::MAX;
    for attr in &input.attrs {
        if attr.path.is_ident("kafka_flexible_versions") {
             if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                if let Some(NestedMeta::Lit(syn::Lit::Str(lit_str))) = meta_list.nested.first() {
                    min_flexible_version = parse_min_version(&lit_str.value());
                }
            }
        }
    }

    let expanded = quote! {
        impl Decode for #name {
            fn decode(buf: &mut impl bytes::Buf, api_version: i16) -> crate::error::protocol::Result<Self> {
                if api_version >= #min_flexible_version {
                    #flexible_decode_impl
                } else {
                    #legacy_decode_impl
                }
            }
        }
        
        impl Encode for #name {
            fn encode(&self, buf: &mut impl bytes::BufMut, api_version: i16) -> crate::error::protocol::Result<()> {
                 if api_version >= #min_flexible_version {
                    #flexible_encode_impl
                } else {
                    #legacy_encode_impl
                }
            }
        }
    };

    TokenStream::from(expanded)
}
