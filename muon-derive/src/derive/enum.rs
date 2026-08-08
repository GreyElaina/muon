use std::mem::take;

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{parse_quote, parse_quote_spanned};

use crate::derive::meta::{AttributeKind, DeriveKind, GeneralImpl, ObserveMeta};
use crate::derive::{FMT_TRAITS, GenericsDetector, GenericsVisitor};

pub fn derive_observe_for_enum(
    input: &syn::DeriveInput,
    variants: &Punctuated<syn::Variant, syn::Token![,]>,
    input_meta: &ObserveMeta,
) -> TokenStream {
    let input_ident = &input.ident;
    let ob_ident = format_ident!("{}Observer", input_ident);
    let ob_initial_ident = format_ident!("{}ObserverInitial", input_ident);
    let ob_variant_ident = format_ident!("{}ObserverVariant", input_ident);
    let input_vis = &input.vis;

    let mut generics_visitor = GenericsVisitor::default();
    generics_visitor.visit_derive_input(input);
    let head = generics_visitor.allocate_ty(parse_quote!(S));
    let depth = generics_visitor.allocate_ty(parse_quote!(N));
    let ob_lt = generics_visitor.allocate_lt(parse_quote!('ob));

    let mut ob_initial_variants = quote! {};
    let mut ob_variant_variants = quote! {};
    let mut initial_observe_arms = quote! {};
    let mut initial_flush_pats = quote! {};
    let mut variant_observe_arms = quote! {};
    let mut variant_relocate_arms = quote! {};
    let mut variant_flush_arms = quote! {};

    let mut errors = quote! {};
    let mut field_tys = vec![];
    let mut ob_field_tys = vec![];
    // Sink predicates for closed (generic-free) variant fields.
    // Containers use unconditional vocabulary projections. Direct
    // fields use `Flush` to propagate nested leaf capability.
    let mut vocab_predicates = quote! {};
    // Generic receivers with a custom (`#[muon(...)]`) observer: their
    // observer type gets a fixed bound in generated code, regardless of
    // the field type.
    let mut general_ob_tys_ser = vec![];
    let mut skipped_tys = vec![];
    let mut general_predicates = vec![];
    let mut has_variant = false;
    let mut has_initial = false;
    for variant in variants {
        let variant_ident = &variant.ident;
        let variant_name = variant.ident.to_string();
        if variant.fields.is_empty() {
            has_initial = true;
            let mut variant = variant.clone();
            take(&mut variant.attrs);
            ob_initial_variants.extend(quote! {
                #variant_ident,
            });
            initial_observe_arms.extend(quote! {
                #input_ident::#variant => #ob_initial_ident::#variant_ident,
            });
            initial_flush_pats.extend(quote! {
                | (#ob_initial_ident::#variant_ident, #input_ident::#variant)
            });
            continue;
        }

        has_variant = true;
        let variant_meta = ObserveMeta::parse_attrs(
            &variant.attrs,
            &mut errors,
            AttributeKind::Variant,
            DeriveKind::Enum,
        );
        let tag_segment = if variant_meta.serde.untagged {
            None
        } else if let Some(rename) = &variant_meta.serde.rename {
            Some(quote! { #rename })
        } else if let Some(expr) = &input_meta.serde.content {
            Some(quote! { #expr })
        } else if input_meta.serde.untagged || input_meta.serde.tag.is_some() {
            None
        } else {
            let segment = input_meta.serde.rename_all.apply(&variant_name);
            Some(quote! { #segment })
        };

        let if_named: Vec<TokenStream> = match &variant.fields {
            syn::Fields::Named(_) => vec![quote! {}],
            _ => vec![],
        };

        let mut idents = vec![];
        let mut ob_idents = vec![];
        let mut value_idents = vec![];
        let mut flush_idents = vec![];
        let mut variant_fields = quote! {};
        let mut observe_fields = quote! {};
        let mut relocate_stmts = quote! {};
        let mut mutation_idents = vec![];
        let mut flush_field_stmts = quote! {};
        let mut has_skipped = false;

        let field_count = variant.fields.len();
        for (index, field) in variant.fields.iter().enumerate() {
            let field_meta = ObserveMeta::parse_attrs(
                &field.attrs,
                &mut errors,
                AttributeKind::Field,
                DeriveKind::Enum,
            );
            let mut field_cloned = field.clone();
            field_cloned.attrs = vec![];
            let field_span = field_cloned.span();
            let field_trivial = !GenericsDetector::detect(&field.ty, &input.generics);
            let field_ty = &field.ty;
            let field_ident = &field.ident;
            let ob_ident = syn::Ident::new(&format!("u{}", index), field_span);
            let value_ident = syn::Ident::new(&format!("v{}", index), field_span);
            if let Some(field_ident) = field_ident {
                idents.push(quote! { #field_ident });
            }
            ob_idents.push(quote! { #ob_ident });
            value_idents.push(quote! { #value_ident });
            let observe_ident = if let Some(field_ident) = field_ident {
                field_ident
            } else {
                &value_ident
            };
            let flush_ident = if let Some(field_ident) = field_ident {
                field_ident
            } else {
                &ob_ident
            };

            if field_meta.skip || field_meta.serde.skip || field_meta.serde.skip_serializing {
                has_skipped = true;
                if !field_trivial {
                    skipped_tys.push(quote! { #field_ty });
                }
                variant_fields.extend(quote! {
                    #(#if_named #field_ident:)* ::muon::helper::Pointer<#field_ty>,
                });
                observe_fields.extend(quote_spanned! { field_span =>
                    #(#if_named #field_ident:)* ::muon::helper::Pointer::new_unchecked(
                        __ptr.with_addr(#observe_ident as *const _ as usize).cast(),
                    ),
                });
                relocate_stmts.extend(quote_spanned! { field_span =>
                    ::muon::helper::Pointer::set(#ob_ident, #value_ident);
                });
                if field_ident.is_none() {
                    flush_idents.push(quote! { _ });
                }
                continue;
            }

            flush_idents.push(quote! { #flush_ident });
            let ob_field_ty: syn::Type = match &field_meta.general_impl {
                None => parse_quote_spanned! { field_span =>
                    ::muon::observe::DefaultObserver<#ob_lt, #field_ty>
                },
                Some(GeneralImpl { ob_ident, .. }) => parse_quote_spanned! { field_span =>
                    ::muon::general::#ob_ident<#ob_lt, #field_ty, #field_ty>
                },
            };
            if let Some(GeneralImpl { bounds, .. }) = &field_meta.general_impl {
                if !field_trivial {
                    skipped_tys.push(quote! { #field_ty });
                    if !bounds.is_empty() {
                        general_predicates.push(quote! { #field_ty: #bounds });
                    }
                }
                general_ob_tys_ser.push(quote! { #ob_field_ty });
            } else if field_trivial {
                // Closed fields add their sink capability below, after
                // the derive selects direct or delegated flushing.
            } else {
                if !field_trivial {
                    field_tys.push(quote! { #field_ty });
                }
                // Every field observer's `Flush<Sk>` capability is
                // sink-conditional; the variant flush must carry the
                // clause even for trivial (generic-free) field types.
                ob_field_tys.push(quote! { #ob_field_ty });
            }
            variant_fields.extend(quote! {
                #(#if_named #field_ident:)* #ob_field_ty,
            });
            observe_fields.extend(quote_spanned! { field_span =>
                #(#if_named #field_ident:)* ::muon::observe::Observer::observe(
                    __ptr.with_addr(#observe_ident as *const _ as usize).cast(),
                ),
            });
            relocate_stmts.extend(quote_spanned! { field_span =>
                ::muon::observe::Observer::relocate(
                    #ob_ident,
                    __ptr.with_addr(#value_ident as *const _ as usize).cast(),
                );
            });

            let mutation_ident;
            let default_segment;
            if let Some(field_ident) = &field_ident {
                let mut field_name = field_ident.to_string();
                if field_name.starts_with("r#") {
                    field_name = field_name[2..].to_string();
                }
                mutation_ident = syn::Ident::new(&format!("mutations_{field_name}"), field_span);
                // serde's `rename_all` applies to variant names only;
                // field names follow `rename_all_fields` (which serde
                // defaults to the field name unchanged). The observer's
                // field segment must match the serde key.
                let segment = input_meta.serde.rename_all_fields.apply(&field_name);
                default_segment = quote! { #segment };
            } else {
                mutation_ident = syn::Ident::new(&format!("mutations_{index}"), field_span);
                default_segment = quote! { #index };
            }

            let segment = if let Some(rename) = &field_meta.serde.rename {
                quote! { #rename }
            } else {
                default_segment
            };
            if field_meta.serde.flatten {
                if field_trivial && field_meta.general_impl.is_none() {
                    vocab_predicates.extend(quote_spanned! { field_span =>
                        #ob_field_ty: ::muon::observe::Flush<Sk>,
                    });
                }
                // A flattened field's changes merge into the parent
                // stream without a segment prefix (the serde
                // decomposition semantics).
                flush_field_stmts.extend(quote_spanned! { field_span =>
                    ::muon::observe::Flush::flush(#flush_ident, sink);
                });
            } else {
                let segment_push = if field_ident.is_some() {
                    quote! { sink.push_field(#segment); }
                } else {
                    quote! { sink.push_index(#segment); }
                };
                if field_trivial && field_meta.general_impl.is_none() {
                    let has_type_param = matches!(
                        field_ty,
                        syn::Type::Path(tp)
                            if matches!(
                                tp.path.segments.last().map(|s| &s.arguments),
                                Some(syn::PathArguments::AngleBracketed(args))
                                    if args
                                        .args
                                        .iter()
                                        .any(|a| matches!(a, syn::GenericArgument::Type(_)))
                            )
                    );
                    if has_type_param {
                        let elem_ob = crate::derive::element_observer(
                            &ob_lt,
                            field_ty,
                            &quote! { #ob_field_ty },
                        );
                        vocab_predicates.extend(quote_spanned! { field_span =>
                            <#ob_field_ty as ::muon::observe::QuasiSink<Sk>>::Operation: ::std::convert::Into<Sk::Operation>,
                            <#ob_field_ty as ::muon::observe::QuasiSink<Sk>>::Identity: ::std::convert::Into<Sk::Identity>,
                        });
                        flush_field_stmts.extend(quote_spanned! { field_span =>
                            #segment_push
                            <#ob_field_ty as ::muon::observe::FlushWith<Sk, #elem_ob>>::flush_with(
                                #flush_ident,
                                sink,
                                |e, s| <#elem_ob as ::muon::observe::Flush<Sk>>::flush(e, s),
                            );
                            sink.pop_segment();
                        });
                    } else {
                        vocab_predicates.extend(quote_spanned! { field_span =>
                            #ob_field_ty: ::muon::observe::Flush<Sk>,
                        });
                        flush_field_stmts.extend(quote_spanned! { field_span =>
                            #segment_push
                            ::muon::observe::Flush::flush(#flush_ident, sink);
                            sink.pop_segment();
                        });
                    }
                } else {
                    flush_field_stmts.extend(quote_spanned! { field_span =>
                        #segment_push
                        ::muon::observe::Flush::flush(#flush_ident, sink);
                        sink.pop_segment();
                    });
                }
            }
            mutation_idents.push(mutation_ident);
        }

        let tag_push = match &tag_segment {
            Some(segment) => quote! { sink.push_field(#segment); },
            None => quote! {},
        };
        let tag_pop = match &tag_segment {
            Some(_) => quote! { sink.pop_segment(); },
            None => quote! {},
        };

        let variant_flush_expr = if flush_field_stmts.is_empty() {
            quote! { {} }
        } else if matches!(&variant.fields, syn::Fields::Unnamed(_)) && field_count == 1 {
            let flush_ident = &flush_idents[0];
            quote! {{
                #tag_push
                ::muon::observe::Flush::flush(#flush_ident, sink);
                #tag_pop
            }}
        } else {
            quote! {{
                #tag_push
                #flush_field_stmts
                #tag_pop
            }}
        };

        match &variant.fields {
            syn::Fields::Named(_) => {
                if has_skipped {
                    flush_idents.push(quote! { .. });
                }
                ob_variant_variants.extend(quote! {
                    #variant_ident { #variant_fields },
                });
                variant_observe_arms.extend(quote! {
                    #input_ident::#variant_ident { #(#idents,)* } => Self::#variant_ident { #observe_fields },
                });
                variant_relocate_arms.extend(quote! {
                    (Self::#variant_ident { #(#idents: #ob_idents,)* }, #input_ident::#variant_ident { #(#idents: #value_idents,)* }) => { #relocate_stmts }
                });
                variant_flush_arms.extend(quote! {
                    Self::#variant_ident { #(#flush_idents),* } => #variant_flush_expr,
                });
            }
            syn::Fields::Unnamed(_) => {
                ob_variant_variants.extend(quote! {
                    #variant_ident(#variant_fields),
                });
                variant_observe_arms.extend(quote! {
                    #input_ident::#variant_ident(#(#value_idents),*) => Self::#variant_ident(#observe_fields),
                });
                variant_relocate_arms.extend(quote! {
                    (Self::#variant_ident(#(#ob_idents),*), #input_ident::#variant_ident(#(#value_idents),*)) => { #relocate_stmts }
                });
                variant_flush_arms.extend(quote! {
                    Self::#variant_ident(#(#flush_idents),*) => #variant_flush_expr,
                });
            }
            syn::Fields::Unit => {
                variant_observe_arms.extend(quote! {
                    #input_ident::#variant_ident => Self::#variant_ident,
                });
                variant_relocate_arms.extend(quote! {
                    (Self::#variant_ident, #input_ident::#variant_ident) => {},
                });
                variant_flush_arms.extend(quote! {
                    Self::#variant_ident => {},
                });
            }
        }
    }
    if !errors.is_empty() {
        return errors;
    }

    if has_variant {
        ob_initial_variants.extend(quote! { __Unknown, });
        initial_observe_arms.extend(quote! {
            _ => #ob_initial_ident::__Unknown,
        });
    }

    ob_variant_variants.extend(quote! { __Unknown, });
    if has_initial {
        variant_observe_arms.extend(quote! {
            _ => Self::__Unknown,
        });
    }
    variant_relocate_arms.extend(quote! {
        (Self::__Unknown, _) => {},
    });
    variant_flush_arms.extend(quote! {
        Self::__Unknown => {},
    });
    let ob_flush_prefix_stmt = if has_initial {
        quote! {
            let initial = this.initial;
            this.initial = #ob_initial_ident::new(value);
        }
    } else {
        quote! {}
    };
    let ob_flush_suffix_stmt = if has_initial {
        quote! {
            match (initial, value) {
                #initial_flush_pats => {},
                _ => sink.replace(None, Some(value as &dyn ::muon::erased_serde::Serialize)),
            }
        }
    } else {
        quote! {
            sink.replace(None, Some(this.as_deref() as &dyn ::muon::erased_serde::Serialize))
        }
    };

    let if_has_initial = match has_initial {
        true => vec![quote! {}],
        false => vec![],
    };
    let if_has_variant = match has_variant {
        true => vec![quote! {}],
        false => vec![],
    };

    let inconsistent_state = format!("inconsistent state for {ob_ident}");

    let mut input_generics = input.generics.clone();
    let input_predicates = match take(&mut input_generics.where_clause) {
        Some(where_clause) => where_clause.predicates.into_iter().collect::<Vec<_>>(),
        None => Default::default(),
    };
    let (input_impl_generics, input_type_generics, _) = input_generics.split_for_impl();

    let mut ob_variant_generics = input_generics.clone();
    ob_variant_generics
        .params
        .insert(0, parse_quote! { #ob_lt });

    let mut ob_generics = ob_variant_generics.clone();
    ob_generics.params.push(parse_quote! { #head: ?Sized });
    ob_generics
        .params
        .push(parse_quote! { #depth = ::muon::helper::Zero });

    let (ob_impl_generics, ob_type_generics, _) = ob_generics.split_for_impl();
    let (ob_variant_impl_generics, ob_variant_type_generics, _) =
        ob_variant_generics.split_for_impl();
    let mut ser_ob_generics = ob_generics.clone();
    ser_ob_generics
        .params
        .push(parse_quote!(Sk: ::muon::observe::Sink + ?Sized));
    let (ser_ob_impl_generics, _, _) = ser_ob_generics.split_for_impl();
    let mut ser_ob_flush_with_generics = ser_ob_generics.clone();
    ser_ob_flush_with_generics
        .params
        .push(parse_quote!(Elem: ?Sized));
    let (ser_ob_flush_with_impl_generics, _, _) = ser_ob_flush_with_generics.split_for_impl();
    let mut into_sink_generics = ob_generics.clone();
    into_sink_generics
        .params
        .push(parse_quote!(Sk: ::muon::observe::Sink + ?Sized));
    let (into_sink_impl_generics, _, _) = into_sink_generics.split_for_impl();

    // The composite observer is a vocabulary aggregator: it declares the
    // sink's vocabulary (`Sk::Operation`) instead of projecting a variant
    // field's, so mixed field vocabularies coexist in one stream. The
    // per-field `vocab_predicates` gates carry the actual vocabulary check.
    let into_sink_op = quote! { Sk::Operation };
    let into_sink_id = quote! { Sk::Identity };

    let input_trivial = input.generics.params.is_empty();
    let input_serialize_predicates = if input_trivial {
        quote! {}
    } else {
        quote! {
            #input_ident #input_type_generics: ::muon::helper::serde::Serialize + 'static,
        }
    };
    let self_serialize_predicates = if input_trivial {
        quote! {}
    } else {
        quote! {
            Self: ::muon::helper::serde::Serialize,
        }
    };

    let derive_idents = &input_meta.derive.0;

    let ob_initial_metas = &input_meta.__initial;
    let ob_initial_impl = quote! {
        #(#[#ob_initial_metas])*
        #[derive(Clone, Copy)]
        #[allow(clippy::enum_variant_names)]
        #input_vis enum #ob_initial_ident {
            #ob_initial_variants
        }

        impl #ob_initial_ident {
            fn new #input_impl_generics(value: &#input_ident #input_type_generics) -> Self
            where
                #(#input_predicates,)*
            {
                match value {
                    #initial_observe_arms
                }
            }
        }
    };

    let ob_variant_metas = &input_meta.__variant;
    let ob_variant_impl = quote! {
        #(#[#ob_variant_metas])*
        #input_vis enum #ob_variant_ident #ob_variant_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe + #ob_lt,)*
            #(#general_predicates,)*
        {
            #ob_variant_variants
        }

        impl #ob_variant_impl_generics #ob_variant_ident #ob_variant_type_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
        {
            unsafe fn observe(__ptr: *mut #input_ident #input_type_generics) -> Self {
                unsafe { match &*__ptr {
                    #variant_observe_arms
                } }
            }

            unsafe fn relocate(&mut self, __ptr: *mut #input_ident #input_type_generics) {
                unsafe {
                    match (self, &*__ptr) {
                        #variant_relocate_arms
                        _ => panic!(#inconsistent_state),
                    }
                }
            }

            fn flush<Sk: ::muon::observe::Sink + ?Sized>(&mut self, __ptr: *const #input_ident #input_type_generics, sink: &mut Sk)
            where
                #input_serialize_predicates
                #vocab_predicates
                #(#ob_field_tys: ::muon::observe::Flush<Sk>,)*
                #(#general_ob_tys_ser: ::muon::observe::Flush<Sk>,)*
            {
                match self {
                    #variant_flush_arms
                }
            }
        }
    };

    let mut output = quote! {
        #(#[::std::prelude::v1::#derive_idents()])*
        #input_vis struct #ob_ident #ob_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe + #ob_lt,)*
            #(#general_predicates,)*
        {
            ptr: ::muon::helper::Pointer<#head>,
            #(#if_has_variant mutated: bool,)*
            #(#if_has_initial initial: #ob_initial_ident,)*
            #(#if_has_variant variant: #ob_variant_ident #ob_variant_type_generics,)*
            phantom: ::std::marker::PhantomData<&#ob_lt mut #depth>,
        }

        #(#if_has_initial #ob_initial_impl)*

        #(#if_has_variant #ob_variant_impl)*

        #[automatically_derived]
        impl #ob_impl_generics ::std::ops::Deref
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
        {
            type Target = ::muon::helper::Pointer<#head>;
            fn deref(&self) -> &Self::Target {
                &self.ptr
            }
        }

        #[automatically_derived]
        impl #ob_impl_generics ::std::ops::DerefMut
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
        {
            fn deref_mut(&mut self) -> &mut Self::Target {
                #(#if_has_variant
                    self.mutated = true;
                    self.variant = #ob_variant_ident::__Unknown;
                )*
                &mut self.ptr
            }
        }

        #[automatically_derived]
        impl #ob_impl_generics ::muon::helper::QuasiObserver
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
            #head: ::muon::helper::AsDeref<#depth>,
            #depth: ::muon::helper::Unsigned,
        {
            type Head = #head;
            type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
            type InnerDepth = #depth;

            fn invalidate(this: &mut Self) {
                #(#if_has_variant
                    this.mutated = true;
                    this.variant = #ob_variant_ident::__Unknown;
                )*
            }
        }

        #[automatically_derived]
        impl #ob_impl_generics ::muon::observe::Observer
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #(#skipped_tys: #ob_lt,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
            #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
            #depth: ::muon::helper::Unsigned,
        {
            unsafe fn observe(head: *mut #head) -> Self {
                unsafe {
                    let __ptr = ::muon::helper::AsDerefPtrExt::as_deref_ptr::<#depth>(head);
                    Self {
                        #(#if_has_variant mutated: false,)*
                        #(#if_has_initial initial: #ob_initial_ident::new(&*__ptr),)*
                        #(#if_has_variant variant: #ob_variant_ident::observe(__ptr),)*
                        ptr: ::muon::helper::Pointer::new_unchecked(head),
                        phantom: ::std::marker::PhantomData,
                    }
                }
            }

            unsafe fn relocate(this: &mut Self, head: *mut #head) {
                #(#if_has_variant
                    let __ptr = unsafe { ::muon::helper::AsDerefPtrExt::as_deref_ptr::<#depth>(head) };
                    unsafe { this.variant.relocate(__ptr) }
                )*
                unsafe { ::muon::helper::Pointer::set_unchecked(this, head) };
            }
        }

        #[automatically_derived]
        impl #into_sink_impl_generics ::muon::observe::QuasiSink<Sk>
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe + #ob_lt,)*
            #(#general_predicates,)*
        {
            type Operation = #into_sink_op;
            type Identity = #into_sink_id;
        }

        #[automatically_derived]
        impl #ser_ob_flush_with_impl_generics ::muon::observe::FlushWith<Sk, Elem>
        for #ob_ident #ob_type_generics
        where
            #input_serialize_predicates
            #(#input_predicates,)*
            #(#skipped_tys: #ob_lt,)*
            #(#field_tys: ::muon::Observe + #ob_lt,)*
            #(#general_predicates,)*
            #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
            #depth: ::muon::helper::Unsigned,
            #vocab_predicates
            #(#ob_field_tys: ::muon::observe::Flush<Sk>,)*
            #(#general_ob_tys_ser: ::muon::observe::Flush<Sk>,)*
        {
            fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
            where
                F: FnMut(&mut Elem, &mut Sk),
            {
                <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
            }
        }

        #[automatically_derived]
        impl #ser_ob_impl_generics ::muon::observe::Flush<Sk>
        for #ob_ident #ob_type_generics
        where
            #input_serialize_predicates
            #(#input_predicates,)*
            #(#skipped_tys: #ob_lt,)*
            #(#field_tys: ::muon::Observe + #ob_lt,)*
            #(#general_predicates,)*
            #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
            #depth: ::muon::helper::Unsigned,
            #vocab_predicates
            #(#ob_field_tys: ::muon::observe::Flush<Sk>,)*
            #(#general_ob_tys_ser: ::muon::observe::Flush<Sk>,)*
        {
            fn flush(this: &mut Self, sink: &mut Sk) {
                let value = this.ptr.as_deref();
                #ob_flush_prefix_stmt
                #(#if_has_variant
                    if !this.mutated {
                        this.variant.flush(value, sink);
                        return;
                    }
                    this.mutated = false;
                    this.variant = #ob_variant_ident::__Unknown;
                )*
                #ob_flush_suffix_stmt
            }
        }

        #[automatically_derived]
        impl #input_impl_generics ::muon::Observe
        for #input_ident #input_type_generics
        where
            #self_serialize_predicates
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
        {
            type Observer<#ob_lt, #head, #depth> = #ob_ident #ob_type_generics
            where
                Self: #ob_lt,
                #(#field_tys: #ob_lt,)*
                #depth: ::muon::helper::Unsigned,
                #head: ::muon::helper::AsDerefMut<#depth, Target = Self> + ?Sized + #ob_lt;
            type Spec = ::muon::observe::DefaultSpec;
        }
    };

    for path in &input_meta.derive.1 {
        // We just assume what the user wants is one of the standard formatting traits.
        if FMT_TRAITS.iter().any(|name| path.is_ident(name)) {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_impl_generics ::std::fmt::#path
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
                    #depth: ::muon::helper::Unsigned,
                {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        ::std::fmt::#path::fmt(self.as_deref(), f)
                    }
                }
            });
        } else if path.is_ident("PartialEq") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_impl_generics ::std::cmp::PartialEq
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
                    #depth: ::muon::helper::Unsigned,
                {
                    fn eq(&self, other: &Self) -> bool {
                        self.as_deref().eq(other.as_deref())
                    }
                }
            });
        } else if path.is_ident("Eq") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_impl_generics ::std::cmp::Eq
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
                    #depth: ::muon::helper::Unsigned,
                {}
            });
        } else if path.is_ident("PartialOrd") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_impl_generics ::std::cmp::PartialOrd
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
                    #depth: ::muon::helper::Unsigned,
                {
                    fn partial_cmp(&self, other: &Self) -> ::std::option::Option<::std::cmp::Ordering> {
                        self.as_deref().partial_cmp(other.as_deref())
                    }
                }
            });
        } else if path.is_ident("Ord") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_impl_generics ::std::cmp::Ord
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #head: ::muon::helper::AsDeref<#depth, Target = #input_ident #input_type_generics>,
                    #depth: ::muon::helper::Unsigned,
                {
                    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
                        self.as_deref().cmp(other.as_deref())
                    }
                }
            });
        }
    }

    if input_meta.expose {
        output
    } else {
        quote! {
            const _: () = {
                #output
            };
        }
    }
}
