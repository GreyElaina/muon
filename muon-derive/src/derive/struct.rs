use std::mem::take;

use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote, quote_spanned};
use syn::parse_quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::Visit;

use crate::derive::meta::{AttributeKind, DeriveKind, GeneralImpl, ObserveMeta};
use crate::derive::{FMT_TRAITS, GenericsDetector, GenericsVisitor};

pub fn derive_observe_for_struct(
    input: &syn::DeriveInput,
    fields: &Punctuated<syn::Field, syn::Token![,]>,
    input_meta: &ObserveMeta,
    is_named: bool,
) -> TokenStream {
    let input_ident = &input.ident;
    let ob_name = format!("{}Observer", input_ident);
    let ob_ident = format_ident!("{}Observer", input_ident);
    let input_vis = &input.vis;

    let mut errors = quote! {};
    let mut generics_visitor = GenericsVisitor::default();
    generics_visitor.visit_derive_input(input);
    let head = generics_visitor.allocate_ty(parse_quote!(S));
    let depth = generics_visitor.allocate_ty(parse_quote!(N));
    let inner = generics_visitor.allocate_ty(parse_quote!(O));
    let ob_lt = generics_visitor.allocate_lt(parse_quote!('ob));

    let if_named = match is_named {
        true => vec![quote! {}],
        false => vec![],
    };

    let mut ob_fields = quote! {};
    let mut observe_field_stmts = quote! {};
    let mut observe_fields = quote! {};
    let mut relocate_stmts = quote! {};
    let mut flush_field_stmts = quote! {};
    let mut debug_chain = quote! {};

    let mut field_tys = vec![];
    let mut skipped_tys = vec![];
    let mut general_predicates = vec![];
    // Bounds for generic `deref` fields with a custom observer. They
    // only reach impls whose generic parameters keep the field's type
    // parameters (the observer struct filters them out); the observer
    // trait bounds (`#inner: Observer`, `#inner: Flush`) already
    // imply them elsewhere.
    let mut deref_general_predicates = vec![];
    let mut ob_field_tys = vec![];
    // Generic-only receivers: concrete receivers get no where-bound, so
    // their concrete `QuasiObserver` impl is not shadowed by the
    // parameter environment (rust-lang/rust#152409).
    let mut ob_field_tys_ser = vec![];
    // Sink predicates for closed (generic-free, non-deref) fields.
    // Containers use unconditional vocabulary projections. Direct
    // fields use `Flush` to propagate nested leaf capability.
    let mut vocab_predicates = quote! {};
    // Extra `Flush` predicates for the serialize impl:
    // deref fields carry their inner observer as a type parameter, so
    // the flush method body needs an explicit bound for it.
    let mut ser_ob_extra_predicates = quote! {};
    // Generic receivers with a custom (`#[muon(...)]`) observer.
    let mut general_ob_tys_ser = vec![];
    let mut deref_fields = vec![];
    let mut non_deref_members = vec![];
    let field_count = fields.len();

    for (index, field) in fields.iter().enumerate() {
        let field_meta = ObserveMeta::parse_attrs(
            &field.attrs,
            &mut errors,
            AttributeKind::Field,
            DeriveKind::Struct,
        );
        let field_vis = &field.vis;
        let field_ident = &field.ident;
        let field_member = match &field.ident {
            Some(ident) => quote! { #ident },
            None => syn::Index::from(index).to_token_stream(),
        };
        let field_span = {
            let mut field_cloned = field.clone();
            field_cloned.attrs = vec![];
            field_cloned.span()
        };

        let observer_ident;
        let default_segment;
        if let Some(ident) = &field.ident {
            let mut field_name = ident.to_string();
            if field_name.starts_with("r#") {
                field_name = field_name[2..].to_string();
            }
            debug_chain.extend(quote_spanned! { field_span =>
                .field(#field_name, &self.#field_member)
            });
            observer_ident = ident.clone();
            let segment = input_meta.serde.rename_all.apply(&field_name);
            default_segment = quote! { #segment };
        } else {
            debug_chain.extend(quote_spanned! { field_span =>
                .field(&self.#field_member)
            });
            observer_ident = syn::Ident::new(&format!("observer_{index}"), field_span);
            default_segment = quote! { #index };
        }
        observe_fields.extend(quote_spanned! { field_span => #observer_ident, });

        let field_ty = &field.ty;
        let field_trivial = !GenericsDetector::detect(field_ty, &input.generics);
        if field_meta.skip || field_meta.serde.skip || field_meta.serde.skip_serializing {
            if !field_trivial {
                skipped_tys.push(quote! { #field_ty });
            }
            ob_fields.extend(quote_spanned! { field_span =>
                #field_vis #(#if_named #field_ident:)* ::muon::helper::Pointer<#field_ty>,
            });
            observe_field_stmts.extend(quote_spanned! { field_span =>
                let #observer_ident = ::muon::helper::Pointer::new_unchecked(&raw mut (*__value).#field_member);
            });
            relocate_stmts.extend(quote_spanned! { field_span =>
                ::muon::helper::Pointer::set_unchecked(&this.#field_member, &raw mut (*__value).#field_member);
            });
            continue;
        }

        // The segment this field's changes are prefixed with.
        let segment = if let Some(rename) = &field_meta.serde.rename {
            quote! { #rename }
        } else {
            default_segment
        };

        if let Some(deref_ident) = field_meta.deref {
            let ob_field_ty = match &field_meta.general_impl {
                None => quote_spanned! { field_span =>
                    ::muon::observe::DefaultObserver<#ob_lt, #field_ty, #head, ::muon::helper::Succ<#depth>>
                },
                Some(GeneralImpl { ob_ident, .. }) => quote_spanned! { field_span =>
                    ::muon::general::#ob_ident<#ob_lt, #field_ty, #head, ::muon::helper::Succ<#depth>>
                },
            };
            if !field_trivial {
                skipped_tys.push(quote! { #field_ty });
                if let Some(GeneralImpl { bounds, .. }) = &field_meta.general_impl
                    && !bounds.is_empty()
                {
                    deref_general_predicates.push(quote! { #field_ty: #bounds });
                }
            }
            let has_general_impl = field_meta.general_impl.is_some();
            deref_fields.push((
                index,
                field,
                ob_field_ty,
                deref_ident,
                observer_ident,
                has_general_impl,
            ));
            ob_field_tys.push(quote! { #inner });
            // Every field observer's `Flush<Sk>` capability is
            // sink-conditional; the derived impl must carry the clause
            // even for trivial (generic-free) field types.
            ob_field_tys_ser.push(quote! { #inner });
            ob_fields.extend(quote_spanned! { field_span =>
                #field_vis #(#if_named #field_ident:)* #inner,
            });
            let segment_push = if field.ident.is_some() {
                quote! { sink.push_field(#segment); }
            } else {
                quote! { sink.push_index(#segment); }
            };
            flush_field_stmts.extend(quote_spanned! { field_span =>
                #segment_push
                ::muon::observe::Flush::flush(&mut this.#field_member, sink);
                sink.pop_segment();
            });
        } else {
            let ob_field_ty = match &field_meta.general_impl {
                None => quote_spanned! { field_span =>
                    ::muon::observe::DefaultObserver<#ob_lt, #field_ty>
                },
                Some(GeneralImpl { ob_ident, .. }) => quote_spanned! { field_span =>
                    ::muon::general::#ob_ident<#ob_lt, #field_ty, #field_ty>
                },
            };
            if !field_trivial {
                if let Some(GeneralImpl { bounds, .. }) = &field_meta.general_impl {
                    skipped_tys.push(quote! { #field_ty });
                    if !bounds.is_empty() {
                        general_predicates.push(quote! { #field_ty: #bounds });
                    }
                } else {
                    field_tys.push(quote! { #field_ty });
                }
            }
            ob_field_tys.push(quote! { #ob_field_ty });
            non_deref_members.push(field_member.clone());
            relocate_stmts.extend(quote_spanned! { field_span =>
                ::muon::observe::Observer::relocate(&mut this.#field_member, &raw mut (*__value).#field_member);
            });
            ob_fields.extend(quote_spanned! { field_span =>
                #field_vis #(#if_named #field_ident:)* #ob_field_ty,
            });
            observe_field_stmts.extend(quote_spanned! { field_span =>
                let #observer_ident = ::muon::observe::Observer::observe(&raw mut (*__value).#field_member);
            });
            let segment_push = if field.ident.is_some() {
                quote! { sink.push_field(#segment); }
            } else {
                quote! { sink.push_index(#segment); }
            };
            if field_trivial {
                // A direct field uses `Flush`, which propagates
                // nested leaf capability through the parent. A
                // container uses unconditional vocabulary projections
                // and delegates element flushing to the closure.
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
                    let elem_ob = crate::derive::element_observer(&ob_lt, field_ty, &ob_field_ty);
                    vocab_predicates.extend(quote_spanned! { field_span =>
                        <#ob_field_ty as ::muon::observe::QuasiSink<Sk>>::Operation: ::std::convert::Into<Sk::Operation>,
                        <#ob_field_ty as ::muon::observe::QuasiSink<Sk>>::Identity: ::std::convert::Into<Sk::Identity>,
                    });
                    flush_field_stmts.extend(quote_spanned! { field_span =>
                        #segment_push
                        <#ob_field_ty as ::muon::observe::FlushWith<Sk, #elem_ob>>::flush_with(
                            &mut this.#field_member,
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
                        ::muon::observe::Flush::flush(&mut this.#field_member, sink);
                        sink.pop_segment();
                    });
                }
            } else if field_meta.general_impl.is_some() {
                general_ob_tys_ser.push(quote! { #ob_field_ty });
                flush_field_stmts.extend(quote_spanned! { field_span =>
                    #segment_push
                    ::muon::observe::Flush::flush(&mut this.#field_member, sink);
                    sink.pop_segment();
                });
            } else {
                ob_field_tys_ser.push(quote! { #ob_field_ty });
                flush_field_stmts.extend(quote_spanned! { field_span =>
                    #segment_push
                    ::muon::observe::Flush::flush(&mut this.#field_member, sink);
                    sink.pop_segment();
                });
            }
        }
    }
    if !errors.is_empty() {
        return errors;
    }

    let mut input_generics = input.generics.clone();
    let input_predicates = match take(&mut input_generics.where_clause) {
        Some(where_clause) => where_clause.predicates.into_iter().collect::<Vec<_>>(),
        None => Default::default(),
    };
    let (input_impl_generics, input_type_generics, _) = input_generics.split_for_impl();

    let mut ob_generics = input_generics.clone();
    let mut ob_quasi_generics;
    let mut ob_observer_generics = input_generics.clone();

    let deref_ident;
    let deref_target;
    let deref_member;
    let deref_mut_impl;
    let invalidate_impl;
    let assignable_impl;
    let observer_impl;
    let ob_quasi_predicates;
    let ob_observer_predicates;
    let input_observe_predicates;
    let input_observer_type_generics;
    let input_deref_ptr_impl;

    if deref_fields.is_empty() {
        ob_generics.params.insert(0, parse_quote! { #ob_lt });
        ob_generics.params.push(parse_quote! { #head: ?Sized });
        ob_generics
            .params
            .push(parse_quote! { #depth = ::muon::helper::Zero });
        ob_quasi_generics = ob_generics.clone();
        ob_quasi_predicates = quote! {
            #head: ::muon::helper::AsDeref<#depth>,
        };
        ob_observer_generics
            .params
            .insert(0, parse_quote! { #ob_lt });
        ob_observer_generics
            .params
            .push(parse_quote! { #head: ?Sized });
        ob_observer_generics
            .params
            .push(parse_quote! { #depth = ::muon::helper::Zero });
        ob_observer_predicates = quote! {
            #head: ::muon::helper::AsDerefMut<#depth, Target = #input_ident #input_type_generics>,
        };

        ob_fields.extend(quote! {
            #(#if_named __ptr:)* ::muon::helper::Pointer<#head>,
            #(#if_named __phantom:)* ::std::marker::PhantomData<&#ob_lt mut #depth>,
        });

        deref_ident = format_ident!("Deref");
        deref_target = quote! { ::muon::helper::Pointer<#head> };
        deref_member = match is_named {
            true => quote! { __ptr },
            false => syn::Index::from(fields.len()).to_token_stream(),
        };
        deref_mut_impl = quote! {
            ::muon::helper::QuasiObserver::invalidate(&mut self.#deref_member);
            #(::muon::helper::QuasiObserver::invalidate(&mut self.#non_deref_members);)*
        };

        invalidate_impl = quote! {
            #(::muon::helper::QuasiObserver::invalidate(&mut this.#non_deref_members);)*
        };

        assignable_impl = quote! {
            type Head = #head;
            type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
            type InnerDepth = #depth;
        };

        let observer_observe_expr = match is_named {
            true => quote! {
                Self {
                    #observe_fields
                    __ptr: ::muon::helper::Pointer::new_unchecked(head),
                    __phantom: ::std::marker::PhantomData,
                }
            },
            false => quote! {
                Self (
                    #observe_fields
                    ::muon::helper::Pointer::new_unchecked(head),
                    ::std::marker::PhantomData,
                )
            },
        };

        observer_impl = quote! {
            #[inline(always)]
            unsafe fn observe(head: *mut #head) -> Self {
                unsafe {
                    let __value = ::muon::helper::AsDeref::<#depth>::as_deref_ptr(head);
                    #observe_field_stmts
                    #observer_observe_expr
                }
            }

            unsafe fn relocate(this: &mut Self, head: *mut #head) {
                unsafe {
                    let __value = ::muon::helper::AsDeref::<#depth>::as_deref_ptr(head);
                    #relocate_stmts
                    ::muon::helper::Pointer::set_unchecked(this, head);
                }
            }
        };

        input_observe_predicates = quote! {};
        input_deref_ptr_impl = quote! {};
        let (_, ob_type_generics, _) = ob_generics.split_for_impl();
        input_observer_type_generics = quote! { #ob_type_generics };
    } else if deref_fields.len() > 1 {
        return deref_fields
            .into_iter()
            .map(|(_, _, _, ident, _, _)| {
                syn::Error::new(ident.span(), "only one field can be marked as `deref`")
                    .to_compile_error()
            })
            .collect();
    } else {
        let (i, field, ob_field_ty, meta_deref_ident, observer_ident, has_general_impl) =
            deref_fields.swap_remove(0);
        let field_ty = &field.ty;
        let field_member = match &field.ident {
            Some(ident) => quote! { #ident },
            None => syn::Index::from(i).to_token_stream(),
        };

        let mut generics_visitor = GenericsVisitor::default();
        for other_field in fields {
            if field.ident == other_field.ident {
                continue;
            }
            generics_visitor.visit_type(&other_field.ty);
        }
        ob_generics.params = ob_generics
            .params
            .into_iter()
            .filter(|param| match param {
                syn::GenericParam::Const(param) => generics_visitor.contains_ty(&param.ident),
                syn::GenericParam::Type(param) => generics_visitor.contains_ty(&param.ident),
                syn::GenericParam::Lifetime(param) => generics_visitor.contains_lt(&param.lifetime),
            })
            .collect();
        // The observer-struct impls carry `#skipped_tys: #ob_lt` in
        // their where-clauses; `'ob` must exist there whenever a
        // generic field is skipped (or deref'd), even for a
        // single-field struct. The struct itself only takes `'ob`
        // when a field observer needs it (E0392 forbids an unused
        // lifetime parameter on a single-field `deref` struct, whose
        // only field is the abstract inner observer).
        if field_count > 1 {
            ob_generics.params.insert(0, parse_quote! { #ob_lt });
        }
        if field_count > 1 || !skipped_tys.is_empty() {
            ob_observer_generics
                .params
                .insert(0, parse_quote! { #ob_lt });
        }
        ob_generics.params.push(parse_quote! { #inner });
        ob_quasi_generics = ob_generics.clone();
        ob_quasi_generics.params.push(parse_quote! { #depth });
        ob_quasi_predicates = quote! {
            #inner: ::muon::helper::QuasiObserver<InnerDepth = ::muon::helper::Succ<#depth>>,
            #inner::Head: ::muon::helper::AsDeref<#depth>,
        };
        ob_observer_generics.params.push(parse_quote! { #inner });
        ob_observer_generics.params.push(parse_quote! { #depth });
        ob_observer_predicates = quote! {
            #inner: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<#depth>>,
            #inner::Head: ::muon::helper::AsDerefMut<#depth, Target = #input_ident #input_type_generics>,
        };
        ser_ob_extra_predicates = quote! {
            #inner: ::muon::observe::Flush<Sk>,
        };

        deref_ident = syn::Ident::new("Deref", meta_deref_ident.span());
        deref_target = quote! { #inner };
        deref_member = quote! { #field_member };
        deref_mut_impl = quote! {};

        invalidate_impl = quote! {
            #(::muon::helper::QuasiObserver::invalidate(&mut this.#non_deref_members);)*
            ::muon::helper::QuasiObserver::invalidate(&mut this.#field_member);
        };

        assignable_impl = quote! {
            type Head = #inner::Head;
            type OuterDepth = ::muon::helper::Succ<#inner::OuterDepth>;
            type InnerDepth = #depth;
        };

        let mut observer_observe_expr = match is_named {
            true => quote! { Self { #observe_fields } },
            false => quote! { Self (#observe_fields) },
        };

        let prepare_value = if field_count > 1 {
            quote! {
                let __value = ::muon::helper::AsDeref::<#depth>::as_deref_ptr(head);
            }
        } else {
            quote! {}
        };

        let prepare_value_ptr = if field_count > 1 {
            quote! {
                let __value = ::muon::helper::AsDeref::<#depth>::as_deref_ptr(head);
            }
        } else {
            quote! {}
        };

        if !non_deref_members.is_empty() {
            observer_observe_expr = quote! {
                let this = #observer_observe_expr;
                let ptr = #inner::as_deref_coinductive(&this.#field_member);
                #(::muon::helper::Pointer::register_observer(ptr, &this.#non_deref_members);)*
                this
            };
        };

        observer_impl = quote! {
            #[inline(always)]
            unsafe fn observe(head: *mut #inner::Head) -> Self {
                unsafe {
                    #prepare_value
                    #observe_field_stmts
                    let #observer_ident = ::muon::observe::Observer::observe(head);
                    #observer_observe_expr
                }
            }

            unsafe fn relocate(this: &mut Self, head: *mut #inner::Head) {
                unsafe {
                    #prepare_value_ptr
                    #relocate_stmts
                    ::muon::observe::Observer::relocate(&mut this.#field_member, head);
                }
            }
        };

        input_observe_predicates = if has_general_impl {
            quote! {}
        } else {
            quote! { #field_ty: ::muon::Observe, }
        };

        input_deref_ptr_impl = quote! {
            #[automatically_derived]
            unsafe impl #input_impl_generics ::muon::helper::DerefPtr
            for #input_ident #input_type_generics
            where
                #(#input_predicates,)*
            {
                unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
                    unsafe { &raw mut (*this).#field_member }
                }
            }
        };

        let ob_type_arguments = ob_generics.params.iter().map(|param| match param {
            syn::GenericParam::Type(ty_param) if ty_param.ident == inner => quote! { #ob_field_ty },
            _ => quote! { #param },
        });
        input_observer_type_generics = quote! { <#(#ob_type_arguments),*> };
    }

    let (ob_impl_generics, ob_type_generics, _) = ob_generics.split_for_impl();
    let (ob_quasi_impl_generics, _, _) = ob_quasi_generics.split_for_impl();
    let (ob_observer_impl_generics, _, _) = ob_observer_generics.split_for_impl();
    let mut ser_ob_generics = ob_observer_generics.clone();
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

    let ob_item = match is_named {
        true => quote! {
            #input_vis struct #ob_ident #ob_generics
            where
                #(#input_predicates,)*
                #(#field_tys: ::muon::Observe + #ob_lt,)*
                #(#general_predicates,)*
            {
                #ob_fields
            }
        },
        false => quote! {
            #input_vis struct #ob_ident #ob_generics (#ob_fields)
            where
                #(#input_predicates,)*
                #(#field_tys: ::muon::Observe + #ob_lt,)*
                #(#general_predicates,)*;
        },
    };

    // The composite flush reports each field's events, prefixing them
    // with the field's path segment.
    let flush_impl = quote! {
        #flush_field_stmts
    };

    // The composite observer is a vocabulary aggregator: it declares the
    // sink's vocabulary (`Sk::Operation`) instead of projecting a field's,
    // so mixed field vocabularies coexist in one stream. The per-field
    // `vocab_predicates` gates (each field's `Operation`/`Identity` must
    // convert into the sink's) carry the actual vocabulary check.
    let into_sink_op = quote! { Sk::Operation };
    let into_sink_id = quote! { Sk::Identity };

    let mut output = quote! {
        #ob_item

        #[automatically_derived]
        impl #ob_impl_generics ::std::ops::#deref_ident
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
        {
            type Target = #deref_target;
            fn deref(&self) -> &Self::Target {
                &self.#deref_member
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
                ::std::ptr::from_mut(self).expose_provenance();
                #deref_mut_impl
                &mut self.#deref_member
            }
        }

        #[automatically_derived]
        impl #ob_quasi_impl_generics ::muon::helper::QuasiObserver
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #ob_quasi_predicates
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
            #depth: ::muon::helper::Unsigned,
        {
            #assignable_impl

            fn invalidate(this: &mut Self) {
                #invalidate_impl
            }
        }

        #[automatically_derived]
        impl #ob_observer_impl_generics ::muon::observe::Observer
        for #ob_ident #ob_type_generics
        where
            #(#input_predicates,)*
            #(#skipped_tys: #ob_lt,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
            #(#deref_general_predicates,)*
            #ob_observer_predicates
            #depth: ::muon::helper::Unsigned,
        {
            #observer_impl
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
            #(#deref_general_predicates,)*
            #ob_observer_predicates
            #ser_ob_extra_predicates
            #depth: ::muon::helper::Unsigned,
            #vocab_predicates
            #(#ob_field_tys_ser: ::muon::observe::Flush<Sk>,)*
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
            #(#deref_general_predicates,)*
            #ob_observer_predicates
            #ser_ob_extra_predicates
            #depth: ::muon::helper::Unsigned,
            #vocab_predicates
            #(#ob_field_tys_ser: ::muon::observe::Flush<Sk>,)*
            #(#general_ob_tys_ser: ::muon::observe::Flush<Sk>,)*
        {
            #[inline(always)]
            fn flush(this: &mut Self, sink: &mut Sk) {
                #flush_impl
            }
        }

        #[automatically_derived]
        impl #input_impl_generics ::muon::Observe
        for #input_ident #input_type_generics
        where
            #self_serialize_predicates
            #input_observe_predicates
            #(#input_predicates,)*
            #(#field_tys: ::muon::Observe,)*
            #(#general_predicates,)*
            #(#deref_general_predicates,)*
        {
            type Observer<#ob_lt, #head, #depth> = #ob_ident #input_observer_type_generics
            where
                Self: #ob_lt,
                #(#field_tys: #ob_lt,)*
                #depth: ::muon::helper::Unsigned,
                #head: ::muon::helper::AsDerefMut<#depth, Target = Self> + ?Sized + #ob_lt;
            type Spec = ::muon::observe::DefaultSpec;
        }

        #input_deref_ptr_impl
    };

    for path in &input_meta.derive.1 {
        // We just assume what the user wants is one of the standard formatting traits.
        if FMT_TRAITS.iter().any(|name| path.is_ident(name)) {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_observer_impl_generics ::std::fmt::#path
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#skipped_tys: #ob_lt,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #(#deref_general_predicates,)*
                    #ob_observer_predicates
                    #depth: ::muon::helper::Unsigned,
                {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        let inner = ::muon::helper::QuasiObserver::untracked_ref(self);
                        ::std::fmt::#path::fmt(inner, f)
                    }
                }
            });
        } else if path.is_ident("Debug") {
            let method = match is_named {
                true => quote! { debug_struct },
                false => quote! { debug_tuple },
            };
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_impl_generics ::std::fmt::Debug
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #(#ob_field_tys: ::std::fmt::Debug,)*
                {
                    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                        f.#method(#ob_name) #debug_chain .finish()
                    }
                }
            });
        } else if path.is_ident("PartialEq") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_observer_impl_generics ::std::cmp::PartialEq
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#skipped_tys: #ob_lt,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #(#deref_general_predicates,)*
                    #ob_observer_predicates
                    #depth: ::muon::helper::Unsigned,
                {
                    fn eq(&self, other: &Self) -> bool {
                        let lhs = ::muon::helper::QuasiObserver::untracked_ref(self);
                        let rhs = ::muon::helper::QuasiObserver::untracked_ref(other);
                        lhs.eq(rhs)
                    }
                }
            });
        } else if path.is_ident("Eq") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_observer_impl_generics ::std::cmp::Eq
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#skipped_tys: #ob_lt,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #(#deref_general_predicates,)*
                    #ob_observer_predicates
                    #depth: ::muon::helper::Unsigned,
                {}
            });
        } else if path.is_ident("PartialOrd") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_observer_impl_generics ::std::cmp::PartialOrd
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#skipped_tys: #ob_lt,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #(#deref_general_predicates,)*
                    #ob_observer_predicates
                    #depth: ::muon::helper::Unsigned,
                {
                    fn partial_cmp(&self, other: &Self) -> ::std::option::Option<::std::cmp::Ordering> {
                        let lhs = ::muon::helper::QuasiObserver::untracked_ref(self);
                        let rhs = ::muon::helper::QuasiObserver::untracked_ref(other);
                        lhs.partial_cmp(rhs)
                    }
                }
            });
        } else if path.is_ident("Ord") {
            output.extend(quote! {
                #[automatically_derived]
                impl #ob_observer_impl_generics ::std::cmp::Ord
                for #ob_ident #ob_type_generics
                where
                    #(#input_predicates,)*
                    #(#skipped_tys: #ob_lt,)*
                    #(#field_tys: ::muon::Observe,)*
                    #(#general_predicates,)*
                    #(#deref_general_predicates,)*
                    #ob_observer_predicates
                    #depth: ::muon::helper::Unsigned,
                {
                    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
                        let lhs = ::muon::helper::QuasiObserver::untracked_ref(self);
                        let rhs = ::muon::helper::QuasiObserver::untracked_ref(other);
                        lhs.cmp(rhs)
                    }
                }
            });
        }
    }

    if input_meta.expose {
        output
    } else {
        quote! {
            // `'ob` legitimately appears only in where-clauses for
            // single-field `deref` structs (it constrains the skipped
            // field type; E0392 forbids it on the struct itself).
            #[allow(clippy::extra_unused_lifetimes)]
            const _: () = {
                #output
            };
        }
    }
}
