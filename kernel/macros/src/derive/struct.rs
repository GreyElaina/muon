use darling::ast::Data;
use proc_macro2::{Ident, TokenStream};
use quote::{ToTokens, format_ident, quote};
use syn::{Index, Path, parse_quote};

use super::meta::{Field, Input};
use super::selection::select_field;
use super::{generic_arguments, lifetime, type_ident, without_defaults};

struct FieldCode {
    member: TokenStream,
    declaration: TokenStream,
    child: TokenStream,
    observer_argument: Option<TokenStream>,
    selection: TokenStream,
    selection_param: Option<Ident>,
    predicate: Option<syn::WherePredicate>,
    quasi_predicate: syn::WherePredicate,
    runtime_predicate: syn::WherePredicate,
    collect_route: Ident,
    construct: TokenStream,
    relocate: TokenStream,
    rebase: TokenStream,
}

pub(super) fn expand(input: &Input) -> TokenStream {
    let Data::Struct(fields) = &input.data else {
        unreachable!()
    };
    let named = fields.style == darling::ast::Style::Struct;
    let unit = fields.style == darling::ast::Style::Unit;
    let input_ident = &input.ident;
    let observer_ident = format_ident!("{}Observer", input_ident);
    let visibility = &input.vis;
    let providers = input.providers();
    let provider_tuple = quote! { (#(#providers,)*) };
    let head = type_ident(&input.generics, "__KernelHead");
    let depth = type_ident(&input.generics, "__KernelDepth");
    let context = type_ident(&input.generics, "__KernelContext");
    let error = type_ident(&input.generics, "__KernelError");
    let semantic = type_ident(&input.generics, "__KernelSemantic");
    let tail = type_ident(&input.generics, "__KernelTail");
    let root_route = type_ident(&input.generics, "__KernelRootRoute");
    let change_lifetime = lifetime(&input.generics, "__kernel_change");
    let deref_index = fields.fields.iter().position(Field::deref);
    let child_params = fields
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            (field.deref() || !field.wrappers().is_empty())
                .then(|| type_ident(&input.generics, &format!("__KernelFieldObserver{index}")))
        })
        .collect::<Vec<_>>();

    let (_, input_type_generics, _) = input.generics.split_for_impl();
    let model = quote! { #input_ident #input_type_generics };
    let input_arguments = generic_arguments(&input.generics);

    let mut field_codes = Vec::new();
    for (index, field) in fields.fields.iter().enumerate() {
        let selection_param = type_ident(&input.generics, &format!("__KernelSelection{index}"));
        let collect_route = type_ident(&input.generics, &format!("__KernelRoute{index}"));
        field_codes.push(field_code(
            field,
            index,
            named,
            &providers,
            &provider_tuple,
            &head,
            &depth,
            child_params[index].as_ref(),
            selection_param,
            collect_route,
        ));
    }

    let selection_params = field_codes
        .iter()
        .filter_map(|field| field.selection_param.as_ref())
        .collect::<Vec<_>>();
    let selection_predicates = field_codes
        .iter()
        .filter_map(|field| field.predicate.clone())
        .collect::<Vec<_>>();
    let children = field_codes
        .iter()
        .map(|field| &field.child)
        .collect::<Vec<_>>();
    let selections = field_codes
        .iter()
        .map(|field| &field.selection)
        .collect::<Vec<_>>();
    let collect_routes = field_codes
        .iter()
        .map(|field| &field.collect_route)
        .collect::<Vec<_>>();
    let members = field_codes
        .iter()
        .map(|field| &field.member)
        .collect::<Vec<_>>();
    let declarations = field_codes
        .iter()
        .map(|field| &field.declaration)
        .collect::<Vec<_>>();
    let constructs = field_codes
        .iter()
        .map(|field| &field.construct)
        .collect::<Vec<_>>();
    let relocates = field_codes
        .iter()
        .map(|field| &field.relocate)
        .collect::<Vec<_>>();
    let rebases = field_codes
        .iter()
        .map(|field| &field.rebase)
        .collect::<Vec<_>>();
    let observer_field_arguments = field_codes
        .iter()
        .filter_map(|field| field.observer_argument.as_ref())
        .collect::<Vec<_>>();
    let quasi_predicates = field_codes
        .iter()
        .map(|field| &field.quasi_predicate)
        .collect::<Vec<_>>();
    let runtime_predicates = field_codes
        .iter()
        .map(|field| &field.runtime_predicate)
        .collect::<Vec<_>>();

    let mut observer_generics = without_defaults(input.generics.clone());
    for selection in &selection_params {
        observer_generics.params.push(parse_quote! { #selection });
    }
    for child in child_params.iter().flatten() {
        observer_generics.params.push(parse_quote! { #child });
    }
    observer_generics
        .params
        .push(parse_quote! { #head: ?Sized });
    observer_generics
        .params
        .push(parse_quote! { #depth = __kernel_runtime::Zero });
    observer_generics
        .make_where_clause()
        .predicates
        .extend(selection_predicates.iter().cloned());
    let observer_declaration_generics = &observer_generics;
    let observer_declaration_where = observer_generics.where_clause.as_ref();
    let (observer_impl_generics, observer_type_generics, observer_where) =
        observer_generics.split_for_impl();

    let observer_arguments = quote! {
        <
            #(#input_arguments,)*
            #(#selection_params,)*
            #(#observer_field_arguments,)*
            #head,
            #depth
        >
    };

    let pointer_member = if named {
        quote! { __kernel_pointer }
    } else {
        Index::from(field_codes.len()).to_token_stream()
    };
    let mutated_member = if named {
        quote! { __kernel_mutated }
    } else if deref_index.is_some() {
        Index::from(field_codes.len()).to_token_stream()
    } else {
        Index::from(field_codes.len() + 1).to_token_stream()
    };

    let pointer_declaration = deref_index
        .is_none()
        .then(|| quote! { __kernel_pointer: __kernel_runtime::Pointer<#head>, });
    let tuple_pointer_declaration = deref_index
        .is_none()
        .then(|| quote! { __kernel_runtime::Pointer<#head>, });

    let observer_struct = if named || unit {
        quote! {
            #visibility struct #observer_ident #observer_declaration_generics
                #observer_declaration_where
            {
                #(#declarations,)*
                #pointer_declaration
                __kernel_mutated: bool,

                __kernel_marker: ::core::marker::PhantomData<(
                    fn(&mut #model),
                    #depth,
                    *mut #head,
                    fn() -> (#(#selection_params,)*),
                )>,
            }
        }
    } else {
        let tuple_declarations = fields
            .fields
            .iter()
            .zip(children.iter())
            .map(|(field, child)| {
                let visibility = &field.vis;
                quote! { #visibility __kernel_runtime::Field<#child> }
            });
        quote! {
            #visibility struct #observer_ident #observer_declaration_generics (
                #(#tuple_declarations,)*
                #tuple_pointer_declaration
                bool,

                ::core::marker::PhantomData<(
                    fn(&mut #model),
                    #depth,
                    *mut #head,
                    fn() -> (#(#selection_params,)*),
                )>,
            ) #observer_declaration_where;
        }
    };

    let pointer_construct = deref_index.is_none().then(
        || quote! { __kernel_pointer: unsafe { __kernel_runtime::Pointer::new_unchecked(head) }, },
    );
    let tuple_pointer_construct = deref_index
        .is_none()
        .then(|| quote! { unsafe { __kernel_runtime::Pointer::new_unchecked(head) }, });
    let constructor = if named || unit {
        quote! {
            Self {
                #(#constructs,)*
                #pointer_construct
                __kernel_mutated: false,
                __kernel_marker: ::core::marker::PhantomData,
            }
        }
    } else {
        quote! {
            Self(
                #(#constructs,)*
                #tuple_pointer_construct
                false,
                ::core::marker::PhantomData,
            )
        }
    };

    let mut quasi_generics = observer_generics.clone();
    {
        let predicates = &mut quasi_generics.make_where_clause().predicates;
        predicates.push(parse_quote! { #depth: __kernel_runtime::Unsigned });
        predicates.push(parse_quote! { #head: __kernel_runtime::AsDeref<#depth, Target = #model> });
        predicates.extend(
            quasi_predicates
                .iter()
                .map(|predicate| (*predicate).clone()),
        );
    }
    let (quasi_impl_generics, quasi_type_generics, quasi_where) = quasi_generics.split_for_impl();

    let mut runtime_generics = observer_generics.clone();
    {
        let predicates = &mut runtime_generics.make_where_clause().predicates;
        predicates.push(parse_quote! { #depth: __kernel_runtime::Unsigned });
        predicates.push(parse_quote! { #head: __kernel_runtime::AsDeref<#depth, Target = #model> });
        predicates.extend(
            runtime_predicates
                .iter()
                .map(|predicate| (*predicate).clone()),
        );
    }
    let (runtime_impl_generics, runtime_type_generics, runtime_where) =
        runtime_generics.split_for_impl();

    let mut collect_generics = without_defaults(input.generics.clone());
    for selection in &selection_params {
        collect_generics.params.push(parse_quote! { #selection });
    }
    for child in child_params.iter().flatten() {
        collect_generics.params.push(parse_quote! { #child });
    }
    collect_generics.params.push(parse_quote! { #head: ?Sized });
    collect_generics.params.push(parse_quote! { #depth });
    collect_generics
        .params
        .push(parse_quote! { #context: ?Sized });
    collect_generics.params.push(parse_quote! { #root_route });
    for route in &collect_routes {
        collect_generics.params.push(parse_quote! { #route });
    }
    collect_generics.params.push(parse_quote! { #error });
    collect_generics.params.push(parse_quote! { #semantic });
    collect_generics.params.push(parse_quote! { #tail });
    {
        let predicates = &mut collect_generics.make_where_clause().predicates;
        predicates.extend(selection_predicates.iter().cloned());
        predicates.push(parse_quote! { #depth: __kernel_runtime::Unsigned });
        predicates.push(parse_quote! { #head: __kernel_runtime::AsDeref<#depth, Target = #model> });
        predicates.extend(
            runtime_predicates
                .iter()
                .map(|predicate| (*predicate).clone()),
        );
        for ((child, route), _) in children
            .iter()
            .zip(collect_routes.iter())
            .zip(members.iter())
        {
            predicates.push(parse_quote! {
                __kernel_runtime::Field<#child>: __kernel_runtime::Collect<
                    #context,
                    #route,
                    #error,
                    __kernel_runtime::Scope<#semantic, #tail>
                >
            });
        }
        predicates.push(parse_quote! {
            for<#change_lifetime> #context: __kernel_runtime::Query<
                __kernel_runtime::Change<#change_lifetime, #model>,
                #root_route,
                #semantic
            >
        });
        predicates.push(parse_quote! {
            for<#change_lifetime> <#context as __kernel_runtime::Query<
                __kernel_runtime::Change<#change_lifetime, #model>,
                #root_route,
                #semantic
            >>::Output: __kernel_runtime::Replace<#model, #model>
        });
        predicates.push(parse_quote! {
            for<#change_lifetime> #error: ::core::convert::From<
                <<#context as __kernel_runtime::Query<
                    __kernel_runtime::Change<#change_lifetime, #model>,
                    #root_route,
                    #semantic
                >>::Output as __kernel_runtime::Replace<#model, #model>>::Error
            >
        });
    }
    let (collect_impl_generics, _, collect_where) = collect_generics.split_for_impl();

    let mut observe_generics = without_defaults(input.generics.clone());
    for selection in &selection_params {
        observe_generics.params.push(parse_quote! { #selection });
    }
    observe_generics
        .make_where_clause()
        .predicates
        .extend(selection_predicates.iter().cloned());
    let (observe_impl_generics, _, observe_where) = observe_generics.split_for_impl();

    let (deref_target, deref_member, outer_depth, quasi_head, deref_mut_body) = if let Some(index) =
        deref_index
    {
        let field = &field_codes[index];
        let child = &field.child;
        let member = &field.member;
        let sibling_members = field_codes
            .iter()
            .enumerate()
            .filter(|(candidate, _)| *candidate != index)
            .map(|(_, field)| &field.member);
        (
            quote! { __kernel_runtime::Field<#child> },
            member.clone(),
            quote! { __kernel_runtime::Succ<<__kernel_runtime::Field<#child> as __kernel_runtime::QuasiObserver>::OuterDepth> },
            quote! { <#child as __kernel_runtime::QuasiObserver>::Head },
            quote! {
                #(__kernel_runtime::QuasiObserver::invalidate(&mut self.#sibling_members);)*
            },
        )
    } else {
        (
            quote! { __kernel_runtime::Pointer<#head> },
            pointer_member.clone(),
            quote! { __kernel_runtime::Succ<__kernel_runtime::Zero> },
            quote! { #head },
            quote! { self.#mutated_member = true; },
        )
    };

    let relocate_pointer = deref_index.is_none().then(|| {
        quote! { __kernel_runtime::Pointer::set_unchecked(&this.#pointer_member, head); }
    });

    let deref_ptr_impl = deref_index.map(|index| {
        let member = &field_codes[index].member;
        let (input_impl_generics, _, input_where) = input.generics.split_for_impl();
        quote! {
            #[automatically_derived]
            unsafe impl #input_impl_generics __kernel_runtime::DerefPtr for #model #input_where {
                unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
                    unsafe { &raw mut (*this).#member }
                }
            }
        }
    });

    let fields_collect = if field_codes.is_empty() {
        quote! { Ok(()) }
    } else {
        quote! {
            let mut fields = __kernel_runtime::Fields::new((#(&mut self.#members,)*));
            __kernel_runtime::Collect::<
                #context,
                (#(#collect_routes,)*),
                #error,
                __kernel_runtime::Scope<#semantic, #tail>
            >::collect(&mut fields, path, context)
        }
    };

    quote! {
        const _: () = {
            #observer_struct

            #[automatically_derived]
            impl #observer_impl_generics ::core::ops::Deref
                for #observer_ident #observer_type_generics
                #observer_where
            {
                type Target = #deref_target;

                fn deref(&self) -> &Self::Target {
                    &self.#deref_member
                }
            }

            #[automatically_derived]
            impl #observer_impl_generics ::core::ops::DerefMut
                for #observer_ident #observer_type_generics
                #observer_where
            {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    #deref_mut_body
                    &mut self.#deref_member
                }
            }

            #[automatically_derived]
            impl #quasi_impl_generics __kernel_runtime::QuasiObserver
                for #observer_ident #quasi_type_generics
                #quasi_where
            {
                type Head = #quasi_head;
                type OuterDepth = #outer_depth;
                type InnerDepth = #depth;

                fn invalidate(this: &mut Self) {
                    this.#mutated_member = true;
                }
            }

            #[automatically_derived]
            unsafe impl #runtime_impl_generics __kernel_runtime::Observer
                for #observer_ident #runtime_type_generics
                #runtime_where
            {
                unsafe fn observe(head: *mut #head) -> Self {
                    unsafe {
                        let value = __kernel_runtime::AsDerefPtrExt::as_deref_ptr::<#depth>(head);
                        let this = #constructor;
                        this
                    }
                }

                unsafe fn relocate(this: &mut Self, head: *mut #head) {
                    unsafe {
                        let value = __kernel_runtime::AsDerefPtrExt::as_deref_ptr::<#depth>(head);
                        #(#relocates)*
                        #relocate_pointer
                    }
                }

                unsafe fn rebase(this: &mut Self, head: *mut #head) {
                    unsafe {
                        let value = __kernel_runtime::AsDerefPtrExt::as_deref_ptr::<#depth>(head);
                        #(#rebases)*
                        #relocate_pointer
                        this.#mutated_member = false;
                    }
                }
            }

            #[automatically_derived]
            impl #collect_impl_generics __kernel_runtime::Collect<
                #context,
                (#root_route, #(#collect_routes,)*),
                #error,
                __kernel_runtime::Scope<#semantic, #tail>
            > for #observer_ident #observer_type_generics
                #collect_where
            {
                fn collect(
                    &mut self,
                    path: &__kernel_runtime::Path<'_>,
                    context: &mut #context,
                ) -> ::core::result::Result<(), #error> {
                    if self.#mutated_member {
                        let value: &#model = __kernel_runtime::QuasiObserver::untracked_ref(self);
                        return __kernel_runtime::emit::<_, _, #context, #root_route, #semantic, #error>(
                            context,
                            __kernel_runtime::Change::Replace {
                                path,
                                before: None,
                                after: value,
                            },
                        );
                    }
                    #fields_collect
                }
            }

            #[automatically_derived]
            impl #observe_impl_generics __kernel_runtime::Observe<
                #model,
                __kernel_runtime::Composite<(#(#selections,)*)>
            > for #model
                #observe_where
            {
                type Observer<#head, #depth> =
                    #observer_ident #observer_arguments
                where
                    #depth: __kernel_runtime::Unsigned,
                    #head: __kernel_runtime::AsDerefMut<#depth, Target = Self> + ?Sized;
            }

            #deref_ptr_impl
        };
    }
}

pub(super) fn expand_shallow(input: &Input) -> TokenStream {
    let input_ident = &input.ident;
    let (_, input_type_generics, _) = input.generics.split_for_impl();
    let model = quote! { #input_ident #input_type_generics };
    let (impl_generics, _, where_clause) = input.generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics __kernel_runtime::Observe for #model #where_clause {
            type Observer<__KernelHead, __KernelDepth> =
                __kernel_runtime::ShallowObserver<Self, __KernelHead, __KernelDepth>
            where
                __KernelDepth: __kernel_runtime::Unsigned,
                __KernelHead: __kernel_runtime::AsDerefMut<__KernelDepth, Target = Self>
                    + ?Sized;
        }
    }
}

pub(super) fn expand_delegated(input: &Input) -> TokenStream {
    let provider = input.with().expect("delegated input");
    let input_ident = &input.ident;
    let (_, input_type_generics, _) = input.generics.split_for_impl();
    let model = quote! { #input_ident #input_type_generics };
    let inner = type_ident(&input.generics, "__KernelInner");
    let mut generics = without_defaults(input.generics.clone());
    generics.params.push(parse_quote! { #inner });
    generics.make_where_clause().predicates.push(parse_quote! {
        __kernel_runtime::Select<(#provider,)>: __kernel_runtime::Observe<
            #model,
            (__kernel_runtime::Current, (__kernel_runtime::Slot<1>, #inner))
        >
    });
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics __kernel_runtime::Observe<
            #model,
            (__kernel_runtime::Current, (__kernel_runtime::Slot<1>, #inner))
        > for #model
            #where_clause
        {
            type Observer<__KernelHead, __KernelDepth> =
                <__kernel_runtime::Select<(#provider,)> as __kernel_runtime::Observe<
                    #model,
                    (__kernel_runtime::Current, (__kernel_runtime::Slot<1>, #inner))
                >>::Observer<__KernelHead, __KernelDepth>
            where
                __KernelDepth: __kernel_runtime::Unsigned,
                __KernelHead: __kernel_runtime::AsDerefMut<__KernelDepth, Target = Self>
                    + ?Sized;
        }
    }
}

fn field_code(
    field: &Field,
    index: usize,
    named: bool,
    providers: &[Path],
    provider_tuple: &TokenStream,
    head: &Ident,
    depth: &Ident,
    observer_child: Option<&Ident>,
    generated_selection_param: Ident,
    collect_route: Ident,
) -> FieldCode {
    let ty = &field.ty;
    let member = field
        .ident
        .as_ref()
        .map(ToTokens::to_token_stream)
        .unwrap_or_else(|| Index::from(index).to_token_stream());
    let step = if let Some(ident) = &field.ident {
        let ident = ident.to_string();
        let name = ident.strip_prefix("r#").unwrap_or(&ident).to_owned();
        quote! { __kernel_runtime::Field::named(__kernel_child, #name) }
    } else {
        quote! { __kernel_runtime::Field::indexed(__kernel_child, #index) }
    };
    let (observer_head, observer_depth) = if field.deref() {
        (quote! { #head }, quote! { __kernel_runtime::Succ<#depth> })
    } else {
        (quote! { #ty }, quote! { __kernel_runtime::Zero })
    };
    let selected = select_field(
        field,
        providers,
        provider_tuple,
        generated_selection_param,
        observer_head,
        observer_depth,
    );

    let observer_argument = observer_child.map(|_| selected.observer.clone());
    let child = if let Some(observer_child) = observer_child {
        observer_child.to_token_stream()
    } else {
        selected.observer.clone()
    };

    let quasi_predicate = if field.deref() {
        parse_quote! {
            #child: __kernel_runtime::QuasiObserver<Head = #head, InnerDepth = __kernel_runtime::Succ<#depth>>
        }
    } else {
        parse_quote! {
            #child: __kernel_runtime::QuasiObserver<Head = #ty, InnerDepth = __kernel_runtime::Zero>
        }
    };
    let runtime_predicate = if field.deref() {
        parse_quote! {
            #child: __kernel_runtime::Observer<Head = #head, InnerDepth = __kernel_runtime::Succ<#depth>>
        }
    } else {
        parse_quote! {
            #child: __kernel_runtime::Observer<Head = #ty, InnerDepth = __kernel_runtime::Zero>
        }
    };

    let visibility = &field.vis;
    let declaration = if named {
        quote! { #visibility #member: __kernel_runtime::Field<#child> }
    } else {
        TokenStream::new()
    };
    let observe_pointer = if field.deref() {
        quote! { head }
    } else {
        quote! { &raw mut (*value).#member }
    };
    let construct = if named {
        quote! {
            #member: {
                let __kernel_child = <#child as __kernel_runtime::Observer>::observe(
                    #observe_pointer,
                );
                #step
            }
        }
    } else {
        quote! {
            {
                let __kernel_child = <#child as __kernel_runtime::Observer>::observe(
                    #observe_pointer,
                );
                #step
            }
        }
    };
    let relocate_pointer = if field.deref() {
        quote! { head }
    } else {
        quote! { &raw mut (*value).#member }
    };
    let relocate = quote! {
        <#child as __kernel_runtime::Observer>::relocate(
            this.#member.observer_mut(),
            #relocate_pointer,
        );
    };
    let rebase = quote! {
        <#child as __kernel_runtime::Observer>::rebase(
            this.#member.observer_mut(),
            #relocate_pointer,
        );
    };

    FieldCode {
        member,
        declaration,
        child,
        observer_argument,
        selection: selected.route,
        selection_param: selected.param,
        predicate: selected.predicate,
        quasi_predicate,
        runtime_predicate,
        collect_route,
        construct,
        relocate,
        rebase,
    }
}
