use darling::ast::{Data, Style};
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::{Path, parse_quote};

use super::meta::{Field, Input};
use super::selection::select_field;
use super::{generic_arguments, lifetime, type_ident, without_defaults};

struct FieldCode {
    child: Ident,
    actual: TokenStream,
    selection: TokenStream,
    selection_param: Option<Ident>,
    selection_predicate: Option<syn::WherePredicate>,
    runtime_predicate: syn::WherePredicate,
    route: Ident,
    name: Option<Ident>,
    index: usize,
}

pub(super) fn expand(input: &Input) -> TokenStream {
    let Data::Enum(variants) = &input.data else {
        unreachable!()
    };
    let input_ident = &input.ident;
    let observer_ident = format_ident!("{}Observer", input_ident);
    let state_ident = format_ident!("{}ObserverState", input_ident);
    let invalid_ident = invalid_variant(variants);
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
    let (_, input_type_generics, _) = input.generics.split_for_impl();
    let model = quote! { #input_ident #input_type_generics };
    let input_arguments = generic_arguments(&input.generics);

    let mut variant_fields = Vec::new();
    let mut flat = 0;
    for variant in variants {
        let _discriminant = &variant.discriminant;
        let mut fields = Vec::new();
        for (index, field) in variant.fields.fields.iter().enumerate() {
            let child = type_ident(&input.generics, &format!("__KernelChild{flat}"));
            let selection = type_ident(&input.generics, &format!("__KernelSelection{flat}"));
            let route = type_ident(&input.generics, &format!("__KernelRoute{flat}"));
            fields.push(field_code(
                field,
                index,
                child,
                selection,
                route,
                &providers,
                &provider_tuple,
            ));
            flat += 1;
        }
        variant_fields.push(fields);
    }

    let fields = variant_fields.iter().flatten().collect::<Vec<_>>();
    let children = fields.iter().map(|field| &field.child).collect::<Vec<_>>();
    let actual_children = fields.iter().map(|field| &field.actual).collect::<Vec<_>>();
    let selections = fields
        .iter()
        .map(|field| &field.selection)
        .collect::<Vec<_>>();
    let selection_params = fields
        .iter()
        .filter_map(|field| field.selection_param.as_ref())
        .collect::<Vec<_>>();
    let selection_predicates = fields
        .iter()
        .filter_map(|field| field.selection_predicate.clone())
        .collect::<Vec<_>>();
    let runtime_predicates = fields
        .iter()
        .map(|field| field.runtime_predicate.clone())
        .collect::<Vec<_>>();
    let routes = fields.iter().map(|field| &field.route).collect::<Vec<_>>();
    let state_arguments = (!children.is_empty()).then(|| quote! { <#(#children,)*> });

    let state_variants = variants
        .iter()
        .zip(&variant_fields)
        .map(|(variant, fields)| {
            let ident = &variant.ident;
            match variant.fields.style {
                Style::Struct => {
                    let members = fields.iter().map(|field| {
                        let name = field.name.as_ref().expect("named field");
                        let child = &field.child;
                        quote! { #name: __kernel_runtime::Field<#child> }
                    });
                    quote! { #ident { #(#members,)* } }
                }
                Style::Tuple => {
                    let members = fields.iter().map(|field| {
                        let child = &field.child;
                        quote! { __kernel_runtime::Field<#child> }
                    });
                    quote! { #ident(#(#members,)*) }
                }
                Style::Unit => quote! { #ident },
            }
        });

    let construct_arms = variants
        .iter()
        .zip(&variant_fields)
        .map(|(variant, fields)| construct_arm(input_ident, &state_ident, variant, fields))
        .collect::<Vec<_>>();
    let relocate_arms = variants
        .iter()
        .zip(&variant_fields)
        .map(|(variant, fields)| relocate_arm(input_ident, &state_ident, variant, fields));
    let rebase_arms = variants
        .iter()
        .zip(&variant_fields)
        .map(|(variant, fields)| rebase_arm(input_ident, &state_ident, variant, fields));
    let collect_arms = variants
        .iter()
        .zip(&variant_fields)
        .map(|(variant, fields)| {
            collect_arm(
                input_ident,
                &state_ident,
                variant,
                fields,
                &context,
                &error,
                &semantic,
                &tail,
            )
        });

    let mut observer_generics = without_defaults(input.generics.clone());
    for selection in &selection_params {
        observer_generics.params.push(parse_quote! { #selection });
    }
    for child in &children {
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
            #(#actual_children,)*
            #head,
            #depth
        >
    };

    let mut runtime_generics = observer_generics.clone();
    {
        let predicates = &mut runtime_generics.make_where_clause().predicates;
        predicates.push(parse_quote! { #depth: __kernel_runtime::Unsigned });
        predicates.push(parse_quote! { #head: __kernel_runtime::AsDeref<#depth, Target = #model> });
        predicates.extend(runtime_predicates.iter().cloned());
    }
    let (runtime_impl_generics, runtime_type_generics, runtime_where) =
        runtime_generics.split_for_impl();

    let mut collect_generics = runtime_generics.clone();
    collect_generics
        .params
        .push(parse_quote! { #context: ?Sized });
    collect_generics.params.push(parse_quote! { #root_route });
    for route in &routes {
        collect_generics.params.push(parse_quote! { #route });
    }
    collect_generics.params.push(parse_quote! { #error });
    collect_generics.params.push(parse_quote! { #semantic });
    collect_generics.params.push(parse_quote! { #tail });
    {
        let predicates = &mut collect_generics.make_where_clause().predicates;
        for (child, route) in children.iter().zip(&routes) {
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
                __kernel_runtime::Change<#change_lifetime, #model>, #root_route, #semantic
            >
        });
        predicates.push(parse_quote! {
            for<#change_lifetime> <#context as __kernel_runtime::Query<
                __kernel_runtime::Change<#change_lifetime, #model>, #root_route, #semantic
            >>::Output: __kernel_runtime::Replace<#model, #model>
        });
        predicates.push(parse_quote! {
            for<#change_lifetime> #error: ::core::convert::From<
                <<#context as __kernel_runtime::Query<
                    __kernel_runtime::Change<#change_lifetime, #model>, #root_route, #semantic
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
    let rebuild_state = quote! { match &mut *value { #(#construct_arms,)* } };

    quote! {
        const _: () = {
            enum #state_ident #state_arguments {
                #(#state_variants,)*
                #invalid_ident,
            }

            #visibility struct #observer_ident #observer_declaration_generics
                #observer_declaration_where
            {
                state: #state_ident #state_arguments,
                pointer: __kernel_runtime::Pointer<#head>,

                marker: ::core::marker::PhantomData<(
                    fn(&mut #model),
                    #depth,
                    fn() -> (#(#selection_params,)*),
                )>,
            }

            #[automatically_derived]
            impl #observer_impl_generics ::core::ops::Deref
                for #observer_ident #observer_type_generics #observer_where
            {
                type Target = __kernel_runtime::Pointer<#head>;

                fn deref(&self) -> &Self::Target { &self.pointer }
            }

            #[automatically_derived]
            impl #observer_impl_generics ::core::ops::DerefMut
                for #observer_ident #observer_type_generics #observer_where
            {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    self.state = #state_ident::#invalid_ident;
                    &mut self.pointer
                }
            }

            #[automatically_derived]
            impl #runtime_impl_generics __kernel_runtime::QuasiObserver
                for #observer_ident #runtime_type_generics #runtime_where
            {
                type Head = #head;
                type OuterDepth = __kernel_runtime::Succ<__kernel_runtime::Zero>;
                type InnerDepth = #depth;

                fn invalidate(this: &mut Self) {
                    this.state = #state_ident::#invalid_ident;
                }
            }

            #[automatically_derived]
            unsafe impl #runtime_impl_generics __kernel_runtime::Observer
                for #observer_ident #runtime_type_generics #runtime_where
            {
                unsafe fn observe(head: *mut #head) -> Self {
                    unsafe {
                        let value = __kernel_runtime::AsDerefPtrExt::as_deref_ptr::<#depth>(head);
                        let state = #rebuild_state;
                        let this = Self {
                            state,
                            pointer: __kernel_runtime::Pointer::new_unchecked(head),
                            marker: ::core::marker::PhantomData,
                        };
                        this
                    }
                }

                unsafe fn relocate(this: &mut Self, head: *mut #head) {
                    unsafe {
                        let value = __kernel_runtime::AsDerefPtrExt::as_deref_ptr::<#depth>(head);
                        let same = match (&mut this.state, &mut *value) {
                            (#state_ident::#invalid_ident, _) => true,
                            #(#relocate_arms,)*
                            _ => false,
                        };
                        if !same { this.state = #state_ident::#invalid_ident; }
                        __kernel_runtime::Pointer::set_unchecked(&this.pointer, head);
                    }
                }

                unsafe fn rebase(this: &mut Self, head: *mut #head) {
                    unsafe {
                        let value = __kernel_runtime::AsDerefPtrExt::as_deref_ptr::<#depth>(head);
                        let same = match (&mut this.state, &mut *value) {
                            #(#rebase_arms,)*
                            _ => false,
                        };
                        if !same { this.state = #rebuild_state; }
                        __kernel_runtime::Pointer::set_unchecked(&this.pointer, head);
                    }
                }
            }

            #[automatically_derived]
            impl #collect_impl_generics __kernel_runtime::Collect<
                #context,
                (#root_route, #(#routes,)*),
                #error,
                __kernel_runtime::Scope<#semantic, #tail>
            > for #observer_ident #observer_type_generics #collect_where
            {
                fn collect(
                    &mut self,
                    path: &__kernel_runtime::Path<'_>,
                    context: &mut #context,
                ) -> ::core::result::Result<(), #error> {
                    let head = unsafe { __kernel_runtime::Pointer::as_ref(&self.pointer) };
                    let value: &#model = __kernel_runtime::AsDeref::<#depth>::as_deref(head);
                    match (&mut self.state, value) {
                        #(#collect_arms,)*
                        _ => __kernel_runtime::emit::<_, _, #context, #root_route, #semantic, #error>(
                            context,
                            __kernel_runtime::Change::Replace { path, before: None, after: value },
                        ),
                    }
                }
            }

            #[automatically_derived]
            impl #observe_impl_generics __kernel_runtime::Observe<
                #model,
                __kernel_runtime::Composite<(#(#selections,)*)>
            > for #model #observe_where
            {
                type Observer<#head, #depth> =
                    #observer_ident #observer_arguments
                where
                    #depth: __kernel_runtime::Unsigned,
                    #head: __kernel_runtime::AsDerefMut<#depth, Target = Self> + ?Sized;
            }
        };
    }
}

fn construct_arm(
    input: &Ident,
    state: &Ident,
    variant: &super::meta::Variant,
    fields: &[FieldCode],
) -> TokenStream {
    let ident = &variant.ident;
    let values = bindings("value", fields);
    let constructs = fields.iter().zip(&values).map(|(field, value)| {
        let child = &field.child;
        let observer = quote! {
            unsafe { <#child as __kernel_runtime::Observer>::observe(::core::ptr::from_mut(#value)) }
        };
        match &field.name {
            Some(name) => quote! { #name: __kernel_runtime::Field::named(#observer, stringify!(#name)) },
            None => {
                let index = field.index;
                quote! { __kernel_runtime::Field::indexed(#observer, #index) }
            }
        }
    });
    match variant.fields.style {
        Style::Struct => {
            let patterns = fields.iter().zip(&values).map(|(field, value)| {
                let name = field.name.as_ref().expect("named field");
                quote! { #name: #value }
            });
            quote! {
                #input::#ident { #(#patterns,)* } => #state::#ident { #(#constructs,)* }
            }
        }
        Style::Tuple => quote! {
            #input::#ident(#(#values,)*) => #state::#ident(#(#constructs,)*)
        },
        Style::Unit => quote! { #input::#ident => #state::#ident },
    }
}

fn relocate_arm(
    input: &Ident,
    state: &Ident,
    variant: &super::meta::Variant,
    fields: &[FieldCode],
) -> TokenStream {
    let ident = &variant.ident;
    let observers = bindings("observer", fields);
    let values = bindings("value", fields);
    let relocate = fields
        .iter()
        .zip(&observers)
        .zip(&values)
        .map(|((field, observer), value)| {
            let child = &field.child;
            quote! {
                unsafe {
                    <#child as __kernel_runtime::Observer>::relocate(
                        #observer.observer_mut(), ::core::ptr::from_mut(#value),
                    );
                }
            }
        });
    match variant.fields.style {
        Style::Struct => {
            let left = named_patterns(fields, &observers);
            let right = named_patterns(fields, &values);
            quote! {
                (#state::#ident { #(#left,)* }, #input::#ident { #(#right,)* }) => {
                    #(#relocate)* true
                }
            }
        }
        Style::Tuple => quote! {
            (#state::#ident(#(#observers,)*), #input::#ident(#(#values,)*)) => {
                #(#relocate)* true
            }
        },
        Style::Unit => quote! { (#state::#ident, #input::#ident) => true },
    }
}

fn rebase_arm(
    input: &Ident,
    state: &Ident,
    variant: &super::meta::Variant,
    fields: &[FieldCode],
) -> TokenStream {
    let ident = &variant.ident;
    let observers = bindings("observer", fields);
    let values = bindings("value", fields);
    let rebase = fields
        .iter()
        .zip(&observers)
        .zip(&values)
        .map(|((field, observer), value)| {
            let child = &field.child;
            quote! {
                unsafe {
                    <#child as __kernel_runtime::Observer>::rebase(
                        #observer.observer_mut(), ::core::ptr::from_mut(#value),
                    );
                }
            }
        });
    match variant.fields.style {
        Style::Struct => {
            let left = named_patterns(fields, &observers);
            let right = named_patterns(fields, &values);
            quote! {
                (#state::#ident { #(#left,)* }, #input::#ident { #(#right,)* }) => {
                    #(#rebase)* true
                }
            }
        }
        Style::Tuple => quote! {
            (#state::#ident(#(#observers,)*), #input::#ident(#(#values,)*)) => {
                #(#rebase)* true
            }
        },
        Style::Unit => quote! { (#state::#ident, #input::#ident) => true },
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_arm(
    input: &Ident,
    state: &Ident,
    variant: &super::meta::Variant,
    fields: &[FieldCode],
    context: &Ident,
    error: &Ident,
    semantic: &Ident,
    tail: &Ident,
) -> TokenStream {
    let ident = &variant.ident;
    let observers = bindings("observer", fields);
    let routes = fields.iter().map(|field| &field.route);
    let collect = if fields.is_empty() {
        quote! { Ok(()) }
    } else {
        let variant_name = ident.to_string();
        quote! {
            let child = path.child(__kernel_runtime::PathStep::Field(#variant_name));
            let mut fields = __kernel_runtime::Fields::new((#(#observers,)*));
            __kernel_runtime::Collect::<
                #context, (#(#routes,)*), #error, __kernel_runtime::Scope<#semantic, #tail>
            >::collect(&mut fields, &child, context)
        }
    };
    match variant.fields.style {
        Style::Struct => {
            let patterns = named_patterns(fields, &observers);
            quote! {
                (#state::#ident { #(#patterns,)* }, #input::#ident { .. }) => { #collect }
            }
        }
        Style::Tuple => quote! {
            (#state::#ident(#(#observers,)*), #input::#ident(..)) => { #collect }
        },
        Style::Unit => quote! { (#state::#ident, #input::#ident) => Ok(()) },
    }
}

fn bindings(prefix: &str, fields: &[FieldCode]) -> Vec<Ident> {
    fields
        .iter()
        .map(|field| format_ident!("__kernel_{prefix}{}", field.index))
        .collect()
}

fn named_patterns<'a>(
    fields: &'a [FieldCode],
    bindings: &'a [Ident],
) -> impl Iterator<Item = TokenStream> + 'a {
    fields.iter().zip(bindings).map(|(field, binding)| {
        let name = field.name.as_ref().expect("named field");
        quote! { #name: #binding }
    })
}

fn invalid_variant(variants: &[super::meta::Variant]) -> Ident {
    for suffix in 0.. {
        let name = if suffix == 0 {
            "__KernelInvalid".to_owned()
        } else {
            format!("__KernelInvalid{suffix}")
        };
        if variants.iter().all(|variant| variant.ident != name) {
            return Ident::new(&name, proc_macro2::Span::call_site());
        }
    }
    unreachable!()
}

#[allow(clippy::too_many_arguments)]
fn field_code(
    field: &Field,
    index: usize,
    child: Ident,
    generated_selection: Ident,
    route: Ident,
    providers: &[Path],
    provider_tuple: &TokenStream,
) -> FieldCode {
    let ty = &field.ty;
    let selection = select_field(
        field,
        providers,
        provider_tuple,
        generated_selection,
        quote! { #ty },
        quote! { __kernel_runtime::Zero },
    );
    let runtime_predicate = parse_quote! {
        #child: __kernel_runtime::Observer<Head = #ty, InnerDepth = __kernel_runtime::Zero>
    };
    FieldCode {
        child,
        actual: selection.observer,
        selection: selection.route,
        selection_param: selection.param,
        selection_predicate: selection.predicate,
        runtime_predicate,
        route,
        name: field.ident.clone(),
        index,
    }
}
