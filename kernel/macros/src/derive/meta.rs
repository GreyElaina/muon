use darling::ast::{Data, Fields};
use darling::util::{Flag, PathList, SpannedValue};
use darling::{Error, FromDeriveInput, FromField, FromMeta, FromVariant};
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::punctuated::Punctuated;
use syn::{Attribute, Expr, Generics, Ident, Path, Token, Type, Visibility};

#[derive(Default, FromMeta)]
#[darling(default, and_then = Self::validate)]
struct Choice {
    with: Option<SpannedValue<Path>>,
    shallow: Flag,
}

impl Choice {
    fn validate(self) -> darling::Result<Self> {
        if self.with.is_some() && self.shallow.is_present() {
            Err(
                Error::custom("`with` and `shallow` select competing observers")
                    .with_span(&self.shallow.span()),
            )
        } else {
            Ok(self)
        }
    }
}

#[derive(FromDeriveInput)]
#[darling(
    attributes(observe),
    supports(struct_any, enum_any),
    and_then = Self::validate
)]
pub(super) struct Input {
    pub(super) ident: Ident,
    pub(super) vis: Visibility,
    pub(super) generics: Generics,

    #[darling(default)]
    providers: PathList,
    #[darling(flatten)]
    choice: Choice,

    pub(super) data: Data<Variant, Field>,
}

impl Input {
    fn validate(self) -> darling::Result<Self> {
        let mut errors = Error::accumulator();
        let providers = self.normalized_providers();
        for (index, provider) in self.providers.iter().enumerate() {
            if self.providers[..index]
                .iter()
                .any(|candidate| same_path(candidate, provider))
            {
                errors.push(Error::custom("duplicate provider").with_span(provider));
            }
        }
        if providers.len() > 12 {
            errors.push(Error::custom("at most 12 providers are supported").with_span(&self.ident));
        }
        if let Some(with) = &self.choice.with
            && !self.providers.is_empty()
        {
            errors.push(
                Error::custom(
                    "`with` delegates the whole model and cannot be combined with `providers`",
                )
                .with_span(with.as_ref()),
            );
        }
        if self.choice.shallow.is_present() && !self.providers.is_empty() {
            errors.push(
                Error::custom("`shallow` cannot be combined with `providers`")
                    .with_span(&self.choice.shallow.span()),
            );
        }
        for field in self.fields() {
            for provider in field.wrappers().iter().chain(field.selected_provider()) {
                if !providers
                    .iter()
                    .any(|candidate| same_path(candidate, provider))
                {
                    errors.push(
                        Error::custom("field provider is not declared by `providers(...)`")
                            .with_span(provider),
                    );
                }
            }
            if let Some(terminal) = field.selected_provider() {
                for wrapper in field
                    .wrappers()
                    .iter()
                    .filter(|wrapper| same_path(wrapper, terminal))
                {
                    errors.push(
                        Error::custom("the terminal provider cannot also wrap itself")
                            .with_span(wrapper),
                    );
                }
            }
        }
        if let Data::Struct(fields) = &self.data {
            if fields.fields.len() > 12 {
                errors.push(
                    Error::custom("at most 12 struct fields are supported").with_span(&self.ident),
                );
            }
            let deref_fields = fields.fields.iter().filter(|field| field.deref()).count();
            if deref_fields > 1 {
                for field in fields.fields.iter().filter(|field| field.deref()) {
                    errors.push(
                        Error::custom("only one field can be marked `deref`").with_span(&field.ty),
                    );
                }
            }
        } else {
            if let Data::Enum(variants) = &self.data {
                for variant in variants {
                    if variant.fields.fields.len() > 12 {
                        errors.push(
                            Error::custom("at most 12 fields per enum variant are supported")
                                .with_span(&variant.ident),
                        );
                    }
                }
            }
            for field in self.fields().filter(|field| field.deref()) {
                errors.push(
                    Error::custom("`deref` is only supported on struct fields")
                        .with_span(&field.ty),
                );
            }
        }
        errors.finish_with(self)
    }

    pub(super) fn expand(self) -> TokenStream {
        if self.shallow() {
            return crate::derive::r#struct::expand_shallow(&self);
        }
        if self.with().is_some() {
            return crate::derive::r#struct::expand_delegated(&self);
        }
        match &self.data {
            Data::Struct(_) => crate::derive::r#struct::expand(&self),
            Data::Enum(_) => crate::derive::r#enum::expand(&self),
        }
    }

    pub(super) fn providers(&self) -> Vec<Path> {
        self.normalized_providers()
    }

    pub(super) fn with(&self) -> Option<&Path> {
        self.choice.with.as_deref()
    }

    pub(super) fn shallow(&self) -> bool {
        self.choice.shallow.is_present()
    }

    fn normalized_providers(&self) -> Vec<Path> {
        self.providers.iter().cloned().collect()
    }

    fn fields(&self) -> Box<dyn Iterator<Item = &Field> + '_> {
        match &self.data {
            Data::Struct(fields) => Box::new(fields.fields.iter()),
            Data::Enum(variants) => Box::new(
                variants
                    .iter()
                    .flat_map(|variant| variant.fields.fields.iter()),
            ),
        }
    }
}

#[derive(FromVariant)]
#[darling(attributes(observe))]
pub(super) struct Variant {
    pub(super) ident: Ident,
    pub(super) discriminant: Option<Expr>,
    pub(super) fields: Fields<Field>,
}

#[derive(FromField)]
#[darling(
    forward_attrs(observe, select, scope, shallow, noop),
    and_then = Self::validate
)]
pub(super) struct Field {
    pub(super) ident: Option<Ident>,
    pub(super) vis: Visibility,
    pub(super) ty: Type,

    attrs: Vec<Attribute>,

    #[darling(skip)]
    wrappers: Vec<Path>,
    #[darling(skip)]
    selected: Option<Path>,
    #[darling(skip)]
    shallow: bool,
    #[darling(skip)]
    noop: bool,
    #[darling(skip)]
    deref: bool,
    #[darling(skip)]
    scope_parent: bool,
}

impl Field {
    fn validate(mut self) -> darling::Result<Self> {
        let mut errors = Error::accumulator();
        for attr in core::mem::take(&mut self.attrs) {
            let result = if attr.path().is_ident("observe") {
                self.parse_observe(&attr)
            } else if attr.path().is_ident("select") {
                self.parse_select(&attr)
            } else if attr.path().is_ident("scope") {
                self.parse_scope(&attr)
            } else if attr.path().is_ident("shallow") {
                set_flag(&mut self.shallow, &attr, "shallow")
            } else if attr.path().is_ident("noop") {
                set_flag(&mut self.noop, &attr, "noop")
            } else {
                Ok(())
            };
            if let Err(error) = result {
                errors.push(error);
            }
        }
        if self.shallow && self.noop {
            errors.push(Error::custom(
                "`shallow` and `noop` select competing terminals",
            ));
        }
        if self.selected.is_some() && (self.shallow || self.noop) {
            errors.push(Error::custom(
                "`select` cannot be combined with `shallow` or `noop`",
            ));
        }
        for (index, wrapper) in self.wrappers.iter().enumerate() {
            if self.wrappers[..index]
                .iter()
                .any(|candidate| same_path(candidate, wrapper))
            {
                errors.push(Error::custom("duplicate observer wrapper").with_span(wrapper));
            }
        }
        errors.finish_with(self)
    }

    fn parse_observe(&mut self, attr: &Attribute) -> darling::Result<()> {
        let paths = attr
            .parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)
            .map_err(|error| Error::custom(error.to_string()).with_span(attr))?;
        if paths.is_empty() {
            return Err(Error::custom("`observe` requires a wrapper or `deref`").with_span(attr));
        }
        for path in paths {
            if path.is_ident("deref") {
                if self.deref {
                    return Err(Error::custom("duplicate `deref` projection").with_span(&path));
                }
                self.deref = true;
            } else {
                self.wrappers.push(path);
            }
        }
        Ok(())
    }

    fn parse_select(&mut self, attr: &Attribute) -> darling::Result<()> {
        let selected = attr
            .parse_args::<Path>()
            .map_err(|error| Error::custom(error.to_string()).with_span(attr))?;
        if self.selected.is_some() {
            return Err(Error::custom("only one `select` is allowed").with_span(attr));
        }
        self.selected = Some(selected);
        Ok(())
    }

    fn parse_scope(&mut self, attr: &Attribute) -> darling::Result<()> {
        let scope = attr
            .parse_args::<Path>()
            .map_err(|error| Error::custom(error.to_string()).with_span(attr))?;
        if !scope.is_ident("parent") {
            return Err(Error::custom("scope must be `parent`").with_span(&scope));
        }
        if self.scope_parent {
            return Err(Error::custom("duplicate `scope(parent)`").with_span(attr));
        }
        self.scope_parent = true;
        Ok(())
    }

    pub(super) fn wrappers(&self) -> &[Path] {
        &self.wrappers
    }

    pub(super) fn selected_provider(&self) -> Option<&Path> {
        self.selected.as_ref()
    }

    pub(super) fn shallow(&self) -> bool {
        self.shallow
    }

    pub(super) fn noop(&self) -> bool {
        self.noop
    }

    pub(super) fn deref(&self) -> bool {
        self.deref
    }

    pub(super) fn scope_parent(&self) -> bool {
        self.scope_parent
    }
}

fn set_flag(flag: &mut bool, attr: &Attribute, name: &str) -> darling::Result<()> {
    if !matches!(attr.meta, syn::Meta::Path(_)) {
        return Err(Error::custom(format!("`{name}` does not take arguments")).with_span(attr));
    }
    if *flag {
        return Err(Error::custom(format!("duplicate `{name}`")).with_span(attr));
    }
    *flag = true;
    Ok(())
}

fn same_path(left: &Path, right: &Path) -> bool {
    left.to_token_stream().to_string() == right.to_token_stream().to_string()
}
