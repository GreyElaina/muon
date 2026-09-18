use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::visit_mut::VisitMut;
use syn::{Expr, ExprClosure, Pat, Token, parse_quote_spanned};

struct Input {
    runtime: syn::Path,
    closure: ExprClosure,
}

impl Parse for Input {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let runtime = input.parse()?;
        input.parse::<Token![,]>()?;
        let closure = input.parse()?;
        if !input.is_empty() {
            return Err(input.error("unexpected tokens after tracked closure"));
        }
        Ok(Self { runtime, closure })
    }
}

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let mut input = syn::parse_macro_input!(input as Input);
    let binding = match input.closure.inputs.first() {
        Some(Pat::Ident(pattern)) if input.closure.inputs.len() == 1 => Some(pattern.ident.clone()),
        _ => None,
    };
    TransformQuasiObserver { binding }.visit_expr_mut(&mut input.closure.body);
    let runtime = input.runtime;
    let body = input.closure.body;
    let span = body.span();
    input.closure.body = Box::new(parse_quote_spanned! { span => {
        #[allow(unused_imports)]
        use #runtime::QuasiObserver as _;
        #body
    }});
    let closure = input.closure;
    quote!(#closure).into()
}

struct TransformQuasiObserver {
    binding: Option<syn::Ident>,
}

impl TransformQuasiObserver {
    fn is_binding(&self, expression: &Expr) -> bool {
        matches!(expression, Expr::Path(path)
            if path.qself.is_none()
                && path.path.leading_colon.is_none()
                && path.path.segments.len() == 1
                && self.binding.as_ref().is_some_and(|binding| path.path.is_ident(binding)))
    }

    fn is_observer_place(&self, expression: &Expr) -> bool {
        match expression {
            Expr::Path(_) => self.is_binding(expression),
            Expr::Field(field) => self.is_observer_place(&field.base),
            Expr::Index(index) => self.is_observer_place(&index.expr),
            Expr::Paren(paren) => self.is_observer_place(&paren.expr),
            Expr::Group(group) => self.is_observer_place(&group.expr),
            _ => false,
        }
    }

    /// Rewrites a place as `*(&mut <place>).tracked_mut()`, reborrowing the binding itself
    /// because the tracked closure receives `&mut Observer`.
    ///
    /// The call must stay a *method* call. The receiver is the observer place, so autoref
    /// specialization selects the implementation: an observer place reaches the observer's
    /// `tracked_mut`, while a place whose trailing projection lands on the model value falls
    /// back to the identity implementation for `&mut T`. A fully qualified call would pin `Self`
    /// to the receiver type instead and leave the `tracked_mut` bounds unprovable as soon as the
    /// place passes through a wrapper observer such as `Box` (`Self::InnerDepth` never
    /// normalizes for `DerefObserver`, which overflows the trait solver).
    fn tracked_mut(&self, expression: &Expr) -> Expr {
        let span = expression.span();
        if self.is_binding(expression) {
            parse_quote_spanned! { span => *(&mut *#expression).tracked_mut() }
        } else {
            parse_quote_spanned! { span => *(&mut #expression).tracked_mut() }
        }
    }

    /// Rewrites an observer place as `*(&<place>).untracked_ref()`. See [`Self::tracked_mut`] for
    /// why the call must use method syntax.
    fn untracked_ref(&self, expression: &Expr) -> Expr {
        if !self.is_observer_place(expression) {
            return expression.clone();
        }
        let span = expression.span();
        if self.is_binding(expression) {
            parse_quote_spanned! { span => *(&*#expression).untracked_ref() }
        } else {
            parse_quote_spanned! { span => *(&#expression).untracked_ref() }
        }
    }
}

impl VisitMut for TransformQuasiObserver {
    fn visit_expr_assign_mut(&mut self, assignment: &mut syn::ExprAssign) {
        syn::visit_mut::visit_expr_assign_mut(self, assignment);
        *assignment.left = self.tracked_mut(&assignment.left);
    }

    fn visit_expr_binary_mut(&mut self, binary: &mut syn::ExprBinary) {
        syn::visit_mut::visit_expr_binary_mut(self, binary);
        match binary.op {
            syn::BinOp::AddAssign(_)
            | syn::BinOp::SubAssign(_)
            | syn::BinOp::MulAssign(_)
            | syn::BinOp::DivAssign(_)
            | syn::BinOp::RemAssign(_)
            | syn::BinOp::BitXorAssign(_)
            | syn::BinOp::BitAndAssign(_)
            | syn::BinOp::BitOrAssign(_)
            | syn::BinOp::ShlAssign(_)
            | syn::BinOp::ShrAssign(_) => {
                *binary.left = self.tracked_mut(&binary.left);
            }
            syn::BinOp::Eq(_)
            | syn::BinOp::Ne(_)
            | syn::BinOp::Le(_)
            | syn::BinOp::Lt(_)
            | syn::BinOp::Ge(_)
            | syn::BinOp::Gt(_) => {
                *binary.left = self.untracked_ref(&binary.left);
                *binary.right = self.untracked_ref(&binary.right);
            }
            syn::BinOp::Add(_)
            | syn::BinOp::Sub(_)
            | syn::BinOp::Mul(_)
            | syn::BinOp::Div(_)
            | syn::BinOp::Rem(_)
            | syn::BinOp::And(_)
            | syn::BinOp::Or(_)
            | syn::BinOp::BitXor(_)
            | syn::BinOp::BitAnd(_)
            | syn::BinOp::BitOr(_)
            | syn::BinOp::Shl(_)
            | syn::BinOp::Shr(_) => {
                *binary.left = self.untracked_ref(&binary.left);
                *binary.right = self.untracked_ref(&binary.right);
            }
            _ => {}
        }
    }

    fn visit_expr_unary_mut(&mut self, unary: &mut syn::ExprUnary) {
        syn::visit_mut::visit_expr_unary_mut(self, unary);
        match unary.op {
            syn::UnOp::Neg(_) | syn::UnOp::Not(_) => {
                *unary.expr = self.untracked_ref(&unary.expr);
            }
            _ => {}
        }
    }

    fn visit_expr_cast_mut(&mut self, cast: &mut syn::ExprCast) {
        syn::visit_mut::visit_expr_cast_mut(self, cast);
        *cast.expr = self.untracked_ref(&cast.expr);
    }

    fn visit_expr_closure_mut(&mut self, _: &mut ExprClosure) {
        // A nested closure has its own bindings and may outlive this observation body.
        // It is a separate syntax boundary, not part of the tracked computation.
    }
}
