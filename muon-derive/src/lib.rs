#![allow(rustdoc::broken_intra_doc_links)]
#![doc = include_str!("../README.md")]

use proc_macro::TokenStream;

mod derive;
mod observe;

/// Derive the [`Observe`](https://docs.rs/muon/latest/muon/trait.Observe.html) trait to enable mutation tracking.
///
/// This macro automatically generates an [`Observe`](https://docs.rs/muon/latest/muon/trait.Observe.html) implementation, producing a
/// default `Observer` type that wraps the struct and tracks mutations
/// to each field according to that field's own `Observe` implementation.
///
/// ## Requirements
///
/// - The struct or enum must also derive or implement `Serialize`
/// - Named structs, tuple structs, and enums are supported
///
/// ## Customizing Behavior
///
/// If a field type `T` does not implement `Observe`, or you need an alternative
/// observer implementation, you can customize this via the `#[muon(...)]` field attribute inside
/// a `#[derive(Observe)]` struct:
///
/// - `#[muon(noop)]` — use `NoopObserver` for this field
/// - `#[muon(shallow)]` — use `ShallowObserver` for this field
/// - `#[muon(snapshot)]` — use `SnapshotObserver` for this field
///
/// These attributes allow you to override the default `Observer` type
/// that would otherwise come from the field's `Observe` implementation.
///
/// ## Example
///
/// ```
/// use serde::Serialize;
/// use muon::Observe;
///
/// #[derive(Serialize, Observe)]
/// struct User {
///     name: String,         // StringObserver
///     age: i32,             // SnapshotObserver<i32>
///
///     #[muon(noop)]
///     cache: String,        // Not tracked
///
///     #[muon(shallow)]
///     metadata: Metadata,   // ShallowObserver<Metadata>
/// }
///
/// #[derive(Serialize, Observe)]
/// struct Metadata {
///     created_at: String,
///     updated_at: String,
/// }
///
/// // `#[muon(shallow)]` requires snapshot machinery: the shallow
/// // observer compares the whole value against a serialized snapshot.
/// impl ::muon::general::Snapshot for Metadata {
///     type Snapshot = ::serde_json::Value;
///     fn to_snapshot(&self) -> Self::Snapshot {
///         ::serde_json::to_value(self).expect("serialize")
///     }
/// }
/// impl ::muon::general::SerializeSnapshot for Metadata {
///     fn flush<S: ::muon::observe::Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
///         let now = ::serde_json::to_value(self).expect("serialize");
///         if now != snapshot {
///             sink.replace(
///                 Some(&snapshot as &dyn ::muon::erased_serde::Serialize),
///                 Some(&now as &dyn ::muon::erased_serde::Serialize),
///             );
///         }
///     }
/// }
/// ```
#[proc_macro_derive(Observe, attributes(muon))]
pub fn derive_observe(input: TokenStream) -> TokenStream {
    let input: syn::DeriveInput = syn::parse_macro_input!(input);
    derive::derive_observe(input).into()
}

/// Observe and collect mutations within a closure.
///
/// This macro wraps a closure's operations to track all mutations that occur within it. The closure
/// receives a mutable reference to the value, and any mutations made are automatically collected
/// and returned.
///
/// ## Syntax
///
/// ```
/// # use muon::observe;
/// # let mut binding = String::new();
/// let changes: ::muon::Changes<()> =
/// observe!(binding => { /* mutations */ });
/// # let f: &dyn Fn(&mut String) -> ::muon::Changes<()> = &
/// observe!(|binding: &mut String| { /* mutations */ });
/// ```
///
/// ## Example
///
/// ```
/// use serde::Serialize;
/// use muon::{Observe, observe};
///
/// #[derive(Serialize, Observe)]
/// struct Point {
///     x: f64,
///     y: f64,
/// }
///
/// let mut point = Point { x: 1.0, y: 2.0 };
///
/// let changes = observe!(point => {
///     point.x += 1.0;
///     point.y *= 2.0;
/// });
/// assert_eq!(
///     changes.into_json(),
///     serde_json::json!([
///         {"path": ["x"], "before": 1.0, "after": 2.0},
///         {"path": ["y"], "before": 2.0, "after": 4.0},
///     ]),
/// );
/// ```
#[proc_macro]
pub fn observe(input: TokenStream) -> TokenStream {
    let input: observe::ObserveInput = syn::parse_macro_input!(input);
    observe::observe(input).into()
}

#[cfg(test)]
mod test {
    use std::env::var;
    use std::fs::{create_dir_all, read_to_string, write};
    use std::path::{Path, PathBuf};

    use macro_expand::Context;
    use pretty_assertions::StrComparison;
    use prettyplease::unparse;
    use walkdir::WalkDir;

    struct TestDiff {
        path: PathBuf,
        expect: String,
        actual: String,
    }

    #[test]
    fn fixtures() {
        let input_dir = "fixtures/input";
        let output_dir = "fixtures/output";
        let mut diffs = vec![];
        let will_emit = var("EMIT").is_ok_and(|v| !v.is_empty());
        for entry in WalkDir::new(input_dir).into_iter().filter_map(Result::ok) {
            let input_path = entry.path();
            if !input_path.is_file() || input_path.extension() != Some("rs".as_ref()) {
                continue;
            }
            let path = input_path.strip_prefix(input_dir).unwrap();
            let output_path = Path::new(output_dir).join(path);
            let input = read_to_string(input_path).unwrap().parse().unwrap();
            let mut ctx = Context::new();
            ctx.module("muon")
                .proc_macro("observe", crate::observe::observe)
                .proc_macro_derive(
                    "Observe",
                    crate::derive::derive_observe,
                    vec!["muon".into()],
                );
            let actual = unparse(&syn::parse2(ctx.transform(input)).expect("transform parse"));
            let expect_result = read_to_string(&output_path);
            if let Ok(expect) = &expect_result
                && expect == &actual
            {
                continue;
            }
            if will_emit {
                create_dir_all(output_path.parent().unwrap()).unwrap();
                write(output_path, &actual).unwrap();
            }
            if let Ok(expect) = expect_result {
                diffs.push(TestDiff {
                    path: path.to_path_buf(),
                    expect,
                    actual,
                });
            }
        }
        let len = diffs.len();
        for diff in diffs {
            eprintln!("diff {}", diff.path.display());
            eprintln!("{}", StrComparison::new(&diff.expect, &diff.actual));
        }
        if len > 0 && !will_emit {
            panic!("Some tests failed");
        }
    }
}
