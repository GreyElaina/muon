//! Compile-fail regressions for derive-generated observer code.
//!
//! Ui files live in `tests/ui/`; cargo does not compile them as
//! integration tests, trybuild drives them with rustc directly.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
