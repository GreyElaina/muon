//! Regression: `observe!`'s natural usage (comparison rewrites and
//! method calls) must be clean under strict aliasing.
//!
//! The macro rewrites comparisons to `*(&x).untracked_ref()` — a
//! single-expression deref that must never hold references across
//! statements. Verified with Miri (`cargo +nightly miri test`).
//!
//! Direct observer manipulation (holding dereference results across
//! statements) is outside the safe contract: the observer
//! infrastructure only guarantees these invariants for the generated
//! paths (see `Pointer`'s safety docs).

#[test]
fn observe_natural_reads_are_safe() {
    let mut vec: Vec<i32> = vec![1, 2, 3];
    let changes = muon::observe!(vec => {
        // Comparison: rewritten to a temporary untracked_ref, used
        // immediately, never held across statements.
        if vec[0] == 1 {
            vec[1] = 20;
        }
        let _ = vec[2] > 1;
        vec.push(4);
    });
    assert!(!changes.is_empty());
}
