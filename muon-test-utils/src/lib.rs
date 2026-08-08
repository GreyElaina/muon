//! Test helper macros for muon observer tests.
//!
//! [`__flush!`] flushes an observer into its change stream
//! ([`Changes<(), ()>`](::muon::Changes)) through the core
//! [`ObserveSink`](::muon::observe::ObserveSink), the whole-value-diff
//! encoding used by `observe!` and `muon-store`.

/// Flushes an observer and returns its change stream as `Changes<(), ()>`.
#[macro_export]
macro_rules! __flush {
    ($ob:expr) => {{
        let mut __sink = ::muon::observe::ObserveSink::new();
        ::muon::observe::Flush::flush($ob, &mut __sink);
        __sink.into_changes()
    }};
}
