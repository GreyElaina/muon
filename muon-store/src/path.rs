use muon::Path;

// ── Types ───────────────────────────────────────────────────────────────

/// A path to a field within a store (root→leaf order).
pub type StorePath = Path;

/// A segment of a store path.
pub use muon::PathSegment as StorePathSegment;
