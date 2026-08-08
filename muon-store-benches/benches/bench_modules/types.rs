//! Shared types for all benchmark suites.

use muon::Observe;
use muon_reactivity::Reactivity;
use muon_store::Track;

// ── Payload types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, Observe, Track, Reactivity)]
pub struct Small {
    pub value: i32,
}

#[derive(Debug, Clone, serde::Serialize, Observe, Track)]
pub struct Profile {
    pub first_name: String,
    pub last_name: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, Observe, Track, Reactivity)]
pub struct NestedRoot {
    pub profile: Profile,
    pub count: i32,
}

impl Default for Small {
    fn default() -> Self {
        Self { value: 42 }
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            first_name: String::from("Alice"),
            last_name: String::from("Smith"),
        }
    }
}

pub const SUBSCRIBER_COUNTS: &[usize] = &[1, 10, 100];
