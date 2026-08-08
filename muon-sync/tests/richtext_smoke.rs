//! Smoke tests for the self-hosted text container: `CrdtString<M>`
//! characters plus style-anchor intervals on the arena sequence
//! engine.
//!
//! Verifies editing, style intervals (Peritext anchors with fixed
//! `After`), clearing, synthesis, serialization round trips and
//! container independence.

use muon::Observe;
use muon_store::Track;
use muon_sync::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Style {
    bold: bool,
}

fn bold() -> Style {
    Style { bold: true }
}

#[test]
fn container_is_independent_and_editable() {
    let mut text: CrdtString<Style> = CrdtString::new();
    assert!(text.is_empty());
    text.insert(0, "hello");
    text.insert(5, " world");
    assert_eq!(text.len(), 11);
    assert_eq!(text.text(), "hello world");
    text.delete(5..6);
    assert_eq!(text.len(), 10);
    assert_eq!(text.text(), "helloworld");
}

#[test]
fn annotate_records_interval_and_runs_merge() {
    let mut text: CrdtString<Style> = CrdtString::new();
    text.insert(0, "abcdef");
    text.annotate(2..4, bold());
    // Runs: "ab" plain, "cd" bold, "ef" plain.
    let spans = text.spans();
    let texts: Vec<&str> = spans.iter().map(|(s, _)| s.as_str()).collect();
    assert_eq!(texts, vec!["ab", "cd", "ef"]);
    let bolds: Vec<bool> = spans
        .iter()
        .map(|(_, styles)| styles.iter().any(|s| s.bold))
        .collect();
    assert_eq!(bolds, vec![false, true, false]);
    assert!(text.styles_at(2).iter().any(|s| s.bold));
    assert!(!text.styles_at(1).iter().any(|s| s.bold));
}

#[test]
fn interval_shrinks_with_deletion_and_follows_inserts() {
    let mut text: CrdtString<Style> = CrdtString::new();
    text.insert(0, "abcdef");
    text.annotate(2..5, bold());
    // Delete inside the interval: "c" gone — the interval covers
    // [start, end) of the *characters*, so "d" and "e" keep bold and
    // "b" was never in it.
    text.delete(2..3);
    let spans = text.spans();
    let bolds: Vec<(char, bool)> = spans
        .iter()
        .flat_map(|(s, styles)| {
            let bold = styles.iter().any(|st| st.bold);
            s.chars().map(move |c| (c, bold)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        bolds,
        vec![
            ('a', false),
            ('b', false),
            ('d', true),
            ('e', true),
            ('f', false)
        ]
    );
}

#[test]
fn unmark_clears_an_interval() {
    let mut text: CrdtString<Style> = CrdtString::new();
    text.insert(0, "abcdef");
    text.annotate(1..5, bold());
    // Clear the middle: "bc" loses bold, "de" keeps it.
    text.unmark(1..3, bold());
    let spans = text.spans();
    let bolds: Vec<(char, bool)> = spans
        .iter()
        .flat_map(|(s, styles)| {
            let bold = styles.iter().any(|st| st.bold);
            s.chars().map(move |c| (c, bold)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        bolds,
        vec![
            ('a', false),
            ('b', false),
            ('c', false),
            ('d', true),
            ('e', true),
            ('f', false)
        ]
    );
}

#[test]
fn serialize_round_trip_preserves_text_and_styles() {
    let mut text: CrdtString<Style> = CrdtString::new();
    text.insert(0, "hello");
    text.annotate(0..5, bold());
    let json = serde_json::to_value(&text).unwrap();
    let restored: CrdtString<Style> = serde_json::from_value(json).unwrap();
    assert_eq!(restored.text(), "hello");
    assert!(restored.styles_at(0).iter().any(|s| s.bold));
    assert_eq!(restored, text);
}

#[test]
fn clone_is_independent() {
    let mut text: CrdtString<Style> = CrdtString::new();
    text.insert(0, "hi");
    let mut copy = text.clone();
    copy.insert(2, "!");
    assert_eq!(text.text(), "hi");
    assert_eq!(copy.text(), "hi!");
}

#[test]
fn overlapping_intervals_nest() {
    let mut text: CrdtString<Style> = CrdtString::new();
    text.insert(0, "abcd");
    text.annotate(0..4, Style { bold: true });
    text.annotate(1..3, Style { bold: false });
    // Overlapping intervals union: inside the inner interval both
    // styles are active (the application decides how to combine
    // them); outside it only the outer one is.
    assert_eq!(
        text.styles_at(0),
        vec![Style { bold: true }],
        "outside the inner interval only the outer style"
    );
    assert_eq!(
        text.styles_at(1),
        vec![Style { bold: true }, Style { bold: false }],
        "inside the overlap both styles are active"
    );
    assert_eq!(
        text.styles_at(3),
        vec![Style { bold: true }],
        "past the inner interval only the outer style"
    );
}
