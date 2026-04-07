//! Filter and routing logic for metadata keys.
//!
//! Determines which keys should be serialized and to which destinations,
//! based on user-configured filter rules stored in the database.

use anyhow::{bail, Result};

use crate::db::Db;
use crate::types::{TargetType, ValueType};

/// Prefix for local-only metadata keys that are never serialized.
pub const META_LOCAL_PREFIX: &str = "meta:local:";

/// The "main" destination name used for the primary ref.
pub const MAIN_DEST: &str = "main";

/// What to do with a key that matches a filter rule.
#[derive(Debug, Clone)]
pub enum FilterAction {
    /// Exclude the key from serialization entirely.
    Exclude,
    /// Route the key to the specified destinations.
    Route(Vec<String>),
}

/// A filter rule consisting of a pattern and an action.
#[derive(Debug, Clone)]
pub struct FilterRule {
    /// The action to take for matching keys.
    pub action: FilterAction,
    /// The segments that form the match pattern.
    pub pattern: Vec<PatternSegment>,
}

/// A single segment in a filter pattern.
#[derive(Debug, Clone)]
pub enum PatternSegment {
    /// Matches exactly this literal segment.
    Literal(String),
    /// Matches one arbitrary segment.
    Star,
    /// Matches zero or more arbitrary segments.
    GlobStar,
}

/// Parse filter rules from the database.
///
/// Reads `meta:local:filter` (higher priority) and `meta:filter` (shared)
/// rules from the project scope, returning them in precedence order.
///
/// # Parameters
///
/// - `db`: the metadata database to read rules from
///
/// # Errors
///
/// Returns an error if a rule is syntactically invalid.
pub fn parse_filter_rules(db: &Db) -> Result<Vec<FilterRule>> {
    let mut rules = Vec::new();

    // meta:local:filter rules first (higher priority)
    if let Some((value, value_type, _)) = db.get(&TargetType::Project, "", "meta:local:filter")? {
        if value_type == ValueType::Set {
            let members: Vec<String> = serde_json::from_str(&value)?;
            for member in members {
                rules.push(parse_rule(&member)?);
            }
        }
    }

    // Then meta:filter rules (shared/corporate)
    if let Some((value, value_type, _)) = db.get(&TargetType::Project, "", "meta:filter")? {
        if value_type == ValueType::Set {
            let members: Vec<String> = serde_json::from_str(&value)?;
            for member in members {
                rules.push(parse_rule(&member)?);
            }
        }
    }

    Ok(rules)
}

/// Parse a single filter rule string.
///
/// Format: `"<action> <pattern> [destinations]"`
fn parse_rule(s: &str) -> Result<FilterRule> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        bail!(
            "invalid filter rule (need at least action and pattern): '{}'",
            s
        );
    }

    let action = match parts[0] {
        "exclude" => FilterAction::Exclude,
        "route" => {
            if parts.len() < 3 {
                bail!("route rule requires a destination: '{}'", s);
            }
            let destinations: Vec<String> = parts[2]
                .split(',')
                .map(|d| d.trim().to_string())
                .filter(|d| !d.is_empty())
                .collect();
            FilterAction::Route(destinations)
        }
        other => bail!("unknown filter action '{}' in rule '{}'", other, s),
    };

    let pattern = parse_pattern(parts[1]);
    Ok(FilterRule { action, pattern })
}

/// Parse a colon-separated pattern into segments.
fn parse_pattern(s: &str) -> Vec<PatternSegment> {
    s.split(':')
        .map(|seg| match seg {
            "**" => PatternSegment::GlobStar,
            "*" => PatternSegment::Star,
            _ => PatternSegment::Literal(seg.to_string()),
        })
        .collect()
}

/// Check whether a pattern matches a sequence of key segments.
fn pattern_matches(pattern: &[PatternSegment], key_segments: &[&str]) -> bool {
    match (pattern.first(), key_segments.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(PatternSegment::GlobStar), _) => {
            if pattern.len() == 1 {
                // trailing ** matches everything remaining
                return true;
            }
            // Try matching ** as zero segments, one segment, two segments, etc.
            for skip in 0..=key_segments.len() {
                if pattern_matches(&pattern[1..], &key_segments[skip..]) {
                    return true;
                }
            }
            false
        }
        (Some(_), None) => false,
        (Some(PatternSegment::Star), Some(_)) => pattern_matches(&pattern[1..], &key_segments[1..]),
        (Some(PatternSegment::Literal(lit)), Some(seg)) => {
            lit == seg && pattern_matches(&pattern[1..], &key_segments[1..])
        }
    }
}

/// Determine the destination(s) for a key based on filter rules.
///
/// Returns `None` if the key should be excluded (either because it starts
/// with `meta:local:` or because an `exclude` rule matched). Returns
/// `Some(destinations)` otherwise, defaulting to `["main"]` if no route
/// rule matched.
///
/// # Parameters
///
/// - `key`: the metadata key to classify
/// - `rules`: the filter rules to check against (in precedence order)
pub fn classify_key(key: &str, rules: &[FilterRule]) -> Option<Vec<String>> {
    // Hard rule: meta:local: keys are never serialized
    if key.starts_with(META_LOCAL_PREFIX) {
        return None;
    }

    let segments: Vec<&str> = key.split(':').collect();
    let mut matched_routes: Vec<String> = Vec::new();
    let mut excluded = false;

    for rule in rules {
        if pattern_matches(&rule.pattern, &segments) {
            match &rule.action {
                FilterAction::Exclude => {
                    excluded = true;
                }
                FilterAction::Route(dests) => {
                    for d in dests {
                        if !matched_routes.contains(d) {
                            matched_routes.push(d.clone());
                        }
                    }
                }
            }
        }
    }

    if excluded {
        return None;
    }

    if matched_routes.is_empty() {
        Some(vec![MAIN_DEST.to_string()])
    } else {
        Some(matched_routes)
    }
}
