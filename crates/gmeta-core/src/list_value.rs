use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha1::{Digest, Sha1};

use crate::error::{Error, Result};

/// A single timestamped entry in a metadata list value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListEntry {
    pub value: String,
    pub timestamp: i64,
}

/// Parse a stored list JSON blob into timestamped entries.
/// Legacy string arrays are assigned deterministic timestamps based on order.
pub fn parse_entries(raw: &str) -> Result<Vec<ListEntry>> {
    let values: Vec<Value> = serde_json::from_str(raw)?;
    let mut entries = Vec::with_capacity(values.len());

    for (idx, value) in values.into_iter().enumerate() {
        match value {
            Value::String(s) => {
                entries.push(ListEntry {
                    value: s,
                    timestamp: legacy_timestamp(idx),
                });
            }
            Value::Object(mut map) => {
                let val_field = map.remove("value").ok_or_else(|| {
                    Error::InvalidValue("list entry missing 'value' field".into())
                })?;
                let value = val_field
                    .as_str()
                    .ok_or_else(|| Error::InvalidValue("list entry 'value' must be string".into()))?
                    .to_string();
                let timestamp = match map.remove("timestamp") {
                    Some(Value::Number(num)) => num.as_i64().ok_or_else(|| {
                        Error::InvalidValue("list entry 'timestamp' must be integer".into())
                    })?,
                    Some(Value::String(s)) => s.parse::<i64>().map_err(|_| {
                        Error::InvalidValue("list entry 'timestamp' must be integer".into())
                    })?,
                    None => legacy_timestamp(idx),
                    Some(other) => {
                        return Err(Error::InvalidValue(format!(
                            "list entry 'timestamp' must be integer, got {other:?}"
                        )))
                    }
                };
                entries.push(ListEntry { value, timestamp });
            }
            other => {
                return Err(Error::InvalidValue(format!(
                    "invalid list entry type: expected string or object, got {other:?}"
                )));
            }
        }
    }

    Ok(entries)
}

/// Serialize list entries back to JSON objects.
pub fn encode_entries(entries: &[ListEntry]) -> Result<String> {
    Ok(serde_json::to_string(entries)?)
}

/// Extract just the string values from a stored list JSON blob.
pub fn list_values_from_json(raw: &str) -> Result<Vec<String>> {
    Ok(parse_entries(raw)?
        .into_iter()
        .map(|entry| entry.value)
        .collect())
}

/// Ensure the proposed timestamp is strictly greater than any existing entry.
pub(crate) fn ensure_unique_timestamp(mut timestamp: i64, entries: &[ListEntry]) -> i64 {
    if let Some(last) = entries.last() {
        if timestamp <= last.timestamp {
            timestamp = last.timestamp + 1;
        }
    }
    timestamp
}

/// Build a deterministic entry name used for Git tree serialization.
pub fn make_entry_name(entry: &ListEntry) -> String {
    make_entry_name_from_parts(entry.timestamp, &entry.value)
}

/// Build a deterministic entry name from a timestamp and value content hash.
pub(crate) fn make_entry_name_from_parts(timestamp: i64, value: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    format!("{}-{}", timestamp, &hash[..5])
}

/// Extract the timestamp from a list entry name (format: `<timestamp>-<hash>`).
pub fn parse_timestamp_from_entry_name(name: &str) -> Option<i64> {
    let idx = name.find('-')?;
    name[..idx].parse().ok()
}

fn legacy_timestamp(idx: usize) -> i64 {
    idx as i64
}
