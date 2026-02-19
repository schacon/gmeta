use anyhow::{bail, Context, Result};
use chrono::Utc;
use std::fs;

use crate::db::Db;
use crate::git_utils;
use crate::types::{Target, ValueType};

pub fn run(
    target_str: &str,
    key: &str,
    value: Option<&str>,
    file: Option<&str>,
    value_type_str: &str,
) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    let value_type = ValueType::from_str(value_type_str)?;

    let raw_value = match (value, file) {
        (Some(_), Some(_)) => bail!("cannot specify both a value and -F/--file"),
        (None, None) => bail!("must specify either a value or -F/--file"),
        (Some(v), None) => v.to_string(),
        (None, Some(path)) => {
            fs::read_to_string(path).with_context(|| format!("failed to read file: {}", path))?
        }
    };

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let timestamp = Utc::now().timestamp_millis();

    let db = Db::open(&db_path)?;

    let stored_value = match value_type {
        ValueType::String => {
            // Store as JSON-encoded string
            serde_json::to_string(&raw_value)?
        }
        ValueType::List => {
            // Value should be a JSON array already; validate it
            let parsed: Vec<serde_json::Value> = serde_json::from_str(&raw_value)?;
            serde_json::to_string(&parsed)?
        }
    };

    db.set(
        target.type_str(),
        target.value_str(),
        key,
        &stored_value,
        value_type.as_str(),
        &email,
        timestamp,
    )?;

    Ok(())
}
