use anyhow::{bail, Result};
use chrono::Utc;

use crate::db::Db;
use crate::git_utils;
use crate::types::validate_key;

const CONFIG_PREFIX: &str = "meta:";

pub fn run(list: bool, unset: bool, key: Option<&str>, value: Option<&str>) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    if list {
        return run_list(&db);
    }

    if unset {
        let key = key.ok_or_else(|| anyhow::anyhow!("--unset requires a key"))?;
        validate_config_key(key)?;
        return run_unset(&repo, &db, key);
    }

    let key = key.ok_or_else(|| anyhow::anyhow!("key is required"))?;
    validate_config_key(key)?;

    match value {
        Some(val) => run_set(&repo, &db, key, val),
        None => run_get(&db, key),
    }
}

fn validate_config_key(key: &str) -> Result<()> {
    if !key.starts_with(CONFIG_PREFIX) {
        bail!(
            "config keys must start with '{}' (got '{}')",
            CONFIG_PREFIX,
            key
        );
    }
    validate_key(key)?;
    Ok(())
}

fn run_set(repo: &gix::Repository, db: &Db, key: &str, value: &str) -> Result<()> {
    let email = git_utils::get_email(repo)?;
    let timestamp = Utc::now().timestamp_millis();
    let stored_value = serde_json::to_string(value)?;

    db.set(
        "project",
        "",
        key,
        &stored_value,
        "string",
        &email,
        timestamp,
    )?;
    Ok(())
}

fn run_get(db: &Db, key: &str) -> Result<()> {
    let result = db.get("project", "", key)?;
    if let Some((value, _value_type, _is_git_ref)) = result {
        let s: String = serde_json::from_str(&value)?;
        println!("{}", s);
    }
    Ok(())
}

fn run_list(db: &Db) -> Result<()> {
    // Use "meta" (without trailing colon) as the prefix, since get_all
    // appends ":" for LIKE matching: "meta" → matches "meta" OR "meta:%"
    let entries = db.get_all("project", "", Some("meta"))?;
    for (key, value, value_type, _is_git_ref) in entries {
        let display = match value_type.as_str() {
            "string" => {
                let s: String = serde_json::from_str(&value)?;
                s
            }
            _ => value,
        };
        println!("{} = {}", key, display);
    }
    Ok(())
}

fn run_unset(repo: &gix::Repository, db: &Db, key: &str) -> Result<()> {
    let email = git_utils::get_email(repo)?;
    let timestamp = Utc::now().timestamp_millis();

    let removed = db.rm("project", "", key, &email, timestamp)?;
    if !removed {
        eprintln!("key '{}' not found", key);
    }
    Ok(())
}
