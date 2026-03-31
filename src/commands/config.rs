use anyhow::{bail, Result};

use crate::context::CommandContext;
use crate::db::Db;
use crate::types::validate_key;

const CONFIG_PREFIX: &str = "meta:";

pub fn run(list: bool, unset: bool, key: Option<&str>, value: Option<&str>) -> Result<()> {
    let ctx = CommandContext::open_gix(None)?;

    if list {
        return run_list(&ctx.db);
    }

    if unset {
        let key = key.ok_or_else(|| anyhow::anyhow!("--unset requires a key"))?;
        validate_config_key(key)?;
        return run_unset(&ctx, key);
    }

    let key = key.ok_or_else(|| anyhow::anyhow!("key is required"))?;
    validate_config_key(key)?;

    match value {
        Some(val) => run_set(&ctx, key, val),
        None => run_get(&ctx.db, key),
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

fn run_set(ctx: &CommandContext, key: &str, value: &str) -> Result<()> {
    let stored_value = serde_json::to_string(value)?;

    ctx.db.set(
        "project",
        "",
        key,
        &stored_value,
        "string",
        &ctx.email,
        ctx.timestamp,
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

fn run_unset(ctx: &CommandContext, key: &str) -> Result<()> {
    let removed = ctx.db.rm("project", "", key, &ctx.email, ctx.timestamp)?;
    if !removed {
        eprintln!("key '{}' not found", key);
    }
    Ok(())
}
