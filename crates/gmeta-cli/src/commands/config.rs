use anyhow::{bail, Result};

use crate::context::CommandContext;
use gmeta_core::types::{validate_key, Target, TargetType, ValueType};
use gmeta_core::Store;

const CONFIG_PREFIX: &str = "meta:";

pub fn run(list: bool, unset: bool, key: Option<&str>, value: Option<&str>) -> Result<()> {
    let ctx = CommandContext::open(None)?;

    if list {
        return run_list(ctx.session.store());
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
        None => run_get(ctx.session.store(), key),
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

fn project_target() -> Target {
    Target {
        target_type: TargetType::Project,
        value: None,
    }
}

fn run_set(ctx: &CommandContext, key: &str, value: &str) -> Result<()> {
    let meta_value = gmeta_core::types::MetaValue::String(value.to_string());
    ctx.session
        .target(&project_target())
        .set_value(key, &meta_value)?;
    Ok(())
}

fn run_get(db: &Store, key: &str) -> Result<()> {
    let result = db.get(&TargetType::Project, "", key)?;
    if let Some(entry) = result {
        let s: String = serde_json::from_str(&entry.value)?;
        println!("{}", s);
    }
    Ok(())
}

fn run_list(db: &Store) -> Result<()> {
    // Use "meta" (without trailing colon) as the prefix, since get_all
    // appends ":" for LIKE matching: "meta" → matches "meta" OR "meta:%"
    let entries = db.get_all(&TargetType::Project, "", Some("meta"))?;
    for entry in entries {
        let display = match entry.value_type {
            ValueType::String => {
                let s: String = serde_json::from_str(&entry.value)?;
                s
            }
            _ => entry.value,
        };
        println!("{} = {}", entry.key, display);
    }
    Ok(())
}

fn run_unset(ctx: &CommandContext, key: &str) -> Result<()> {
    let removed = ctx.session.target(&project_target()).remove(key)?;
    if !removed {
        eprintln!("key '{}' not found", key);
    }
    Ok(())
}
