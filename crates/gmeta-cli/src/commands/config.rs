use anyhow::{bail, Result};

use crate::context::CommandContext;
use gmeta::types::{validate_key, MetaValue, Target};

const CONFIG_PREFIX: &str = "meta:";

pub fn run(list: bool, unset: bool, key: Option<&str>, value: Option<&str>) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let handle = ctx.session.target(&project_target());

    if list {
        return run_list(&handle);
    }

    if unset {
        let key = key.ok_or_else(|| anyhow::anyhow!("--unset requires a key"))?;
        validate_config_key(key)?;
        return run_unset(&handle, key);
    }

    let key = key.ok_or_else(|| anyhow::anyhow!("key is required"))?;
    validate_config_key(key)?;

    match value {
        Some(val) => run_set(&handle, key, val),
        None => run_get(&handle, key),
    }
}

fn validate_config_key(key: &str) -> Result<()> {
    if !key.starts_with(CONFIG_PREFIX) {
        bail!("config keys must start with '{CONFIG_PREFIX}' (got '{key}')");
    }
    validate_key(key)?;
    Ok(())
}

fn project_target() -> Target {
    Target::project()
}

fn run_set(handle: &gmeta::SessionTargetHandle<'_>, key: &str, value: &str) -> Result<()> {
    let meta_value = MetaValue::String(value.to_string());
    handle.set(key, meta_value)?;
    Ok(())
}

fn run_get(handle: &gmeta::SessionTargetHandle<'_>, key: &str) -> Result<()> {
    if let Some(meta_value) = handle.get_value(key)? {
        match meta_value {
            MetaValue::String(s) => println!("{s}"),
            other => println!("{other:?}"),
        }
    }
    Ok(())
}

fn run_list(handle: &gmeta::SessionTargetHandle<'_>) -> Result<()> {
    let entries = handle.get_all_values(Some("meta"))?;
    for (key, value) in entries {
        let display = match value {
            MetaValue::String(s) => s,
            other => format!("{other:?}"),
        };
        println!("{key} = {display}");
    }
    Ok(())
}

fn run_unset(handle: &gmeta::SessionTargetHandle<'_>, key: &str) -> Result<()> {
    let removed = handle.remove(key)?;
    if !removed {
        eprintln!("key '{key}' not found");
    }
    Ok(())
}
