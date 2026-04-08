use anyhow::{bail, Result};

use crate::context::CommandContext;
use gmeta_core::types::{validate_key, MetaValue, Target, TargetType};

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

fn run_set(handle: &gmeta_core::SessionTargetHandle<'_>, key: &str, value: &str) -> Result<()> {
    let meta_value = MetaValue::String(value.to_string());
    handle.set_value(key, &meta_value)?;
    Ok(())
}

fn run_get(handle: &gmeta_core::SessionTargetHandle<'_>, key: &str) -> Result<()> {
    if let Some(meta_value) = handle.get_value(key)? {
        match meta_value {
            MetaValue::String(s) => println!("{}", s),
            other => println!("{:?}", other),
        }
    }
    Ok(())
}

fn run_list(handle: &gmeta_core::SessionTargetHandle<'_>) -> Result<()> {
    let entries = handle.get_all_values(Some("meta"))?;
    for (key, value) in entries {
        let display = match value {
            MetaValue::String(s) => s,
            other => format!("{:?}", other),
        };
        println!("{} = {}", key, display);
    }
    Ok(())
}

fn run_unset(handle: &gmeta_core::SessionTargetHandle<'_>, key: &str) -> Result<()> {
    let removed = handle.remove(key)?;
    if !removed {
        eprintln!("key '{}' not found", key);
    }
    Ok(())
}
