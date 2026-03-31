use std::fs;

use anyhow::{bail, Context, Result};

use crate::context::CommandContext;
use crate::list_value::{encode_entries, parse_entries};
use crate::types::{validate_key, Target, ValueType, GIT_REF_THRESHOLD};

fn print_result(action: &str, key: &str, target: &Target, json: bool) {
    let target_str = match &target.value {
        Some(v) => format!("{} {}", target.type_str(), v),
        None => target.type_str().to_string(),
    };
    if json {
        let json_obj = serde_json::json!({
            "action": action,
            "key": key,
            "target_type": target.type_str(),
            "target_value": target.value.as_deref().unwrap_or(""),
        });
        println!("{}", serde_json::to_string(&json_obj).unwrap());
    } else {
        println!("{} key {} for {}", action, key, target_str);
    }
}

pub fn run(
    target_str: &str,
    key: &str,
    value: Option<&str>,
    file: Option<&str>,
    value_type_str: &str,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open_gix(timestamp)?;
    ctx.resolve_target(&mut target)?;

    let value_type = ValueType::from_str(value_type_str)?;

    let from_file = file.is_some();
    let raw_value = match (value, file) {
        (Some(_), Some(_)) => bail!("cannot specify both a value and -F/--file"),
        (None, None) => bail!("must specify either a value or -F/--file"),
        (Some(v), None) => v.to_string(),
        (None, Some(path)) => {
            fs::read_to_string(path).with_context(|| format!("failed to read file: {}", path))?
        }
    };

    // For large file imports (>1KB via -F), store as a git blob reference
    let use_git_ref =
        from_file && matches!(value_type, ValueType::String) && raw_value.len() > GIT_REF_THRESHOLD;

    if use_git_ref {
        let git2_repo = ctx.git2_repo()?;
        let blob_oid = git2_repo.blob(raw_value.as_bytes())?;
        ctx.db.set_with_git_ref(
            None,
            target.type_str(),
            target.value_str(),
            key,
            &blob_oid.to_string(),
            value_type.as_str(),
            &ctx.email,
            ctx.timestamp,
            true,
        )?;
    } else {
        let stored_value = match value_type {
            ValueType::String => serde_json::to_string(&raw_value)?,
            ValueType::List => {
                let entries = parse_entries(&raw_value)?;
                encode_entries(&entries)?
            }
            ValueType::Set => {
                let values: Vec<String> = serde_json::from_str(&raw_value)?;
                serde_json::to_string(&values)?
            }
        };

        ctx.db.set(
            target.type_str(),
            target.value_str(),
            key,
            &stored_value,
            value_type.as_str(),
            &ctx.email,
            ctx.timestamp,
        )?;
    }

    print_result("set", key, &target, json);
    Ok(())
}

pub fn run_add(
    target_str: &str,
    key: &str,
    value: &str,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open_gix(timestamp)?;
    ctx.resolve_target(&mut target)?;

    ctx.db.set_add(
        target.type_str(),
        target.value_str(),
        key,
        value,
        &ctx.email,
        ctx.timestamp,
    )?;
    print_result("added", key, &target, json);
    Ok(())
}

pub fn run_rm(
    target_str: &str,
    key: &str,
    value: &str,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open_gix(timestamp)?;
    ctx.resolve_target(&mut target)?;

    ctx.db.set_rm(
        target.type_str(),
        target.value_str(),
        key,
        value,
        &ctx.email,
        ctx.timestamp,
    )?;
    print_result("removed", key, &target, json);
    Ok(())
}
