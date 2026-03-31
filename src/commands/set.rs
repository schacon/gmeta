use std::fs;

use anyhow::{bail, Context, Result};
use chrono::Utc;

use crate::db::Db;
use crate::git_utils;
use crate::list_value::{encode_entries, parse_entries};
use crate::types::{validate_key, Target, ValueType, GIT_REF_THRESHOLD};

struct CommandContext {
    target: Target,
    db: Db,
    email: String,
    timestamp: i64,
}

fn open_context(
    target_str: &str,
    key: &str,
    timestamp_override: Option<i64>,
) -> Result<CommandContext> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let timestamp = timestamp_override.unwrap_or_else(|| Utc::now().timestamp_millis());
    let db = Db::open(&db_path)?;

    Ok(CommandContext {
        target,
        db,
        email,
        timestamp,
    })
}

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
    let ctx = open_context(target_str, key, timestamp)?;
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
        let repo = git_utils::git2_discover_repo()?;
        let blob_oid = repo.blob(raw_value.as_bytes())?;
        ctx.db.set_with_git_ref(
            None,
            ctx.target.type_str(),
            ctx.target.value_str(),
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
            ctx.target.type_str(),
            ctx.target.value_str(),
            key,
            &stored_value,
            value_type.as_str(),
            &ctx.email,
            ctx.timestamp,
        )?;
    }

    print_result("set", key, &ctx.target, json);
    Ok(())
}

pub fn run_add(
    target_str: &str,
    key: &str,
    value: &str,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let ctx = open_context(target_str, key, timestamp)?;
    ctx.db.set_add(
        ctx.target.type_str(),
        ctx.target.value_str(),
        key,
        value,
        &ctx.email,
        ctx.timestamp,
    )?;
    print_result("added", key, &ctx.target, json);
    Ok(())
}

pub fn run_rm(
    target_str: &str,
    key: &str,
    value: &str,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let ctx = open_context(target_str, key, timestamp)?;
    ctx.db.set_rm(
        ctx.target.type_str(),
        ctx.target.value_str(),
        key,
        value,
        &ctx.email,
        ctx.timestamp,
    )?;
    print_result("removed", key, &ctx.target, json);
    Ok(())
}
