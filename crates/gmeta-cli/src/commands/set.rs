use std::fs;

use anyhow::{bail, Context, Result};

use crate::context::CommandContext;
use gmeta_core::list_value::parse_entries;
use gmeta_core::types::{validate_key, MetaValue, Target, ValueType, GIT_REF_THRESHOLD};

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
        println!("{}", serde_json::to_string(&json_obj).unwrap_or_default());
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

    let ctx = CommandContext::open(timestamp)?;
    ctx.session.resolve_target(&mut target)?;

    let value_type = value_type_str.parse::<ValueType>()?;

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
        // Git-ref path: store large file as a blob, then record the OID.
        // This is a power-user path that bypasses the MetaValue API.
        let repo = ctx.session.repo();
        let email = ctx.session.email().to_string();
        let ts = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
        let blob_oid: gix::ObjectId = repo.write_blob(raw_value.as_bytes())?.into();
        ctx.session.store().set_with_git_ref(
            None,
            &target.target_type,
            target.value_str(),
            key,
            &blob_oid.to_string(),
            &value_type,
            &email,
            ts,
            true,
        )?;
    } else {
        let handle = ctx.session.target(&target);
        let meta_value = match value_type {
            ValueType::String => MetaValue::String(raw_value),
            ValueType::List => {
                let entries = parse_entries(&raw_value)?;
                MetaValue::List(entries)
            }
            ValueType::Set => {
                let values: Vec<String> = serde_json::from_str(&raw_value)?;
                MetaValue::Set(values.into_iter().collect())
            }
            _ => bail!("unsupported value type"),
        };
        handle.set_value(key, &meta_value)?;
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

    let ctx = CommandContext::open(timestamp)?;
    ctx.session.resolve_target(&mut target)?;

    ctx.session.target(&target).set_add(key, value)?;
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

    let ctx = CommandContext::open(timestamp)?;
    ctx.session.resolve_target(&mut target)?;

    ctx.session.target(&target).set_remove(key, value)?;
    print_result("removed", key, &target, json);
    Ok(())
}
