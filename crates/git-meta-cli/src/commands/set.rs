use std::fs;

use anyhow::{bail, Context, Result};

use crate::context::CommandContext;
use git_meta_lib::types::{MetaValue, Target, ValueType, GIT_REF_THRESHOLD};

/// Print a one-line confirmation for a single-value mutation.
fn print_result(action: &str, key: &str, target: &Target, json: bool) {
    let target_str = match target.value() {
        Some(v) => format!("{} {}", target.target_type().as_str(), v),
        None => target.target_type().as_str().to_string(),
    };
    if json {
        let json_obj = serde_json::json!({
            "action": action,
            "key": key,
            "target_type": target.target_type().as_str(),
            "target_value": target.value().unwrap_or(""),
        });
        println!("{}", serde_json::to_string(&json_obj).unwrap_or_default());
    } else {
        println!("{action} key {key} for {target_str}");
    }
}

/// Set a string value for `key` on `target_str`.
///
/// `git-meta set` only writes string values. Lists and sets have their own
/// dedicated verbs (`list:push`, `list:pop`, `set:add`, `set:rm`).
///
/// Either `value` or `file` must be provided. When `file` is set and the
/// resulting payload exceeds [`GIT_REF_THRESHOLD`] bytes, the value is stored
/// as a Git blob and only its OID is recorded in the database.
///
/// # Errors
///
/// Returns an error if both `value` and `file` are provided, neither is
/// provided, the file cannot be read, or the underlying store operation fails.
pub fn run(
    target_str: &str,
    key: &str,
    value: Option<&str>,
    file: Option<&str>,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let ctx = CommandContext::open(timestamp)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;

    let from_file = file.is_some();
    let raw_value = match (value, file) {
        (Some(_), Some(_)) => bail!("cannot specify both a value and -F/--file"),
        (None, None) => bail!("must specify either a value or -F/--file"),
        (Some(v), None) => v.to_string(),
        (None, Some(path)) => {
            fs::read_to_string(path).with_context(|| format!("failed to read file: {path}"))?
        }
    };

    // For large file imports (>1KB via -F), store as a git blob reference
    let use_git_ref = from_file && raw_value.len() > GIT_REF_THRESHOLD;

    if use_git_ref {
        // Git-ref path: store large file as a blob, then record the OID.
        // This is a power-user path that bypasses the MetaValue API.
        let repo = ctx.session.repo();
        let email = ctx.session.email().to_string();
        let ts = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
        let blob_oid: gix::ObjectId = repo.write_blob(raw_value.as_bytes())?.into();
        ctx.session.store().set_with_git_ref(
            None,
            &target,
            key,
            &blob_oid.to_string(),
            &ValueType::String,
            &email,
            ts,
            true,
        )?;
    } else {
        ctx.session
            .target(&target)
            .set(key, MetaValue::String(raw_value))?;
    }

    print_result("set", key, &target, json);
    Ok(())
}

/// Add a value to the set stored at `key` on `target_str`.
///
/// # Errors
///
/// Returns an error if the target cannot be opened or the store fails to
/// record the addition.
pub fn run_add(
    target_str: &str,
    key: &str,
    value: &str,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let ctx = CommandContext::open(timestamp)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;

    ctx.session.target(&target).set_add(key, value)?;
    print_result("added", key, &target, json);
    Ok(())
}

/// Remove a value from the set stored at `key` on `target_str`.
///
/// # Errors
///
/// Returns an error if the target cannot be opened or the store fails to
/// record the removal.
pub fn run_rm(
    target_str: &str,
    key: &str,
    value: &str,
    json: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let ctx = CommandContext::open(timestamp)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;

    ctx.session.target(&target).set_remove(key, value)?;
    print_result("removed", key, &target, json);
    Ok(())
}
