use anyhow::Result;

use crate::context::CommandContext;
use gmeta_core::types::{validate_key, Target};

pub fn run_push(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open_gix(None)?;
    ctx.resolve_target(&mut target)?;

    ctx.db.list_push(
        &target.target_type,
        target.value_str(),
        key,
        value,
        &ctx.email,
        ctx.timestamp,
    )?;

    Ok(())
}

pub fn run_rm(target_str: &str, key: &str, index: Option<usize>) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open_gix(None)?;
    ctx.resolve_target(&mut target)?;

    let entries = ctx
        .db
        .list_entries(&target.target_type, target.value_str(), key)?;

    match index {
        None => {
            // Display mode: show entries with indices
            if entries.is_empty() {
                println!("(empty list)");
            } else {
                for (i, entry) in entries.iter().enumerate() {
                    let preview = if entry.value.len() > 80 {
                        format!("{}...", &entry.value[..77])
                    } else {
                        entry.value.clone()
                    };
                    println!("[{}] {}", i, preview);
                }
            }
        }
        Some(idx) => {
            ctx.db.list_rm(
                &target.target_type,
                target.value_str(),
                key,
                idx,
                &ctx.email,
                ctx.timestamp,
            )?;
        }
    }

    Ok(())
}

pub fn run_pop(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open_gix(None)?;
    ctx.resolve_target(&mut target)?;

    ctx.db.list_pop(
        &target.target_type,
        target.value_str(),
        key,
        value,
        &ctx.email,
        ctx.timestamp,
    )?;

    Ok(())
}
