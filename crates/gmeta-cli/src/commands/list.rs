use anyhow::Result;

use crate::context::CommandContext;
use gmeta_core::types::{validate_key, Target};

pub fn run_push(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open(None)?;
    ctx.session.resolve_target(&mut target)?;

    ctx.session.store().list_push(
        &target.target_type,
        target.value_str(),
        key,
        value,
        ctx.session.email(),
        ctx.timestamp,
    )?;

    Ok(())
}

pub fn run_rm(target_str: &str, key: &str, index: Option<usize>) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open(None)?;
    ctx.session.resolve_target(&mut target)?;

    let entries = ctx
        .session
        .store()
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
            ctx.session.store().list_remove(
                &target.target_type,
                target.value_str(),
                key,
                idx,
                ctx.session.email(),
                ctx.timestamp,
            )?;
        }
    }

    Ok(())
}

pub fn run_pop(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open(None)?;
    ctx.session.resolve_target(&mut target)?;

    ctx.session.store().list_pop(
        &target.target_type,
        target.value_str(),
        key,
        value,
        ctx.session.email(),
        ctx.timestamp,
    )?;

    Ok(())
}
