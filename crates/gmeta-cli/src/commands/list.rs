use anyhow::Result;

use crate::context::CommandContext;
use gmeta::types::Target;

pub fn run_push(target_str: &str, key: &str, value: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;

    ctx.session.target(&target).list_push(key, value)?;

    Ok(())
}

pub fn run_rm(target_str: &str, key: &str, index: Option<usize>) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;

    let entries = ctx.session.store().list_entries(&target, key)?;

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
                    println!("[{i}] {preview}");
                }
            }
        }
        Some(idx) => {
            ctx.session.target(&target).list_remove(key, idx)?;
        }
    }

    Ok(())
}

pub fn run_pop(target_str: &str, key: &str, value: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;

    ctx.session.target(&target).list_pop(key, value)?;

    Ok(())
}
