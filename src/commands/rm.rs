use anyhow::Result;

use crate::context::CommandContext;
use crate::types::{validate_key, Target};

pub fn run(target_str: &str, key: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open_gix(None)?;
    ctx.resolve_target(&mut target)?;

    let removed = ctx.db.rm(
        target.type_str(),
        target.value_str(),
        key,
        &ctx.email,
        ctx.timestamp,
    )?;

    if !removed {
        eprintln!("key '{}' not found", key);
    }

    Ok(())
}
