use anyhow::Result;

use crate::context::CommandContext;
use gmeta_core::types::{validate_key, Target};

pub fn run(target_str: &str, key: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;
    validate_key(key)?;

    let ctx = CommandContext::open(None)?;
    ctx.session.resolve_target(&mut target)?;

    let removed = ctx.session.target(&target).remove(key)?;

    if !removed {
        eprintln!("key '{}' not found", key);
    }

    Ok(())
}
