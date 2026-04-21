use anyhow::Result;

use crate::context::CommandContext;
use git_meta_lib::types::Target;

pub fn run(target_str: &str, key: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;

    let removed = ctx.session.target(&target).remove(key)?;

    if !removed {
        eprintln!("key '{key}' not found");
    }

    Ok(())
}
