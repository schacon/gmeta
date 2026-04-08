use anyhow::Result;

use crate::context::CommandContext;

pub fn run(_verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let output = ctx.session.serialize()?;

    if output.changes == 0 {
        println!("no metadata to serialize");
        return Ok(());
    }

    for ref_name in &output.refs_written {
        println!("serialized to {}", ref_name);
    }

    if output.pruned > 0 {
        println!("auto-pruned {} entries", output.pruned);
    }

    Ok(())
}
