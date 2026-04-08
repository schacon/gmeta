use anyhow::Result;

use crate::context::CommandContext;

pub fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;

    if verbose {
        let ns = ctx.session.namespace();
        let remote_name = ctx.session.resolve_remote(remote)?;
        let fetch_refspec = format!("refs/{ns}/main:refs/{ns}/remotes/main");
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] fetch refspec: {}", fetch_refspec);
    }

    let output = ctx.session.pull(remote)?;

    if !output.materialized {
        println!("Already up-to-date.");
        return Ok(());
    }

    if output.new_commits > 0 {
        eprintln!(
            "Fetched {} new commit{}.",
            output.new_commits,
            if output.new_commits == 1 { "" } else { "s" }
        );
    }

    if output.indexed_keys > 0 {
        eprintln!(
            "Indexed {} keys from history (available on demand).",
            output.indexed_keys
        );
    }

    println!("Pulled metadata from {}", output.remote_name);
    Ok(())
}
