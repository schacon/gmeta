use anyhow::Result;

use crate::context::CommandContext;

pub fn run() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let ns = ctx.session.namespace();

    // Remove the SQLite database
    let db_path = repo.git_dir().join("git-meta.sqlite");
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
        println!("Removed {}", db_path.display());
    } else {
        println!("No database found at {}", db_path.display());
    }

    // Remove all refs under refs/{namespace}/
    let ref_prefix = format!("refs/{ns}/");
    let mut deleted_refs = Vec::new();

    let platform = repo.references()?;
    for reference in platform.all()? {
        let reference = reference.map_err(|e| anyhow::anyhow!("{e}"))?;
        let name = reference.name().as_bstr().to_string();
        if name.starts_with(&ref_prefix) {
            deleted_refs.push(name);
        }
    }

    if deleted_refs.is_empty() {
        println!("No meta refs found under {ref_prefix}");
    } else {
        for refname in &deleted_refs {
            let reference = repo.find_reference(refname)?;
            reference.delete().map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Deleted ref {refname}");
        }
    }

    println!("Teardown complete.");
    Ok(())
}
