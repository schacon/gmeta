use anyhow::Result;

use crate::context::CommandContext;
use crate::git_utils;

pub fn run() -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;
    let ns = git_utils::git2_get_namespace(repo)?;

    // Remove the SQLite database
    let db_path = git_utils::git2_db_path(repo)?;
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
        println!("Removed {}", db_path.display());
    } else {
        println!("No database found at {}", db_path.display());
    }

    // Remove all refs under refs/{namespace}/
    let ref_prefix = format!("refs/{}/", ns);
    let references: Vec<String> = repo
        .references_glob(&format!("{}*", ref_prefix))?
        .filter_map(|r| r.ok())
        .filter_map(|r| r.name().map(String::from))
        .collect();

    if references.is_empty() {
        println!("No meta refs found under {}", ref_prefix);
    } else {
        for refname in &references {
            let mut reference = repo.find_reference(refname)?;
            reference.delete()?;
            println!("Deleted ref {}", refname);
        }
    }

    println!("Teardown complete.");
    Ok(())
}
