use anyhow::Result;

use crate::git_utils;

pub fn run() -> Result<()> {
    let repo = git_utils::git2_discover_repo()?;
    let ns = git_utils::git2_get_namespace(&repo)?;

    // Remove the SQLite database
    let db = git_utils::git2_db_path(&repo)?;
    if db.exists() {
        std::fs::remove_file(&db)?;
        println!("Removed {}", db.display());
    } else {
        println!("No database found at {}", db.display());
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
