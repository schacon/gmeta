use anyhow::{bail, Result};

use crate::git_utils;

/// Expand shorthand "owner/repo" to a full GitHub SSH URL.
fn expand_url(url: &str) -> String {
    // Already a full URL or path — leave it alone
    if url.contains(':') || url.starts_with('/') || url.starts_with('.') {
        return url.to_string();
    }
    // "owner/repo" shorthand (exactly one slash, no other path separators)
    if url.matches('/').count() == 1 {
        let url = url.strip_suffix(".git").unwrap_or(url);
        return format!("git@github.com:{}.git", url);
    }
    url.to_string()
}

pub fn run_add(url: &str, name: &str) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let ns = git_utils::get_namespace(&repo)?;
    let url = expand_url(url);

    // Check if this remote name already exists
    let existing = repo.remotes()?;
    for existing_name in existing.iter().flatten() {
        if existing_name == name {
            bail!("remote '{}' already exists", name);
        }
    }

    // Write git config entries for the meta remote
    let mut config = repo.config()?;
    let prefix = format!("remote.{}", name);

    config.set_str(&format!("{}.url", prefix), &url)?;
    config.set_str(
        &format!("{}.fetch", prefix),
        &format!("+refs/{ns}/main:refs/{ns}/remotes/main"),
    )?;
    config.set_bool(&format!("{}.meta", prefix), true)?;
    config.set_bool(&format!("{}.promisor", prefix), true)?;
    config.set_str(&format!("{}.partialclonefilter", prefix), "blob:none")?;

    println!("Added meta remote '{}' -> {}", name, url);

    // Initial blobless fetch
    let fetch_refspec = format!("refs/{ns}/main:refs/{ns}/remotes/main");
    print!("Fetching metadata...");
    match git_utils::run_git(
        &repo,
        &["fetch", "--filter=blob:none", name, &fetch_refspec],
    ) {
        Ok(_) => {
            println!(" done.");

            // Hydrate tip tree blobs so libgit2 can read the metadata
            let remote_ref = format!("{ns}/remotes/main");
            git_utils::hydrate_tip_blobs(&repo, name, &remote_ref)?;
        }
        Err(e) => {
            eprintln!(
                "\nWarning: initial fetch failed (remote may not have metadata yet): {}",
                e
            );
            eprintln!("You can fetch later with: git fetch {name}");
        }
    }

    Ok(())
}

pub fn run_remove(name: &str) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let ns = git_utils::get_namespace(&repo)?;

    // Verify this is a meta remote
    let config = repo.config()?;
    let meta_key = format!("remote.{}.meta", name);
    match config.get_bool(&meta_key) {
        Ok(true) => {}
        _ => bail!("'{}' is not a metadata remote (no meta = true)", name),
    }

    // Remove the git config section for this remote
    let mut config = repo.config()?;
    config.remove_multivar(&format!("remote.{}.url", name), ".*")?;
    config.remove_multivar(&format!("remote.{}.fetch", name), ".*")?;
    let _ = config.remove_multivar(&format!("remote.{}.meta", name), ".*");
    let _ = config.remove_multivar(&format!("remote.{}.promisor", name), ".*");
    let _ = config.remove_multivar(&format!("remote.{}.partialclonefilter", name), ".*");

    // Delete refs under refs/{ns}/remotes/
    let ref_prefix = format!("refs/{}/remotes/", ns);
    let references: Vec<String> = repo
        .references_glob(&format!("{}*", ref_prefix))?
        .filter_map(|r| r.ok())
        .filter_map(|r| r.name().map(String::from))
        .collect();

    for refname in &references {
        let mut reference = repo.find_reference(refname)?;
        reference.delete()?;
        println!("Deleted ref {}", refname);
    }

    // Also delete refs under refs/{ns}/local/
    let local_prefix = format!("refs/{}/local/", ns);
    let local_refs: Vec<String> = repo
        .references_glob(&format!("{}*", local_prefix))?
        .filter_map(|r| r.ok())
        .filter_map(|r| r.name().map(String::from))
        .collect();

    for refname in &local_refs {
        let mut reference = repo.find_reference(refname)?;
        reference.delete()?;
        println!("Deleted ref {}", refname);
    }

    println!("Removed meta remote '{}'", name);
    Ok(())
}

pub fn run_list() -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let remotes = git_utils::list_meta_remotes(&repo)?;

    if remotes.is_empty() {
        println!("No metadata remotes configured.");
        println!("Add one with: gmeta remote add <url>");
    } else {
        for (name, url) in &remotes {
            println!("{}\t{}", name, url);
        }
    }

    Ok(())
}
