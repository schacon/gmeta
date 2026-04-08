use anyhow::{bail, Result};

use crate::commands::{materialize, pull, serialize};
use crate::context::CommandContext;
use gmeta_core::git_utils;

/// Expand shorthand "owner/repo" to a full GitHub SSH URL.
fn expand_url(url: &str) -> String {
    // Already a full URL or path -- leave it alone
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

/// Scan ls-remote output for meta refs under a given namespace.
/// Returns (has_match, other_namespaces) where other_namespaces are
/// namespace prefixes that contain a "main" ref (e.g. "altmeta" from "refs/altmeta/main").
fn check_remote_refs(repo: &gix::Repository, url: &str, ns: &str) -> Result<(bool, Vec<String>)> {
    let output = git_utils::run_git(repo, &["ls-remote", url])?;

    let expected_ref = format!("refs/{}/main", ns);
    let mut has_match = false;
    let mut other_namespaces = Vec::new();

    for line in output.lines() {
        // ls-remote format: "<sha>\t<refname>"
        let refname = match line.split('\t').nth(1) {
            Some(r) => r.trim(),
            None => continue,
        };

        if refname == expected_ref {
            has_match = true;
        } else if let Some(rest) = refname.strip_prefix("refs/") {
            // Look for refs/*/main patterns that could be meta namespaces
            if let Some(candidate_ns) = rest.strip_suffix("/main") {
                // Skip standard git namespaces
                if !matches!(
                    candidate_ns,
                    "heads" | "tags" | "remotes" | "notes" | "stash"
                ) && !candidate_ns.contains('/')
                {
                    other_namespaces.push(candidate_ns.to_string());
                }
            }
        }
    }

    Ok((has_match, other_namespaces))
}

pub fn run_add(url: &str, name: &str, namespace_override: Option<&str>) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();
    let ns = namespace_override.unwrap_or(ctx.namespace()).to_string();
    let url = expand_url(url);

    // Check if this remote name already exists
    let config = repo.config_snapshot();
    let remote_url_key = format!("remote.{}.url", name);
    if config.string(&remote_url_key).is_some() {
        bail!("remote '{}' already exists", name);
    }

    // Check the remote for meta refs before configuring
    eprintln!("Checking {}...", url);
    match check_remote_refs(repo, &url, &ns) {
        Ok((has_match, other_namespaces)) => {
            if !has_match {
                if other_namespaces.is_empty() {
                    bail!(
                        "no metadata refs found on {}\n\n\
                         The remote does not have refs/{}/main or any other recognizable metadata refs.\n\
                         If this is a new remote that will receive metadata via push, use:\n  \
                         gmeta remote add {} --name {} --namespace {}",
                        url, ns, url, name, ns,
                    );
                } else {
                    let found_refs = other_namespaces
                        .iter()
                        .map(|alt| format!("  refs/{}/main", alt))
                        .collect::<Vec<_>>()
                        .join("\n");
                    let suggestions = other_namespaces
                        .iter()
                        .map(|alt| format!("  gmeta remote add {} --namespace={}", url, alt))
                        .collect::<Vec<_>>()
                        .join("\n");
                    bail!(
                        "no metadata refs found under refs/{}/main on {}\n\n\
                         However, metadata refs were found under other namespaces:\n{}\n\n\
                         To use one of these, re-run with --namespace:\n{}",
                        ns,
                        url,
                        found_refs,
                        suggestions,
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("Warning: could not inspect remote refs: {}", e);
            eprintln!("Proceeding with setup anyway...");
        }
    }

    // Write git config entries for the meta remote via subprocess
    // (gix's config mutation API is limited; using git config is the reliable path)
    let git_dir = repo.path();
    let git_dir_str = git_dir.to_string_lossy();
    let run = |args: &[&str]| -> Result<()> {
        let mut full_args = vec!["--git-dir", &git_dir_str, "config"];
        full_args.extend_from_slice(args);
        let output = std::process::Command::new("git")
            .args(&full_args)
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("git config failed: {}", stderr.trim());
        }
        Ok(())
    };

    let prefix = format!("remote.{}", name);
    run(&[&format!("{}.url", prefix), &url])?;
    run(&[
        &format!("{}.fetch", prefix),
        &format!("+refs/{ns}/main:refs/{ns}/remotes/main"),
    ])?;
    run(&[&format!("{}.meta", prefix), "true"])?;
    run(&[&format!("{}.promisor", prefix), "true"])?;
    run(&[&format!("{}.partialclonefilter", prefix), "blob:none"])?;

    // If a non-default namespace was specified, store it so other commands can find it
    if namespace_override.is_some() {
        run(&[&format!("{}.metanamespace", prefix), &ns])?;
    }

    println!("Added meta remote '{}' -> {}", name, url);

    // Initial blobless fetch
    let fetch_refspec = format!("refs/{ns}/main:refs/{ns}/remotes/main");
    eprint!("Fetching metadata (blobless)...");
    match git_utils::run_git(repo, &["fetch", "--filter=blob:none", name, &fetch_refspec]) {
        Ok(_) => {
            eprintln!(" done.");

            // Verify the tracking ref was created
            let remote_ref = format!("{ns}/remotes/main");
            let tracking_ref_name = format!("refs/{}", remote_ref);
            match repo.find_reference(&tracking_ref_name) {
                Ok(r) => {
                    let tip_oid = r.into_fully_peeled_id()?.detach();
                    eprintln!(
                        "  tracking ref: {} -> {}",
                        tracking_ref_name,
                        &tip_oid.to_string()[..12]
                    );
                }
                Err(e) => {
                    eprintln!(
                        "  warning: tracking ref {} not found after fetch: {}",
                        tracking_ref_name, e
                    );
                    eprintln!("You can try again with: gmeta pull");
                    return Ok(());
                }
            }

            // Hydrate tip tree blobs so gix can read the metadata
            eprint!("Hydrating tip blobs...");
            let blob_count = git_utils::hydrate_tip_blobs_counted(repo, name, &remote_ref)?;
            eprintln!(" {} blobs fetched.", blob_count);

            // Materialize remote metadata into local SQLite
            eprint!("Serializing local metadata...");
            serialize::run(false)?;
            eprintln!(" done.");

            eprint!("Materializing remote metadata...");
            materialize::run(None, false, false)?;
            eprintln!(" done.");

            // Index historical keys as promisor entries
            let tracking_ref_name = format!("refs/{}/remotes/main", ns);
            if let Ok(r) = repo.find_reference(&tracking_ref_name) {
                if let Ok(tip_id) = r.into_fully_peeled_id() {
                    let count = pull::insert_promisor_entries_pub(
                        repo,
                        ctx.store(),
                        tip_id.detach(),
                        None,
                        false,
                    )?;
                    if count > 0 {
                        eprintln!("Indexed {} keys from history (available on demand).", count);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("\nWarning: initial fetch failed: {}", e);
            eprintln!("You can fetch later with: gmeta pull");
        }
    }

    Ok(())
}

pub fn run_remove(name: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();
    let ns = ctx.namespace();

    // Verify this is a meta remote
    let config = repo.config_snapshot();
    let meta_key = format!("remote.{}.meta", name);
    let is_meta = config.boolean(&meta_key).unwrap_or(false);
    if !is_meta {
        bail!("'{}' is not a metadata remote (no meta = true)", name);
    }

    // Remove the git config section for this remote via subprocess
    let git_dir = repo.path();
    let git_dir_str = git_dir.to_string_lossy();
    let unset = |key: &str| {
        let _ = std::process::Command::new("git")
            .args(["--git-dir", &git_dir_str, "config", "--unset-all", key])
            .output();
    };

    unset(&format!("remote.{}.url", name));
    unset(&format!("remote.{}.fetch", name));
    unset(&format!("remote.{}.meta", name));
    unset(&format!("remote.{}.promisor", name));
    unset(&format!("remote.{}.partialclonefilter", name));
    unset(&format!("remote.{}.metanamespace", name));

    // Delete refs under refs/{ns}/remotes/
    let ref_prefix = format!("refs/{}/remotes/", ns);
    let mut refs_to_delete = Vec::new();

    let platform = repo.references()?;
    for reference in platform.all()? {
        let reference = reference.map_err(|e| anyhow::anyhow!("{e}"))?;
        let name_str = reference.name().as_bstr().to_string();
        if name_str.starts_with(&ref_prefix) {
            refs_to_delete.push(name_str);
        }
    }

    for refname in &refs_to_delete {
        let reference = repo.find_reference(refname)?;
        reference.delete().map_err(|e| anyhow::anyhow!("{e}"))?;
        println!("Deleted ref {}", refname);
    }

    // Also delete refs under refs/{ns}/local/
    let local_prefix = format!("refs/{}/local/", ns);
    let mut local_refs_to_delete = Vec::new();

    let platform = repo.references()?;
    for reference in platform.all()? {
        let reference = reference.map_err(|e| anyhow::anyhow!("{e}"))?;
        let name_str = reference.name().as_bstr().to_string();
        if name_str.starts_with(&local_prefix) {
            local_refs_to_delete.push(name_str);
        }
    }

    for refname in &local_refs_to_delete {
        let reference = repo.find_reference(refname)?;
        reference.delete().map_err(|e| anyhow::anyhow!("{e}"))?;
        println!("Deleted ref {}", refname);
    }

    println!("Removed meta remote '{}'", name);
    Ok(())
}

pub fn run_list() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();
    let remotes = git_utils::list_meta_remotes(repo)?;

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
