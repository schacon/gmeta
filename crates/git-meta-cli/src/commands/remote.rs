use anyhow::{bail, Result};

use crate::commands::{materialize, serialize};
use crate::context::CommandContext;

/// Expand shorthand "owner/repo" to a full GitHub SSH URL.
fn expand_url(url: &str) -> String {
    // Already a full URL or path -- leave it alone
    if url.contains(':') || url.starts_with('/') || url.starts_with('.') {
        return url.to_string();
    }
    // "owner/repo" shorthand (exactly one slash, no other path separators)
    if url.matches('/').count() == 1 {
        let url = url.strip_suffix(".git").unwrap_or(url);
        return format!("git@github.com:{url}.git");
    }
    url.to_string()
}

/// Scan ls-remote output for meta refs under a given namespace.
/// Returns (has_match, other_namespaces) where other_namespaces are
/// namespace prefixes that contain a "main" ref (e.g. "altmeta" from "refs/altmeta/main").
fn check_remote_refs(
    session: &git_meta_lib::Session,
    url: &str,
    ns: &str,
) -> Result<(bool, Vec<String>)> {
    let output = git_meta_lib::git_utils::run_git(session.repo(), &["ls-remote", url])?;

    let expected_ref = format!("refs/{ns}/main");
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
    let repo = ctx.session.repo();
    let ns = namespace_override
        .unwrap_or(ctx.session.namespace())
        .to_string();
    let url = expand_url(url);

    // Check if this remote name already exists
    let config = repo.config_snapshot();
    let remote_url_key = format!("remote.{name}.url");
    if config.string(&remote_url_key).is_some() {
        bail!("remote '{name}' already exists");
    }

    // Check the remote for meta refs before configuring. If none are found
    // under the requested namespace and the user has opted in (either via
    // `--init` or by confirming an interactive prompt), we will initialize
    // the remote with a README commit on `refs/{ns}/main` after configuring.
    eprintln!("Checking {url}...");
    let mut should_init = false;
    match check_remote_refs(&ctx.session, &url, &ns) {
        Ok((has_match, other_namespaces)) => {
            if !has_match {
                if !other_namespaces.is_empty() {
                    let found_refs = other_namespaces
                        .iter()
                        .map(|alt| format!("  refs/{alt}/main"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    let suggestions = other_namespaces
                        .iter()
                        .map(|alt| format!("  git meta remote add {url} --namespace={alt}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    bail!(
                        "no metadata refs found under refs/{ns}/main on {url}\n\n\
                         However, metadata refs were found under other namespaces:\n{found_refs}\n\n\
                         To use one of these, re-run with --namespace:\n{suggestions}",
                    );
                }

                // No metadata refs anywhere on the remote. Decide whether to
                // initialize it with a starter README commit.
                should_init = init || prompt_for_init(&url, &ns)?;
                if !should_init {
                    bail!(
                        "no metadata refs found on {url}\n\n\
                         The remote does not have refs/{ns}/main or any other recognizable metadata refs.\n\
                         If this is a new metadata remote, re-run with --init to create refs/{ns}/main with a README:\n  \
                         git meta remote add {url} --name {name} --namespace {ns} --init",
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("Warning: could not inspect remote refs: {e}");
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

    let prefix = format!("remote.{name}");
    run(&[&format!("{prefix}.url"), &url])?;
    run(&[
        &format!("{prefix}.fetch"),
        &format!("+refs/{ns}/main:refs/{ns}/remotes/main"),
    ])?;
    run(&[&format!("{prefix}.meta"), "true"])?;
    run(&[&format!("{prefix}.promisor"), "true"])?;
    run(&[&format!("{prefix}.partialclonefilter"), "blob:none"])?;

    // If a non-default namespace was specified, store it so other commands can find it
    if namespace_override.is_some() {
        run(&[&format!("{prefix}.metanamespace"), &ns])?;
    }

    println!("Added meta remote '{name}' -> {url}");

    // If we are initializing a fresh remote, create a starter commit on
    // `refs/{ns}/local/main` (or reuse one if it already exists) and push it
    // so the subsequent fetch has something to track.
    if should_init {
        let origin_url = config
            .string("remote.origin.url")
            .map_or_else(|| url.clone(), |s| s.to_string());
        ensure_local_meta_ref(&ctx, &ns, &origin_url, &url)?;

        let push_refspec = format!("refs/{ns}/local/main:refs/{ns}/main");
        eprint!("Initializing refs/{ns}/main on {name}...");
        match git_meta_lib::git_utils::run_git(repo, &["push", name, &push_refspec]) {
            Ok(_) => eprintln!(" done."),
            Err(e) => {
                eprintln!(" failed.");
                bail!(
                    "could not push the initial metadata commit to {name} ({url}): {e}\n\n\
                     The remote was configured locally. To retry the push:\n  \
                     git meta push --remote {name}",
                );
            }
        }
    }

    // Initial blobless fetch
    let fetch_refspec = format!("refs/{ns}/main:refs/{ns}/remotes/main");
    eprint!("Fetching metadata (blobless)...");
    match git_meta_lib::git_utils::run_git(
        repo,
        &["fetch", "--filter=blob:none", name, &fetch_refspec],
    ) {
        Ok(_) => {
            eprintln!(" done.");

            // Verify the tracking ref was created
            let remote_ref = format!("{ns}/remotes/main");
            let tracking_ref_name = format!("refs/{remote_ref}");
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
                        "  warning: tracking ref {tracking_ref_name} not found after fetch: {e}"
                    );
                    eprintln!("You can try again with: git meta pull");
                    return Ok(());
                }
            }

            // Hydrate tip tree blobs so gix can read the metadata
            eprint!("Hydrating tip blobs...");
            let blob_count =
                git_meta_lib::git_utils::hydrate_tip_blobs_counted(repo, name, &remote_ref)?;
            eprintln!(" {blob_count} blobs fetched.");

            // Materialize remote metadata into local SQLite
            eprint!("Serializing local metadata...");
            serialize::run(false)?;
            eprintln!(" done.");

            eprint!("Materializing remote metadata...");
            materialize::run(None, false, false)?;
            eprintln!(" done.");

            // Index historical keys as promisor entries
            let tracking_ref_name = format!("refs/{ns}/remotes/main");
            if let Ok(r) = repo.find_reference(&tracking_ref_name) {
                if let Ok(tip_id) = r.into_fully_peeled_id() {
                    let count = git_meta_lib::sync::insert_promisor_entries(
                        repo,
                        ctx.session.store(),
                        tip_id.detach(),
                        None,
                    )?;
                    if count > 0 {
                        eprintln!("Indexed {count} keys from history (available on demand).");
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("\nWarning: initial fetch failed: {e}");
            eprintln!("You can fetch later with: git meta pull");
        }
    }

    Ok(())
}

pub fn run_remove(name: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let ns = ctx.session.namespace();

    // Verify this is a meta remote
    let config = repo.config_snapshot();
    let meta_key = format!("remote.{name}.meta");
    let is_meta = config.boolean(&meta_key).unwrap_or(false);
    if !is_meta {
        bail!("'{name}' is not a metadata remote (no meta = true)");
    }

    // Remove the git config section for this remote via subprocess
    let git_dir = repo.path();
    let git_dir_str = git_dir.to_string_lossy();
    let unset = |key: &str| {
        let _ = std::process::Command::new("git")
            .args(["--git-dir", &git_dir_str, "config", "--unset-all", key])
            .output();
    };

    unset(&format!("remote.{name}.url"));
    unset(&format!("remote.{name}.fetch"));
    unset(&format!("remote.{name}.meta"));
    unset(&format!("remote.{name}.promisor"));
    unset(&format!("remote.{name}.partialclonefilter"));
    unset(&format!("remote.{name}.metanamespace"));

    // Delete refs under refs/{ns}/remotes/
    let ref_prefix = format!("refs/{ns}/remotes/");
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
        println!("Deleted ref {refname}");
    }

    // Also delete refs under refs/{ns}/local/
    let local_prefix = format!("refs/{ns}/local/");
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
        println!("Deleted ref {refname}");
    }

    println!("Removed meta remote '{name}'");
    Ok(())
}

pub fn run_list() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let remotes = git_meta_lib::git_utils::list_meta_remotes(ctx.session.repo())?;

    if remotes.is_empty() {
        println!("No metadata remotes configured.");
        println!("Add one with: git meta remote add <url>");
    } else {
        for (name, url) in &remotes {
            println!("{name}\t{url}");
        }
    }

    Ok(())
}
