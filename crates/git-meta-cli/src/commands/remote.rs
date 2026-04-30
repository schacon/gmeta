use std::io::IsTerminal;

use anyhow::{bail, Context, Result};
use dialoguer::Confirm;
use gix::refs::transaction::PreviousValue;

use crate::commands::materialize;
use crate::context::CommandContext;
use crate::style::Style;

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

/// Prompt the user to confirm initializing a fresh metadata remote.
///
/// Returns `Ok(true)` if the user accepts. Returns `Ok(false)` when stdin is
/// not a terminal (so the caller can bail with an actionable hint instead of
/// hanging in CI), or when the user declines the prompt.
fn prompt_for_init(url: &str, ns: &str) -> Result<bool> {
    if !std::io::stdin().is_terminal() {
        return Ok(false);
    }
    eprintln!();
    eprintln!("No metadata refs (refs/{ns}/main) found on {url}.");
    eprintln!("This looks like a fresh metadata remote.");
    let answer = Confirm::new()
        .with_prompt(format!(
            "Initialize refs/{ns}/main with a starter README commit?"
        ))
        .default(true)
        .interact()
        .unwrap_or(false);
    Ok(answer)
}

/// Ensure `refs/{ns}/local/main` exists, creating it with a README commit if
/// it does not. Returns the OID at the tip of that ref.
///
/// If the local ref already exists (e.g. from a previous project on the same
/// machine), it is reused as-is and no new commit is created -- the caller
/// will simply push whatever is there.
///
/// # Parameters
/// - `ctx`: command context with the open session
/// - `ns`: metadata namespace (e.g. `"meta"`)
/// - `origin_url`: URL of the project's `origin` remote, embedded in the
///   README so the metadata remote is self-describing
/// - `meta_url`: URL of the metadata remote being added, also embedded in
///   the README
fn ensure_local_meta_ref(
    ctx: &CommandContext,
    ns: &str,
    origin_url: &str,
    meta_url: &str,
) -> Result<gix::ObjectId> {
    let repo = ctx.session.repo();
    let local_ref = format!("refs/{ns}/local/main");

    let s = Style::detect_stderr();

    if let Ok(reference) = repo.find_reference(&local_ref) {
        let tip = reference
            .into_fully_peeled_id()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .detach();
        eprintln!(
            "{} existing {local_ref} {}",
            s.ok("Reusing"),
            s.dim(&format!("(tip {})", &tip.to_string()[..12])),
        );
        return Ok(tip);
    }

    let readme = meta_readme_content(origin_url, meta_url, ns);
    let blob_oid: gix::ObjectId = repo
        .write_blob(readme.as_bytes())
        .context("write README blob")?
        .into();
    let tree_oid = {
        let mut editor = repo
            .empty_tree()
            .edit()
            .context("create tree editor for README")?;
        editor
            .upsert("README.md", gix::objs::tree::EntryKind::Blob, blob_oid)
            .context("insert README into tree")?;
        editor.write().context("write README tree")?
    };

    let sig = gix::actor::Signature {
        name: ctx.session.name().into(),
        email: ctx.session.email().into(),
        time: gix::date::Time::now_local_or_utc(),
    };
    let commit = gix::objs::Commit {
        message: format!(
            "git-meta: initialize {ns} metadata\n\n\
             First commit on refs/{ns}/local/main, created by `git meta remote add --init`.\n\
             Stores a README that documents the metadata layout for new contributors."
        )
        .into(),
        tree: tree_oid.into(),
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: vec![].into(),
        extra_headers: Default::default(),
    };

    let commit_oid = repo
        .write_object(&commit)
        .context("write initial metadata commit")?
        .detach();

    repo.reference(
        local_ref.as_str(),
        commit_oid,
        PreviousValue::MustNotExist,
        format!("git-meta: initialize {local_ref}"),
    )
    .map_err(|e| anyhow::anyhow!("create {local_ref}: {e}"))?;

    eprintln!(
        "{} {local_ref} with initial README commit {}",
        s.ok("Created"),
        s.dim(&format!("({})", &commit_oid.to_string()[..12])),
    );
    Ok(commit_oid)
}

/// Generate the README body for the initial metadata commit.
fn meta_readme_content(origin_url: &str, meta_url: &str, namespace: &str) -> String {
    format!(
        r#"# Git Metadata Repository

This ref stores structured metadata for the project at:

    {origin_url}

It is managed by [git meta](https://git-meta.com/), which associates
key-value metadata with Git objects (commits, branches, paths, change-ids,
and project-wide settings) and synchronises them across repositories using
ordinary Git transports.

## How it works

Metadata lives locally in a SQLite database (`.git/git-meta.sqlite`) and is
serialized into Git trees and commits under `refs/{namespace}/` for transport.
This remote stores the canonical history under `refs/{namespace}/main`; the
`main` branch you may see at the repository root is unrelated and only exists
for browsing.

Other contributors do **not** clone this repository directly. Instead they
configure it as a metadata remote on top of their existing checkout:

```
git meta remote add {meta_url} --name meta --namespace {namespace}
git meta pull
```

After that, reading and writing metadata works against the project's normal
checkout:

```
git meta get commit:HEAD
git meta set commit:HEAD review:status approved
git meta push
```

## Important notes

- Metadata is exchanged on `refs/{namespace}/main`, never on `refs/heads/main`.
- Never push directly to `refs/{namespace}/main` -- always go through
  `git meta push`, which serializes local changes and resolves conflicts.
- This README only lives in the very first commit on `refs/{namespace}/main`;
  later metadata commits replace the tip tree with the metadata layout.
"#
    )
}

pub fn run_add(url: &str, name: &str, namespace_override: Option<&str>, init: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let ns = namespace_override
        .unwrap_or(ctx.session.namespace())
        .to_string();
    let url = expand_url(url);

    let s_err = Style::detect_stderr();
    let s_out = Style::detect_stdout();

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
    eprintln!("{} {url}...", s_err.step("Checking"));
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
            eprintln!(
                "{}: could not inspect remote refs: {e}",
                s_err.warn("Warning")
            );
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

    println!("{} meta remote '{name}' -> {url}", s_out.ok("Added"));

    // If we are initializing a fresh remote, create a starter commit on
    // `refs/{ns}/local/main` (or reuse one if it already exists) and push it
    // so the subsequent fetch has something to track.
    if should_init {
        let origin_url = config
            .string("remote.origin.url")
            .map_or_else(|| url.clone(), |s| s.to_string());
        ensure_local_meta_ref(&ctx, &ns, &origin_url, &url)?;

        let push_refspec = format!("refs/{ns}/local/main:refs/{ns}/main");
        eprint!("{} refs/{ns}/main on {name}...", s_err.step("Initializing"));
        match git_meta_lib::git_utils::run_git(repo, &["push", name, &push_refspec]) {
            Ok(_) => eprintln!(" {}", s_err.ok("done.")),
            Err(e) => {
                eprintln!(" {}", s_err.err("failed."));
                bail!(
                    "could not push the initial metadata commit to {name} ({url}): {e}\n\n\
                     The remote was configured locally. To retry the push:\n  \
                     git meta push {name}",
                );
            }
        }
    }

    // Initial blobless fetch
    let fetch_refspec = format!("refs/{ns}/main:refs/{ns}/remotes/main");
    eprint!("{} metadata (blobless)...", s_err.step("Fetching"));
    match git_meta_lib::git_utils::run_git(
        repo,
        &["fetch", "--filter=blob:none", name, &fetch_refspec],
    ) {
        Ok(_) => {
            eprintln!(" {}", s_err.ok("done."));

            // Verify the tracking ref was created
            let remote_ref = format!("{ns}/remotes/main");
            let tracking_ref_name = format!("refs/{remote_ref}");
            match repo.find_reference(&tracking_ref_name) {
                Ok(r) => {
                    let tip_oid = r.into_fully_peeled_id()?.detach();
                    eprintln!(
                        "  {} {} -> {}",
                        s_err.dim("tracking ref:"),
                        tracking_ref_name,
                        s_err.dim(&tip_oid.to_string()[..12]),
                    );
                }
                Err(e) => {
                    eprintln!(
                        "  {}: tracking ref {tracking_ref_name} not found after fetch: {e}",
                        s_err.warn("warning"),
                    );
                    eprintln!("You can try again with: git meta pull");
                    return Ok(());
                }
            }

            // Hydrate tip tree blobs so gix can read the metadata
            eprint!("{} tip blobs...", s_err.step("Hydrating"));
            let blob_count =
                git_meta_lib::git_utils::hydrate_tip_blobs_counted(repo, name, &remote_ref)?;
            eprintln!(" {}", s_err.ok(&format!("{blob_count} blobs fetched.")));

            // Materialize remote metadata into local SQLite
            eprint!("{} local metadata...", s_err.step("Serializing"));
            let _ = ctx.session.serialize()?;
            eprintln!(" {}", s_err.ok("done."));

            eprint!("{} remote metadata...", s_err.step("Materializing"));
            materialize::run(None, false, false, false)?;
            eprintln!(" {}", s_err.ok("done."));

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
                        eprintln!(
                            "{} {count} keys from history {}",
                            s_err.ok("Indexed"),
                            s_err.dim("(available on demand)."),
                        );
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("\n{}: initial fetch failed: {e}", s_err.warn("Warning"));
            eprintln!("You can fetch later with: git meta pull");
        }
    }

    Ok(())
}

pub fn run_remove(name: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let ns = ctx.session.namespace();
    let s_out = Style::detect_stdout();

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
        println!("{} ref {refname}", s_out.ok("Deleted"));
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
        println!("{} ref {refname}", s_out.ok("Deleted"));
    }

    println!("{} meta remote '{name}'", s_out.ok("Removed"));
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
