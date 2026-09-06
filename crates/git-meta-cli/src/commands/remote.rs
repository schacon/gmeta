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

/// Give a brand-new metadata store a working auto-prune policy.
///
/// Without one the published tree grows without limit, and every consumer pays
/// for that growth on every clone. A project that wants unbounded metadata can
/// unset these; a project that never thinks about it gets a tree that stays a
/// reasonable size.
///
/// Ten thousand keys is roughly a few megabytes of tree at typical value sizes
/// — small enough to clone quickly, large enough that a project has to be
/// genuinely busy before anything is dropped. Pruning back to five thousand
/// buys room to grow again, so it happens occasionally rather than on every
/// publish.
///
/// Existing configuration is never overwritten: these are defaults for a store
/// that has none, not a policy imposed on one that does.
///
/// Returns whether defaults were written.
fn write_default_prune_rules(ctx: &CommandContext) -> Result<bool> {
    const DEFAULT_MAX_KEYS: &str = "10000";
    const DEFAULT_MIN_KEYS: &str = "5000";

    let project = git_meta_lib::types::Target::project();
    let store = ctx.session.store();
    if store.get(&project, "meta:prune:max-keys")?.is_some()
        || store.get(&project, "meta:prune:max-size")?.is_some()
    {
        return Ok(false);
    }

    let handle = ctx.session.target(&project);
    handle.set(
        "meta:prune:max-keys",
        git_meta_lib::types::MetaValue::String(DEFAULT_MAX_KEYS.to_string()),
    )?;
    handle.set(
        "meta:prune:min-keys",
        git_meta_lib::types::MetaValue::String(DEFAULT_MIN_KEYS.to_string()),
    )?;

    let s = Style::detect_stderr();
    eprintln!(
        "{} auto-prune {}",
        s.ok("Configured"),
        s.dim(&format!(
            "(keep the published tree under {DEFAULT_MAX_KEYS} keys, pruning back to {DEFAULT_MIN_KEYS})"
        )),
    );
    Ok(true)
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

fn has_primary_meta_remote(repo: &gix::Repository) -> bool {
    let config = repo.config_snapshot();
    repo.remote_names().iter().any(|name| {
        config.boolean(&format!("remote.{name}.meta")) == Some(true)
            && config.boolean(&format!("remote.{name}.metaside")) != Some(true)
    })
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

/// Render an elapsed duration for progress output.
///
/// Setting up against a large metadata ref takes minutes, and which phase the
/// time went to is the useful part.
fn elapsed(started: std::time::Instant) -> String {
    let seconds = started.elapsed().as_secs_f64();
    if seconds < 1.0 {
        format!("{:.0} ms", seconds * 1000.0)
    } else if seconds < 60.0 {
        format!("{seconds:.1} s")
    } else {
        format!("{:.0} m {:02.0} s", seconds / 60.0, seconds % 60.0)
    }
}

pub(crate) fn run_add(
    url: &str,
    name: &str,
    namespace_override: Option<&str>,
    init: bool,
    depth: Option<u32>,
) -> Result<()> {
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
    let side_ref = has_primary_meta_remote(repo);

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
    let configured_tracking_ref = if side_ref {
        format!("refs/{ns}/remotes/{name}/main")
    } else {
        format!("refs/{ns}/remotes/main")
    };
    run(&[
        &format!("{prefix}.fetch"),
        &format!("+refs/{ns}/main:{configured_tracking_ref}"),
    ])?;
    run(&[&format!("{prefix}.meta"), "true"])?;
    if side_ref {
        run(&[&format!("{prefix}.metaside"), "true"])?;
    }
    run(&[&format!("{prefix}.promisor"), "true"])?;
    run(&[&format!("{prefix}.partialclonefilter"), "blob:none"])?;

    // If a non-default namespace was specified, store it so other commands can find it
    if namespace_override.is_some() {
        run(&[&format!("{prefix}.metanamespace"), &ns])?;
    }

    if side_ref {
        println!("{} side meta remote '{name}' -> {url}", s_out.ok("Added"));
    } else {
        println!("{} meta remote '{name}' -> {url}", s_out.ok("Added"));
    }

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
    let tracking_ref = if side_ref {
        format!("refs/{ns}/remotes/{name}/main")
    } else {
        format!("refs/{ns}/remotes/main")
    };
    let fetch_refspec = format!("refs/{ns}/main:{tracking_ref}");
    let depth_arg = depth.map(|depth| format!("--depth={depth}"));
    match depth_arg.as_deref() {
        Some(_) => eprint!(
            "{} metadata (blobless, {} commits)...",
            s_err.step("Fetching"),
            depth.unwrap_or_default()
        ),
        None => eprint!("{} metadata (blobless)...", s_err.step("Fetching")),
    }
    let started = std::time::Instant::now();
    let mut fetch_args = vec!["fetch", "--filter=blob:none"];
    if let Some(arg) = depth_arg.as_deref() {
        fetch_args.push(arg);
    }
    fetch_args.push(name);
    fetch_args.push(&fetch_refspec);
    match git_meta_lib::git_utils::run_git(repo, &fetch_args) {
        Ok(_) => {
            eprintln!(" {}", s_err.ok(&format!("done in {}.", elapsed(started))));

            // Verify the tracking ref was created
            let remote_ref = if side_ref {
                format!("{ns}/remotes/{name}/main")
            } else {
                format!("{ns}/remotes/main")
            };
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
            let started = std::time::Instant::now();
            eprint!(" {}", s_err.dim("(fetching, this can take a while)"));
            let blob_count =
                git_meta_lib::git_utils::hydrate_tip_blobs_counted(repo, name, &remote_ref)?;
            eprintln!(
                " {}",
                s_err.ok(&format!(
                    "{blob_count} blobs fetched in {}.",
                    elapsed(started)
                ))
            );

            // Materialize remote metadata into local SQLite
            eprint!("{} local metadata...", s_err.step("Serializing"));
            let started = std::time::Instant::now();
            let _ = ctx.session.serialize()?;
            eprintln!(" {}", s_err.ok(&format!("done in {}.", elapsed(started))));

            eprint!("{} remote metadata...", s_err.step("Materializing"));
            let started = std::time::Instant::now();
            materialize::run(None, false, false, false)?;
            eprintln!(" {}", s_err.ok(&format!("done in {}.", elapsed(started))));

            // Index historical keys as promisor entries
            let tracking_ref_name = if side_ref {
                format!("refs/{ns}/remotes/{name}/main")
            } else {
                format!("refs/{ns}/remotes/main")
            };
            // Indexing walks every commit in the history, which on a
            // long-lived project is minutes of work. Setup does not need to
            // wait for it: the index only affects fetching keys that are not
            // in the tip, and it checkpoints as it goes, so it survives the
            // terminal closing.
            if repo.find_reference(&tracking_ref_name).is_ok() {
                // Spawned by main once this command has closed the database;
                // starting it here would make our own exit wait behind it.
                eprintln!(
                    "{} history in the background {}",
                    s_err.step("Indexing"),
                    s_err.dim("(keys outside the current tip become fetchable as it runs;"),
                );
                eprintln!(
                    "  {}",
                    s_err.dim("check on it with: git meta index-history)"),
                );
            }
        }
        Err(e) => {
            eprintln!("\n{}: initial fetch failed: {e}", s_err.warn("Warning"));
            eprintln!("You can fetch later with: git meta pull");
        }
    }

    // Last, so nothing in this command publishes them: the defaults are
    // recorded locally and travel with the metadata ref on the first real
    // publish, which is the first moment there is anything to prune anyway.
    if should_init {
        let _ = write_default_prune_rules(&ctx)?;
    }

    Ok(())
}

pub(crate) fn run_remove(name: &str) -> Result<()> {
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
    unset(&format!("remote.{name}.metaside"));
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

pub(crate) fn run_list() -> Result<()> {
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
