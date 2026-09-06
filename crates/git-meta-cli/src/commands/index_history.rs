//! `git meta index-history` — walk the metadata history recording which keys
//! exist and where, so they can be fetched on demand later.
//!
//! On a long-lived project this is minutes of work, so it normally runs in the
//! background and checkpoints its progress. Any `git meta` command that finds
//! unfinished work restarts it from the checkpoint.

use anyhow::Result;

use crate::context::CommandContext;
use crate::style::Style;

/// Run the indexer in the foreground, resuming from a checkpoint if present.
pub(crate) fn run(quiet: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let s_err = Style::detect_stderr();

    let Some(tip) = tracking_tip(&ctx) else {
        if !quiet {
            eprintln!("No metadata remote to index. Add one with: git meta remote add <url>");
        }
        return Ok(());
    };

    let now_ms = now_ms();
    if let Some(state) = git_meta_lib::index_state::load(repo)? {
        if state.indexes(&tip) && state.complete {
            if !quiet {
                eprintln!("History already indexed ({} keys).", state.keys_indexed);
            }
            return Ok(());
        }
        if state.looks_active(now_ms) {
            if !quiet {
                eprintln!(
                    "Another indexer is already running (pid {}, {} commits so far).",
                    state.pid, state.commits_indexed
                );
            }
            return Ok(());
        }
        if state.indexes(&tip) && state.resume_from.is_some() && !quiet {
            eprintln!(
                "{} from {} commits in.",
                s_err.step("Resuming"),
                state.commits_indexed
            );
        }
    }

    let total = total_commits(&ctx);
    let started = std::time::Instant::now();
    let mut last_drawn = std::time::Instant::now();

    let count = git_meta_lib::sync::index_history_resumable(
        repo,
        ctx.session.store(),
        tip,
        now_ms,
        |progress| {
            if quiet || last_drawn.elapsed().as_millis() < 250 {
                return;
            }
            last_drawn = std::time::Instant::now();
            let position = match total {
                Some(total) if total > 0 => format!(
                    "{}% ({}/{} commits)",
                    progress.commits_walked.min(total) * 100 / total,
                    progress.commits_walked,
                    total
                ),
                _ => format!("{} commits", progress.commits_walked),
            };
            eprint!(
                "\r  {position}, {} keys, {:.0}s elapsed   ",
                progress.keys_indexed,
                started.elapsed().as_secs_f64()
            );
        },
    )?;

    if !quiet {
        eprintln!(
            "\r  {}                                        ",
            s_err.ok(&format!(
                "{count} keys indexed in {:.1}s (available on demand).",
                started.elapsed().as_secs_f64()
            ))
        );
    }
    Ok(())
}

/// Start an indexer in the background if there is unfinished work and nothing
/// is already doing it.
///
/// Called before every command, so a run that was interrupted resumes on the
/// user's next interaction without them having to know it stopped.
pub(crate) fn resume_in_background_if_needed() {
    let Ok(ctx) = CommandContext::open(None) else {
        return;
    };
    let repo = ctx.session.repo();
    let Some(tip) = tracking_tip(&ctx) else {
        return;
    };
    let Ok(Some(state)) = git_meta_lib::index_state::load(repo) else {
        return;
    };
    if !state.is_resumable(&tip, now_ms()) {
        return;
    }
    drop(ctx);
    spawn_background();
}

/// Launch `git meta index-history` as a detached background process.
pub(crate) fn spawn_background() {
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let _ = std::process::Command::new(exe)
        .args(["index-history", "--quiet"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

fn tracking_tip(ctx: &CommandContext) -> Option<gix::ObjectId> {
    let repo = ctx.session.repo();
    let tracking_ref = format!("refs/{}/remotes/main", ctx.session.namespace());
    repo.find_reference(&tracking_ref)
        .ok()?
        .into_fully_peeled_id()
        .ok()
        .map(gix::Id::detach)
}

fn total_commits(ctx: &CommandContext) -> Option<usize> {
    let tracking_ref = format!("refs/{}/remotes/main", ctx.session.namespace());
    git_meta_lib::git_utils::run_git(ctx.session.repo(), &["rev-list", "--count", &tracking_ref])
        .ok()
        .and_then(|out| out.trim().parse().ok())
}

fn now_ms() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000
}
