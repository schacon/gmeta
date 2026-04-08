//! `gmeta log` — walk the commit history and print metadata for each commit,
//! matching the output of scripts/log.rb.

use anyhow::{Context, Result};
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;

use crate::context::CommandContext;
use gmeta_core::types::TargetType;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const BLUE: &str = "\x1b[34m";

pub fn run(
    start_ref: Option<&str>, // commit-ish to start from (default HEAD)
    count: usize,            // max commits to show
    metadata_only: bool,     // skip commits with no metadata
) -> Result<()> {
    let mut ctx = CommandContext::open(None)?;
    // Attach a repo to the Store so blob-ref values are resolved during reads.
    let git_dir = ctx.session.repo().git_dir().to_owned();
    ctx.session.store_mut().set_repo(gix::open(git_dir)?);
    let repo = ctx.session.repo();

    // Resolve start ref -> OID
    let start_oid = resolve_start(repo, start_ref)?;

    // Walk commits
    let walk = repo.rev_walk(Some(start_oid));
    let iter = walk.all()?;

    let mut printed = 0usize;

    for info_result in iter {
        if printed >= count {
            break;
        }

        let info = info_result.context("error walking commits")?;
        let oid = info.id;
        let commit_obj = oid.attach(repo).object()?.into_commit();
        let sha = oid.to_string();

        // Fetch metadata before deciding whether to print the commit
        let entries = ctx
            .session
            .store()
            .get_all(&TargetType::Commit, &sha, None)
            .unwrap_or_default();
        // get_all returns (key, value, value_type, is_git_ref)
        // value is a JSON-encoded string for string types
        let meta: Vec<(String, String)> = entries
            .into_iter()
            .map(|entry| {
                // Decode JSON string wrapper -> raw value for display
                let raw = serde_json::from_str::<String>(&entry.value).unwrap_or(entry.value);
                (entry.key, raw)
            })
            .collect();

        if metadata_only && meta.is_empty() {
            continue;
        }

        // Blank line between entries
        if printed > 0 {
            println!();
        }
        printed += 1;

        let short_sha = &sha[..10];
        let decoded = commit_obj.decode()?;
        let author_name = decoded
            .author()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .name
            .to_str_lossy();
        let author_email = decoded
            .author()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .email
            .to_str_lossy();

        println!(
            "{YELLOW}commit {short_sha}{RESET} {DIM}---{RESET} \
             {GREEN}{author_name}{RESET} {DIM}<{author_email}>{RESET}"
        );

        let message = decoded.message.to_str_lossy();
        let message = message.trim().to_string();
        let nonempty_lines: Vec<&str> = message.lines().filter(|l| !l.trim().is_empty()).collect();
        let shown = nonempty_lines.len().min(4);
        for line in &nonempty_lines[..shown] {
            println!("  {line}");
        }
        if nonempty_lines.len() > 4 {
            let extra = nonempty_lines.len() - 4;
            println!("  {DIM}... ({extra} more lines){RESET}");
        }

        if !meta.is_empty() {
            println!("  {CYAN}--- metadata ---{RESET}");
            for (key, value) in &meta {
                let preview = format_value_preview(value);
                println!("  {BLUE}|{RESET} {BOLD}{key}{RESET}  {preview}");
            }
            println!("  {BLUE}.{RESET}");
        }
    }

    if printed == 0 {
        if metadata_only {
            println!("No commits with metadata found.");
        } else {
            println!("No commits found.");
        }
    }

    Ok(())
}
/// Resolve a ref name or commit-ish to an OID.  Falls back to HEAD.
fn resolve_start(repo: &gix::Repository, start_ref: Option<&str>) -> Result<gix::ObjectId> {
    let spec = start_ref.unwrap_or("HEAD");
    let obj = repo
        .rev_parse_single(spec)
        .with_context(|| format!("could not resolve ref '{}'", spec))?;
    let commit = obj.object()?.peel_tags_to_end()?.into_commit();
    Ok(commit.id().detach())
}

/// Format a raw (already-decoded) metadata value for display.
/// Mirrors the Ruby format_value_preview function:
///   - JSON arrays   -> "[list: N items]"
///   - JSON objects  -> "{object: N keys}"
///   - strings       -> first line, truncated to 50 chars; " ..." appended
///     if there are more lines
fn format_value_preview(value: &str) -> String {
    // Try JSON parse for arrays/objects
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(value) {
        if let Some(arr) = json.as_array() {
            return format!("{CYAN}[list: {} items]{RESET}", arr.len());
        }
        if let Some(obj) = json.as_object() {
            return format!("{CYAN}{{object: {} keys}}{RESET}", obj.len());
        }
    }

    // Plain string
    let first_line = value.lines().next().unwrap_or("");
    let has_more_lines = value.contains('\n') && value.trim_end_matches('\n') != first_line;

    let mut preview = if first_line.len() > 50 {
        format!("{}...", &first_line[..50])
    } else {
        first_line.to_string()
    };

    if has_more_lines && first_line.len() <= 50 {
        preview.push_str(" ...");
    }

    format!("{DIM}{preview}{RESET}")
}
