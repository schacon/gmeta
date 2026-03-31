//! `gmeta log` — walk the commit history and print metadata for each commit,
//! matching the output of scripts/log.rb.

use anyhow::{Context, Result};
use git2::{Oid, Repository, Sort};

use crate::context::CommandContext;
use crate::db::Db;
use crate::git_utils;

// ── ANSI colours ──────────────────────────────────────────────────────────────
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
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;
    // log needs a separate Db with an embedded repo for blob resolution during reads
    let db_path = git_utils::git2_db_path(repo)?;
    let db = Db::open_with_repo(&db_path, git_utils::git2_discover_repo()?)?;

    // Resolve start ref → OID
    let start_oid = resolve_start(repo, start_ref)?;

    // Walk commits
    let mut revwalk = repo.revwalk()?;
    revwalk.set_sorting(Sort::TIME)?;
    revwalk.push(start_oid)?;

    let mut printed = 0usize;

    for oid_result in revwalk {
        if printed >= count {
            break;
        }

        let oid: Oid = oid_result.context("error walking commits")?;
        let commit = repo.find_commit(oid)?;
        let sha = oid.to_string();

        // Fetch metadata before deciding whether to print the commit
        let entries = db.get_all("commit", &sha, None).unwrap_or_default();
        // get_all returns (key, value, value_type, is_git_ref)
        // value is a JSON-encoded string for string types
        let meta: Vec<(String, String)> = entries
            .into_iter()
            .map(|(key, value, _vtype, _is_ref)| {
                // Decode JSON string wrapper → raw value for display
                let raw = serde_json::from_str::<String>(&value).unwrap_or(value);
                (key, raw)
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

        // ── Header line ───────────────────────────────────────────────────────
        let short_sha = &sha[..10];
        let author = commit.author();
        let author_name = author.name().unwrap_or("unknown");
        let author_email = author.email().unwrap_or("");

        println!(
            "{YELLOW}commit {short_sha}{RESET} {DIM}—{RESET} \
             {GREEN}{author_name}{RESET} {DIM}<{author_email}>{RESET}"
        );

        // ── Commit message (first 4 non-empty lines) ──────────────────────────
        let message = commit.message().unwrap_or("").trim().to_string();
        let nonempty_lines: Vec<&str> = message.lines().filter(|l| !l.trim().is_empty()).collect();
        let shown = nonempty_lines.len().min(4);
        for line in &nonempty_lines[..shown] {
            println!("  {line}");
        }
        if nonempty_lines.len() > 4 {
            let extra = nonempty_lines.len() - 4;
            println!("  {DIM}... ({extra} more lines){RESET}");
        }

        // ── Metadata block ────────────────────────────────────────────────────
        if !meta.is_empty() {
            println!("  {CYAN}╶── metadata ──{RESET}");
            for (key, value) in &meta {
                let preview = format_value_preview(value);
                println!("  {BLUE}│{RESET} {BOLD}{key}{RESET}  {preview}");
            }
            println!("  {BLUE}╵{RESET}");
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

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Resolve a ref name or commit-ish to an OID.  Falls back to HEAD.
fn resolve_start(repo: &Repository, start_ref: Option<&str>) -> Result<Oid> {
    let spec = start_ref.unwrap_or("HEAD");
    repo.revparse_single(spec)
        .with_context(|| format!("could not resolve ref '{}'", spec))?
        .peel_to_commit()
        .with_context(|| format!("'{}' does not point to a commit", spec))?
        .id()
        .pipe(Ok)
}

/// Format a raw (already-decoded) metadata value for display.
/// Mirrors the Ruby format_value_preview function:
///   - JSON arrays   → "[list: N items]"
///   - JSON objects  → "{object: N keys}"
///   - strings       → first line, truncated to 50 chars; " ..." appended
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

// Simple extension trait to pipe a value through Ok() without a temp binding
trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}
