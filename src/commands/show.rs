//! `gmeta show <commit-sha>` — display commit details with any associated metadata.

use std::process::Command;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use git2::Repository;

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

pub fn run(commit_ref: &str) -> Result<()> {
    let repo = git_utils::git2_discover_repo()?;

    // Resolve the ref to a full commit SHA
    let obj = repo
        .revparse_single(commit_ref)
        .with_context(|| format!("could not resolve: {}", commit_ref))?;
    let commit = obj
        .peel_to_commit()
        .with_context(|| format!("'{}' does not point to a commit", commit_ref))?;
    let sha = commit.id().to_string();

    // ── Header ──────────────────────────────────────────────────────────────
    println!("{YELLOW}Commit:{RESET}     {CYAN}{sha}{RESET}");

    // Try to get change-id from GitButler
    let change_id = get_change_id(&repo, &sha);
    if let Some(ref cid) = change_id {
        println!("{YELLOW}Change-ID:{RESET}  {CYAN}{cid}{RESET}");
    }

    // Author
    let author = commit.author();
    let author_name = author.name().unwrap_or("unknown");
    let author_email = author.email().unwrap_or("");
    println!("{YELLOW}Author:{RESET}     {GREEN}{author_name} <{author_email}>{RESET}");

    // Date with relative time
    let epoch = commit.time().seconds();
    let offset_minutes = commit.time().offset_minutes();
    let offset_secs = (offset_minutes as i64) * 60;
    let local_time = chrono::FixedOffset::east_opt(offset_secs as i32)
        .unwrap()
        .timestamp_opt(epoch, 0)
        .single()
        .unwrap();
    let relative = format_relative_time(epoch);
    println!(
        "{YELLOW}Date:{RESET}       {GREEN}{}{RESET} {DIM}({relative}){RESET}",
        local_time.format("%Y-%m-%d %H:%M:%S %z")
    );

    // ── Commit message ──────────────────────────────────────────────────────
    println!();
    if let Some(message) = commit.message() {
        for line in message.trim_end().lines() {
            println!("{line}");
        }
    }

    // ── Files changed ───────────────────────────────────────────────────────
    let parent = commit.parent(0).ok();
    let parent_tree = parent.as_ref().and_then(|p| p.tree().ok());
    let commit_tree = commit.tree()?;

    let diff = repo.diff_tree_to_tree(parent_tree.as_ref(), Some(&commit_tree), None)?;

    let deltas: Vec<_> = diff.deltas().collect();
    if !deltas.is_empty() {
        println!();
        println!("{BOLD}Files changed:{RESET}");
        for delta in &deltas {
            let status_char = match delta.status() {
                git2::Delta::Added => 'A',
                git2::Delta::Deleted => 'D',
                git2::Delta::Modified => 'M',
                git2::Delta::Renamed => 'R',
                git2::Delta::Copied => 'C',
                _ => '?',
            };
            let path = delta
                .new_file()
                .path()
                .or_else(|| delta.old_file().path())
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "???".to_string());
            let status_color = match delta.status() {
                git2::Delta::Added => GREEN,
                git2::Delta::Deleted => "\x1b[31m", // red
                _ => YELLOW,
            };
            println!("  {status_color}{status_char}{RESET} {path}");
        }
    }

    // ── Metadata ────────────────────────────────────────────────────────────
    let db_path = git_utils::git2_db_path(&repo)?;
    let db = Db::open(&db_path)?;

    // Collect metadata from both commit SHA and change-id
    let mut meta_entries: Vec<(String, String, String)> = Vec::new(); // (source, key, display_value)

    // Metadata on commit:<sha>
    let commit_entries = db.get_all("commit", &sha, None).unwrap_or_default();
    for (key, value, value_type, _is_git_ref) in &commit_entries {
        let display = format_meta_value(value, value_type);
        meta_entries.push(("commit".to_string(), key.clone(), display));
    }

    // Metadata on change-id:<cid>
    if let Some(ref cid) = change_id {
        let cid_entries = db.get_all("change-id", cid, None).unwrap_or_default();
        for (key, value, value_type, _is_git_ref) in &cid_entries {
            let display = format_meta_value(value, value_type);
            meta_entries.push(("change-id".to_string(), key.clone(), display));
        }
    }

    if !meta_entries.is_empty() {
        println!();
        println!("{CYAN}Metadata:{RESET}");
        for (source, key, value) in &meta_entries {
            println!("  {BLUE}{source}{RESET}  {BOLD}{key}{RESET}  {DIM}{value}{RESET}");
        }
    }

    Ok(())
}

/// Format a stored metadata value for display.
fn format_meta_value(value: &str, value_type: &str) -> String {
    match value_type {
        "string" => serde_json::from_str::<String>(value).unwrap_or_else(|_| value.to_string()),
        "list" => {
            if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(value) {
                format!("[list: {} items]", arr.len())
            } else {
                value.to_string()
            }
        }
        "set" => {
            if let Ok(arr) = serde_json::from_str::<Vec<String>>(value) {
                format!("[set: {} members]", arr.len())
            } else {
                value.to_string()
            }
        }
        _ => value.to_string(),
    }
}

/// Get a change-id for a commit. First tries `but show --json`, then falls back
/// to looking for a Change-Id trailer in the commit message.
fn get_change_id(repo: &Repository, sha: &str) -> Option<String> {
    // Try GitButler CLI first
    if let Some(workdir) = repo.workdir() {
        let output = Command::new("but")
            .args(["show", sha, "--json"])
            .current_dir(workdir)
            .output()
            .ok()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&stdout) {
                if let Some(cid) = json["changeId"].as_str() {
                    return Some(cid.to_string());
                }
            }
        }
    }

    // Fall back: look for a Change-Id trailer in the commit message
    let commit = repo.find_commit(git2::Oid::from_str(sha).ok()?).ok()?;
    let message = commit.message()?;
    for line in message.lines().rev() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Change-Id:") {
            return Some(rest.trim().to_string());
        }
    }

    None
}

/// Format seconds-since-epoch as a human-readable relative time string.
fn format_relative_time(epoch: i64) -> String {
    let now = Utc::now().timestamp();
    let diff = now - epoch;

    if diff < 0 {
        return "in the future".to_string();
    }

    let minutes = diff / 60;
    let hours = diff / 3600;
    let days = diff / 86400;
    let weeks = diff / 604800;
    let months = diff / 2592000;
    let years = diff / 31536000;

    if diff < 60 {
        format!("{diff}s ago")
    } else if minutes < 60 {
        format!("{minutes}m ago")
    } else if hours < 24 {
        format!("{hours}h ago")
    } else if days < 7 {
        format!("{days}d ago")
    } else if weeks < 5 {
        format!("{weeks}w ago")
    } else if months < 12 {
        format!("{months}mo ago")
    } else {
        format!("{years}y ago")
    }
}
