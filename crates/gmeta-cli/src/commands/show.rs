//! `gmeta show <commit-sha>` — display commit details with any associated metadata.

use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use std::process::Command;

use anyhow::{Context, Result};
use time::OffsetDateTime;

use crate::context::CommandContext;
use gmeta_core::types::{TargetType, ValueType};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const BLUE: &str = "\x1b[34m";

pub fn run(commit_ref: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();

    // Resolve the ref to a full commit SHA
    let spec = repo
        .rev_parse_single(commit_ref)
        .with_context(|| format!("could not resolve: {}", commit_ref))?;
    let commit_obj = spec.object()?.peel_tags_to_end()?.into_commit();
    let sha = commit_obj.id().to_string();

    println!("{YELLOW}Commit:{RESET}     {CYAN}{sha}{RESET}");

    // Try to get change-id from GitButler
    let change_id = get_change_id(repo, &sha);
    if let Some(ref cid) = change_id {
        println!("{YELLOW}Change-ID:{RESET}  {CYAN}{cid}{RESET}");
    }

    // Author
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
    println!("{YELLOW}Author:{RESET}     {GREEN}{author_name} <{author_email}>{RESET}");

    // Date with relative time
    let epoch = decoded
        .author()
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .time()
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .seconds;
    let offset_secs = decoded
        .author()
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .time()
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .offset;
    let utc_offset =
        time::UtcOffset::from_whole_seconds(offset_secs).unwrap_or(time::UtcOffset::UTC);
    let local_time = OffsetDateTime::from_unix_timestamp(epoch)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .to_offset(utc_offset);
    let relative = format_relative_time(epoch);
    let date_fmt = local_time
        .format(
            &time::format_description::parse(
                "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory][offset_minute]",
            )
            .unwrap_or_default(),
        )
        .unwrap_or_else(|_| "?".to_string());
    println!("{YELLOW}Date:{RESET}       {GREEN}{date_fmt}{RESET} {DIM}({relative}){RESET}");

    println!();
    let message = decoded.message.to_str_lossy();
    for line in message.trim_end().lines() {
        println!("{line}");
    }

    // Show diff stats using git subprocess (gix diff API is complex, this is simpler)
    let git_dir = repo.path();
    let diff_output = Command::new("git")
        .args(["--git-dir", &git_dir.to_string_lossy()])
        .args(["diff-tree", "--no-commit-id", "-r", "--name-status", &sha])
        .output()
        .ok();

    if let Some(output) = diff_output {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let lines: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();
            if !lines.is_empty() {
                println!();
                println!("{BOLD}Files changed:{RESET}");
                for line in &lines {
                    let parts: Vec<&str> = line.splitn(2, '\t').collect();
                    if parts.len() == 2 {
                        let status_char = parts[0].chars().next().unwrap_or('?');
                        let path = parts[1];
                        let status_color = match status_char {
                            'A' => GREEN,
                            'D' => "\x1b[31m", // red
                            _ => YELLOW,
                        };
                        println!("  {status_color}{status_char}{RESET} {path}");
                    }
                }
            }
        }
    }

    // Collect metadata from both commit SHA and change-id
    let mut meta_entries: Vec<(String, String, String)> = Vec::new(); // (source, key, display_value)

    // Metadata on commit:<sha>
    let commit_entries = ctx
        .db
        .get_all(&TargetType::Commit, &sha, None)
        .unwrap_or_default();
    for (key, value, value_type, _is_git_ref) in &commit_entries {
        let display = format_meta_value(value, value_type);
        meta_entries.push(("commit".to_string(), key.clone(), display));
    }

    // Metadata on change-id:<cid>
    if let Some(ref cid) = change_id {
        let cid_entries = ctx
            .db
            .get_all(&TargetType::ChangeId, cid, None)
            .unwrap_or_default();
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
fn format_meta_value(value: &str, value_type: &ValueType) -> String {
    match value_type {
        ValueType::String => {
            serde_json::from_str::<String>(value).unwrap_or_else(|_| value.to_string())
        }
        ValueType::List => {
            if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(value) {
                format!("[list: {} items]", arr.len())
            } else {
                value.to_string()
            }
        }
        ValueType::Set => {
            if let Ok(arr) = serde_json::from_str::<Vec<String>>(value) {
                format!("[set: {} members]", arr.len())
            } else {
                value.to_string()
            }
        }
        _ => "[unknown type]".to_string(),
    }
}

/// Get a change-id for a commit. First tries `but show --json`, then falls back
/// to looking for a Change-Id trailer in the commit message.
fn get_change_id(repo: &gix::Repository, sha: &str) -> Option<String> {
    // Try GitButler CLI first
    let workdir = repo.workdir()?;
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

    // Fall back: look for a Change-Id trailer in the commit message
    let oid = gix::ObjectId::from_hex(sha.as_bytes()).ok()?;
    let commit_obj = oid.attach(repo).object().ok()?.into_commit();
    let decoded = commit_obj.decode().ok()?;
    let message = decoded.message.to_str_lossy();
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
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let diff = now - epoch;

    if diff < 0 {
        return "in the future".to_string();
    }

    let dur = time::Duration::seconds(diff);
    let minutes = dur.whole_minutes();
    let hours = dur.whole_hours();
    let days = dur.whole_days();
    let weeks = days / 7;
    let months = days / 30;
    let years = days / 365;

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
