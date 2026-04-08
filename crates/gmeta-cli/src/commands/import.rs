use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::context::CommandContext;
use gmeta_core::types::{TargetType, ValueType, GIT_REF_THRESHOLD};
use gmeta_core::Store;

/// Supported import source formats.
#[derive(Debug, Clone, PartialEq)]
pub enum ImportFormat {
    /// Import the entire git history.
    Entire,
    /// Import from git-ai format.
    GitAi,
}

impl ImportFormat {
    /// Parse an import format string.
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "entire" => Ok(ImportFormat::Entire),
            "git-ai" => Ok(ImportFormat::GitAi),
            other => bail!("unsupported import format: {other}"),
        }
    }
}

pub fn run(format: ImportFormat, dry_run: bool, since: Option<&str>) -> Result<()> {
    let since_epoch = match since {
        Some(date_str) => {
            let date_fmt =
                time::format_description::parse("[year]-[month]-[day]").unwrap_or_default();
            let date = time::Date::parse(date_str, &date_fmt).with_context(|| {
                format!("invalid --since date '{}', expected YYYY-MM-DD", date_str)
            })?;
            let odt = time::OffsetDateTime::new_utc(date, time::Time::MIDNIGHT);
            Some(odt.unix_timestamp())
        }
        None => None,
    };

    match format {
        ImportFormat::Entire => run_entire(dry_run, since_epoch),
        ImportFormat::GitAi => run_git_ai(dry_run, since_epoch),
    }
}

fn run_entire(dry_run: bool, since_epoch: Option<i64>) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();
    let email = ctx.email();
    let fallback_ts = ctx.timestamp;

    let db = if dry_run { None } else { Some(ctx.store()) };

    let mut imported_count = 0u64;

    // Resolve the checkpoints tree (local or remote)
    let checkpoints_tree_id = resolve_entire_ref(repo, "entire/checkpoints/v1")?;
    if checkpoints_tree_id.is_none() {
        eprintln!("No entire/checkpoints/v1 ref found (local or remote), skipping checkpoints");
    }

    // Step 1: Walk all commits looking for Entire-Checkpoint trailers.
    if let Some(cp_tree_id) = checkpoints_tree_id {
        if let Some(ts) = since_epoch {
            let date_str = time::OffsetDateTime::from_unix_timestamp(ts)
                .ok()
                .and_then(|d| {
                    d.format(
                        &time::format_description::parse("[year]-[month]-[day]")
                            .unwrap_or_default(),
                    )
                    .ok()
                })
                .unwrap_or_else(|| "unknown".to_string());
            eprintln!(
                "Scanning commits for Entire-Checkpoint trailers (since {})...",
                date_str
            );
        } else {
            eprintln!("Scanning commits for Entire-Checkpoint trailers...");
        }
        imported_count +=
            import_checkpoints_from_commits(repo, cp_tree_id, db, email, dry_run, since_epoch)?;
    }

    // Step 2: Import trails
    if let Some(tree_id) = resolve_entire_ref(repo, "entire/trails/v1")? {
        eprintln!("Processing entire/trails/v1...");
        imported_count += import_trails(repo, tree_id, db, email, fallback_ts, dry_run)?;
    } else {
        eprintln!("No entire/trails/v1 ref found, skipping trails");
    }

    if dry_run {
        eprintln!("Dry run: would have imported {} keys", imported_count);
    } else {
        eprintln!("Imported {} keys", imported_count);
    }

    Ok(())
}

/// Resolve an entire ref to the tree OID of its tip commit.
fn resolve_entire_ref(repo: &gix::Repository, refname: &str) -> Result<Option<gix::ObjectId>> {
    let reference = repo
        .find_reference(&format!("refs/heads/{}", refname))
        .or_else(|_| repo.find_reference(&format!("refs/remotes/origin/{}", refname)))
        .or_else(|_| repo.find_reference(refname));

    match reference {
        Ok(r) => {
            let refname_used = r.name().as_bstr().to_string();
            eprintln!("  Resolved {} via {}", refname, refname_used);
            let commit_id = r.into_fully_peeled_id()?.detach();
            let commit_obj = commit_id.attach(repo).object()?.into_commit();
            let tree_id = commit_obj.tree_id()?.detach();
            Ok(Some(tree_id))
        }
        Err(_) => Ok(None),
    }
}

/// Walk all commits across all refs, find Entire-Checkpoint trailers,
/// look up each checkpoint in the checkpoints tree, and import it.
fn import_checkpoints_from_commits(
    repo: &gix::Repository,
    checkpoints_tree_id: gix::ObjectId,
    db: Option<&Store>,
    email: &str,
    dry_run: bool,
    since_epoch: Option<i64>,
) -> Result<u64> {
    let mut count = 0u64;
    let mut seen_commits: HashSet<gix::ObjectId> = HashSet::new();
    let mut found = 0u64;
    let mut skipped = 0u64;
    let mut missing = 0u64;

    // Collect all ref tips to walk
    let mut start_oids: Vec<gix::ObjectId> = Vec::new();
    let platform = repo.references()?;
    for r in platform.all()?.flatten() {
        let name = r.name().as_bstr().to_string();
        if name.contains("/entire/") {
            continue;
        }
        if let Ok(id) = r.into_fully_peeled_id() {
            start_oids.push(id.detach());
        }
    }

    // Walk each ref tip
    let mut scanned = 0u64;
    for start_oid in &start_oids {
        let walk = repo.rev_walk(Some(*start_oid));
        let iter = match walk.all() {
            Ok(it) => it,
            Err(_) => continue,
        };

        for info_result in iter {
            let info = match info_result {
                Ok(i) => i,
                Err(_) => continue,
            };
            let oid = info.id;
            if !seen_commits.insert(oid) {
                continue;
            }
            scanned += 1;

            let commit_obj = oid.attach(repo).object()?.into_commit();
            let decoded = commit_obj.decode()?;

            // Skip commits older than --since date
            if let Some(cutoff) = since_epoch {
                if {
                    let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                    a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
                } < cutoff
                {
                    continue;
                }
            }

            let msg = decoded.message.to_str_lossy().to_string();

            // Look for Entire-Checkpoint trailer(s)
            for line in msg.lines() {
                let line = line.trim();
                let checkpoint_id = match line.strip_prefix("Entire-Checkpoint:") {
                    Some(id) => id.trim(),
                    None => continue,
                };
                if checkpoint_id.is_empty() {
                    continue;
                }

                let commit_sha = oid.to_string();

                // Skip if already imported
                if let Some(db) = db {
                    if let Ok(Some(_mv)) =
                        db.get(&TargetType::Commit, &commit_sha, "agent:checkpoint-id")
                    {
                        skipped += 1;
                        continue;
                    }
                }

                // Look up checkpoint in the sharded tree: first2/rest/
                let shard = &checkpoint_id[..2.min(checkpoint_id.len())];
                let rest = &checkpoint_id[2.min(checkpoint_id.len())..];

                let checkpoint_tree_id = (|| -> Result<Option<gix::ObjectId>> {
                    let shard_id = match entry_to_tree_id(repo, checkpoints_tree_id, shard)? {
                        Some(t) => t,
                        None => return Ok(None),
                    };
                    entry_to_tree_id(repo, shard_id, rest)
                })()?;

                let checkpoint_tree_id = match checkpoint_tree_id {
                    Some(t) => t,
                    None => {
                        missing += 1;
                        eprintln!(
                            "  Commit {} has Entire-Checkpoint: {} but checkpoint not found in tree",
                            &commit_sha[..7],
                            checkpoint_id
                        );
                        continue;
                    }
                };

                found += 1;
                eprintln!(
                    "  Commit {} <- checkpoint {}",
                    &commit_sha[..7],
                    checkpoint_id,
                );

                // Use the commit's author date as the metadata timestamp
                let mut ts = {
                    let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                    a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
                } * 1000;

                // Store checkpoint ID
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    &commit_sha,
                    "agent:checkpoint-id",
                    &json_string(checkpoint_id),
                    &ValueType::String,
                    email,
                    ts,
                )?;
                ts += 1;

                // Import top-level metadata.json (checkpoint summary)
                if let Some(content) = entry_to_blob(repo, checkpoint_tree_id, "metadata.json")? {
                    let meta: Value = serde_json::from_str(&content).unwrap_or(Value::Null);
                    let checkpoint_fields: &[(&str, &[&str])] = &[
                        ("strategy", &["strategy"]),
                        ("branch", &["branch"]),
                        ("files-changed", &["filesChanged", "files_changed"]),
                        ("token-usage", &["tokenUsage", "token_usage"]),
                    ];
                    for (gmeta_key, aliases) in checkpoint_fields {
                        if let Some(val) = aliases.iter().find_map(|a| meta.get(*a)) {
                            let key = format!("agent:{}", gmeta_key);
                            let json_val = json_encode_value(val)?;
                            count += set_value(
                                repo,
                                db,
                                dry_run,
                                &TargetType::Commit,
                                &commit_sha,
                                &key,
                                &json_val,
                                &ValueType::String,
                                email,
                                ts,
                            )?;
                            ts += 1;
                        }
                    }
                }

                // Import session slots (0, 1, 2, ...)
                let mut session_idx = 0u32;
                loop {
                    let slot_name = session_idx.to_string();
                    let session_tree_id =
                        match entry_to_tree_id(repo, checkpoint_tree_id, &slot_name)? {
                            Some(t) => t,
                            None => break,
                        };

                    let key_prefix = if session_idx == 0 {
                        "agent".to_string()
                    } else {
                        format!("agent:session-{}", session_idx)
                    };

                    count += import_session(
                        repo,
                        session_tree_id,
                        db,
                        &commit_sha,
                        &key_prefix,
                        email,
                        &mut ts,
                        dry_run,
                    )?;

                    session_idx += 1;
                }
            }
        }
    }

    eprintln!(
        "Scanned {} commits: {} checkpoints imported, {} already present, {} not found in tree",
        scanned, found, skipped, missing
    );

    Ok(count)
}

/// Import a single session slot's data.
fn import_session(
    repo: &gix::Repository,
    session_tree_id: gix::ObjectId,
    db: Option<&Store>,
    commit_sha: &str,
    key_prefix: &str,
    email: &str,
    ts: &mut i64,
    dry_run: bool,
) -> Result<u64> {
    let mut count = 0u64;

    // Session metadata.json
    if let Some(content) = entry_to_blob(repo, session_tree_id, "metadata.json")? {
        let meta: Value =
            serde_json::from_str(&content).context("parsing session metadata.json")?;

        let string_fields = [
            ("agent", "agent"),
            ("model", "model"),
            ("turnId", "turn-id"),
            ("turn_id", "turn-id"),
            ("sessionId", "session-id"),
            ("session_id", "session-id"),
        ];
        for (json_key, gmeta_key) in &string_fields {
            if let Some(val) = meta.get(json_key) {
                let key = format!("{}:{}", key_prefix, gmeta_key);
                let json_val = json_encode_value(val)?;
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    commit_sha,
                    &key,
                    &json_val,
                    &ValueType::String,
                    email,
                    *ts,
                )?;
                *ts += 1;
            }
        }

        let object_fields = [
            ("attribution", "attribution"),
            ("summary", "summary"),
            ("tokenUsage", "token-usage"),
            ("token_usage", "token-usage"),
        ];
        for (json_key, gmeta_key) in &object_fields {
            if let Some(val) = meta.get(json_key) {
                let key = format!("{}:{}", key_prefix, gmeta_key);
                let json_val = json_encode_value(val)?;
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    commit_sha,
                    &key,
                    &json_val,
                    &ValueType::String,
                    email,
                    *ts,
                )?;
                *ts += 1;
            }
        }
    }

    // prompt.txt
    if let Some(content) = entry_to_blob(repo, session_tree_id, "prompt.txt")? {
        let key = format!("{}:prompt", key_prefix);
        count += set_value(
            repo,
            db,
            dry_run,
            &TargetType::Commit,
            commit_sha,
            &key,
            &json_string(&content),
            &ValueType::String,
            email,
            *ts,
        )?;
        *ts += 1;
    }

    // full.jsonl
    if let Some(content) = entry_to_blob(repo, session_tree_id, "full.jsonl")? {
        let key = format!("{}:transcript", key_prefix);
        if !content.trim().is_empty() {
            let json_val = json_string(&content);
            count += set_value(
                repo,
                db,
                dry_run,
                &TargetType::Commit,
                commit_sha,
                &key,
                &json_val,
                &ValueType::String,
                email,
                *ts,
            )?;
            *ts += 1;
        }
    }

    // content_hash.txt
    if let Some(content) = entry_to_blob(repo, session_tree_id, "content_hash.txt")? {
        let key = format!("{}:content-hash", key_prefix);
        count += set_value(
            repo,
            db,
            dry_run,
            &TargetType::Commit,
            commit_sha,
            &key,
            &json_string(content.trim()),
            &ValueType::String,
            email,
            *ts,
        )?;
        *ts += 1;
    }

    // tasks/ directory
    if let Some(tasks_tree_id) = entry_to_tree_id(repo, session_tree_id, "tasks")? {
        let tasks_tree = tasks_tree_id.attach(repo).object()?.into_tree();
        for task_entry_result in tasks_tree.iter() {
            let task_entry = task_entry_result?;
            let tool_use_id = task_entry.filename().to_str_lossy().to_string();
            if tool_use_id.is_empty() || !task_entry.mode().is_tree() {
                continue;
            }
            let task_tree_id = task_entry.object_id();

            if let Some(content) = entry_to_blob(repo, task_tree_id, "checkpoint.json")? {
                let key = format!(
                    "{}:tasks:{}:checkpoint",
                    key_prefix,
                    sanitize_key_segment(&tool_use_id)
                );
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    commit_sha,
                    &key,
                    &json_string(&content),
                    &ValueType::String,
                    email,
                    *ts,
                )?;
                *ts += 1;
            }

            let task_tree = task_tree_id.attach(repo).object()?.into_tree();
            for agent_entry_result in task_tree.iter() {
                let agent_entry = agent_entry_result?;
                let name = agent_entry.filename().to_str_lossy().to_string();
                if name.starts_with("agent-")
                    && name.ends_with(".jsonl")
                    && agent_entry.mode().is_blob()
                {
                    let blob = agent_entry.object_id().attach(repo).object()?.into_blob();
                    let content = String::from_utf8_lossy(&blob.data);
                    let agent_id = name
                        .strip_prefix("agent-")
                        .unwrap_or(&name)
                        .strip_suffix(".jsonl")
                        .unwrap_or(&name);
                    let key = format!(
                        "{}:tasks:{}:agent-{}",
                        key_prefix,
                        sanitize_key_segment(&tool_use_id),
                        sanitize_key_segment(agent_id),
                    );
                    let lines: Vec<&str> =
                        content.lines().filter(|l| !l.trim().is_empty()).collect();
                    if !lines.is_empty() {
                        let mut entries = Vec::new();
                        for (i, line) in lines.iter().enumerate() {
                            entries.push(gmeta_core::list_value::ListEntry {
                                value: line.to_string(),
                                timestamp: *ts + i as i64,
                            });
                        }
                        let encoded = gmeta_core::list_value::encode_entries(&entries)?;
                        count += set_value(
                            repo,
                            db,
                            dry_run,
                            &TargetType::Commit,
                            commit_sha,
                            &key,
                            &encoded,
                            &ValueType::List,
                            email,
                            *ts,
                        )?;
                        *ts += lines.len() as i64 + 1;
                    }
                }
            }
        }
    }

    Ok(count)
}

/// Read a blob from a tree entry by name.
fn entry_to_blob(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    name: &str,
) -> Result<Option<String>> {
    let tree = tree_id.attach(repo).object()?.into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result?;
        if entry.filename().to_str_lossy() == name && entry.mode().is_blob() {
            let blob = entry.object_id().attach(repo).object()?.into_blob();
            return Ok(Some(String::from_utf8_lossy(&blob.data).to_string()));
        }
    }
    Ok(None)
}

/// Read a subtree OID from a tree entry by name.
fn entry_to_tree_id(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    name: &str,
) -> Result<Option<gix::ObjectId>> {
    let tree = tree_id.attach(repo).object()?.into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result?;
        if entry.filename().to_str_lossy() == name && entry.mode().is_tree() {
            return Ok(Some(entry.object_id()));
        }
    }
    Ok(None)
}

/// Load the set of trail IDs that have already been imported.
fn load_imported_trail_ids(db: Option<&Store>) -> Result<HashSet<String>> {
    match db {
        Some(db) => Ok(db.imported_trail_ids()?),
        None => Ok(HashSet::new()),
    }
}

fn import_trails(
    repo: &gix::Repository,
    root_tree_id: gix::ObjectId,
    db: Option<&Store>,
    email: &str,
    base_ts: i64,
    dry_run: bool,
) -> Result<u64> {
    let mut count = 0u64;
    let mut ts = base_ts;
    let imported_trails = load_imported_trail_ids(db)?;

    let root_tree = root_tree_id.attach(repo).object()?.into_tree();
    for shard_entry_result in root_tree.iter() {
        let shard_entry = shard_entry_result?;
        let shard_name = shard_entry.filename().to_str_lossy().to_string();
        if shard_name.len() != 2 || !shard_entry.mode().is_tree() {
            continue;
        }

        let shard_tree = shard_entry.object_id().attach(repo).object()?.into_tree();
        for item_entry_result in shard_tree.iter() {
            let item_entry = item_entry_result?;
            let rest_name = item_entry.filename().to_str_lossy().to_string();
            let trail_id = format!("{}{}", shard_name, rest_name);

            if imported_trails.contains(&trail_id) {
                eprintln!("  Trail {} (already imported, skipping)", trail_id);
                continue;
            }

            if !item_entry.mode().is_tree() {
                continue;
            }
            let item_tree_id = item_entry.object_id();

            let meta_content = match entry_to_blob(repo, item_tree_id, "metadata.json")? {
                Some(c) => c,
                None => {
                    eprintln!("  Skipping trail {} (no metadata.json)", trail_id);
                    continue;
                }
            };
            let meta: Value =
                serde_json::from_str(&meta_content).context("parsing trail metadata.json")?;

            let branch_name = meta
                .get("branch")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let branch_uuid = format!(
                "{}-{}",
                branch_name,
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("0000")
            );
            eprintln!(
                "  Trail {} (branch {}) -> branch:{}",
                trail_id, branch_name, branch_uuid
            );

            count += set_value(
                repo,
                db,
                dry_run,
                &TargetType::Branch,
                &branch_uuid,
                "review:trail-id",
                &json_string(&trail_id),
                &ValueType::String,
                email,
                ts,
            )?;
            ts += 1;

            let string_fields = [
                "title", "body", "status", "type", "author", "priority", "base",
            ];
            for field in &string_fields {
                if let Some(val) = meta.get(field) {
                    let key = format!("review:{}", field);
                    let json_val = json_encode_value(val)?;
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        &key,
                        &json_val,
                        &ValueType::String,
                        email,
                        ts,
                    )?;
                    ts += 1;
                }
            }

            let json_fields = ["assignees", "labels", "reviewers"];
            for field in &json_fields {
                if let Some(val) = meta.get(field) {
                    let key = format!("review:{}", field);
                    let json_val = json_encode_value(val)?;
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        &key,
                        &json_val,
                        &ValueType::String,
                        email,
                        ts,
                    )?;
                    ts += 1;
                }
            }

            if let Some(content) = entry_to_blob(repo, item_tree_id, "checkpoints.json")? {
                let arr: Vec<Value> = serde_json::from_str(&content).unwrap_or_default();
                if !arr.is_empty() {
                    let mut entries = Vec::new();
                    for (i, item) in arr.iter().enumerate() {
                        entries.push(gmeta_core::list_value::ListEntry {
                            value: serde_json::to_string(item)?,
                            timestamp: ts + i as i64,
                        });
                    }
                    let encoded = gmeta_core::list_value::encode_entries(&entries)?;
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        "review:checkpoints",
                        &encoded,
                        &ValueType::List,
                        email,
                        ts,
                    )?;
                    ts += arr.len() as i64 + 1;
                }
            }

            if let Some(content) = entry_to_blob(repo, item_tree_id, "discussion.json")? {
                let disc: Value = serde_json::from_str(&content).unwrap_or(Value::Null);
                if disc != Value::Null {
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        "review:discussion",
                        &json_encode_value(&disc)?,
                        &ValueType::String,
                        email,
                        ts,
                    )?;
                    ts += 1;
                }
            }
        }
    }

    Ok(count)
}

/// Store a value in the database (or just count it for dry run).
/// Large string values (> GIT_REF_THRESHOLD bytes) are stored as git blob refs.
fn set_value(
    repo: &gix::Repository,
    db: Option<&Store>,
    dry_run: bool,
    target_type: &TargetType,
    target_value: &str,
    key: &str,
    value: &str,
    value_type: &ValueType,
    email: &str,
    timestamp: i64,
) -> Result<u64> {
    let use_git_ref = *value_type == ValueType::String && value.len() > GIT_REF_THRESHOLD;

    if dry_run {
        eprintln!(
            "    [dry-run] {}:{} {} = {}{}",
            target_type.as_str(),
            &target_value[..7.min(target_value.len())],
            key,
            truncate(value, 80),
            if use_git_ref { " [git-ref]" } else { "" },
        );
        return Ok(1);
    }

    if let Some(db) = db {
        if use_git_ref {
            let blob_oid: gix::ObjectId = repo.write_blob(value.as_bytes())?.into();
            db.set_with_git_ref(
                None,
                target_type,
                target_value,
                key,
                &blob_oid.to_string(),
                value_type,
                email,
                timestamp,
                true,
            )?;
        } else {
            db.set(
                target_type,
                target_value,
                key,
                value,
                value_type,
                email,
                timestamp,
            )?;
        }
    }
    Ok(1)
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s))
}

fn json_encode_value(val: &Value) -> Result<String> {
    if let Some(s) = val.as_str() {
        Ok(json_string(s))
    } else {
        let serialized = serde_json::to_string(val)?;
        Ok(json_string(&serialized))
    }
}

fn sanitize_key_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c == '/' || c == '\0' || c == ':' {
                '-'
            } else {
                c
            }
        })
        .collect()
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

const NOTES_REFS: &[&str] = &["refs/remotes/notes/ai", "refs/notes/ai"];

fn run_git_ai(dry_run: bool, since_epoch: Option<i64>) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();
    let email = ctx.email();

    let db = if dry_run { None } else { Some(ctx.store()) };

    // Locate the notes ref
    let notes_ref = NOTES_REFS
        .iter()
        .find(|&&r| repo.find_reference(r).is_ok())
        .copied();

    let notes_ref = match notes_ref {
        Some(r) => r,
        None => bail!(
            "no git-ai notes ref found; expected one of: {}",
            NOTES_REFS.join(", ")
        ),
    };

    eprintln!("importing git-ai notes from {}", notes_ref);

    // Resolve to the notes tree OID
    let notes_commit_id = repo
        .find_reference(notes_ref)?
        .into_fully_peeled_id()?
        .detach();
    let notes_commit = notes_commit_id.attach(repo).object()?.into_commit();
    let notes_tree_id = notes_commit.tree_id()?.detach();

    // Walk the two-level fanout tree
    let mut total = 0u64;
    let mut imported = 0u64;
    let mut skipped_date = 0u64;
    let mut skipped_exists = 0u64;
    let mut errors = 0u64;

    let notes_tree = notes_tree_id.attach(repo).object()?.into_tree();
    for shard_entry_result in notes_tree.iter() {
        let shard_entry = match shard_entry_result {
            Ok(e) => e,
            Err(_) => continue,
        };
        let shard_name = shard_entry.filename().to_str_lossy().to_string();
        // Only descend into two-char hex shard dirs.
        if shard_name.len() != 2 || !shard_name.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        if !shard_entry.mode().is_tree() {
            continue;
        }
        let shard_tree = match shard_entry.object_id().attach(repo).object() {
            Ok(o) => o.into_tree(),
            Err(_) => continue,
        };

        for note_entry_result in shard_tree.iter() {
            let note_entry = match note_entry_result {
                Ok(e) => e,
                Err(_) => continue,
            };
            let rest = note_entry.filename().to_str_lossy().to_string();
            let commit_sha = format!("{}{}", shard_name, rest);

            // Verify the annotated commit exists and is within --since range.
            let commit_oid = match gix::ObjectId::from_hex(commit_sha.as_bytes()) {
                Ok(o) => o,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let annotated_commit = match commit_oid.attach(repo).object() {
                Ok(o) => o.into_commit(),
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let decoded = match annotated_commit.decode() {
                Ok(d) => d,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            if let Some(since) = since_epoch {
                if {
                    let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                    a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
                } < since
                {
                    skipped_date += 1;
                    continue;
                }
            }
            let commit_ts = {
                let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
            } * 1000;

            total += 1;

            // Read the note blob.
            let blob = match note_entry.object_id().attach(repo).object() {
                Ok(o) => o.into_blob(),
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let note_text = match std::str::from_utf8(&blob.data) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };

            // Parse the note.
            let parsed = match parse_git_ai_note(&note_text) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!(
                        "  warning: could not parse note for {}: {}",
                        &commit_sha[..8],
                        e
                    );
                    errors += 1;
                    continue;
                }
            };

            // Check whether we already have data for this commit
            if let Some(db) = db {
                if db
                    .get(&TargetType::Commit, &commit_sha, "agent.blame")?
                    .is_some()
                {
                    skipped_exists += 1;
                    continue;
                }
            }

            eprintln!(
                "  commit {}  schema={}{}",
                &commit_sha[..8],
                parsed.schema_version,
                if parsed.model == "unknown" {
                    String::new()
                } else {
                    format!("  model={}", parsed.model)
                },
            );

            if let Some(db) = db {
                // agent.blame -- store as git blob ref if large
                let (blame_val, is_ref) = if parsed.blame.len() > GIT_REF_THRESHOLD {
                    let oid: gix::ObjectId = repo.write_blob(parsed.blame.as_bytes())?.into();
                    (oid.to_string(), true)
                } else {
                    (json_string(&parsed.blame), false)
                };
                db.set_with_git_ref(
                    None,
                    &TargetType::Commit,
                    &commit_sha,
                    "agent.blame",
                    &blame_val,
                    &ValueType::String,
                    email,
                    commit_ts,
                    is_ref,
                )?;

                db.set(
                    &TargetType::Commit,
                    &commit_sha,
                    "agent.git-ai.schema-version",
                    &json_string(&parsed.schema_version),
                    &ValueType::String,
                    email,
                    commit_ts,
                )?;

                if let Some(ref ver) = parsed.git_ai_version {
                    db.set(
                        &TargetType::Commit,
                        &commit_sha,
                        "agent.git-ai.version",
                        &json_string(ver),
                        &ValueType::String,
                        email,
                        commit_ts,
                    )?;
                }

                if parsed.model != "unknown" {
                    db.set(
                        &TargetType::Commit,
                        &commit_sha,
                        "agent.model",
                        &json_string(&parsed.model),
                        &ValueType::String,
                        email,
                        commit_ts,
                    )?;
                }
            }

            imported += 1;
        }
    }

    eprintln!();
    if dry_run {
        eprintln!(
            "dry-run: would import {} commits ({} skipped: date filter={}, already exists={}; {} errors)",
            total.saturating_sub(skipped_date + skipped_exists + errors),
            skipped_date + skipped_exists,
            skipped_date,
            skipped_exists,
            errors,
        );
    } else {
        eprintln!(
            "imported {} commits  (skipped: date={} already-exists={}  errors={})",
            imported, skipped_date, skipped_exists, errors,
        );
    }

    Ok(())
}

struct GitAiNote {
    blame: String,
    schema_version: String,
    git_ai_version: Option<String>,
    model: String,
}

fn parse_git_ai_note(text: &str) -> Result<GitAiNote> {
    let (blame_raw, json_raw) = if let Some(rest) = text.strip_prefix("---\n") {
        ("", rest)
    } else {
        let sep = "\n---\n";
        match text.find(sep) {
            Some(pos) => (&text[..pos], &text[pos + sep.len()..]),
            None => bail!("no '---' separator found in note"),
        }
    };

    let blame = blame_raw.trim_end().to_string();

    let json: Value = serde_json::from_str(json_raw.trim()).context("failed to parse note JSON")?;

    let schema_version = json
        .get("schema_version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let git_ai_version = json
        .get("git_ai_version")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut models: Vec<String> = Vec::new();
    if let Some(prompts) = json.get("prompts").and_then(|v| v.as_object()) {
        for prompt in prompts.values() {
            if let Some(agent_id) = prompt.get("agent_id") {
                let tool = agent_id
                    .get("tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let model = agent_id
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let entry = format!("{}/{}", tool, model);
                if !models.contains(&entry) {
                    models.push(entry);
                }
            }
        }
    }
    let model = if models.is_empty() {
        "unknown".to_string()
    } else {
        models.join(", ")
    };

    Ok(GitAiNote {
        blame,
        schema_version,
        git_ai_version,
        model,
    })
}
