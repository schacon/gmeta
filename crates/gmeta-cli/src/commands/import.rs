use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use git2::Repository;
use serde_json::Value;

use crate::context::CommandContext;
use gmeta_core::db::Db;
use gmeta_core::types::{ImportFormat, TargetType, ValueType, GIT_REF_THRESHOLD};

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
        _ => bail!("unsupported import format"),
    }
}

fn run_entire(dry_run: bool, since_epoch: Option<i64>) -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;
    let email = &ctx.email;
    let fallback_ts = ctx.timestamp;

    let db = if dry_run { None } else { Some(&ctx.db) };

    let mut imported_count = 0u64;

    // Resolve the checkpoints tree (local or remote)
    let checkpoints_tree = resolve_entire_ref(repo, "entire/checkpoints/v1")?;
    if checkpoints_tree.is_none() {
        eprintln!("No entire/checkpoints/v1 ref found (local or remote), skipping checkpoints");
    }

    // Step 1: Walk all commits looking for Entire-Checkpoint trailers.
    // For each one, look up the checkpoint in the checkpoints tree and import.
    if let Some(ref cp_tree) = checkpoints_tree {
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
            import_checkpoints_from_commits(repo, cp_tree, db, email, dry_run, since_epoch)?;
    }

    // Step 2: Import trails
    if let Some(tree) = resolve_entire_ref(repo, "entire/trails/v1")? {
        eprintln!("Processing entire/trails/v1...");
        imported_count += import_trails(repo, &tree, db, email, fallback_ts, dry_run)?;
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

/// Resolve an entire ref to the tree of its tip commit.
fn resolve_entire_ref<'a>(repo: &'a Repository, refname: &str) -> Result<Option<git2::Tree<'a>>> {
    let reference = repo
        .find_reference(&format!("refs/heads/{}", refname))
        .or_else(|_| repo.find_reference(&format!("refs/remotes/origin/{}", refname)))
        .or_else(|_| repo.find_reference(refname));

    match reference {
        Ok(r) => {
            let refname_used = r.name().unwrap_or("unknown");
            eprintln!("  Resolved {} via {}", refname, refname_used);
            let commit = r.peel_to_commit()?;
            Ok(Some(commit.tree()?))
        }
        Err(_) => Ok(None),
    }
}

/// Walk all commits across all refs, find Entire-Checkpoint trailers,
/// look up each checkpoint in the checkpoints tree, and import it.
fn import_checkpoints_from_commits(
    repo: &Repository,
    checkpoints_tree: &git2::Tree,
    db: Option<&Db>,
    email: &str,
    dry_run: bool,
    since_epoch: Option<i64>,
) -> Result<u64> {
    let mut count = 0u64;
    let mut seen_commits: HashSet<git2::Oid> = HashSet::new();
    let mut found = 0u64;
    let mut skipped = 0u64;
    let mut missing = 0u64;

    let mut revwalk = repo.revwalk()?;
    // Push all refs except entire/* branches themselves
    if let Ok(refs) = repo.references() {
        for r in refs.flatten() {
            let name = r.name().unwrap_or("");
            if name.contains("/entire/") {
                continue;
            }
            if let Some(oid) = r.target() {
                revwalk.push(oid).ok();
            }
        }
    }
    revwalk.set_sorting(git2::Sort::TIME)?;

    let mut scanned = 0u64;
    for oid in revwalk {
        let oid = oid?;
        if !seen_commits.insert(oid) {
            continue;
        }
        scanned += 1;

        let commit = repo.find_commit(oid)?;

        // Skip commits older than --since date
        if let Some(cutoff) = since_epoch {
            if commit.time().seconds() < cutoff {
                continue;
            }
        }

        let msg = commit.message().unwrap_or("");

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
                if let Ok(Some(_)) = db.get(&TargetType::Commit, &commit_sha, "agent:checkpoint-id")
                {
                    skipped += 1;
                    continue;
                }
            }

            // Look up checkpoint in the sharded tree: first2/rest/
            let shard = &checkpoint_id[..2.min(checkpoint_id.len())];
            let rest = &checkpoint_id[2.min(checkpoint_id.len())..];

            let checkpoint_tree = (|| -> Result<Option<git2::Tree>> {
                let shard_tree = match entry_to_tree(repo, checkpoints_tree, shard)? {
                    Some(t) => t,
                    None => return Ok(None),
                };
                entry_to_tree(repo, &shard_tree, rest)
            })()?;

            let checkpoint_tree = match checkpoint_tree {
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
                "  Commit {} ← checkpoint {}",
                &commit_sha[..7],
                checkpoint_id,
            );

            // Use the commit's author date as the metadata timestamp
            let mut ts = commit.author().when().seconds() * 1000;

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
            if let Some(content) = entry_to_blob(repo, &checkpoint_tree, "metadata.json")? {
                let meta: Value = serde_json::from_str(&content).unwrap_or(Value::Null);
                // Each tuple is (gmeta-key, [json aliases in priority order]).
                // Only the first alias found is imported, avoiding duplicate writes.
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
                let session_tree = match entry_to_tree(repo, &checkpoint_tree, &slot_name)? {
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
                    &session_tree,
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

    eprintln!(
        "Scanned {} commits: {} checkpoints imported, {} already present, {} not found in tree",
        scanned, found, skipped, missing
    );

    Ok(count)
}

/// Import a single session slot's data.
fn import_session(
    repo: &Repository,
    session_tree: &git2::Tree,
    db: Option<&Db>,
    commit_sha: &str,
    key_prefix: &str,
    email: &str,
    ts: &mut i64,
    dry_run: bool,
) -> Result<u64> {
    let mut count = 0u64;

    // Session metadata.json
    if let Some(content) = entry_to_blob(repo, session_tree, "metadata.json")? {
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
    if let Some(content) = entry_to_blob(repo, session_tree, "prompt.txt")? {
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

    // full.jsonl → transcript stored as a single string blob (large, so typically a git ref)
    if let Some(content) = entry_to_blob(repo, session_tree, "full.jsonl")? {
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
    if let Some(content) = entry_to_blob(repo, session_tree, "content_hash.txt")? {
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

    // tasks/ directory → subagent data
    if let Some(tasks_tree) = entry_to_tree(repo, session_tree, "tasks")? {
        for task_entry in tasks_tree.iter() {
            let tool_use_id = task_entry.name().unwrap_or("").to_string();
            if tool_use_id.is_empty() {
                continue;
            }
            let task_obj = task_entry.to_object(repo)?;
            let task_tree = match task_obj.as_tree() {
                Some(t) => t,
                None => continue,
            };

            if let Some(content) = entry_to_blob(repo, task_tree, "checkpoint.json")? {
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

            for agent_entry in task_tree.iter() {
                let name = agent_entry.name().unwrap_or("");
                if name.starts_with("agent-") && name.ends_with(".jsonl") {
                    let agent_obj = agent_entry.to_object(repo)?;
                    if let Some(blob) = agent_obj.as_blob() {
                        let content = String::from_utf8_lossy(blob.content());
                        let agent_id = name
                            .strip_prefix("agent-")
                            .unwrap_or(name)
                            .strip_suffix(".jsonl")
                            .unwrap_or(name);
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
    }

    Ok(count)
}

/// Read a blob from a tree entry by name.
fn entry_to_blob(repo: &Repository, tree: &git2::Tree, name: &str) -> Result<Option<String>> {
    match tree.get_name(name) {
        Some(entry) => {
            let obj = entry.to_object(repo)?;
            match obj.as_blob() {
                Some(blob) => Ok(Some(String::from_utf8_lossy(blob.content()).to_string())),
                None => Ok(None),
            }
        }
        None => Ok(None),
    }
}

/// Read a subtree from a tree entry by name.
fn entry_to_tree<'a>(
    repo: &'a Repository,
    tree: &git2::Tree,
    name: &str,
) -> Result<Option<git2::Tree<'a>>> {
    match tree.get_name(name) {
        Some(entry) => {
            let obj = entry.to_object(repo)?;
            match obj.into_tree() {
                Ok(t) => Ok(Some(t)),
                Err(_) => Ok(None),
            }
        }
        None => Ok(None),
    }
}

/// Load the set of trail IDs that have already been imported.
fn load_imported_trail_ids(db: Option<&Db>) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    if let Some(db) = db {
        let mut stmt = db.conn.prepare(
            "SELECT value FROM metadata WHERE key = 'review:trail-id' AND target_type = 'branch'",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let val = row?;
            if let Ok(s) = serde_json::from_str::<String>(&val) {
                ids.insert(s);
            }
        }
    }
    Ok(ids)
}

fn import_trails(
    repo: &Repository,
    root_tree: &git2::Tree,
    db: Option<&Db>,
    email: &str,
    base_ts: i64,
    dry_run: bool,
) -> Result<u64> {
    let mut count = 0u64;
    let mut ts = base_ts;
    let imported_trails = load_imported_trail_ids(db)?;

    for shard_entry in root_tree.iter() {
        let shard_name = shard_entry.name().unwrap_or("");
        if shard_name.len() != 2 {
            continue;
        }
        let shard_obj = shard_entry.to_object(repo)?;
        let shard_tree = match shard_obj.as_tree() {
            Some(t) => t,
            None => continue,
        };

        for item_entry in shard_tree.iter() {
            let rest_name = item_entry.name().unwrap_or("");
            let trail_id = format!("{}{}", shard_name, rest_name);

            if imported_trails.contains(&trail_id) {
                eprintln!("  Trail {} (already imported, skipping)", trail_id);
                continue;
            }

            let item_obj = item_entry.to_object(repo)?;
            let item_tree = match item_obj.as_tree() {
                Some(t) => t,
                None => continue,
            };

            let meta_content = match entry_to_blob(repo, item_tree, "metadata.json")? {
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
                "  Trail {} (branch {}) → branch:{}",
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

            if let Some(content) = entry_to_blob(repo, item_tree, "checkpoints.json")? {
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

            if let Some(content) = entry_to_blob(repo, item_tree, "discussion.json")? {
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
    repo: &Repository,
    db: Option<&Db>,
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
            let blob_oid = repo.blob(value.as_bytes())?;
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

//
// Reads the git-ai authorship notes stored in refs/remotes/notes/ai (or the
// local mirror refs/notes/ai).  Each blob in that fanout tree is a note for
// one commit.  The note format is:
//
//   <path>
//     <prompt_id> <lines>
//   ...
//   ---
//   { JSON with schema_version, git_ai_version, prompts: { <id>: { agent_id: { tool, model }, ... } } }
//
// We import three metadata keys per commit:
//   agent.blame           — the raw blame section (paths + line ranges), as-is
//   agent.git-ai.schema-version — schema_version from the JSON
//   agent.git-ai.version  — git_ai_version from the JSON (omitted if absent)
//   agent.model           — comma-separated "tool/model" for each unique prompt
//
// When multiple prompts exist the model values are deduplicated and joined with
// ", " so the field stays a simple string.

const NOTES_REFS: &[&str] = &["refs/remotes/notes/ai", "refs/notes/ai"];

fn run_git_ai(dry_run: bool, since_epoch: Option<i64>) -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;
    let email = &ctx.email;

    let db = if dry_run { None } else { Some(&ctx.db) };

    // Locate the notes ref — prefer remote mirror, fall back to local.
    let notes_ref = NOTES_REFS
        .iter()
        .find(|r| repo.find_reference(r).is_ok())
        .copied();

    let notes_ref = match notes_ref {
        Some(r) => r,
        None => bail!(
            "no git-ai notes ref found; expected one of: {}",
            NOTES_REFS.join(", ")
        ),
    };

    eprintln!("importing git-ai notes from {}", notes_ref);

    // Resolve to the notes tree OID (the ref points to a commit whose tree is
    // the fanout directory).
    let notes_tree_oid = {
        let reference = repo.find_reference(notes_ref)?;
        let obj = reference.peel(git2::ObjectType::Commit)?;
        let commit = obj.as_commit().context("notes ref is not a commit")?;
        commit.tree_id()
    };
    let notes_tree = repo.find_tree(notes_tree_oid)?;

    // Walk the two-level fanout tree: top-level entries are two-hex-char
    // directory names; their children are blobs named with the remaining 38
    // chars of the commit SHA.
    let mut total = 0u64;
    let mut imported = 0u64;
    let mut skipped_date = 0u64;
    let mut skipped_exists = 0u64;
    let mut errors = 0u64;

    for shard_entry in notes_tree.iter() {
        let shard_name = match shard_entry.name() {
            Some(n) => n.to_string(),
            None => continue,
        };
        // Only descend into two-char hex shard dirs.
        if shard_name.len() != 2 || !shard_name.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        let shard_tree = match repo.find_tree(shard_entry.id()) {
            Ok(t) => t,
            Err(_) => continue,
        };

        for note_entry in shard_tree.iter() {
            let rest = match note_entry.name() {
                Some(n) => n.to_string(),
                None => continue,
            };
            // Full commit SHA = shard prefix + remaining chars.
            let commit_sha = format!("{}{}", shard_name, rest);

            // Verify the annotated commit exists and is within --since range.
            let commit_oid = match git2::Oid::from_str(&commit_sha) {
                Ok(o) => o,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let annotated_commit = match repo.find_commit(commit_oid) {
                Ok(c) => c,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            if let Some(since) = since_epoch {
                if annotated_commit.time().seconds() < since {
                    skipped_date += 1;
                    continue;
                }
            }
            let commit_ts = annotated_commit.author().when().seconds() * 1000;

            total += 1;

            // Read the note blob.
            let blob = match repo.find_blob(note_entry.id()) {
                Ok(b) => b,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let note_text = match std::str::from_utf8(blob.content()) {
                Ok(s) => s,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };

            // Parse the note.
            let parsed = match parse_git_ai_note(note_text) {
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

            // Check whether we already have data for this commit so we can
            // report skips without touching the DB on a real run.
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
                // agent.blame — store as git blob ref if large
                let (blame_val, is_ref) = if parsed.blame.len() > GIT_REF_THRESHOLD {
                    let oid = repo.blob(parsed.blame.as_bytes())?;
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
    blame: String, // raw blame section (everything before ---)
    schema_version: String,
    git_ai_version: Option<String>,
    model: String, // deduplicated "tool/model" joined with ", "
}

fn parse_git_ai_note(text: &str) -> Result<GitAiNote> {
    // Split on the `---` separator between blame and JSON.
    // Notes with no blame section start with `---\n` directly.
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

    // Collect unique "tool/model" strings across all prompts.
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
