use anyhow::{bail, Result};

use crate::context::CommandContext;
use crate::git_utils;

pub fn run() -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;
    let ns = git_utils::git2_get_namespace(repo)?;

    let tracking_ref = format!("refs/{}/remotes/main", ns);
    let tip_commit = match repo.find_reference(&tracking_ref) {
        Ok(r) => r.peel_to_commit()?,
        Err(_) => bail!(
            "no remote tracking ref ({}).\nAdd a remote first: gmeta remote add <url>",
            tracking_ref
        ),
    };

    let tip_oid = tip_commit.id();
    eprintln!("Walking history from {} ...", &tip_oid.to_string()[..12]);

    let mut revwalk = repo.revwalk()?;
    revwalk.push(tip_oid)?;

    let mut commits_walked = 0;
    let mut commits_parsed = 0;
    let mut commits_unparseable = 0;
    let mut inserted = 0;
    let mut skipped_existing = 0;
    let mut skipped_deletes = 0;
    let mut is_tip = true;

    for oid_result in revwalk {
        let oid = oid_result?;
        commits_walked += 1;

        if is_tip {
            is_tip = false;
            let msg_first_line = tip_commit
                .message()
                .unwrap_or("")
                .lines()
                .next()
                .unwrap_or("");
            eprintln!(
                "  {} (tip, skipped — already materialized) {}",
                &oid.to_string()[..12],
                msg_first_line,
            );
            continue;
        }

        let commit = repo.find_commit(oid)?;
        let message = commit.message().unwrap_or("");
        let first_line = message.lines().next().unwrap_or("");

        match super::pull::parse_commit_changes_pub(message) {
            Some(changes) => {
                commits_parsed += 1;
                let mut commit_inserted = 0;
                let mut commit_skipped = 0;
                let mut commit_deletes = 0;

                for (op, target_type, target_value, key) in &changes {
                    if *op == 'D' {
                        commit_deletes += 1;
                        skipped_deletes += 1;
                        continue;
                    }
                    if ctx
                        .db
                        .insert_promised(target_type, target_value, key, "string")?
                    {
                        commit_inserted += 1;
                        inserted += 1;
                    } else {
                        commit_skipped += 1;
                        skipped_existing += 1;
                    }
                }

                eprintln!(
                    "  {} ({} changes: +{} inserted, ~{} existing, -{} deletes) {}",
                    &oid.to_string()[..12],
                    changes.len(),
                    commit_inserted,
                    commit_skipped,
                    commit_deletes,
                    first_line,
                );
            }
            None if commit.parent_count() == 0 => {
                // Root commit without a change list — walk its tree
                let tree = commit.tree()?;
                let keys = super::pull::extract_keys_from_tree_pub(repo, &tree)?;
                commits_parsed += 1;
                let mut commit_inserted = 0;
                let mut commit_skipped = 0;

                for (target_type, target_value, key) in &keys {
                    if ctx
                        .db
                        .insert_promised(target_type, target_value, key, "string")?
                    {
                        commit_inserted += 1;
                        inserted += 1;
                    } else {
                        commit_skipped += 1;
                        skipped_existing += 1;
                    }
                }

                eprintln!(
                    "  {} (root, {} tree keys: +{} inserted, ~{} existing) {}",
                    &oid.to_string()[..12],
                    keys.len(),
                    commit_inserted,
                    commit_skipped,
                    first_line,
                );
            }
            None => {
                commits_unparseable += 1;
                eprintln!(
                    "  {} (no change list) {}",
                    &oid.to_string()[..12],
                    first_line,
                );
            }
        }
    }

    eprintln!();
    println!(
        "Walked {} commits ({} parsed, {} without change lists)",
        commits_walked, commits_parsed, commits_unparseable,
    );
    println!(
        "Inserted {} promisor keys ({} already existed, {} deletes skipped)",
        inserted, skipped_existing, skipped_deletes,
    );

    Ok(())
}
