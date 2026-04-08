use anyhow::{bail, Result};
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;

use crate::context::CommandContext;
use gmeta_core::types::{TargetType, ValueType};

pub fn run() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();
    let ns = ctx.namespace();

    let tracking_ref = format!("refs/{}/remotes/main", ns);
    let tip_oid = match repo.find_reference(&tracking_ref) {
        Ok(r) => r.into_fully_peeled_id()?.detach(),
        Err(_) => bail!(
            "no remote tracking ref ({}).\nAdd a remote first: gmeta remote add <url>",
            tracking_ref
        ),
    };

    eprintln!("Walking history from {} ...", &tip_oid.to_string()[..12]);

    let walk = repo.rev_walk(Some(tip_oid));
    let iter = walk.all()?;

    let mut commits_walked = 0;
    let mut commits_parsed = 0;
    let mut commits_unparseable = 0;
    let mut inserted = 0;
    let mut skipped_existing = 0;
    let mut skipped_deletes = 0;
    let mut is_tip = true;

    for info_result in iter {
        let info = info_result?;
        let oid = info.id;
        commits_walked += 1;

        let commit_obj = oid.attach(repo).object()?.into_commit();
        let decoded = commit_obj.decode()?;

        if is_tip {
            is_tip = false;
            let msg_first_line = decoded.message.to_str_lossy();
            let msg_first_line = msg_first_line.lines().next().unwrap_or("");
            eprintln!(
                "  {} (tip, skipped -- already materialized) {}",
                &oid.to_string()[..12],
                msg_first_line,
            );
            continue;
        }

        let message = decoded.message.to_str_lossy().to_string();
        let first_line = message.lines().next().unwrap_or("");

        match super::pull::parse_commit_changes_pub(&message) {
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
                    let tt = TargetType::from_str(target_type)?;
                    if ctx
                        .store()
                        .insert_promised(&tt, target_value, key, &ValueType::String)?
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
            None if decoded.parents().count() == 0 => {
                // Root commit without a change list -- walk its tree
                let tree_id = decoded.tree();
                let keys = super::pull::extract_keys_from_tree_pub(repo, tree_id)?;
                commits_parsed += 1;
                let mut commit_inserted = 0;
                let mut commit_skipped = 0;

                for (target_type, target_value, key) in &keys {
                    let tt = TargetType::from_str(target_type)?;
                    if ctx
                        .store()
                        .insert_promised(&tt, target_value, key, &ValueType::String)?
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
