use anyhow::{bail, Result};
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;

use crate::context::CommandContext;
use git_meta_lib::types::{TargetType, ValueType};

pub fn run() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let ns = ctx.session.namespace();

    let tracking_ref = format!("refs/{ns}/remotes/main");
    let tip_oid = match repo.find_reference(&tracking_ref) {
        Ok(r) => r.into_fully_peeled_id()?.detach(),
        Err(_) => bail!(
            "no remote tracking ref ({tracking_ref}).\nAdd a remote first: git meta remote add <url>"
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

        match git_meta_lib::sync::parse_commit_changes(&message) {
            Some(changes) => {
                commits_parsed += 1;
                let mut commit_inserted = 0;
                let mut commit_skipped = 0;
                let mut commit_deletes = 0;

                for change in &changes {
                    if change.op == 'D' {
                        commit_deletes += 1;
                        skipped_deletes += 1;
                        continue;
                    }
                    let tt = change.target_type.parse::<TargetType>()?;
                    let target = if tt == TargetType::Project {
                        git_meta_lib::types::Target::project()
                    } else {
                        git_meta_lib::types::Target::from_parts(
                            tt,
                            Some(change.target_value.clone()),
                        )
                    };
                    if ctx.session.store().insert_promised(
                        &target,
                        &change.key,
                        &ValueType::String,
                    )? {
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
            None if decoded.parents().count() == 0
                || git_meta_lib::sync::commit_changes_omitted(&message) =>
            {
                // Root commits and omitted-change commits do not have an
                // inline per-key list, so discover keys from tree paths.
                let reason = if decoded.parents().count() == 0 {
                    "root"
                } else {
                    "changes omitted"
                };
                let tree_id = decoded.tree();
                let keys = git_meta_lib::sync::extract_keys_from_tree(repo, tree_id)?;
                commits_parsed += 1;
                let mut commit_inserted = 0;
                let mut commit_skipped = 0;

                for (target_type, target_value, key) in &keys {
                    let tt = target_type.parse::<TargetType>()?;
                    let target = if tt == TargetType::Project {
                        git_meta_lib::types::Target::project()
                    } else {
                        git_meta_lib::types::Target::from_parts(tt, Some(target_value.clone()))
                    };
                    if ctx
                        .session
                        .store()
                        .insert_promised(&target, key, &ValueType::String)?
                    {
                        commit_inserted += 1;
                        inserted += 1;
                    } else {
                        commit_skipped += 1;
                        skipped_existing += 1;
                    }
                }

                eprintln!(
                    "  {} ({reason}, {} tree keys: +{} inserted, ~{} existing) {}",
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
        "Walked {commits_walked} commits ({commits_parsed} parsed, {commits_unparseable} without change lists)",
    );
    println!(
        "Inserted {inserted} promisor keys ({skipped_existing} already existed, {skipped_deletes} deletes skipped)",
    );

    Ok(())
}
