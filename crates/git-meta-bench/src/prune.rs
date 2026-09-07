//! Prune-setting comparison.
//!
//! Applies the auto-prune rules to a generated metadata tree at several
//! retention windows and records what each setting costs and keeps.

use anyhow::Result;
use git_meta_lib::prune::{compute_tree_size_for, parse_since_to_cutoff_ms};
use git_meta_lib::serialize::{build_filtered_tree, count_prune_stats};
use git_meta_lib::types::TargetType;
use git_meta_lib::Session;
use serde::Serialize;

/// Which prune produced a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum PruneImpl {
    /// Rebuild the tree from SQLite rows newer than a cutoff, the way
    /// `git meta prune` does.
    DbRebuild,
}

/// One prune configuration and its measured effect.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PruneResult {
    pub implementation: PruneImpl,
    pub label: String,
    pub since: String,
    pub min_size: Option<u64>,
    pub keys_before: u64,
    pub keys_after: u64,
    pub keys_dropped: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub prune_ms: u128,
}

/// A prune setting to evaluate.
#[derive(Debug, Clone)]
pub(crate) struct PruneSetting {
    pub label: String,
    pub since: String,
    pub min_size: Option<u64>,
}

impl PruneSetting {
    pub(crate) fn new(label: &str, since: &str, min_size: Option<u64>) -> Self {
        PruneSetting {
            label: label.to_string(),
            since: since.to_string(),
            min_size,
        }
    }
}

/// Apply each setting to `tree_oid` and measure the result.
///
/// `now_ms` is the simulated "current time" that retention windows are measured
/// back from — it must match the end of the generated history, not the wall
/// clock, or every entry looks ancient.
pub(crate) fn compare(
    session: &Session,
    tree_oid: gix::ObjectId,
    settings: &[PruneSetting],
    now_ms: i64,
) -> Result<Vec<PruneResult>> {
    let repo = session.repo();
    let bytes_before = compute_tree_size_for(repo, tree_oid)?;
    let (_, keys_before) = count_prune_stats(repo, tree_oid, tree_oid)?;

    let mut results = Vec::new();
    for setting in settings {
        results.push(db_rebuild(
            session,
            setting,
            tree_oid,
            keys_before,
            bytes_before,
            now_ms,
        )?);
    }

    Ok(results)
}

/// Rebuild the serialized tree from the SQLite rows newer than the cutoff.
///
/// This mirrors `git meta prune`, which does not walk the existing tree at all
/// — it re-serializes the surviving rows. Project-scoped metadata is always
/// retained.
fn rebuild_tree(session: &Session, setting: &PruneSetting, now_ms: i64) -> Result<gix::ObjectId> {
    let store = session.store();
    let cutoff_ms = parse_since_to_cutoff_ms(&setting.since, now_ms)?;
    let keep = |target_type: &TargetType, timestamp: i64| {
        *target_type == TargetType::Project || timestamp >= cutoff_ms
    };

    let metadata: Vec<_> = store
        .get_all_metadata()?
        .into_iter()
        .filter(|entry| keep(&entry.target_type, entry.last_timestamp))
        .collect();
    let tombstones: Vec<_> = store
        .get_all_tombstones()?
        .into_iter()
        .filter(|record| keep(&record.target_type, record.timestamp))
        .collect();
    let set_tombstones: Vec<_> = store
        .get_all_set_tombstones()?
        .into_iter()
        .filter(|record| keep(&record.target_type, record.timestamp))
        .collect();
    let list_tombstones: Vec<_> = store
        .get_all_list_tombstones()?
        .into_iter()
        .filter(|record| keep(&record.target_type, record.timestamp))
        .collect();

    build_filtered_tree(
        session.repo(),
        &metadata,
        &tombstones,
        &set_tombstones,
        &list_tombstones,
    )
    .map_err(Into::into)
}

/// Measure the DB-rebuild prune path against `original_tree`.
fn db_rebuild(
    session: &Session,
    setting: &PruneSetting,
    original_tree: gix::ObjectId,
    keys_before: u64,
    bytes_before: u64,
    now_ms: i64,
) -> Result<PruneResult> {
    let repo = session.repo();
    let started = std::time::Instant::now();
    let rebuilt = rebuild_tree(session, setting, now_ms)?;
    let prune_ms = started.elapsed().as_millis();

    let (keys_dropped, keys_after) = count_prune_stats(repo, original_tree, rebuilt)?;
    Ok(PruneResult {
        implementation: PruneImpl::DbRebuild,
        label: setting.label.clone(),
        since: setting.since.clone(),
        min_size: setting.min_size,
        keys_before,
        keys_after,
        keys_dropped,
        bytes_before,
        bytes_after: compute_tree_size_for(repo, rebuilt)?,
        prune_ms,
    })
}

/// Apply a prune and commit the result on top of the local metadata ref.
///
/// Returns the new commit OID. This mirrors what `git meta prune` does, so the
/// sparse-fetch scenario can publish a genuinely pruned history.
pub(crate) fn prune_and_commit(
    session: &Session,
    setting: &PruneSetting,
    now_ms: i64,
) -> Result<Option<gix::ObjectId>> {
    let repo = session.repo();
    let ref_name = format!("refs/{}/local/main", session.namespace());
    let Ok(reference) = repo.find_reference(&ref_name) else {
        return Ok(None);
    };
    let parent = reference.into_fully_peeled_id()?.detach();
    let tree_oid = repo.find_object(parent)?.into_commit().tree_id()?.detach();

    // Use the DB-rebuild path: it is what `git meta prune` does, and it is the
    // only one that actually drops old string values from the tip tree.
    let pruned_tree = rebuild_tree(session, setting, now_ms)?;
    let (dropped, retained) = count_prune_stats(repo, tree_oid, pruned_tree)?;

    let signature = gix::actor::Signature {
        name: session.name().into(),
        email: session.email().into(),
        time: gix::date::Time::now_local_or_utc(),
    };
    let commit = gix::objs::Commit {
        message: format!(
            "git-meta: prune --since={}\n\npruned: true\nsince: {}\nkeys-dropped: {dropped}\nkeys-retained: {retained}",
            setting.since, setting.since
        )
        .into(),
        tree: pruned_tree,
        author: signature.clone(),
        committer: signature,
        encoding: None,
        parents: vec![parent].into(),
        extra_headers: Vec::new(),
    };
    let commit_oid = repo.write_object(&commit)?.detach();
    repo.reference(
        ref_name.as_str(),
        commit_oid,
        gix::refs::transaction::PreviousValue::Any,
        "git-meta-bench: prune",
    )?;

    Ok(Some(commit_oid))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use git_meta_lib::types::MetaValue;

    use super::*;
    use crate::workload::{Mode, Workload};

    const DAY_MS: i64 = 24 * 60 * 60 * 1000;

    /// Build a history whose writes are spread across `span_days`, and return
    /// the session plus the simulated "now" at the end of that span.
    fn history(dir: &std::path::Path, span_days: i64) -> (Session, i64) {
        crate::support::init_repo(dir).expect("init repo");
        let workload = Workload {
            commits: 40,
            targets_per_commit: 1,
            list_entries: 2,
            value_sizes: crate::workload::SizeMix::uniform(16),
            mode: Mode::Grow,
            span_days,
            checkpoint_every: 0,
            ..Workload::default()
        };
        crate::workload::generate(dir, &workload, false).expect("generate");
        let session = Session::open(dir).expect("open session");
        (session, workload.start_ms + span_days * DAY_MS)
    }

    fn tip_tree(session: &Session) -> gix::ObjectId {
        crate::reads::local_tip_tree(session)
            .expect("read tip")
            .expect("tip tree exists")
    }

    /// A pruned tip plus an unpruned parent is what makes a value "deep":
    /// prune must actually move keys out of the tip tree.
    #[test]
    fn prune_and_commit_moves_keys_out_of_the_tip_tree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo_dir = dir.path().join("repo");
        let (session, now_ms) = history(&repo_dir, 400);
        let before = tip_tree(&session);

        let setting = PruneSetting::new("keep 30d", "30d", None);
        let commit = prune_and_commit(&session, &setting, now_ms)
            .expect("prune")
            .expect("a commit was written");

        let after = session
            .repo()
            .find_object(commit)
            .expect("find commit")
            .into_commit()
            .tree_id()
            .expect("tree id")
            .detach();

        let (dropped, retained) = count_prune_stats(session.repo(), before, after).expect("stats");
        assert!(dropped > 0, "prune dropped nothing");
        assert!(retained > 0, "prune dropped everything");
    }

    /// Project metadata is exempt from retention: config written once at the
    /// start of a project must not age out.
    #[test]
    fn db_rebuild_keeps_project_metadata_regardless_of_age() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo_dir = dir.path().join("repo");
        let (session, now_ms) = history(&repo_dir, 400);

        let project = git_meta_lib::types::Target::project();
        let session = session.with_timestamp(
            git_meta_lib::prune::parse_since_to_cutoff_ms("365d", now_ms).expect("cutoff")
                - 10 * DAY_MS,
        );
        session
            .target(&project)
            .set("meta:prune:since", MetaValue::String("30d".into()))
            .expect("set project key");

        let setting = PruneSetting::new("keep 30d", "30d", None);
        let rebuilt = rebuild_tree(&session, &setting, now_ms).expect("rebuild");

        let path = git_meta_lib::tree_paths::tree_path(&project, "meta:prune:since").expect("path");
        let found = git_meta_lib::git_utils::find_blob_oid_in_tree(session.repo(), rebuilt, &path)
            .expect("lookup");
        assert!(found.is_some(), "project metadata was pruned");
    }
}
