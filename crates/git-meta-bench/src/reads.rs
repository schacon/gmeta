//! Retrieval benchmarks: local store reads, tip-tree reads, and deep-history reads.

use anyhow::{Context, Result};
use git_meta_lib::types::Target;
use git_meta_lib::{git_utils, tree_paths, Session};
use gix::prelude::ObjectIdExt;
use serde::Serialize;

use crate::support::{Latency, Rng};

/// Read latencies at one point in the run.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReadResults {
    /// `get_value` against the local SQLite store.
    pub store_get: Latency,
    /// Path resolution of a key in the serialized tip tree.
    pub tip_tree_lookup: Latency,
    /// Fraction of sampled keys found in the tip tree (1.0 unless pruned).
    pub tip_hit_rate: f64,
}

/// Resolve the tip tree of the local metadata ref.
pub(crate) fn local_tip_tree(session: &Session) -> Result<Option<gix::ObjectId>> {
    let ref_name = format!("refs/{}/local/main", session.namespace());
    let Ok(reference) = session.repo().find_reference(&ref_name) else {
        return Ok(None);
    };
    let commit = reference.into_fully_peeled_id()?.object()?.into_commit();
    Ok(Some(commit.tree_id()?.detach()))
}

/// Sample point reads across the generated key space.
pub(crate) fn measure(
    session: &Session,
    targets: &[Target],
    samples: usize,
    seed: u64,
) -> Result<ReadResults> {
    let mut rng = Rng::new(seed);
    let tip_tree = local_tip_tree(session)?;

    let mut store_micros = Vec::with_capacity(samples);
    let mut tree_micros = Vec::with_capacity(samples);
    let mut tip_hits = 0usize;
    let mut tree_attempts = 0usize;

    for _ in 0..samples.min(targets.len().max(1) * 4) {
        if targets.is_empty() {
            break;
        }
        let target = &targets[rng.below(targets.len())];
        let key = crate::workload::STRING_KEYS[rng.below(crate::workload::STRING_KEYS.len())];

        let started = std::time::Instant::now();
        let value = session.target(target).get_value(key)?;
        store_micros.push(started.elapsed().as_secs_f64() * 1e6);
        // Touch the value so the read is not optimized away.
        std::hint::black_box(&value);

        if let Some(tree) = tip_tree {
            let path = tree_paths::tree_path(target, key)?;
            let started = std::time::Instant::now();
            let found = git_utils::find_blob_oid_in_tree(session.repo(), tree, &path)?;
            tree_micros.push(started.elapsed().as_secs_f64() * 1e6);
            tree_attempts += 1;
            if found.is_some() {
                tip_hits += 1;
            }
        }
    }

    Ok(ReadResults {
        store_get: Latency::from_micros(store_micros),
        tip_tree_lookup: Latency::from_micros(tree_micros),
        tip_hit_rate: if tree_attempts == 0 {
            0.0
        } else {
            tip_hits as f64 / tree_attempts as f64
        },
    })
}

/// Outcome of resolving one key by walking metadata history backwards.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DeepRead {
    pub found: bool,
    /// Metadata commits inspected before the key was found (or history ended).
    pub commits_walked: usize,
    /// Trees opened during the walk.
    pub trees_opened: usize,
    pub micros: f64,
}

/// Find a key by walking metadata commits from `tip` backwards.
///
/// This is the retrieval path a consumer is left with once the tip tree has
/// been pruned: the value is no longer in the current tree, so the only way to
/// reach it is to walk back until some ancestor tree still carries the path.
pub(crate) fn deep_read(
    session: &Session,
    tip: gix::ObjectId,
    target: &Target,
    key: &str,
    max_commits: usize,
) -> Result<DeepRead> {
    let repo = session.repo();
    let path = tree_paths::tree_path(target, key)?;

    let started = std::time::Instant::now();
    let mut commits_walked = 0usize;
    let mut trees_opened = 0usize;
    let mut found = false;

    let walk = repo.rev_walk(Some(tip)).all().context("rev walk failed")?;
    for info in walk {
        let info = info?;
        commits_walked += 1;
        if commits_walked > max_commits {
            break;
        }
        let tree = info
            .id
            .attach(repo)
            .object()?
            .into_commit()
            .tree_id()?
            .detach();
        trees_opened += 1;
        if git_utils::find_blob_oid_in_tree(repo, tree, &path)?.is_some() {
            found = true;
            break;
        }
    }

    Ok(DeepRead {
        found,
        commits_walked,
        trees_opened,
        micros: started.elapsed().as_secs_f64() * 1e6,
    })
}
