//! Sparse (blobless) fetch scenario.
//!
//! Publishes a generated metadata history to a local bare remote, clones it
//! both fully and with `--filter=blob:none`, and measures what a sparse
//! consumer pays to pull, to index history, and to retrieve individual values
//! — including values that have been pruned out of the tip tree and survive
//! only deeper in history.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use git_meta_lib::types::Target;
use git_meta_lib::{git_utils, tree_paths, Session};
use gix::prelude::ObjectIdExt;
use serde::Serialize;

use crate::reads::{self, DeepRead};
use crate::support::{git, Latency};

/// Measurements for one clone of the metadata remote.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CloneResult {
    pub label: String,
    pub blobless: bool,
    /// Whether the metadata ref itself was fetched with a blob filter.
    pub filtered_meta_fetch: bool,
    /// Objects the consumer does not have locally after the pull.
    pub missing_objects: usize,
    /// Phase breakdown of what the consumer actually did.
    pub phases: ConsumerPhases,
    pub clone_ms: u128,
    pub clone_bytes: u64,
    pub pull_ms: u128,
    pub new_commits: usize,
    pub indexed_keys: usize,
    /// Why the single-call `pull` failed, if it did.
    pub pull_error: Option<String>,
    pub promised_keys: u64,
    /// Object-store bytes after the pull, excluding the local SQLite database.
    pub objects_bytes_after_pull: u64,
    /// Size of the local SQLite metadata database after the pull.
    pub sqlite_bytes_after_pull: u64,
    /// Reading a key that is still present in the pruned tip tree.
    pub tip_key_read: Option<HydrateResult>,
    /// Reading a key that prune dropped from the tip tree.
    pub pruned_key_read: Option<HydrateResult>,
    /// History-walk cost across many pruned keys of differing age.
    pub deep_read_latency: Latency,
    /// How many of those pruned keys were still reachable in history.
    pub deep_read_hits: usize,
    pub deep_read_samples: usize,
    /// Commits walked before the key was found, averaged over the hits.
    pub deep_read_mean_commits: f64,
}

/// Where a consumer's time goes between an empty directory and a usable store.
#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct ConsumerPhases {
    /// `git clone` of the code refs.
    pub clone_ms: u128,
    /// Fetching the metadata ref.
    pub fetch_meta_ms: u128,
    /// Backfilling the blobs of the tip tree so the tree can be read.
    pub hydrate_tip_ms: u128,
    /// Blobs backfilled.
    pub hydrated_blobs: usize,
    /// Pack files present after hydration, before any repack.
    pub packs_after_hydrate: usize,
    /// Repacking those into one pack so the object database can be opened.
    pub repack_ms: u128,
    /// Rebuilding the local SQLite store from the fetched tree.
    pub materialize_ms: u128,
    /// Keys present in SQLite once materialization finished.
    pub keys_recreated: u64,
    /// Indexing history so pruned/older keys are known but not fetched.
    pub index_history_ms: u128,
    /// Promisor entries inserted by that indexing pass.
    pub indexed_keys: usize,
}

/// Cost of retrieving one value in a sparse clone.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct HydrateResult {
    /// Whether the blob path was located at all.
    pub located: bool,
    /// Whether it was located in the tip tree (vs. only deeper in history).
    pub in_tip_tree: bool,
    /// History walk cost, when the tip tree did not have it.
    pub deep: Option<DeepRead>,
    /// Wall time to fetch the blob from the promisor remote.
    pub fetch_ms: u128,
    /// Total wall time for the whole retrieval.
    pub total_ms: u128,
}

/// Publish the producer's local metadata ref to a fresh bare remote.
pub(crate) fn publish(producer_dir: &Path, origin_dir: &Path, namespace: &str) -> Result<()> {
    std::fs::create_dir_all(origin_dir)?;
    git(
        origin_dir,
        &["init", "--bare", "--quiet", "--initial-branch=main"],
    )?;
    // Without these the server silently ignores `--filter` ("filtering not
    // recognized by server") and every blobless arm degrades to a full fetch.
    // Real hosts that support partial clone advertise both.
    git(origin_dir, &["config", "uploadpack.allowFilter", "true"])?;
    git(
        origin_dir,
        &["config", "uploadpack.allowAnySHA1InWant", "true"],
    )?;

    let origin_url = origin_dir
        .to_str()
        .context("origin path is not valid UTF-8")?;
    git(producer_dir, &["remote", "add", "origin", origin_url])?;
    git(producer_dir, &["push", "--quiet", "origin", "main"])?;
    git(
        producer_dir,
        &[
            "push",
            "--quiet",
            "origin",
            &format!("refs/{namespace}/local/main:refs/{namespace}/main"),
        ],
    )?;
    Ok(())
}

/// Clone the remote and configure it as a git-meta metadata remote.
fn clone_consumer(
    origin_dir: &Path,
    consumer_dir: &Path,
    namespace: &str,
    blobless: bool,
    prefetch_filtered: bool,
) -> Result<u128> {
    let parent = consumer_dir
        .parent()
        .context("consumer directory has no parent")?;
    std::fs::create_dir_all(parent)?;
    let origin_url = origin_dir
        .to_str()
        .context("origin path is not valid UTF-8")?;
    let target = consumer_dir
        .to_str()
        .context("consumer path is not valid UTF-8")?;

    // `--no-local` is required: for a filesystem path git would otherwise take
    // the local-clone shortcut, which copies the whole object store and ignores
    // `--filter` entirely, making the two arms identical.
    let mut args: Vec<&str> = vec!["clone", "--quiet", "--no-local"];
    if blobless {
        args.push("--filter=blob:none");
    }
    args.push(origin_url);
    args.push(target);

    let started = std::time::Instant::now();
    git(parent, &args)?;
    let clone_ms = started.elapsed().as_millis();

    git(
        consumer_dir,
        &["config", "user.email", "consumer@git-meta.example"],
    )?;
    git(
        consumer_dir,
        &["config", "user.name", "git-meta bench consumer"],
    )?;
    git(consumer_dir, &["config", "remote.origin.meta", "true"])?;
    git(
        consumer_dir,
        &[
            "config",
            "remote.origin.fetch",
            &format!("+refs/{namespace}/main:refs/{namespace}/remotes/main"),
        ],
    )?;
    if blobless {
        // `git clone --filter` already sets promisor and partialclonefilter;
        // setting them again keeps the two arms configured identically apart
        // from the filter itself.
        git(consumer_dir, &["config", "remote.origin.promisor", "true"])?;
        git(
            consumer_dir,
            &["config", "remote.origin.partialclonefilter", "blob:none"],
        )?;
        git(
            consumer_dir,
            &["config", "extensions.partialClone", "origin"],
        )?;
    }

    if prefetch_filtered {
        // Fetch the metadata ref with the blob filter applied. `Session::pull`
        // fetches the same refspec without a filter, so doing it here first is
        // the only way to get a genuinely blobless metadata history — pull then
        // finds the tracking ref already current and fetches nothing more.
        git(
            consumer_dir,
            &[
                "fetch",
                "--filter=blob:none",
                "origin",
                &format!("+refs/{namespace}/main:refs/{namespace}/remotes/main"),
            ],
        )?;
    }

    Ok(clone_ms)
}

/// Retrieve one value in a sparse consumer, walking history if the tip tree
/// no longer carries it, and fetching the blob from the promisor remote.
fn hydrate_one(
    session: &Session,
    tip: gix::ObjectId,
    target: &Target,
    key: &str,
    max_commits: usize,
) -> Result<HydrateResult> {
    let repo = session.repo();
    let path = tree_paths::tree_path(target, key)?;
    let tip_tree = repo.find_object(tip)?.into_commit().tree_id()?.detach();

    let started = std::time::Instant::now();
    let mut deep = None;
    let mut in_tip_tree = false;

    let mut blob = git_utils::find_blob_oid_in_tree(repo, tip_tree, &path)?;
    if blob.is_some() {
        in_tip_tree = true;
    } else {
        let walk = reads::deep_read(session, tip, target, key, max_commits)?;
        if walk.found {
            // Re-resolve at the commit the walk stopped on to get the OID.
            blob = find_in_history(repo, tip, &path, max_commits)?;
        }
        deep = Some(walk);
    }

    let mut fetch_ms = 0;
    if let Some(oid) = blob {
        if repo.find_object(oid).is_err() {
            let fetch_started = std::time::Instant::now();
            git_utils::fetch_blob_oids(repo, "origin", &[oid])?;
            fetch_ms = fetch_started.elapsed().as_millis();
        }
    }

    Ok(HydrateResult {
        located: blob.is_some(),
        in_tip_tree,
        deep,
        fetch_ms,
        total_ms: started.elapsed().as_millis(),
    })
}

/// Clone again and run the consumer's work with each phase timed separately.
///
/// `Session::pull` does fetch, hydrate, serialize, materialize and history
/// indexing in one call, so the only way to attribute its time is to redo the
/// same sequence step by step.
fn measure_phases(
    origin_dir: &Path,
    consumer_dir: &Path,
    namespace: &str,
    blobless: bool,
    clone_ms: u128,
) -> Result<ConsumerPhases> {
    clone_consumer(origin_dir, consumer_dir, namespace, blobless, false)?;
    let tracking_ref = format!("refs/{namespace}/remotes/main");

    let started = std::time::Instant::now();
    git(
        consumer_dir,
        &[
            "fetch",
            "--quiet",
            "origin",
            &format!("+refs/{namespace}/main:{tracking_ref}"),
        ],
    )?;
    let fetch_meta_ms = started.elapsed().as_millis();

    let short_ref = tracking_ref
        .strip_prefix("refs/")
        .unwrap_or(&tracking_ref)
        .to_string();
    let started = std::time::Instant::now();
    let hydrated_blobs = {
        // Scope the session: hydration runs `git fetch` in batches, and each
        // batch lands a new pack. gix sizes its object-database slot map when
        // the repository is opened, so a handle opened before hydration cannot
        // see what hydration adds.
        let session = Session::open(consumer_dir)?;
        git_utils::hydrate_tip_blobs_counted(session.repo(), "origin", &short_ref)?
    };
    let hydrate_tip_ms = started.elapsed().as_millis();
    let packs_after_hydrate = count_packs(consumer_dir);

    // Collapse those packs into one. Without this, opening the repository fails
    // outright once hydration has produced more packs than the slot map holds
    // ("The slotmap turned out to be too small"), so this is not tuning — it is
    // what makes the rest of the work possible at all.
    let started = std::time::Instant::now();
    git(consumer_dir, &["repack", "-a", "-d", "-q"])?;
    let repack_ms = started.elapsed().as_millis();

    let session = Session::open(consumer_dir)?;

    // `materialize` takes a ref-namespace segment, not a remote name: passing
    // the remote name matches `refs/{ns}/origin*` and silently finds nothing.
    // `None` is what `pull` itself passes, and matches the tracking ref.
    let started = std::time::Instant::now();
    let _ = session.materialize(None)?;
    let materialize_ms = started.elapsed().as_millis();

    let (sqlite_rows, git_ref_rows) = session.store().stats_storage_counts()?;

    // Index the whole history directly rather than through `pull`: having
    // already materialized above, a pull here would decide it has nothing left
    // to index and report a cost of zero.
    let tip = session
        .repo()
        .find_reference(&tracking_ref)?
        .into_fully_peeled_id()?
        .detach();
    let started = std::time::Instant::now();
    let indexed_keys =
        git_meta_lib::sync::insert_promisor_entries(session.repo(), session.store(), tip, None)?;
    let index_history_ms = started.elapsed().as_millis();

    Ok(ConsumerPhases {
        clone_ms,
        fetch_meta_ms,
        hydrate_tip_ms,
        hydrated_blobs,
        packs_after_hydrate,
        repack_ms,
        materialize_ms,
        keys_recreated: sqlite_rows + git_ref_rows,
        index_history_ms,
        indexed_keys,
    })
}

/// Pack files in a repository's object store.
fn count_packs(repo_dir: &Path) -> usize {
    let pack_dir = crate::support::git_dir(repo_dir)
        .join("objects")
        .join("pack");
    let Ok(entries) = std::fs::read_dir(pack_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "pack")
        })
        .count()
}

/// Count objects reachable from `ref_name` that are not present locally.
///
/// A nonzero count is the whole point of a blobless fetch: those objects are
/// promised by the remote and fetched only if something asks for them.
fn count_missing_objects(consumer_dir: &Path, ref_name: &str) -> Result<usize> {
    let output = git(
        consumer_dir,
        &["rev-list", "--objects", "--missing=print", ref_name],
    )?;
    Ok(output.lines().filter(|line| line.starts_with('?')).count())
}

/// Walk history for the first tree that still carries `path`.
fn find_in_history(
    repo: &gix::Repository,
    tip: gix::ObjectId,
    path: &str,
    max_commits: usize,
) -> Result<Option<gix::ObjectId>> {
    let mut walked = 0;
    for info in repo.rev_walk(Some(tip)).all()? {
        let info = info?;
        walked += 1;
        if walked > max_commits {
            break;
        }
        let tree = info
            .id
            .attach(repo)
            .object()?
            .into_commit()
            .tree_id()?
            .detach();
        if let Some(oid) = git_utils::find_blob_oid_in_tree(repo, tree, path)? {
            return Ok(Some(oid));
        }
    }
    Ok(None)
}

/// Run both clone arms and measure retrieval in each.
///
/// `tip_target` is expected to survive the prune; `pruned_target` is expected
/// to have been dropped from the tip tree.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    workspace: &Path,
    origin_dir: &Path,
    namespace: &str,
    tip_target: Option<&Target>,
    pruned_target: Option<&Target>,
    pruned_samples: &[Target],
    key: &str,
    max_commits: usize,
) -> Result<Vec<CloneResult>> {
    let mut results = Vec::new();

    let arms = [
        // What a consumer gets today.
        ("full clone", false, false),
        // What the blobless-clone documentation suggests doing.
        ("blobless clone", true, false),
    ];

    for (label, blobless, prefetch_filtered) in arms {
        let consumer_dir: PathBuf = workspace.join(label.replace(' ', "-"));
        let clone_ms = clone_consumer(
            origin_dir,
            &consumer_dir,
            namespace,
            blobless,
            prefetch_filtered,
        )?;
        let clone_bytes =
            crate::support::dir_size(&crate::support::git_dir(&consumer_dir).join("objects"));

        let session = Session::open(&consumer_dir)?;
        let pull_started = std::time::Instant::now();
        let pull_result = session.pull(Some("origin"));
        let pull_ms = pull_started.elapsed().as_millis();
        let (pull, pull_error) = match pull_result {
            Ok(pull) => (Some(pull), None),
            Err(error) => {
                eprintln!("  pull failed on the {label}: {error}");
                (None, Some(error.to_string()))
            }
        };

        // `pull` is the end-to-end number a user waits on. To see where that
        // time goes, run the same work again on a second, identical clone with
        // the phases separated.
        let phases = measure_phases(
            origin_dir,
            &workspace.join(format!("{}-phases", label.replace(' ', "-"))),
            namespace,
            blobless,
            clone_ms,
        )?;

        let promised_keys = session
            .store()
            .count_promised_keys()?
            .iter()
            .map(|(_, count)| count)
            .sum::<u64>();

        let tracking_ref = format!("refs/{namespace}/remotes/main");
        let tip = session
            .repo()
            .find_reference(&tracking_ref)
            .ok()
            .and_then(|r| r.into_fully_peeled_id().ok())
            .map(gix::Id::detach);
        let Some(tip) = tip else {
            bail!("consumer has no tracking ref {tracking_ref} after pull");
        };

        let tip_key_read = tip_target
            .map(|target| hydrate_one(&session, tip, target, key, max_commits))
            .transpose()?;
        let pruned_key_read = pruned_target
            .map(|target| hydrate_one(&session, tip, target, key, max_commits))
            .transpose()?;

        let mut deep_reads = Vec::with_capacity(pruned_samples.len());
        for target in pruned_samples {
            deep_reads.push(reads::deep_read(&session, tip, target, key, max_commits)?);
        }
        let hits: Vec<&DeepRead> = deep_reads.iter().filter(|read| read.found).collect();
        let mean_commits = if hits.is_empty() {
            0.0
        } else {
            hits.iter()
                .map(|read| read.commits_walked as f64)
                .sum::<f64>()
                / hits.len() as f64
        };

        results.push(CloneResult {
            label: label.to_string(),
            blobless,
            filtered_meta_fetch: prefetch_filtered,
            missing_objects: count_missing_objects(&consumer_dir, &tracking_ref)?,
            phases,
            clone_ms,
            clone_bytes,
            pull_ms,
            new_commits: pull.as_ref().map_or(0, |pull| pull.new_commits),
            indexed_keys: pull.as_ref().map_or(0, |pull| pull.indexed_keys),
            pull_error,
            promised_keys,
            objects_bytes_after_pull: crate::support::dir_size(
                &crate::support::git_dir(&consumer_dir).join("objects"),
            ),
            sqlite_bytes_after_pull: std::fs::metadata(
                crate::support::git_dir(&consumer_dir).join("git-meta.sqlite"),
            )
            .map(|meta| meta.len())
            .unwrap_or(0),
            tip_key_read,
            pruned_key_read,
            deep_read_latency: summarize_deep(&deep_reads),
            deep_read_hits: hits.len(),
            deep_read_samples: deep_reads.len(),
            deep_read_mean_commits: mean_commits,
        });
    }

    Ok(results)
}

/// Summarize a set of deep reads.
pub(crate) fn summarize_deep(reads: &[DeepRead]) -> Latency {
    Latency::from_micros(reads.iter().map(|r| r.micros).collect())
}
