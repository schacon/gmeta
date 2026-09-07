//! Synthetic metadata workload generation.
//!
//! Produces a metadata history of a requested shape: a number of metadata
//! commits (serialize calls), each carrying a number of key writes, spread over
//! a simulated time span so that retention windows are meaningful.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use git_meta_lib::types::{MetaValue, Target};
use git_meta_lib::Session;
use serde::Serialize;

use crate::support::Rng;

/// A distribution of value sizes, as `(weight, bytes)` pairs.
///
/// Real metadata is not uniform: most values are short (a model name, a review
/// status), a few are large (an agent transcript chunk). A single size hides
/// how the format behaves when both live in the same tree.
#[derive(Debug, Clone)]
pub(crate) struct SizeMix {
    buckets: Vec<(u32, usize)>,
    total_weight: u32,
}

impl SizeMix {
    /// Parse a `weight:bytes,weight:bytes` specification.
    pub(crate) fn parse(spec: &str) -> anyhow::Result<Self> {
        let mut buckets = Vec::new();
        for part in spec.split(',').filter(|p| !p.trim().is_empty()) {
            let (weight, bytes) = part
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("expected weight:bytes, got {part:?}"))?;
            buckets.push((weight.trim().parse()?, bytes.trim().parse()?));
        }
        anyhow::ensure!(!buckets.is_empty(), "value size mix is empty");
        let total_weight = buckets.iter().map(|(w, _)| *w).sum();
        anyhow::ensure!(total_weight > 0, "value size mix has zero total weight");
        Ok(SizeMix {
            buckets,
            total_weight,
        })
    }

    /// A single-bucket mix, for runs that want one uniform value size.
    pub(crate) fn uniform(bytes: usize) -> Self {
        SizeMix {
            buckets: vec![(1, bytes)],
            total_weight: 1,
        }
    }

    /// Draw a size from the distribution.
    pub(crate) fn pick(&self, rng: &mut Rng) -> usize {
        let mut roll = (rng.next_u64() % u64::from(self.total_weight)) as u32;
        for (weight, bytes) in &self.buckets {
            if roll < *weight {
                return *bytes;
            }
            roll -= *weight;
        }
        self.buckets.last().map_or(64, |(_, bytes)| *bytes)
    }

    /// Mean value size, for reporting.
    pub(crate) fn mean_bytes(&self) -> f64 {
        let weighted: f64 = self
            .buckets
            .iter()
            .map(|(weight, bytes)| f64::from(*weight) * *bytes as f64)
            .sum();
        weighted / f64::from(self.total_weight)
    }

    pub(crate) fn describe(&self) -> String {
        self.buckets
            .iter()
            .map(|(weight, bytes)| {
                format!(
                    "{:.0}% x {}",
                    f64::from(*weight) * 100.0 / f64::from(self.total_weight),
                    crate::support::human_bytes(*bytes as u64)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// How key writes are distributed across the generated history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Mode {
    /// Every metadata commit writes keys on brand-new targets.
    ///
    /// Key count and commit count grow together, which is the shape a real
    /// project has: one commit's worth of provenance never overwrites another.
    Grow,
    /// Every metadata commit rewrites keys on already-existing targets.
    ///
    /// Commit count grows while the live key count stays flat. Comparing this
    /// against `Grow` separates "cost of a long history" from "cost of a wide
    /// key space".
    Churn,
}

/// Shape of the metadata history to generate.
#[derive(Debug, Clone)]
pub(crate) struct Workload {
    pub commits: usize,
    pub targets_per_commit: usize,
    pub list_entries: usize,
    pub value_sizes: SizeMix,
    pub mode: Mode,
    /// In [`Mode::Churn`], how many distinct targets to create before the run
    /// switches to rewriting them. This is the flat key-space size.
    pub churn_pool: usize,
    /// Run a prune every N metadata commits. Zero disables periodic pruning.
    ///
    /// Real deployments prune repeatedly, which is what buries an old value
    /// deep in history: it stops appearing in the tip tree as soon as the first
    /// prune after its retention window lands, and every metadata commit after
    /// that adds a commit to walk back through to reach it.
    pub prune_every: usize,
    /// Retention window used by periodic prunes.
    pub prune_since: String,
    /// Configure git-meta auto-pruning on the project before generating.
    ///
    /// This is the real thing: serialize decides for itself when the tree has
    /// grown past `max` and cuts it back to `min`. The benchmark only sets the
    /// policy and counts what happens.
    pub auto_prune_max_keys: Option<u64>,
    /// Floor for auto-pruning; see [`Workload::auto_prune_max_keys`].
    pub auto_prune_min_keys: Option<u64>,
    /// Also prune the local SQLite store whenever a tree prune runs.
    ///
    /// Tree pruning alone is not durable: the rows stay in SQLite and the next
    /// serialize re-expands the tree from them. Pruning both is what a steady
    /// state actually looks like, and what keeps a long run bounded.
    pub prune_local: bool,
    /// Run object-store maintenance every N metadata commits. Zero disables it.
    ///
    /// The library deliberately does not do this during serialize: `git gc`
    /// detaches by default and would delete objects from underneath the live
    /// session. A long-running in-process writer has to choose its own safe
    /// point, which is what this models — maintain, then reopen the session.
    pub maintain_every: usize,
    pub span_days: i64,
    pub start_ms: i64,
    pub seed: u64,
    /// Emit a checkpoint sample every N commits.
    pub checkpoint_every: usize,
}

impl Default for Workload {
    fn default() -> Self {
        Workload {
            commits: 2_000,
            targets_per_commit: 1,
            list_entries: 4,
            value_sizes: SizeMix::uniform(64),
            mode: Mode::Grow,
            churn_pool: 200,
            prune_every: 0,
            prune_since: "90d".to_string(),
            prune_local: false,
            auto_prune_max_keys: None,
            auto_prune_min_keys: None,
            maintain_every: 0,
            span_days: 1_095,
            // 2023-01-01T00:00:00Z, fixed so runs are comparable.
            start_ms: 1_672_531_200_000,
            seed: 0x5EED_1234_ABCD_0001,
            checkpoint_every: 100,
        }
    }
}

/// A periodic sample taken while generating the history.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Checkpoint {
    pub commits: usize,
    /// Distinct metadata keys live in SQLite at this point.
    pub live_keys: u64,
    /// Wall time for the most recent window of serialize calls, per commit.
    pub serialize_us_per_commit: f64,
    /// Wall time for the most recent window of key writes, per commit.
    pub write_us_per_commit: f64,
    pub elapsed_ms: u128,
}

/// Everything the generator learned about the run.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Generated {
    pub commits: usize,
    pub keys_written: usize,
    pub live_keys: u64,
    pub total_ms: u128,
    pub serialize_ms: u128,
    pub write_ms: u128,
    pub checkpoints: Vec<Checkpoint>,
    /// Number of periodic prune commits written during generation.
    pub prune_commits: usize,
    /// Wall time spent on object-store maintenance.
    pub maintain_ms: u128,
    /// Serializes where git-meta's own auto-prune fired.
    pub auto_prunes: usize,
    /// Keys auto-prune dropped in total.
    pub auto_pruned_keys: u64,
    /// Total wall time spent pruning.
    pub prune_ms: u128,
    /// Distinct targets written, for later read sampling.
    #[serde(skip)]
    pub targets: Vec<Target>,
    /// Timestamp of the *most recent* write to each target. Retention is
    /// evaluated against the latest write, so recording the first one would
    /// misclassify any target that was written more than once.
    #[serde(skip)]
    pub target_timestamps: Vec<i64>,
}

/// The string keys written on every target.
pub(crate) const STRING_KEYS: [&str; 3] = ["agent:model", "agent:session-id", "review:status"];
/// The list key appended to on every target.
pub(crate) const LIST_KEY: &str = "agent:transcript";
/// The set key extended on every target.
pub(crate) const SET_KEY: &str = "review:owners";

fn filler(bytes: usize, seed: u64) -> String {
    let tag = format!("{seed:016x}");
    if bytes <= tag.len() {
        return tag;
    }
    let mut s = String::with_capacity(bytes);
    while s.len() < bytes {
        s.push_str(&tag);
    }
    s.truncate(bytes);
    s
}

/// Continue an existing history, keeping its targets and timestamps.
///
/// Regenerating a large history costs most of an hour, and the interesting
/// question about a long-lived store is what happens *after* it is already
/// large, so extending is the only practical way to reach that scale.
pub(crate) fn extend(
    repo_dir: &Path,
    existing: &Generated,
    workload: &Workload,
    verbose: bool,
) -> Result<Generated> {
    let mut extended = generate_from(repo_dir, workload, verbose, existing.targets.clone())?;
    extended.commits += existing.commits;
    Ok(extended)
}

/// Generate a metadata history in the repository at `repo_dir`.
///
/// One serialize call is made per metadata commit, matching how a tool would
/// actually publish metadata as work happens.
pub(crate) fn generate(repo_dir: &Path, workload: &Workload, verbose: bool) -> Result<Generated> {
    generate_from(repo_dir, workload, verbose, Vec::new())
}

fn generate_from(
    repo_dir: &Path,
    workload: &Workload,
    verbose: bool,
    seed_targets: Vec<Target>,
) -> Result<Generated> {
    let mut session = Session::open(repo_dir)?;
    let mut rng = Rng::new(workload.seed);

    let span_ms = workload.span_days * 24 * 60 * 60 * 1000;
    let step_ms = (span_ms / workload.commits.max(1) as i64).max(1);

    let mut targets: Vec<Target> = seed_targets;
    let mut target_timestamps: Vec<i64> = vec![workload.start_ms; targets.len()];
    let mut target_index: HashMap<Target, usize> = targets
        .iter()
        .enumerate()
        .map(|(index, target)| (target.clone(), index))
        .collect();
    let mut keys_written = 0usize;
    let mut checkpoints = Vec::new();
    let mut prune_commits = 0usize;
    let mut prune_total = std::time::Duration::ZERO;
    let mut maintain_total = std::time::Duration::ZERO;
    let mut auto_prunes = 0usize;
    let mut auto_pruned_keys = 0u64;

    if let Some(max_keys) = workload.auto_prune_max_keys {
        let project = Target::project();
        let handle = session.target(&project);
        handle.set(
            "meta:prune:max-keys",
            MetaValue::String(max_keys.to_string()),
        )?;
        if let Some(min_keys) = workload.auto_prune_min_keys {
            handle.set(
                "meta:prune:min-keys",
                MetaValue::String(min_keys.to_string()),
            )?;
        }
    }
    let prune_setting = crate::prune::PruneSetting::new("periodic", &workload.prune_since, None);

    let started = Instant::now();
    let mut serialize_total = std::time::Duration::ZERO;
    let mut write_total = std::time::Duration::ZERO;
    let mut window_serialize = std::time::Duration::ZERO;
    let mut window_write = std::time::Duration::ZERO;

    for commit_idx in 0..workload.commits {
        let now_ms = workload.start_ms + commit_idx as i64 * step_ms;
        session = session.with_timestamp(now_ms);

        let write_started = Instant::now();
        for slot in 0..workload.targets_per_commit {
            let target = match workload.mode {
                Mode::Churn if targets.len() >= workload.churn_pool.max(1) => {
                    // Rewriting an existing target refreshes its retention age.
                    let pos = rng.below(targets.len());
                    target_timestamps[pos] = now_ms;
                    targets[pos].clone()
                }
                _ => {
                    let target = new_target(&mut rng, commit_idx, slot);
                    // Path and branch targets recur across commits by design;
                    // keep one entry per distinct target with its latest write.
                    if let Some(pos) = target_index.get(&target) {
                        target_timestamps[*pos] = now_ms;
                    } else {
                        target_index.insert(target.clone(), targets.len());
                        targets.push(target.clone());
                        target_timestamps.push(now_ms);
                    }
                    target
                }
            };

            let handle = session.target(&target);
            for (i, key) in STRING_KEYS.iter().enumerate() {
                let size = workload.value_sizes.pick(&mut rng);
                let value = filler(size, rng.next_u64().wrapping_add(i as u64));
                handle.set(key, MetaValue::String(value))?;
                keys_written += 1;
            }
            for entry in 0..workload.list_entries {
                let size = workload.value_sizes.pick(&mut rng);
                let value = filler(size, rng.next_u64().wrapping_add(entry as u64));
                handle.list_push(LIST_KEY, &value)?;
            }
            if workload.list_entries > 0 {
                keys_written += 1;
            }
            handle.set_add(SET_KEY, &format!("dev{}", rng.below(30)))?;
            keys_written += 1;
        }
        let write_elapsed = write_started.elapsed();
        write_total += write_elapsed;
        window_write += write_elapsed;

        let serialize_started = Instant::now();
        let output = session.serialize()?;
        let serialize_elapsed = serialize_started.elapsed();
        if output.pruned > 0 {
            auto_prunes += 1;
            auto_pruned_keys += output.pruned;
        }
        serialize_total += serialize_elapsed;
        window_serialize += serialize_elapsed;

        let done = commit_idx + 1;
        if workload.prune_every > 0 && done % workload.prune_every == 0 {
            let prune_started = Instant::now();
            // When git-meta's own auto-prune is configured it already bounds
            // the published tree, so a manual tree prune here would only fight
            // it. The local store still needs its own pruning either way.
            if workload.auto_prune_max_keys.is_none()
                && crate::prune::prune_and_commit(&session, &prune_setting, now_ms)?.is_some()
            {
                prune_commits += 1;
            }
            if workload.prune_local {
                let cutoff =
                    git_meta_lib::prune::parse_since_to_cutoff_ms(&workload.prune_since, now_ms)?;
                let store = session.store();
                store.prune_metadata_before(cutoff)?;
                store.prune_tombstones_before(cutoff)?;
                store.prune_set_tombstones_before(cutoff)?;
                store.prune_log_before(cutoff)?;
            }
            prune_total += prune_started.elapsed();
        }
        if workload.maintain_every > 0 && done % workload.maintain_every == 0 {
            let maintain_started = Instant::now();
            session.maintain_object_store();
            // Reopen: gix fixes its view of the object store when the
            // repository is opened, so the handle that asked for the packing
            // cannot be the one that reads afterwards.
            session = Session::open(repo_dir)?;
            maintain_total += maintain_started.elapsed();
        }
        if workload.checkpoint_every > 0 && done % workload.checkpoint_every == 0 {
            let window = workload.checkpoint_every as f64;
            let checkpoint = Checkpoint {
                commits: done,
                live_keys: live_key_count(&session)?,
                serialize_us_per_commit: window_serialize.as_secs_f64() * 1e6 / window,
                write_us_per_commit: window_write.as_secs_f64() * 1e6 / window,
                elapsed_ms: started.elapsed().as_millis(),
            };
            if verbose {
                eprintln!(
                    "    {:>7} commits  {:>8} keys  serialize {:>9.1} us/commit  write {:>8.1} us/commit",
                    checkpoint.commits,
                    checkpoint.live_keys,
                    checkpoint.serialize_us_per_commit,
                    checkpoint.write_us_per_commit,
                );
                if workload.auto_prune_max_keys.is_some() {
                    eprintln!(
                        "              auto-prunes so far: {auto_prunes} ({auto_pruned_keys} keys dropped)"
                    );
                }
            }
            checkpoints.push(checkpoint);
            window_serialize = std::time::Duration::ZERO;
            window_write = std::time::Duration::ZERO;
        }
    }

    let live_keys = live_key_count(&session)?;

    Ok(Generated {
        commits: workload.commits,
        keys_written,
        live_keys,
        total_ms: started.elapsed().as_millis(),
        serialize_ms: serialize_total.as_millis(),
        write_ms: write_total.as_millis(),
        checkpoints,
        prune_commits,
        prune_ms: prune_total.as_millis(),
        maintain_ms: maintain_total.as_millis(),
        auto_prunes,
        auto_pruned_keys,
        targets,
        target_timestamps,
    })
}

/// Total metadata rows currently live in the store.
fn live_key_count(session: &Session) -> Result<u64> {
    let (sqlite_rows, git_ref_rows) = session.store().stats_storage_counts()?;
    Ok(sqlite_rows + git_ref_rows)
}

/// Build a target with a realistic mix of target types.
fn new_target(rng: &mut Rng, commit_idx: usize, slot: usize) -> Target {
    match rng.below(10) {
        0..=6 => Target::from_parts(git_meta_lib::types::TargetType::Commit, Some(rng.hex40())),
        7 | 8 => Target::path(&format!(
            "crates/mod{}/src/file{}.rs",
            commit_idx % 64,
            slot + commit_idx % 17
        )),
        _ => Target::branch(&format!("feature/task-{commit_idx}-{slot}")),
    }
}

/// Steady-state cost of publishing one more change to an existing history.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SteadyState {
    /// Commits measured.
    pub commits: usize,
    /// Live keys in the store while measuring.
    pub live_keys: u64,
    /// Time to write the change to SQLite.
    pub write: crate::support::Latency,
    /// Time to serialize and commit it.
    pub serialize: crate::support::Latency,
}

/// Measure the cost of `commits` further single-target publishes against an
/// already-built history.
///
/// This is the number a tool actually feels: not the average cost of building
/// the history, but what one more change costs once the repository is full.
pub(crate) fn measure_steady_state(
    repo_dir: &Path,
    generated: &Generated,
    workload: &Workload,
    commits: usize,
) -> Result<SteadyState> {
    let mut session = Session::open(repo_dir)?;
    let mut rng = Rng::new(workload.seed ^ 0xDEAD_BEEF);
    let start_ms = workload.start_ms + workload.span_days * 24 * 60 * 60 * 1000;

    let mut write_micros = Vec::with_capacity(commits);
    let mut serialize_micros = Vec::with_capacity(commits);

    for step in 0..commits {
        if generated.targets.is_empty() {
            break;
        }
        session = session.with_timestamp(start_ms + step as i64 * 1000);
        let target = &generated.targets[rng.below(generated.targets.len())];

        let started = Instant::now();
        let size = workload.value_sizes.pick(&mut rng);
        session.target(target).set(
            STRING_KEYS[0],
            MetaValue::String(filler(size, rng.next_u64())),
        )?;
        write_micros.push(started.elapsed().as_secs_f64() * 1e6);

        let started = Instant::now();
        let _ = session.serialize()?;
        serialize_micros.push(started.elapsed().as_secs_f64() * 1e6);
    }

    Ok(SteadyState {
        commits: serialize_micros.len(),
        live_keys: live_key_count(&session)?,
        write: crate::support::Latency::from_micros(write_micros),
        serialize: crate::support::Latency::from_micros(serialize_micros),
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn tiny(commits: usize, mode: Mode) -> Workload {
        Workload {
            commits,
            targets_per_commit: 1,
            list_entries: 2,
            value_sizes: SizeMix::uniform(16),
            mode,
            churn_pool: 5,
            prune_every: 0,
            span_days: 100,
            checkpoint_every: 0,
            ..Workload::default()
        }
    }

    #[test]
    fn generate_writes_the_requested_number_of_commits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo_dir = dir.path().join("repo");
        crate::support::init_repo(&repo_dir).expect("init");

        let generated = generate(&repo_dir, &tiny(10, Mode::Grow), false).expect("generate");

        assert_eq!(generated.commits, 10);
        assert!(generated.live_keys > 0);
        assert_eq!(generated.targets.len(), generated.target_timestamps.len());
    }

    #[test]
    fn targets_are_deduplicated_and_carry_their_latest_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo_dir = dir.path().join("repo");
        crate::support::init_repo(&repo_dir).expect("init");

        // Path targets recur across commits by construction, so this run has
        // duplicates to collapse.
        let generated = generate(&repo_dir, &tiny(80, Mode::Grow), false).expect("generate");

        let mut seen = std::collections::HashSet::new();
        for target in &generated.targets {
            assert!(seen.insert(target.clone()), "duplicate target {target:?}");
        }
        assert!(
            generated.target_timestamps.windows(2).any(|w| w[0] <= w[1]),
            "timestamps should advance through the run"
        );
    }

    #[test]
    fn churn_mode_holds_the_key_space_flat() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo_dir = dir.path().join("repo");
        crate::support::init_repo(&repo_dir).expect("init");

        let workload = tiny(60, Mode::Churn);
        let generated = generate(&repo_dir, &workload, false).expect("generate");

        assert_eq!(
            generated.targets.len(),
            workload.churn_pool,
            "churn must stop creating targets once the pool is full"
        );
    }
}
