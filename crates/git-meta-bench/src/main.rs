//! Scale benchmarks for git-meta.
//!
//! Generates large metadata histories and measures where the design holds up
//! and where it does not:
//!
//! - write cost as the key space and the commit history grow (separately)
//! - retrieval cost from the local store and from the serialized tree
//! - what different prune retention windows cost and keep
//! - what a sparse (blobless) consumer pays to pull and to read values,
//!   including values pruned out of the tip tree

mod prune;
mod reads;
mod report;
mod sparse;
mod support;
mod workload;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use git_meta_lib::Session;

use workload::{Mode, Workload};

#[derive(Debug, Parser)]
#[command(name = "git-meta-bench", about = "Scale benchmarks for git-meta")]
struct Args {
    /// Directory to build benchmark repositories in.
    #[arg(long, global = true)]
    workspace: Option<PathBuf>,

    /// Keep the generated repositories after the run.
    #[arg(long, global = true)]
    keep: bool,

    /// Write the full results as JSON to this path.
    #[arg(long, global = true)]
    json: Option<PathBuf>,

    /// Print per-checkpoint progress while generating.
    #[arg(long, short, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write and read scaling across a range of history sizes.
    Scale(ScaleArgs),
    /// Compare prune retention settings on a generated history.
    Prune(ScaleArgs),
    /// Full-vs-blobless clone, pull, and value retrieval including pruned keys.
    Sparse(ScaleArgs),
    /// Run every scenario and emit a combined report.
    All(ScaleArgs),
}

impl Command {
    fn args(&self) -> &ScaleArgs {
        match self {
            Command::Scale(a) | Command::Prune(a) | Command::Sparse(a) | Command::All(a) => a,
        }
    }
}

#[derive(Debug, Clone, clap::Args)]
struct ScaleArgs {
    /// Metadata commit counts to test, smallest first.
    #[arg(long, value_delimiter = ',', default_values_t = [500usize, 2_000, 8_000])]
    tiers: Vec<usize>,

    /// Targets written per metadata commit.
    #[arg(long, default_value_t = 1)]
    targets_per_commit: usize,

    /// List entries appended per target.
    #[arg(long, default_value_t = 4)]
    list_entries: usize,

    /// Size in bytes of each string value, when `--value-sizes` is not given.
    #[arg(long, default_value_t = 64)]
    value_bytes: usize,

    /// Distribution of value sizes as `weight:bytes` pairs, e.g.
    /// `80:64,15:1024,5:32768`. Overrides `--value-bytes`.
    #[arg(long)]
    value_sizes: Option<String>,

    /// After generating, measure this many further single-target publishes to
    /// get the steady-state cost of one more change.
    #[arg(long, default_value_t = 0)]
    steady: usize,

    /// Simulated span of the generated history, in days.
    #[arg(long, default_value_t = 1_095)]
    span_days: i64,

    /// Simulated span for the commits added by `--extend`, in days.
    #[arg(long, default_value_t = 1_000)]
    extend_span_days: i64,

    /// Checkpoint sampling interval, in metadata commits.
    #[arg(long, default_value_t = 100)]
    checkpoint_every: usize,

    /// Point reads sampled per tier.
    #[arg(long, default_value_t = 500)]
    read_samples: usize,

    /// Run a prune every N metadata commits while generating (0 disables).
    #[arg(long, default_value_t = 0)]
    prune_every: usize,

    /// Retention window for periodic prunes and for the sparse scenario.
    #[arg(long, default_value = "90d")]
    prune_since: String,

    /// Run object-store maintenance every N metadata commits (0 disables).
    #[arg(long, default_value_t = 0)]
    maintain_every: usize,

    /// Cap on commits walked during a deep-history read.
    #[arg(long, default_value_t = 20_000)]
    max_walk: usize,

    /// Skip the churn tier that otherwise runs alongside the largest grow tier.
    #[arg(long)]
    no_churn: bool,

    /// Continue an existing repository with `--tiers` more commits, instead of
    /// building a new one. Timestamps carry on from where it left off.
    #[arg(long)]
    extend: Option<PathBuf>,

    /// Also prune the local SQLite store whenever a tree prune runs.
    #[arg(long)]
    prune_local: bool,

    /// Configure git-meta auto-pruning: tree key ceiling.
    #[arg(long)]
    auto_prune_max_keys: Option<u64>,

    /// Configure git-meta auto-pruning: tree key floor.
    #[arg(long)]
    auto_prune_min_keys: Option<u64>,

    /// Measure an already-generated repository instead of building one.
    ///
    /// Generation dominates a large run, so this makes the prune and consumer
    /// scenarios re-runnable against a history that already exists.
    #[arg(long)]
    reuse: Option<PathBuf>,

    /// Skip `git gc` and packed-size measurement (much faster).
    #[arg(long)]
    no_pack: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let scale_args = args.command.args().clone();

    let workspace = match &args.workspace {
        Some(path) => path.clone(),
        None => std::env::temp_dir().join(format!(
            "git-meta-bench-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        )),
    };
    std::fs::create_dir_all(&workspace)?;
    eprintln!("workspace: {}", workspace.display());

    let value_sizes = match &scale_args.value_sizes {
        Some(spec) => workload::SizeMix::parse(spec)?,
        None => workload::SizeMix::uniform(scale_args.value_bytes),
    };
    eprintln!("value sizes: {}", value_sizes.describe());

    let mut report = report::Report::new(&scale_args, &value_sizes);

    let run_scale = matches!(args.command, Command::Scale(_) | Command::All(_));
    let run_prune = matches!(args.command, Command::Prune(_) | Command::All(_));
    let run_sparse = matches!(args.command, Command::Sparse(_) | Command::All(_));

    // Every scenario needs at least one generated history. Scale generates all
    // tiers; the other scenarios only need the largest.
    let tiers: Vec<usize> = if run_scale {
        let mut tiers = scale_args.tiers.clone();
        tiers.sort_unstable();
        tiers
    } else {
        vec![*scale_args.tiers.iter().max().unwrap_or(&2_000)]
    };

    let mut largest: Option<(PathBuf, Workload, workload::Generated)> = None;

    if let Some(existing) = &scale_args.extend {
        let commits = *scale_args.tiers.iter().max().unwrap_or(&1_000);
        eprintln!("\n[extend] loading {}", existing.display());
        let previous = load_existing(existing)?;
        eprintln!(
            "  existing: {} commits, {} distinct targets, {} live keys",
            previous.commits,
            previous.targets.len(),
            previous.live_keys
        );

        // Carry on after the existing span so the retention window keeps
        // meaning what it meant, and so old keys genuinely age out.
        let previous_end =
            Workload::default().start_ms + scale_args.span_days * 24 * 60 * 60 * 1000;
        let workload = Workload {
            commits,
            targets_per_commit: scale_args.targets_per_commit,
            list_entries: scale_args.list_entries,
            value_sizes: value_sizes.clone(),
            mode: Mode::Grow,
            prune_every: scale_args.prune_every,
            prune_since: scale_args.prune_since.clone(),
            prune_local: scale_args.prune_local,
            auto_prune_max_keys: scale_args.auto_prune_max_keys,
            auto_prune_min_keys: scale_args.auto_prune_min_keys,
            maintain_every: scale_args.maintain_every,
            span_days: scale_args.extend_span_days,
            checkpoint_every: scale_args.checkpoint_every,
            start_ms: previous_end,
            ..Workload::default()
        };

        eprintln!(
            "  adding {commits} commits over {} days, pruning {} every {} commits{}",
            workload.span_days,
            workload.prune_since,
            workload.prune_every,
            if workload.prune_local {
                " (tree and local store)"
            } else {
                " (tree only)"
            }
        );

        let generated = workload::extend(existing, &previous, &workload, args.verbose)?;

        let session = Session::open(existing)?;
        let read_results = reads::measure(
            &session,
            &generated.targets,
            scale_args.read_samples,
            0xBEEF_0004,
        )?;
        drop(session);

        let steady = if scale_args.steady > 0 {
            Some(workload::measure_steady_state(
                existing,
                &generated,
                &workload,
                scale_args.steady,
            )?)
        } else {
            None
        };

        report.push_tier(report::TierReport {
            label: format!("extended/{}", generated.commits),
            mode: "grow".to_string(),
            generated: generated.clone(),
            reads: read_results,
            storage: report::storage_snapshot(existing, !scale_args.no_pack)?,
            steady,
        });
        report.print_remaining();

        largest = Some((existing.clone(), workload, generated));
    }

    if let Some(existing) = &scale_args.reuse {
        eprintln!("\n[reuse] loading targets from {}", existing.display());
        let generated = load_existing(existing)?;
        eprintln!(
            "  {} distinct targets, {} live keys",
            generated.targets.len(),
            generated.live_keys
        );
        let workload = Workload {
            commits: generated.commits,
            targets_per_commit: scale_args.targets_per_commit,
            list_entries: scale_args.list_entries,
            value_sizes: value_sizes.clone(),
            mode: Mode::Grow,
            prune_every: scale_args.prune_every,
            prune_since: scale_args.prune_since.clone(),
            span_days: scale_args.span_days,
            checkpoint_every: scale_args.checkpoint_every,
            ..Workload::default()
        };
        let session = Session::open(existing)?;
        let read_results = reads::measure(
            &session,
            &generated.targets,
            scale_args.read_samples,
            0xBEEF_0003,
        )?;
        drop(session);

        let steady = if scale_args.steady > 0 {
            Some(workload::measure_steady_state(
                existing,
                &generated,
                &workload,
                scale_args.steady,
            )?)
        } else {
            None
        };

        report.push_tier(report::TierReport {
            label: format!("reused/{}", generated.commits),
            mode: "grow".to_string(),
            generated: generated.clone(),
            reads: read_results,
            storage: report::storage_snapshot(existing, !scale_args.no_pack)?,
            steady,
        });

        largest = Some((existing.clone(), workload, generated));
    }

    for (index, commits) in tiers.iter().enumerate() {
        if scale_args.reuse.is_some() || scale_args.extend.is_some() {
            break;
        }
        let workload = Workload {
            commits: *commits,
            targets_per_commit: scale_args.targets_per_commit,
            list_entries: scale_args.list_entries,
            value_sizes: value_sizes.clone(),
            mode: Mode::Grow,
            prune_every: scale_args.prune_every,
            prune_since: scale_args.prune_since.clone(),
            prune_local: scale_args.prune_local,
            auto_prune_max_keys: scale_args.auto_prune_max_keys,
            auto_prune_min_keys: scale_args.auto_prune_min_keys,
            maintain_every: scale_args.maintain_every,
            span_days: scale_args.span_days,
            checkpoint_every: scale_args.checkpoint_every,
            ..Workload::default()
        };

        let repo_dir = workspace.join(format!("grow-{commits}"));
        eprintln!(
            "\n[grow] generating {commits} metadata commits in {}",
            repo_dir.display()
        );
        support::init_repo(&repo_dir)?;
        let generated = workload::generate(&repo_dir, &workload, args.verbose)?;

        let session = Session::open(&repo_dir)?;
        let read_results = reads::measure(
            &session,
            &generated.targets,
            scale_args.read_samples,
            0xBEEF_0001,
        )?;
        let storage = report::storage_snapshot(&repo_dir, !scale_args.no_pack)?;
        let steady = if scale_args.steady > 0 {
            Some(workload::measure_steady_state(
                &repo_dir,
                &generated,
                &workload,
                scale_args.steady,
            )?)
        } else {
            None
        };

        report.push_tier(report::TierReport {
            label: format!("grow/{commits}"),
            mode: "grow".to_string(),
            generated: generated.clone(),
            reads: read_results,
            storage,
            steady,
        });

        let is_last = index == tiers.len() - 1;
        if is_last {
            largest = Some((repo_dir, workload, generated));
        }
    }

    if run_scale
        && !scale_args.no_churn
        && scale_args.reuse.is_none()
        && scale_args.extend.is_none()
    {
        let commits = *tiers.last().unwrap_or(&2_000);
        let workload = Workload {
            commits,
            targets_per_commit: scale_args.targets_per_commit,
            list_entries: scale_args.list_entries,
            value_sizes,
            mode: Mode::Churn,
            churn_pool: (commits / 10).max(100),
            span_days: scale_args.span_days,
            checkpoint_every: scale_args.checkpoint_every,
            ..Workload::default()
        };
        let repo_dir = workspace.join(format!("churn-{commits}"));
        eprintln!(
            "\n[churn] generating {commits} metadata commits in {}",
            repo_dir.display()
        );
        support::init_repo(&repo_dir)?;
        let generated = workload::generate(&repo_dir, &workload, args.verbose)?;
        let session = Session::open(&repo_dir)?;
        let read_results = reads::measure(
            &session,
            &generated.targets,
            scale_args.read_samples,
            0xBEEF_0002,
        )?;
        let storage = report::storage_snapshot(&repo_dir, !scale_args.no_pack)?;
        report.push_tier(report::TierReport {
            label: format!("churn/{commits}"),
            mode: "churn".to_string(),
            generated,
            reads: read_results,
            storage,
            steady: None,
        });
    }

    let (repo_dir, workload, generated) = largest.context("no history was generated")?;
    let end_ms = workload.start_ms + workload.span_days * 24 * 60 * 60 * 1000;

    if run_prune {
        eprintln!("\n[prune] comparing retention settings");
        let session = Session::open(&repo_dir)?;
        let Some(tip_tree) = reads::local_tip_tree(&session)? else {
            anyhow::bail!("no serialized metadata tree to prune");
        };
        let settings = vec![
            prune::PruneSetting::new("keep 30d", "30d", None),
            prune::PruneSetting::new("keep 90d", "90d", None),
            prune::PruneSetting::new("keep 1y", "1y", None),
            prune::PruneSetting::new("keep 90d, min-size 4k", "90d", Some(4 * 1024)),
        ];
        report.prune = prune::compare(&session, tip_tree, &settings, end_ms)?;
        report.print_prune_section();
    }

    if run_sparse {
        eprintln!("\n[sparse] publishing and cloning");
        let sparse_dir = workspace.join("sparse");
        std::fs::create_dir_all(&sparse_dir)?;

        // Work on a copy so the prune commit does not disturb the scale repo.
        let producer_dir = sparse_dir.join("producer");
        copy_dir(&repo_dir, &producer_dir)?;

        let session = Session::open(&producer_dir)?;
        let namespace = session.namespace().to_string();
        let setting = prune::PruneSetting::new(
            &format!("keep {}", scale_args.prune_since),
            &scale_args.prune_since,
            None,
        );
        let cutoff_ms = git_meta_lib::prune::parse_since_to_cutoff_ms(&setting.since, end_ms)?;
        prune::prune_and_commit(&session, &setting, end_ms)?;
        drop(session);

        let recent = pick_target(&generated, |ts| ts >= cutoff_ms);
        let old = pick_target(&generated, |ts| ts < cutoff_ms);
        let pruned_samples = sample_targets(&generated, cutoff_ms, 25);
        if old.is_none() {
            eprintln!("  warning: no target falls outside the retention window; widen --span-days");
        }

        let origin_dir = sparse_dir.join("origin.git");
        sparse::publish(&producer_dir, &origin_dir, &namespace)?;
        report.sparse = sparse::run(
            &sparse_dir,
            &origin_dir,
            &namespace,
            recent.as_ref(),
            old.as_ref(),
            &pruned_samples,
            workload::STRING_KEYS[0],
            scale_args.max_walk,
        )?;
        report.prune_cutoff_ms = Some(cutoff_ms);
    }

    report.print_remaining();

    if let Some(path) = &args.json {
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
        eprintln!("\njson written to {}", path.display());
    }

    if args.keep {
        eprintln!("\nkept workspace: {}", workspace.display());
    } else {
        std::fs::remove_dir_all(&workspace).ok();
    }

    Ok(())
}

/// Pick a generated target whose write timestamp satisfies `predicate`.
fn pick_target(
    generated: &workload::Generated,
    predicate: impl Fn(i64) -> bool,
) -> Option<git_meta_lib::types::Target> {
    generated
        .target_timestamps
        .iter()
        .position(|ts| predicate(*ts))
        .and_then(|idx| generated.targets.get(idx).cloned())
}

/// Read the targets and key count of an already-generated repository.
fn load_existing(repo_dir: &std::path::Path) -> Result<workload::Generated> {
    let session = Session::open(repo_dir)?;
    let mut targets = Vec::new();
    let mut target_timestamps = Vec::new();
    let mut seen = std::collections::HashMap::new();

    for entry in session.store().get_all_metadata()? {
        let target =
            git_meta_lib::types::Target::from_parts(entry.target_type, Some(entry.target_value));
        if let Some(index) = seen.get(&target) {
            let slot: &mut i64 = &mut target_timestamps[*index];
            *slot = (*slot).max(entry.last_timestamp);
        } else {
            seen.insert(target.clone(), targets.len());
            targets.push(target);
            target_timestamps.push(entry.last_timestamp);
        }
    }

    let (sqlite_rows, git_ref_rows) = session.store().stats_storage_counts()?;
    let tip = session
        .repo()
        .find_reference(&format!("refs/{}/local/main", session.namespace()))?
        .into_fully_peeled_id()?
        .detach();
    let commits = session.repo().rev_walk(Some(tip)).all()?.count();

    Ok(workload::Generated {
        commits,
        keys_written: 0,
        live_keys: sqlite_rows + git_ref_rows,
        total_ms: 0,
        serialize_ms: 0,
        write_ms: 0,
        checkpoints: Vec::new(),
        prune_commits: 0,
        prune_ms: 0,
        maintain_ms: 0,
        auto_prunes: 0,
        auto_pruned_keys: 0,
        targets,
        target_timestamps,
    })
}

/// Sample up to `count` targets written before the retention cutoff, spread
/// evenly across the pruned span so the deep-read cost curve covers a range of
/// history depths rather than one point.
fn sample_targets(
    generated: &workload::Generated,
    cutoff_ms: i64,
    count: usize,
) -> Vec<git_meta_lib::types::Target> {
    let old: Vec<usize> = generated
        .target_timestamps
        .iter()
        .enumerate()
        .filter(|(_, ts)| **ts < cutoff_ms)
        .map(|(idx, _)| idx)
        .collect();
    if old.is_empty() || count == 0 {
        return Vec::new();
    }
    let stride = (old.len() / count).max(1);
    old.iter()
        .step_by(stride)
        .take(count)
        .filter_map(|idx| generated.targets.get(*idx).cloned())
        .collect()
}

/// Recursively copy a directory.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}
