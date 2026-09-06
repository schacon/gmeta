//! Result aggregation and human-readable output.

use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::prune::PruneResult;
use crate::reads::ReadResults;
use crate::sparse::CloneResult;
use crate::support::{dir_size, git_dir, human_bytes, loose_object_count, packed_size};
use crate::workload::Generated;

/// Object-store measurements for a generated repository.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Storage {
    pub sqlite_bytes: u64,
    pub loose_objects: u64,
    pub objects_bytes: u64,
    pub packed_bytes: Option<u64>,
}

/// Measure the on-disk footprint of a generated repository.
pub(crate) fn storage_snapshot(repo_dir: &Path, pack: bool) -> Result<Storage> {
    let git = git_dir(repo_dir);
    let sqlite_bytes = std::fs::metadata(git.join("git-meta.sqlite"))
        .map(|m| m.len())
        .unwrap_or(0);
    let loose_objects = loose_object_count(&git);
    let objects_bytes = dir_size(&git.join("objects"));
    let packed_bytes = if pack {
        Some(packed_size(repo_dir, &git)?)
    } else {
        None
    };
    Ok(Storage {
        sqlite_bytes,
        loose_objects,
        objects_bytes,
        packed_bytes,
    })
}

/// One generated tier and everything measured against it.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct TierReport {
    pub label: String,
    pub mode: String,
    pub generated: Generated,
    pub reads: ReadResults,
    pub storage: Storage,
    /// Cost of one further publish once this tier's history exists.
    pub steady: Option<crate::workload::SteadyState>,
}

/// The full benchmark result set.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Report {
    pub tiers: Vec<TierReport>,
    pub prune: Vec<PruneResult>,
    pub sparse: Vec<CloneResult>,
    pub prune_cutoff_ms: Option<i64>,
    pub config: Config,
}

/// The knobs the run was invoked with.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Config {
    pub tiers: Vec<usize>,
    pub targets_per_commit: usize,
    pub list_entries: usize,
    pub value_sizes: String,
    pub mean_value_bytes: f64,
    pub span_days: i64,
    pub read_samples: usize,
}

impl Report {
    pub(crate) fn new(args: &crate::ScaleArgs, value_sizes: &crate::workload::SizeMix) -> Self {
        Report {
            tiers: Vec::new(),
            prune: Vec::new(),
            sparse: Vec::new(),
            prune_cutoff_ms: None,
            config: Config {
                tiers: args.tiers.clone(),
                targets_per_commit: args.targets_per_commit,
                list_entries: args.list_entries,
                value_sizes: value_sizes.describe(),
                mean_value_bytes: value_sizes.mean_bytes(),
                span_days: args.span_days,
                read_samples: args.read_samples,
            },
        }
    }

    pub(crate) fn push_tier(&mut self, tier: TierReport) {
        self.tiers.push(tier);
    }

    /// Print the prune comparison as soon as it is available.
    ///
    /// A long run can fail in a later scenario, and results already measured
    /// should not be lost with it.
    pub(crate) fn print_prune_section(&self) {
        if !self.prune.is_empty() {
            self.print_prune();
        }
    }

    /// Print everything the prune section did not already cover.
    pub(crate) fn print_remaining(&self) {
        if !self.tiers.is_empty() {
            self.print_tiers();
            self.print_scaling_curve();
        }
        self.print_steady_state();
        if !self.sparse.is_empty() {
            self.print_sparse();
        }
    }

    fn print_tiers(&self) {
        println!("\n== Generation and storage ==\n");
        println!(
            "{:<16} {:>8} {:>10} {:>12} {:>12} {:>11} {:>11}",
            "tier", "commits", "live keys", "total (s)", "ser (s)", "objects", "packed"
        );
        for tier in &self.tiers {
            println!(
                "{:<16} {:>8} {:>10} {:>12.1} {:>12.1} {:>11} {:>11}",
                tier.label,
                tier.generated.commits,
                tier.generated.live_keys,
                tier.generated.total_ms as f64 / 1000.0,
                tier.generated.serialize_ms as f64 / 1000.0,
                tier.storage.loose_objects,
                tier.storage
                    .packed_bytes
                    .map_or_else(|| "-".to_string(), human_bytes),
            );
        }

        println!("\n== Read latency ==\n");
        println!(
            "{:<16} {:>12} {:>12} {:>14} {:>14} {:>10}",
            "tier", "store p50", "store p95", "tree p50", "tree p95", "tip hits"
        );
        for tier in &self.tiers {
            println!(
                "{:<16} {:>11.1}u {:>11.1}u {:>13.1}u {:>13.1}u {:>9.0}%",
                tier.label,
                tier.reads.store_get.p50_us,
                tier.reads.store_get.p95_us,
                tier.reads.tip_tree_lookup.p50_us,
                tier.reads.tip_tree_lookup.p95_us,
                tier.reads.tip_hit_rate * 100.0,
            );
        }
        println!(
            "\n  store = SQLite point read; tree = path resolution in the serialized tip tree"
        );
    }

    /// The load-bearing question: does per-commit serialize cost stay flat as
    /// the history and key space grow?
    fn print_scaling_curve(&self) {
        println!("\n== Serialize cost as history grows ==\n");
        for tier in &self.tiers {
            if tier.generated.checkpoints.len() < 2 {
                continue;
            }
            println!("{} ({} mode)", tier.label, tier.mode);
            println!(
                "  {:>10} {:>10} {:>18} {:>16}",
                "commits", "live keys", "serialize us/commit", "write us/commit"
            );
            let checkpoints = &tier.generated.checkpoints;
            let stride = (checkpoints.len() / 8).max(1);
            for checkpoint in checkpoints.iter().step_by(stride) {
                println!(
                    "  {:>10} {:>10} {:>18.1} {:>16.1}",
                    checkpoint.commits,
                    checkpoint.live_keys,
                    checkpoint.serialize_us_per_commit,
                    checkpoint.write_us_per_commit,
                );
            }
            if let (Some(first), Some(last)) = (checkpoints.first(), checkpoints.last()) {
                let ratio = if first.serialize_us_per_commit > 0.0 {
                    last.serialize_us_per_commit / first.serialize_us_per_commit
                } else {
                    0.0
                };
                let key_ratio = if first.live_keys > 0 {
                    last.live_keys as f64 / first.live_keys as f64
                } else {
                    0.0
                };
                println!(
                    "  -> keys grew {key_ratio:.1}x, serialize cost per commit grew {ratio:.1}x"
                );
            }
            println!();
        }
    }

    /// What one more change costs once the history already exists. This is the
    /// number a tool feels in practice, unlike the average over a build-up.
    fn print_steady_state(&self) {
        let with_steady: Vec<&TierReport> =
            self.tiers.iter().filter(|t| t.steady.is_some()).collect();
        if with_steady.is_empty() {
            return;
        }

        println!("\n== Steady state: cost of one more publish ==\n");
        println!(
            "{:<16} {:>11} {:>9} {:>11} {:>11} {:>11} {:>11}",
            "tier", "live keys", "commits", "write p50", "write p95", "ser p50", "ser p95"
        );
        for tier in with_steady {
            let Some(steady) = &tier.steady else { continue };
            println!(
                "{:<16} {:>11} {:>9} {:>10.0}u {:>10.0}u {:>10.1}m {:>10.1}m",
                tier.label,
                steady.live_keys,
                steady.commits,
                steady.write.p50_us,
                steady.write.p95_us,
                steady.serialize.p50_us / 1000.0,
                steady.serialize.p95_us / 1000.0,
            );
        }
        println!("\n  write = SQLite set; ser = serialize + commit (u = us, m = ms)");
    }

    fn print_prune(&self) {
        println!("\n== Prune settings ==\n");
        println!(
            "{:<14} {:<24} {:>9} {:>9} {:>9} {:>11} {:>11} {:>9}",
            "impl", "setting", "keys in", "keys out", "dropped", "bytes in", "bytes out", "ms"
        );
        for result in &self.prune {
            println!(
                "{:<14} {:<24} {:>9} {:>9} {:>9} {:>11} {:>11} {:>9}",
                match result.implementation {
                    crate::prune::PruneImpl::DbRebuild => "cli (db)",
                },
                result.label,
                result.keys_before,
                result.keys_after,
                result.keys_dropped,
                human_bytes(result.bytes_before),
                human_bytes(result.bytes_after),
                result.prune_ms,
            );
        }
        println!(
            "\n  cli (db) = rebuild the tree from SQLite rows newer than the cutoff (`git meta prune`)\
             \n  bytes    = total blob bytes reachable from the tree (uncompressed)"
        );
    }

    fn print_sparse(&self) {
        println!("\n== Sparse fetch ==\n");
        println!(
            "{:<21} {:>9} {:>10} {:>13} {:>10} {:>9} {:>9}",
            "arm", "pull ms", "objects", "sqlite", "indexed", "promised", "missing"
        );
        for result in &self.sparse {
            println!(
                "{:<21} {:>9} {:>10} {:>13} {:>10} {:>9} {:>9}",
                result.label,
                result.pull_ms,
                human_bytes(result.objects_bytes_after_pull),
                human_bytes(result.sqlite_bytes_after_pull),
                result.indexed_keys,
                result.promised_keys,
                result.missing_objects,
            );
        }
        println!(
            "\n  objects/sqlite = on-disk footprint after pull\
             \n  missing        = objects promised by the remote but not fetched"
        );

        println!("\n-- where the consumer's time goes --\n");
        println!(
            "{:<16} {:>8} {:>10} {:>11} {:>11} {:>11} {:>12} {:>12}",
            "arm",
            "clone",
            "fetch meta",
            "hydrate tip",
            "materialize",
            "index hist",
            "keys rebuilt",
            "keys indexed"
        );
        for result in &self.sparse {
            let phases = &result.phases;
            println!(
                "{:<16} {:>7}m {:>9}m {:>10}m {:>10}m {:>10}m {:>12} {:>12}",
                result.label,
                phases.clone_ms,
                phases.fetch_meta_ms,
                phases.hydrate_tip_ms,
                phases.materialize_ms,
                phases.index_history_ms,
                phases.keys_recreated,
                phases.indexed_keys,
            );
        }
        for result in &self.sparse {
            let phases = &result.phases;
            println!(
                "  {:<16} hydrated {} blobs into {} packs, repacked in {} ms",
                result.label, phases.hydrated_blobs, phases.packs_after_hydrate, phases.repack_ms,
            );
        }
        for result in &self.sparse {
            if let Some(error) = &result.pull_error {
                println!("  {:<16} single-call pull FAILED: {error}", result.label);
            }
        }

        println!("\n-- value retrieval --\n");
        for result in &self.sparse {
            println!("{}", result.label);
            print_hydrate("  key still in tip tree ", result.tip_key_read.as_ref());
            print_hydrate("  key pruned from tip   ", result.pruned_key_read.as_ref());
            if result.deep_read_samples > 0 {
                println!(
                    "  pruned-key history walk over {} sampled keys: {}/{} still reachable, \
                     mean {:.0} commits walked, p50 {:.1} ms, p95 {:.1} ms",
                    result.deep_read_samples,
                    result.deep_read_hits,
                    result.deep_read_samples,
                    result.deep_read_mean_commits,
                    result.deep_read_latency.p50_us / 1000.0,
                    result.deep_read_latency.p95_us / 1000.0,
                );
            }
            println!();
        }
    }
}

fn print_hydrate(label: &str, result: Option<&crate::sparse::HydrateResult>) {
    match result {
        None => println!("{label}: (not sampled)"),
        Some(result) => {
            let where_found = if result.in_tip_tree {
                "tip tree".to_string()
            } else if result.located {
                match &result.deep {
                    Some(deep) => format!("history, {} commits back", deep.commits_walked),
                    None => "history".to_string(),
                }
            } else {
                match &result.deep {
                    Some(deep) => {
                        format!("NOT FOUND after walking {} commits", deep.commits_walked)
                    }
                    None => "NOT FOUND".to_string(),
                }
            };
            let deep_ms = result
                .deep
                .as_ref()
                .map_or(0.0, |deep| deep.micros / 1000.0);
            println!(
                "{label}: {where_found}; walk {deep_ms:.1} ms, blob fetch {} ms, total {} ms",
                result.fetch_ms, result.total_ms
            );
        }
    }
}
