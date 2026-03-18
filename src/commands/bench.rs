use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::{Duration, Instant};

use crate::db::Db;
use crate::git_utils;

pub fn run() -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let repo2 = git_utils::discover_repo()?;
    let db = Db::open_with_repo(&db_path, repo2)?;

    let keys = db.get_all_keys()?;
    let total = keys.len();

    if total == 0 {
        println!("no metadata stored");
        return Ok(());
    }

    println!("benchmarking {} key reads...", total);

    let tmp_path = std::env::temp_dir().join("gmeta-bench.tmp");
    let mut tmp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)?;

    let mut durations: Vec<Duration> = Vec::with_capacity(total);
    let mut sizes: Vec<usize> = Vec::with_capacity(total);
    let mut errors = 0u64;

    for (target_type, target_value, key) in &keys {
        let t0 = Instant::now();
        match db.get(target_type, target_value, key) {
            Ok(Some((value, _vtype, _is_git_ref))) => {
                let elapsed = t0.elapsed();
                let bytes = value.len();
                tmp_file.write_all(value.as_bytes())?;
                durations.push(elapsed);
                sizes.push(bytes);
            }
            Ok(None) => {
                errors += 1;
            }
            Err(_) => {
                errors += 1;
            }
        }
    }

    let n = durations.len();
    if n == 0 {
        println!("no values could be read ({} errors)", errors);
        return Ok(());
    }

    // Timing stats
    durations.sort_unstable();
    let total_time: Duration = durations.iter().sum();
    let mean_us = total_time.as_micros() as f64 / n as f64;
    let p50 = durations[n / 2].as_micros();
    let p95 = durations[(n * 95) / 100].as_micros();
    let p99 = durations[(n * 99) / 100].as_micros();
    let max_us = durations[n - 1].as_micros();

    println!();
    println!("timing ({} reads, {} errors):", n, errors);
    println!("  mean  {:>8} µs", mean_us.round() as u64);
    println!("  p50   {:>8} µs", p50);
    println!("  p95   {:>8} µs", p95);
    println!("  p99   {:>8} µs", p99);
    println!("  max   {:>8} µs", max_us);
    println!("  total {:>8} ms", total_time.as_millis());

    // Size histogram
    let boundaries: &[(usize, &str)] = &[
        (64, "<64B     "),
        (1024, "64B–1KB  "),
        (4096, "1KB–4KB  "),
        (16384, "4KB–16KB "),
        (65536, "16KB–64KB"),
        (usize::MAX, "64KB+    "),
    ];
    let mut counts = vec![0u64; boundaries.len()];
    for &sz in &sizes {
        for (i, (limit, _)) in boundaries.iter().enumerate() {
            if sz < *limit {
                counts[i] += 1;
                break;
            }
        }
    }

    println!();
    println!("value sizes:");
    let max_count = counts.iter().copied().max().unwrap_or(1).max(1);
    let bar_width = 30usize;
    for ((_, label), count) in boundaries.iter().zip(counts.iter()) {
        let filled = ((*count as f64 / max_count as f64) * bar_width as f64).round() as usize;
        let bar = "#".repeat(filled);
        println!("  {}  {:30}  {}", label, bar, count);
    }

    Ok(())
}
