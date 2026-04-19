use anyhow::Result;
use std::time::{Duration, Instant};

use git_meta_lib::types::{Target, TargetType};
use git_meta_lib::Session;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const BLUE: &str = "\x1b[34m";

pub fn run() -> Result<()> {
    let session = Session::discover()?;
    let db = session.store();

    let keys = db.get_all_keys()?;
    let total = keys.len();

    if total == 0 {
        println!("no metadata stored");
        return Ok(());
    }

    println!("{BOLD}benchmarking {total} key reads...{RESET}");

    let mut durations: Vec<Duration> = Vec::with_capacity(total);
    let mut sizes: Vec<usize> = Vec::with_capacity(total);
    let mut errors = 0u64;

    for (target_type_str, target_value, key) in &keys {
        let t0 = Instant::now();
        let Ok(target_type) = target_type_str.parse::<TargetType>() else {
            errors += 1;
            continue;
        };
        let target = if target_type == TargetType::Project {
            Target::project()
        } else {
            Target::from_parts(target_type, Some(target_value.clone()))
        };
        match db.get(&target, key) {
            Ok(Some(mv)) => {
                let elapsed = t0.elapsed();
                let bytes = mv.value.len();
                drop(mv);
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
        println!("no values could be read ({errors} errors)");
        return Ok(());
    }

    // Timing stats
    durations.sort_unstable();
    let total_time: Duration = durations.iter().sum();
    let mean_s = total_time.as_secs_f64() / n as f64;
    let p50 = durations[n / 2].as_secs_f64();
    let p95 = durations[(n * 95) / 100].as_secs_f64();
    let p99 = durations[(n * 99) / 100].as_secs_f64();
    let max_s = durations[n - 1].as_secs_f64();
    let total_s = total_time.as_secs_f64();

    let err_color = if errors > 0 { RED } else { DIM };

    println!();
    println!(
        "{}timing{} ({}{} reads{}, {}{}{}{})",
        BOLD,
        RESET,
        CYAN,
        n,
        RESET,
        err_color,
        errors,
        if errors == 1 { " error" } else { " errors" },
        RESET,
    );
    println!("  {DIM}mean{RESET}  {YELLOW}{mean_s:>10.6} s{RESET}");
    println!("  {DIM}p50{RESET}   {GREEN}{p50:>10.6} s{RESET}");
    println!("  {DIM}p95{RESET}   {YELLOW}{p95:>10.6} s{RESET}");
    println!("  {DIM}p99{RESET}   {RED}{p99:>10.6} s{RESET}");
    println!("  {DIM}max{RESET}   {RED}{max_s:>10.6} s{RESET}");
    println!("  {DIM}total{RESET} {CYAN}{total_s:>10.6} s{RESET}");

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
    println!("{BOLD}value sizes:{RESET}");
    let max_count = counts.iter().copied().max().unwrap_or(1).max(1);
    let bar_width = 30usize;
    for ((_, label), count) in boundaries.iter().zip(counts.iter()) {
        let filled = ((*count as f64 / max_count as f64) * bar_width as f64).round() as usize;
        let bar = format!("{}{}{}", BLUE, "#".repeat(filled), RESET);
        println!("  {DIM}{label}{RESET}  {bar:<30}  {CYAN}{count}{RESET}",);
    }

    Ok(())
}
