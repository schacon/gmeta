//! Shared helpers: deterministic RNG, latency statistics, repo scaffolding.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::Serialize;

/// Deterministic xorshift64* PRNG.
///
/// The benchmark must be reproducible across runs and machines, so it does not
/// use a seeded system RNG.
#[derive(Debug)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        // xorshift needs a nonzero state, but `seed | 1` would collapse each
        // even seed onto its odd neighbour and make the two runs identical.
        Rng(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform value in `0..n`. Returns 0 when `n` is 0.
    pub(crate) fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next_u64() % n as u64) as usize
    }

    /// A synthetic 40-character hex object name.
    pub(crate) fn hex40(&mut self) -> String {
        format!(
            "{:016x}{:016x}{:08x}",
            self.next_u64(),
            self.next_u64(),
            self.next_u64() as u32
        )
    }
}

/// Latency summary for a batch of identical operations.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Latency {
    pub samples: usize,
    pub total_ms: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub max_us: f64,
}

impl Latency {
    /// Summarize a set of durations in microseconds.
    pub(crate) fn from_micros(mut micros: Vec<f64>) -> Self {
        if micros.is_empty() {
            return Latency {
                samples: 0,
                total_ms: 0.0,
                mean_us: 0.0,
                p50_us: 0.0,
                p95_us: 0.0,
                max_us: 0.0,
            };
        }
        micros.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let total: f64 = micros.iter().sum();
        // Nearest-rank percentile: the smallest sample at or above the
        // requested rank.
        let idx = |q: f64| -> f64 {
            let rank = (q * micros.len() as f64).ceil() as usize;
            micros[rank.saturating_sub(1).min(micros.len() - 1)]
        };
        Latency {
            samples: micros.len(),
            total_ms: total / 1000.0,
            mean_us: total / micros.len() as f64,
            p50_us: idx(0.50),
            p95_us: idx(0.95),
            max_us: idx(1.0),
        }
    }
}

/// Run a git command in `dir`, returning stdout.
pub(crate) fn git(dir: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .with_context(|| format!("failed to spawn git {args:?}"))?;
    if !out.status.success() {
        bail!(
            "git {args:?} failed in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Initialize a working repository configured for git-meta.
pub(crate) fn init_repo(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    git(dir, &["init", "--quiet", "--initial-branch=main"])?;
    git(dir, &["config", "user.email", "bench@git-meta.example"])?;
    git(dir, &["config", "user.name", "git-meta bench"])?;
    // A commit target is only a string key, but a real repo keeps gix happy.
    std::fs::write(dir.join("README.md"), "bench\n")?;
    git(dir, &["add", "README.md"])?;
    git(dir, &["commit", "--quiet", "-m", "init"])?;
    Ok(())
}

/// Total size in bytes of a directory tree.
pub(crate) fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += dir_size(&entry.path());
        } else {
            total += meta.len();
        }
    }
    total
}

/// Object-store size for a repository, after packing.
pub(crate) fn packed_size(repo_dir: &Path, git_dir: &Path) -> Result<u64> {
    git(repo_dir, &["gc", "--quiet", "--aggressive", "--prune=now"])?;
    Ok(dir_size(&git_dir.join("objects")))
}

/// Count loose objects in a git directory.
pub(crate) fn loose_object_count(git_dir: &Path) -> u64 {
    let objects = git_dir.join("objects");
    let mut count = 0;
    let Ok(entries) = std::fs::read_dir(&objects) else {
        return 0;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit()) {
            if let Ok(inner) = std::fs::read_dir(entry.path()) {
                count += inner.flatten().count() as u64;
            }
        }
    }
    count
}

/// Resolve the `.git` directory for a working repository.
pub(crate) fn git_dir(repo_dir: &Path) -> PathBuf {
    repo_dir.join(".git")
}

/// Format a byte count for human-readable output.
pub(crate) fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn rng_is_deterministic_for_a_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        let mut different = Rng::new(43);
        let first = a.next_u64();
        assert_eq!(first, b.next_u64());
        assert_ne!(first, different.next_u64());
        for _ in 0..64 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn rng_below_stays_in_range() {
        let mut rng = Rng::new(7);
        for _ in 0..1_000 {
            assert!(rng.below(10) < 10);
        }
        assert_eq!(rng.below(0), 0, "below(0) must not divide by zero");
    }

    #[test]
    fn hex40_is_forty_hex_characters() {
        let mut rng = Rng::new(99);
        for _ in 0..100 {
            let hex = rng.hex40();
            assert_eq!(hex.len(), 40, "got {hex}");
            assert!(hex.chars().all(|c| c.is_ascii_hexdigit()), "got {hex}");
        }
    }

    #[test]
    fn latency_percentiles_pick_the_right_samples() {
        let latency = Latency::from_micros((1..=100).map(f64::from).collect());
        assert_eq!(latency.samples, 100);
        assert_eq!(latency.max_us, 100.0);
        assert_eq!(latency.p50_us, 50.0);
        assert_eq!(latency.p95_us, 95.0);
        assert!((latency.mean_us - 50.5).abs() < 1e-9);
    }

    #[test]
    fn latency_of_nothing_is_zero_rather_than_a_panic() {
        let latency = Latency::from_micros(Vec::new());
        assert_eq!(latency.samples, 0);
        assert_eq!(latency.p95_us, 0.0);
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.00 KB");
        assert_eq!(human_bytes(3 * 1024 * 1024), "3.00 MB");
    }
}
