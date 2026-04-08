//! Fanout scheme benchmark.
//!
//! Creates a temp git repo, writes N synthetic commit-SHA objects under three
//! different shard layouts, then measures:
//!   1. Write throughput  — insert 1 000 new objects into an already-populated tree
//!   2. Read throughput   — look up 1 000 objects by known SHA
//!   3. Pack file size    — run `git gc` and report pack size + object count
//!   4. Diff speed        — compare two trees (base vs base+1000 changes)
//!
//! Usage:  gmeta fanout-bench [--objects N]   (default N = 1_000_000)

use anyhow::{Context, Result};
use gix::prelude::ObjectIdExt;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const BLUE: &str = "\x1b[34m";
const MAGENTA: &str = "\x1b[35m";
#[derive(Clone, Copy, Debug)]
pub enum Scheme {
    First2,      // commit/{aa}/{full_sha}
    First3,      // commit/{aaa}/{full_sha}
    First2Next2, // commit/{aa}/{bb}/{full_sha}
}

impl Scheme {
    fn name(self) -> &'static str {
        match self {
            Scheme::First2 => "{first2}",
            Scheme::First3 => "{first3}",
            Scheme::First2Next2 => "{first2}/{next2}",
        }
    }

    fn shard_path(self, sha: &str) -> String {
        match self {
            Scheme::First2 => format!("commit/{}/{}", &sha[..2], sha),
            Scheme::First3 => format!("commit/{}/{}", &sha[..3], sha),
            Scheme::First2Next2 => {
                format!("commit/{}/{}/{}", &sha[..2], &sha[2..4], sha)
            }
        }
    }

    fn bucket_count(self) -> usize {
        match self {
            Scheme::First2 => 256,
            Scheme::First3 => 4_096,
            Scheme::First2Next2 => 65_536,
        }
    }
}

/// Build a full tree from a flat path-to-content map using the gix tree editor.
///
/// Each path is split on `/` and upserted into the editor, which handles
/// creating intermediate tree objects automatically.
///
/// # Parameters
///
/// - `repo`: the gix repository to write objects into
/// - `files`: mapping from slash-separated paths to blob content
///
/// # Returns
///
/// The OID of the root Git tree object.
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn build_tree(repo: &gix::Repository, files: &BTreeMap<String, Vec<u8>>) -> Result<gix::ObjectId> {
    let mut editor = repo.empty_tree().edit()?;
    for (path, content) in files {
        let blob_id = repo.write_blob(content)?.detach();
        editor.upsert(path, gix::objs::tree::EntryKind::Blob, blob_id)?;
    }
    Ok(editor.write()?.detach())
}

/// Incremental tree update: start from an existing tree and apply only the
/// changed paths. The gix tree editor handles loading unchanged subtrees by
/// OID and only rewriting the spine of paths that changed.
///
/// # Parameters
///
/// - `repo`: the gix repository to write objects into
/// - `base_oid`: OID of the base tree to start from
/// - `new_entries`: list of (path, content) pairs to upsert
///
/// # Returns
///
/// The OID of the updated root Git tree object.
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn build_tree_incremental(
    repo: &gix::Repository,
    base_oid: gix::ObjectId,
    new_entries: &[(&str, Vec<u8>)],
) -> Result<gix::ObjectId> {
    let base_tree = base_oid.attach(repo).object()?.into_tree();
    let mut editor = base_tree.edit()?;

    for (path, content) in new_entries {
        let blob_id = repo.write_blob(content)?.detach();
        editor.upsert(*path, gix::objs::tree::EntryKind::Blob, blob_id)?;
    }

    Ok(editor.write()?.detach())
}

/// Write a minimal commit object pointing at the given tree, with optional parent.
///
/// Uses `gix::objs::Commit` and `repo.write_object()` to create the commit
/// without updating any refs.
///
/// # Parameters
///
/// - `repo`: the gix repository
/// - `tree_oid`: OID of the tree to commit
/// - `parent_oid`: optional parent commit OID
///
/// # Returns
///
/// The OID of the newly written commit.
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn write_commit(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    parent_oid: Option<gix::ObjectId>,
) -> Result<gix::ObjectId> {
    let sig = gix::actor::Signature {
        name: "bench".into(),
        email: "bench@bench".into(),
        time: gix::date::Time::new(0, 0),
    };
    let parents: Vec<gix::ObjectId> = parent_oid.into_iter().collect();
    let commit = gix::objs::Commit {
        message: "bench".into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: parents.into(),
        extra_headers: Default::default(),
    };
    Ok(repo.write_object(&commit)?.detach())
}

/// Deterministic fake SHAs: we hash an integer with a simple xor-shift so every
/// "SHA" looks like a real 40-hex-char commit hash and has uniform distribution
/// across all prefix bytes.
fn fake_sha(n: u64) -> String {
    let mut x = n.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    let y = x
        .wrapping_mul(0x6c62272e07bb0142)
        .wrapping_add(0x62b821756295c58d);
    format!("{:016x}{:016x}{:08x}", x, y, (x ^ y) as u32)
}
fn print_header(label: &str) {
    println!(
        "\n{}{} ══════════════════════════════════════════════{}\n",
        BOLD, label, RESET
    );
}

fn fmt_ms(secs: f64) -> String {
    format!("{:.1} ms", secs * 1000.0)
}

fn fmt_us(secs: f64) -> String {
    format!("{:.1} µs", secs * 1_000_000.0)
}

fn bar(value: f64, max: f64, width: usize, color: &str) -> String {
    let filled = ((value / max) * width as f64).round() as usize;
    format!("{}{}{}", color, "█".repeat(filled.min(width)), RESET)
}

/// Diff two trees using `git diff-tree` subprocess.
///
/// Returns the number of changed paths between the two trees.
///
/// # Parameters
///
/// - `repo_path`: path to the bare git repository
/// - `base_oid`: OID of the base tree
/// - `new_oid`: OID of the new tree
///
/// # Returns
///
/// The count of changed entries.
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn diff_trees(
    repo_path: &std::path::Path,
    base_oid: gix::ObjectId,
    new_oid: gix::ObjectId,
) -> Result<usize> {
    let output = std::process::Command::new("git")
        .args(["--git-dir", &repo_path.to_string_lossy()])
        .args([
            "diff-tree",
            "-r",
            "--name-only",
            &base_oid.to_string(),
            &new_oid.to_string(),
        ])
        .output()
        .context("failed to run git diff-tree")?;
    if !output.status.success() {
        anyhow::bail!("git diff-tree failed");
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().filter(|l| !l.is_empty()).count())
}
fn pack_size_bytes(repo_path: &std::path::Path) -> Result<u64> {
    let pack_dir = repo_path.join("objects").join("pack");
    let mut total = 0u64;
    if pack_dir.exists() {
        for entry in std::fs::read_dir(&pack_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "pack").unwrap_or(false) {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
}

fn run_git_gc(repo_path: &std::path::Path) -> Result<()> {
    let status = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .arg("gc")
        .arg("--quiet")
        .arg("--aggressive")
        .status()
        .context("failed to run git gc")?;
    if !status.success() {
        anyhow::bail!("git gc failed");
    }
    Ok(())
}

fn count_loose_objects(repo_path: &std::path::Path) -> Result<usize> {
    let obj_dir = repo_path.join("objects");
    let mut count = 0usize;
    for entry in std::fs::read_dir(&obj_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        // two-char hex directories only
        if s.len() == 2 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            for sub in std::fs::read_dir(entry.path())? {
                let _ = sub?;
                count += 1;
            }
        }
    }
    Ok(count)
}
struct SchemeResult {
    scheme: Scheme,
    /// Batch write: incremental tree update inserting SAMPLE entries at once
    write_batch_secs: f64,
    /// Sequential write: 1 entry -> commit (with parent) x SAMPLE, total time
    write_seq_secs: f64,
    read_1k_secs: f64,
    pack_bytes: u64,
    loose_objects_before_gc: usize,
    diff_secs: f64,
    diff_count: usize,
    log: Vec<String>,
}

const SAMPLE: usize = 1_000;

fn entry_content(sha: &str) -> Vec<u8> {
    format!("{{\"sha\":\"{}\"}}", sha).into_bytes()
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
fn bench_scheme(
    scheme: Scheme,
    n_base: usize,
    shas: Arc<Vec<String>>,
    tmp_base: &std::path::Path,
) -> Result<SchemeResult> {
    let mut log: Vec<String> = Vec::new();

    let repo_path: PathBuf = tmp_base.join(match scheme {
        Scheme::First2 => "first2",
        Scheme::First3 => "first3",
        Scheme::First2Next2 => "first2next2",
    });
    std::fs::create_dir_all(&repo_path)?;
    let repo = gix::init_bare(&repo_path)?;

    let base_files: BTreeMap<String, Vec<u8>> = shas[..n_base]
        .iter()
        .map(|sha| (scheme.shard_path(sha), entry_content(sha)))
        .collect();

    let t0 = Instant::now();
    let base_tree_oid = build_tree(&repo, &base_files)?;
    let base_build_secs = t0.elapsed().as_secs_f64();
    log.push(format!(
        "    build base tree ({} objects)   {}{}{}",
        n_base,
        GREEN,
        fmt_ms(base_build_secs),
        RESET
    ));

    // Commit the base tree so sequential writes have a real parent chain.
    let base_commit_oid = write_commit(&repo, base_tree_oid, None)?;

    // Incremental: apply SAMPLE new entries onto the existing base tree in one
    // shot. Only the shard subtrees touched by the new entries are rebuilt;
    // everything else is reused by OID. This is what a correct gmeta serialize
    // would do after accumulating N changes since the last commit.
    let new_shas = &shas[n_base..n_base + SAMPLE];
    let batch_incremental: Vec<(String, Vec<u8>)> = new_shas
        .iter()
        .map(|sha| (scheme.shard_path(sha), entry_content(sha)))
        .collect();
    let batch_ref: Vec<(&str, Vec<u8>)> = batch_incremental
        .iter()
        .map(|(p, c)| (p.as_str(), c.clone()))
        .collect();

    let t0 = Instant::now();
    let batch_tree_oid = build_tree_incremental(&repo, base_tree_oid, &batch_ref)?;
    let write_batch_secs = t0.elapsed().as_secs_f64();
    log.push(format!(
        "    write batch  ({} entries, 1 tree)  {}{}{}",
        SAMPLE,
        YELLOW,
        fmt_ms(write_batch_secs),
        RESET
    ));

    // Add 1 entry, write an incremental tree, write a commit with the previous
    // commit as parent — repeat SAMPLE times. Models frequent single-change
    // serializes (e.g. one gmeta set -> serialize per user action).
    let t0 = Instant::now();
    let mut prev_tree_oid = base_tree_oid;
    let mut prev_commit_oid = base_commit_oid;
    for sha in new_shas {
        let path = scheme.shard_path(sha);
        let single: Vec<(&str, Vec<u8>)> = vec![(path.as_str(), entry_content(sha))];
        prev_tree_oid = build_tree_incremental(&repo, prev_tree_oid, &single)?;
        prev_commit_oid = write_commit(&repo, prev_tree_oid, Some(prev_commit_oid))?;
    }
    let write_seq_secs = t0.elapsed().as_secs_f64();
    log.push(format!(
        "    write seq    ({} × 1 entry + commit)  {}{}{}  ({} µs/commit)",
        SAMPLE,
        YELLOW,
        fmt_ms(write_seq_secs),
        RESET,
        (write_seq_secs * 1_000_000.0 / SAMPLE as f64) as u64,
    ));

    let read_shas: Vec<&String> = shas[..n_base]
        .iter()
        .step_by((n_base / SAMPLE).max(1))
        .take(SAMPLE)
        .collect();

    // Read test: look up entries by traversing the tree path
    let base_tree = base_tree_oid.attach(&repo).object()?.into_tree();
    let t0 = Instant::now();
    let mut read_hits = 0usize;
    for sha in &read_shas {
        let path = scheme.shard_path(sha);
        // Walk the tree path component by component
        if find_entry_in_tree(&repo, base_tree.id().detach(), &path)? {
            read_hits += 1;
        }
    }
    let read_secs = t0.elapsed().as_secs_f64();
    log.push(format!(
        "    read  1 000 lookups            {}{}{}  ({}/{} hits)",
        GREEN,
        fmt_ms(read_secs),
        RESET,
        read_hits,
        read_shas.len()
    ));

    let t0 = Instant::now();
    let diff_count = diff_trees(&repo_path, base_tree_oid, batch_tree_oid)?;
    let diff_secs = t0.elapsed().as_secs_f64();
    log.push(format!(
        "    diff  base vs +1 000           {}{}{}  ({} changed paths)",
        CYAN,
        fmt_ms(diff_secs),
        RESET,
        diff_count
    ));

    let loose_before = count_loose_objects(&repo_path)?;
    let t0 = Instant::now();
    run_git_gc(&repo_path)?;
    let gc_secs = t0.elapsed().as_secs_f64();
    let pack_bytes = pack_size_bytes(&repo_path)?;
    log.push(format!(
        "    git gc                         {}{}{}  → pack {}{:.2} MB{}  ({} loose before)",
        DIM,
        fmt_ms(gc_secs),
        RESET,
        MAGENTA,
        pack_bytes as f64 / 1_048_576.0,
        RESET,
        loose_before,
    ));

    Ok(SchemeResult {
        scheme,
        write_batch_secs,
        write_seq_secs,
        read_1k_secs: read_secs,
        pack_bytes,
        loose_objects_before_gc: loose_before,
        diff_secs,
        diff_count,
        log,
    })
}

/// Look up a path in a tree by walking component by component.
///
/// Returns `true` if the full path resolves to an existing blob entry.
fn find_entry_in_tree(repo: &gix::Repository, tree_oid: gix::ObjectId, path: &str) -> Result<bool> {
    use gix::prelude::ObjectIdExt;
    let parts: Vec<&str> = path.split('/').collect();
    let mut current_tree_oid = tree_oid;

    for (i, part) in parts.iter().enumerate() {
        let tree = current_tree_oid.attach(repo).object()?.into_tree();
        let mut found = false;
        for entry_result in tree.iter() {
            let entry = entry_result?;
            let name = std::str::from_utf8(entry.filename()).unwrap_or("");
            if name == *part {
                if i == parts.len() - 1 {
                    // Last component — we found the entry
                    return Ok(true);
                }
                if entry.mode().is_tree() {
                    current_tree_oid = entry.object_id();
                    found = true;
                    break;
                }
                return Ok(false);
            }
        }
        if !found && i < parts.len() - 1 {
            return Ok(false);
        }
    }
    Ok(false)
}
fn print_report(results: &[SchemeResult], n_base: usize) {
    print_header("RESULTS SUMMARY");

    let bar_w = 28usize;

    macro_rules! maxf {
        ($field:ident) => {
            results
                .iter()
                .map(|r| r.$field)
                .fold(0.0f64, f64::max)
                .max(1e-9)
        };
    }
    macro_rules! maxu {
        ($field:ident) => {
            results.iter().map(|r| r.$field).max().unwrap_or(1)
        };
    }

    let max_write_batch = maxf!(write_batch_secs);
    let max_write_seq = maxf!(write_seq_secs);
    let max_read = maxf!(read_1k_secs);
    let max_pack = results.iter().map(|r| r.pack_bytes).max().unwrap_or(1) as f64;
    let max_loose = maxu!(loose_objects_before_gc);
    let max_diff = maxf!(diff_secs);

    println!(
        "{}Write A — batch: {} new entries → 1 incremental tree (into {}-object base){}",
        BOLD, SAMPLE, n_base, RESET
    );
    println!(
        "  {}(only the touched shard subtrees are rebuilt; unchanged subtrees reused by OID){}",
        DIM, RESET
    );
    for r in results {
        let b = bar(r.write_batch_secs, max_write_batch, bar_w, YELLOW);
        println!(
            "  {:<18}  {}  {}{}{}",
            r.scheme.name(),
            b,
            YELLOW,
            fmt_ms(r.write_batch_secs),
            RESET
        );
    }

    println!(
        "\n{}Write B — sequential: 1 entry → incremental tree → commit × {} (chained parents){}",
        BOLD, SAMPLE, RESET
    );
    println!(
        "  {}(models frequent single-change serializes; total time for {} commits){}",
        DIM, SAMPLE, RESET
    );
    for r in results {
        let b = bar(r.write_seq_secs, max_write_seq, bar_w, YELLOW);
        let per_commit_us = (r.write_seq_secs * 1_000_000.0 / SAMPLE as f64) as u64;
        println!(
            "  {:<18}  {}  {}{}{}  {}({} µs/commit){}",
            r.scheme.name(),
            b,
            YELLOW,
            fmt_ms(r.write_seq_secs),
            RESET,
            DIM,
            per_commit_us,
            RESET
        );
    }

    println!("\n{}Read 1 000 objects by known SHA{}", BOLD, RESET);
    println!(
        "  {}(lower = faster; tree.get_path() walk per lookup){}",
        DIM, RESET
    );
    for r in results {
        let b = bar(r.read_1k_secs, max_read, bar_w, GREEN);
        println!(
            "  {:<18}  {}  {}{}{}",
            r.scheme.name(),
            b,
            GREEN,
            fmt_ms(r.read_1k_secs),
            RESET
        );
    }

    println!("\n{}Diff base tree vs base + 1 000 changes{}", BOLD, RESET);
    println!(
        "  {}(lower = faster; git diff-tree subprocess){}  [changed blobs: {}{}{}]",
        DIM,
        RESET,
        CYAN,
        results.first().map(|r| r.diff_count).unwrap_or(0),
        RESET
    );
    for r in results {
        let b = bar(r.diff_secs, max_diff, bar_w, CYAN);
        println!(
            "  {:<18}  {}  {}{}{}",
            r.scheme.name(),
            b,
            CYAN,
            fmt_ms(r.diff_secs),
            RESET
        );
    }

    println!(
        "\n{}Pack file size (after git gc --aggressive){}",
        BOLD, RESET
    );
    println!("  {}(lower = better compression){}", DIM, RESET);
    for r in results {
        let mb = r.pack_bytes as f64 / 1_048_576.0;
        let b = bar(r.pack_bytes as f64, max_pack, bar_w, MAGENTA);
        println!(
            "  {:<18}  {}  {}{:.2} MB{}",
            r.scheme.name(),
            b,
            MAGENTA,
            mb,
            RESET
        );
    }

    println!("\n{}Loose object count (before gc){}", BOLD, RESET);
    println!(
        "  {}(tree objects + blob objects written to ODB){}",
        DIM, RESET
    );
    for r in results {
        let b = bar(
            r.loose_objects_before_gc as f64,
            max_loose as f64,
            bar_w,
            RED,
        );
        println!(
            "  {:<18}  {}  {}{}{}",
            r.scheme.name(),
            b,
            RED,
            r.loose_objects_before_gc,
            RESET
        );
    }

    println!(
        "\n{}Per-object read latency (avg over 1 000){}",
        BOLD, RESET
    );
    for r in results {
        let per_obj = r.read_1k_secs / SAMPLE as f64;
        println!(
            "  {:<18}  {}{}{}",
            r.scheme.name(),
            BLUE,
            fmt_us(per_obj),
            RESET
        );
    }

    println!("\n{} ── Verdict ──{}  (bucket counts: ", BOLD, RESET);
    for r in results {
        print!(
            "  {}{}{} → {}{}{} buckets",
            DIM,
            r.scheme.name(),
            RESET,
            CYAN,
            r.scheme.bucket_count(),
            RESET
        );
    }
    println!(")\n");
    println!("{} ── Verdict ──{}", BOLD, RESET);
    let fastest_write = results
        .iter()
        .min_by(|a, b| {
            a.write_batch_secs
                .partial_cmp(&b.write_batch_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(&results[0]);
    let fastest_read = results
        .iter()
        .min_by(|a, b| {
            a.read_1k_secs
                .partial_cmp(&b.read_1k_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(&results[0]);
    let smallest_pack = results
        .iter()
        .min_by_key(|r| r.pack_bytes)
        .unwrap_or(&results[0]);
    let fastest_diff = results
        .iter()
        .min_by(|a, b| {
            a.diff_secs
                .partial_cmp(&b.diff_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(&results[0]);

    println!(
        "  fastest write  {}{}{}",
        GREEN,
        fastest_write.scheme.name(),
        RESET
    );
    println!(
        "  fastest read   {}{}{}",
        GREEN,
        fastest_read.scheme.name(),
        RESET
    );
    println!(
        "  fastest diff   {}{}{}",
        GREEN,
        fastest_diff.scheme.name(),
        RESET
    );
    println!(
        "  smallest pack  {}{}{}",
        GREEN,
        smallest_pack.scheme.name(),
        RESET
    );
}
pub fn run(n_objects: usize) -> Result<()> {
    let schemes = [Scheme::First2, Scheme::First3, Scheme::First2Next2];

    println!(
        "\n{}gmeta fanout benchmark{}  —  {}{} base objects{}, {} new per test",
        BOLD, RESET, CYAN, n_objects, RESET, SAMPLE
    );
    println!("{}running all three schemes in parallel{}\n", DIM, RESET);

    // Pre-generate all SHAs we'll ever need (base + SAMPLE extra for write test)
    let total_needed = n_objects + SAMPLE;
    print!(
        "{}generating {} synthetic SHAs...{} ",
        DIM, total_needed, RESET
    );
    let _ = std::io::stdout().flush();
    let t0 = Instant::now();
    let shas: Arc<Vec<String>> = Arc::new((0..total_needed as u64).map(fake_sha).collect());
    println!(
        "{}done ({}){}  ",
        GREEN,
        fmt_ms(t0.elapsed().as_secs_f64()),
        RESET
    );

    // Temp directory for all repos — use a unique subdir under system temp
    let tmp_path = std::env::temp_dir().join(format!(
        "gmeta-fanout-bench-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ));
    std::fs::create_dir_all(&tmp_path).context("failed to create temp dir")?;
    println!("{}repos in: {}{}{}", DIM, RESET, tmp_path.display(), RESET);

    println!(
        "\n{}  {:<20}  {:<12}  {:<14}  {:<14}  {:<12}  {:<10}{}",
        DIM, "scheme", "base build", "write batch", "write seq", "read 1k", "diff", RESET
    );
    println!("  {}", "─".repeat(98));

    // Print a live "running…" line per scheme before threads start
    for scheme in &schemes {
        println!(
            "  {}{:<20}{}  {}running…{}",
            BOLD,
            scheme.name(),
            RESET,
            DIM,
            RESET
        );
    }

    let wall_t0 = Instant::now();

    let handles: Vec<_> = schemes
        .iter()
        .map(|&scheme| {
            let shas = Arc::clone(&shas);
            let tmp = tmp_path.clone();
            std::thread::spawn(move || bench_scheme(scheme, n_objects, shas, &tmp))
        })
        .collect();

    // Collect results in original scheme order
    let mut results: Vec<SchemeResult> = Vec::with_capacity(schemes.len());
    let mut errors: Vec<String> = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Ok(r)) => results.push(r),
            Ok(Err(e)) => errors.push(format!("{:?}", e)),
            Err(_) => errors.push("thread panicked".to_string()),
        }
    }

    let wall_secs = wall_t0.elapsed().as_secs_f64();

    if !errors.is_empty() {
        for e in &errors {
            eprintln!("{}error:{} {}", RED, RESET, e);
        }
        anyhow::bail!("one or more scheme benchmarks failed");
    }

    // Sort results back into canonical order (First2, First3, First2Next2)
    results.sort_by_key(|r| match r.scheme {
        Scheme::First2 => 0,
        Scheme::First3 => 1,
        Scheme::First2Next2 => 2,
    });

    // Move cursor up past the "running…" lines we printed earlier, then
    // overwrite with actual results. Fall back to plain sequential output if
    // the terminal doesn't support ANSI cursor movement.
    let n_lines = schemes.len() as u16 + 1; // +1 for the separator row
                                            // Move cursor up n_lines
    print!("\x1b[{}A", n_lines);

    println!(
        "  {}{:<20}  {:<12}  {:<14}  {:<14}  {:<12}  {:<10}{}",
        DIM, "scheme", "base build", "write batch", "write seq", "read 1k", "diff", RESET
    );
    println!("  {}", "─".repeat(98));
    for r in &results {
        println!(
            "  {}{:<20}{}  {}{:<12}{}  {}{:<14}{}  {}{:<14}{}  {}{:<12}{}  {}{:<10}{}",
            BOLD,
            r.scheme.name(),
            RESET,
            DIM,
            fmt_ms(0.0),
            RESET, // base build not shown inline (parallel)
            YELLOW,
            fmt_ms(r.write_batch_secs),
            RESET,
            YELLOW,
            fmt_ms(r.write_seq_secs),
            RESET,
            GREEN,
            fmt_ms(r.read_1k_secs),
            RESET,
            CYAN,
            fmt_ms(r.diff_secs),
            RESET,
        );
    }

    println!(
        "\n{}wall time (all three parallel): {}{}{}",
        DIM,
        GREEN,
        fmt_ms(wall_secs),
        RESET
    );

    for r in &results {
        println!("\n{}── {} ──{}", DIM, r.scheme.name(), RESET);
        for line in &r.log {
            println!("{}", line);
        }
    }

    print_report(&results, n_objects);

    // Clean up temp repos
    let _ = std::fs::remove_dir_all(&tmp_path);
    println!(
        "\n{}note:{} temp repos removed ({})\n",
        DIM,
        RESET,
        tmp_path.display()
    );

    Ok(())
}
