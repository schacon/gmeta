//! History-walker benchmark.
//!
//! Synthesises a sequence of N meta commits on a temp bare repo, each of which
//! introduces or modifies metadata on a set of tracked commit SHAs, then times
//! how long it takes to walk from the tip back to the root and reconstruct the
//! full set of commit SHAs that ever had metadata written against them.
//!
//! ## Data model
//!
//!   Each entry in the tree represents a *commit SHA* (the target) with a
//!   metadata value stored at its path.  The commit SHA is the key — it does not
//!   change.  "Modify" means updating the metadata *value* stored for an existing
//!   commit SHA, not replacing the commit SHA itself.
//!
//! ## Commit generation rules
//!
//!   - Each commit changes between 1 and 200 entries, weighted toward the low end
//!     (exponential-ish: ~50 % of commits touch ≤10 entries).
//!   - 95 % of changes introduce a new commit SHA target with a metadata value.
//!   - 5 % modify the metadata value on an existing commit SHA target (the path
//!     stays the same; only the blob content changes).
//!   - Whenever the live tree would exceed 5 000 total entries a *prune commit* is
//!     written instead: it keeps only the 500 newest entries and records
//!     `pruned: true` in the commit message (mirroring the format in prune.md).
//!
//! ## Walk phase
//!
//!   Starting from the tip commit, walk the parent chain.  At each non-prune
//!   commit, diff the tree against its parent to find added blobs (new commit SHA
//!   targets), read the path to extract the commit SHA, and accumulate into a set.
//!   Prune commits are detected via their commit message and skipped (they add no
//!   new targets).  Stop when the root is reached.  The final set should equal the
//!   full collection of commit SHAs introduced during generation.
//!
//! Usage:  gmeta history-walker [--commits N]   (default N = 500)

use anyhow::Result;
use gix::prelude::ObjectIdExt;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const MAGENTA: &str = "\x1b[35m";
const RED: &str = "\x1b[31m";

const PRUNE_THRESHOLD: usize = 5_000;
const PRUNE_KEEP: usize = 500;
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Fake 40-char hex SHA deterministically from an integer (same as fanout_bench).
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

/// Sample a change-count in [1, 200], heavily weighted toward the low end.
/// Strategy: draw a uniform value in [0,1) and map it through a concave curve
/// so small counts are much more probable.
///
///   count = ceil(200 * u^2.5)   where u ~ Uniform(0,1)
///
/// This gives roughly:
///   P(count <= 10)  ~= 55 %
///   P(count <= 50)  ~= 87 %
///   P(count <= 100) ~= 96 %
fn sample_change_count(rng: &mut u64) -> usize {
    let raw = xorshift(rng);
    let u = (raw >> 11) as f64 / (1u64 << 53) as f64;
    let count = (200.0 * u.powf(2.5)).ceil() as usize;
    count.clamp(1, 200)
}

/// Build a full tree from a flat path-to-content map using the gix tree editor.
fn build_full_tree(
    repo: &gix::Repository,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<gix::ObjectId> {
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
fn build_tree_incremental(
    repo: &gix::Repository,
    base_oid: gix::ObjectId,
    changed: &[(&str, Vec<u8>)],
) -> Result<gix::ObjectId> {
    let base_tree = base_oid.attach(repo).object()?.into_tree();
    let mut editor = base_tree.edit()?;

    for (path, content) in changed {
        let blob_id = repo.write_blob(content)?.detach();
        editor.upsert(*path, gix::objs::tree::EntryKind::Blob, blob_id)?;
    }

    Ok(editor.write()?.detach())
}

/// Tree path for a commit-SHA target's metadata blob.
/// Layout mirrors the real gmeta serialization: commit/{first2}/{sha}/bench/__value
/// The commit SHA is encoded in the path; the blob holds arbitrary metadata.
fn value_path(commit_sha: &str) -> String {
    format!("commit/{}/{}/bench/__value", &commit_sha[..2], commit_sha)
}

/// Write a commit object (no ref update -- caller tracks the OID).
///
/// Uses `gix::objs::Commit` and `repo.write_object()`.
fn write_commit(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    parent_oid: Option<gix::ObjectId>,
    msg: &str,
) -> Result<gix::ObjectId> {
    let sig = gix::actor::Signature {
        name: "bench".into(),
        email: "bench@bench".into(),
        time: gix::date::Time::new(0, 0),
    };
    let parents: Vec<gix::ObjectId> = parent_oid.into_iter().collect();
    let commit = gix::objs::Commit {
        message: msg.into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: parents.into(),
        extra_headers: Default::default(),
    };
    Ok(repo.write_object(&commit)?.detach())
}
struct GenerationStats {
    n_normal_commits: usize,
    n_prune_commits: usize,
    /// Total distinct SHAs ever written (the ground truth the walker must recover)
    total_shas_written: usize,
    /// OIDs of every commit in order (index 0 = root, last = tip)
    commit_chain: Vec<gix::ObjectId>,
    /// How many values were live at tip
    live_count_at_tip: usize,
    elapsed_secs: f64,
}

fn generate_history(
    repo: &gix::Repository,
    n_commits: usize,
    rng: &mut u64,
) -> Result<GenerationStats> {
    // Each entry represents a commit SHA (the target).  The path encodes the
    // commit SHA; the blob content is a metadata string (not another SHA).
    // "Modify" updates the blob content while the path (and therefore the commit
    // SHA) stays the same.
    //
    // live_paths: ordered Vec of active tree paths -- O(1) random index access.
    // live_meta:  path -> current metadata string (for incremental tree writes).
    // insertion_serial: path -> serial, for prune eviction ordering.
    let mut live_paths: Vec<String> = Vec::new();
    let mut live_meta: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut insertion_serial: BTreeMap<String, u64> = BTreeMap::new();
    let mut serial: u64 = 0;

    // Ground truth: all commit SHAs that were *introduced* (path added).
    // Modifications don't add a new commit SHA -- they update existing metadata.
    let mut all_commit_shas: BTreeSet<String> = BTreeSet::new();

    // Counter for generating unique commit SHAs (used as tree path keys).
    let mut sha_counter: u64 = 0;

    // Simple metadata value generator -- just a small counter string so blob
    // content changes on modify without burning another fake commit SHA.
    let mut meta_counter: u64 = 0;
    let next_meta = |c: &mut u64| -> Vec<u8> {
        *c += 1;
        format!("meta-value-{}", c).into_bytes()
    };

    let mut commit_chain: Vec<gix::ObjectId> = Vec::with_capacity(n_commits);
    let mut parent_oid: Option<gix::ObjectId> = None;
    let mut current_tree_oid: Option<gix::ObjectId> = None;
    let mut n_normal = 0usize;
    let mut n_prune = 0usize;

    let t0 = Instant::now();
    let progress_interval = (n_commits / 40).max(1); // ~40 ticks across the run

    for commit_idx in 0..n_commits {
        let is_prune = live_paths.len() >= PRUNE_THRESHOLD;

        let tree_oid = if is_prune {
            // Keep PRUNE_KEEP newest entries by insertion serial.
            let mut by_age: Vec<(u64, &str)> = live_paths
                .iter()
                .map(|p| (insertion_serial[p.as_str()], p.as_str()))
                .collect();
            by_age.sort_unstable_by_key(|(s, _)| *s);

            let to_drop = by_age.len().saturating_sub(PRUNE_KEEP);
            let evicted: Vec<String> = by_age[..to_drop]
                .iter()
                .map(|(_, p)| p.to_string())
                .collect();
            for path in &evicted {
                live_meta.remove(path);
                insertion_serial.remove(path);
            }
            live_paths.retain(|p| !evicted.contains(p));

            // Rebuild full tree from surviving entries.
            let files: BTreeMap<String, Vec<u8>> = live_meta.clone();
            let oid = build_full_tree(repo, &files)?;
            n_prune += 1;
            oid
        } else {
            let n_changes = sample_change_count(rng);
            let mut changed: Vec<(String, Vec<u8>)> = Vec::with_capacity(n_changes);

            for _ in 0..n_changes {
                // 5 % chance: modify metadata on an existing commit SHA target.
                let do_modify = !live_paths.is_empty() && (xorshift(rng) % 100 < 5);

                if do_modify {
                    let idx = (xorshift(rng) as usize) % live_paths.len();
                    let path = live_paths[idx].clone();
                    let new_content = next_meta(&mut meta_counter);
                    live_meta.insert(path.clone(), new_content.clone());
                    changed.push((path, new_content));
                } else {
                    sha_counter += 1;
                    let commit_sha = fake_sha(sha_counter);
                    let path = value_path(&commit_sha);
                    let content = next_meta(&mut meta_counter);
                    all_commit_shas.insert(commit_sha);
                    live_paths.push(path.clone());
                    live_meta.insert(path.clone(), content.clone());
                    insertion_serial.insert(path.clone(), serial);
                    serial += 1;
                    changed.push((path, content));
                }
            }

            let refs: Vec<(&str, Vec<u8>)> = changed
                .iter()
                .map(|(p, c)| (p.as_str(), c.clone()))
                .collect();

            let oid = if let Some(base) = current_tree_oid {
                build_tree_incremental(repo, base, &refs)?
            } else {
                let files: BTreeMap<String, Vec<u8>> = changed.into_iter().collect();
                build_full_tree(repo, &files)?
            };
            n_normal += 1;
            oid
        };

        current_tree_oid = Some(tree_oid);

        let kind = if is_prune { "prune" } else { "serialize" };
        let msg = if is_prune {
            format!(
                "gmeta: prune\n\npruned: true\nkept: {}\nevicted: {}",
                live_paths.len(),
                PRUNE_THRESHOLD - PRUNE_KEEP,
            )
        } else {
            format!("gmeta: serialize (commit {})", commit_idx + 1)
        };

        let commit_oid = write_commit(repo, tree_oid, parent_oid, &msg)?;
        commit_chain.push(commit_oid);
        parent_oid = Some(commit_oid);

        // Per-commit progress line
        if (commit_idx + 1) % progress_interval == 0 || commit_idx + 1 == n_commits {
            let elapsed = t0.elapsed().as_secs_f64();
            let pct = (commit_idx + 1) * 100 / n_commits;
            print!(
                "\r  {}{:>3}%{}  commit {}/{}  live={}  total_shas={}  {}{:.1}s{}  {}",
                CYAN,
                pct,
                RESET,
                commit_idx + 1,
                n_commits,
                live_paths.len(),
                all_commit_shas.len(),
                DIM,
                elapsed,
                RESET,
                kind,
            );
            let _ = std::io::stdout().flush();
        }
    }
    println!(); // newline after progress

    let elapsed = t0.elapsed().as_secs_f64();

    Ok(GenerationStats {
        n_normal_commits: n_normal,
        n_prune_commits: n_prune,
        total_shas_written: all_commit_shas.len(),
        commit_chain,
        live_count_at_tip: live_paths.len(),
        elapsed_secs: elapsed,
    })
}
struct WalkStats {
    commits_visited: usize,
    shas_recovered: usize,
    prune_commits_encountered: usize,
    elapsed_secs: f64,
}

/// Walk from `tip_oid` back to the root.
///
/// At each *non-prune* commit, diff against the parent and collect the commit
/// SHA encoded in the path of every *Added* blob (new targets only -- Modified
/// means metadata update on an already-known target, Deleted means a prune
/// removed an entry we already counted).  Prune commits are skipped entirely
/// since they introduce no new commit SHA targets.
///
/// Uses `git diff-tree --no-commit-id -r --name-status` subprocess for diffing,
/// matching the pattern used in show.rs.
fn walk_history(repo: &gix::Repository, tip_oid: gix::ObjectId) -> Result<WalkStats> {
    use gix::bstr::ByteSlice;

    let mut recovered: BTreeSet<String> = BTreeSet::new();
    let mut commits_visited = 0usize;
    let mut prune_commits = 0usize;

    let t0 = Instant::now();
    let git_dir = repo.path();

    let mut current_oid = tip_oid;
    loop {
        let commit_obj = current_oid.attach(repo).object()?.into_commit();
        let decoded = commit_obj.decode()?;
        commits_visited += 1;

        let message = decoded.message.to_str_lossy();
        let is_prune = message.contains("pruned: true");
        if is_prune {
            prune_commits += 1;
        } else {
            // Use git diff-tree subprocess to find added files
            let output = std::process::Command::new("git")
                .args(["--git-dir", &git_dir.to_string_lossy()])
                .args([
                    "diff-tree",
                    "--no-commit-id",
                    "-r",
                    "--name-status",
                    &current_oid.to_string(),
                ])
                .output()?;

            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if line.is_empty() {
                        continue;
                    }
                    let parts_line: Vec<&str> = line.splitn(2, '\t').collect();
                    if parts_line.len() != 2 {
                        continue;
                    }
                    // Only Added deltas represent newly introduced commit SHA targets.
                    if parts_line[0] != "A" {
                        continue;
                    }
                    let path = parts_line[1];
                    let path_parts: Vec<&str> = path.split('/').collect();
                    // path layout: commit / {first2} / {full_sha} / bench / __value
                    if path_parts.len() >= 3 && path_parts[0] == "commit" {
                        let sha = path_parts[2].to_string();
                        if sha.len() == 40 {
                            recovered.insert(sha);
                        }
                    }
                }
            }
        }

        // Walk to parent
        let parent_ids: Vec<gix::ObjectId> = decoded.parents().collect();
        if let Some(parent_id) = parent_ids.first() {
            current_oid = *parent_id;
        } else {
            break;
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();

    Ok(WalkStats {
        commits_visited,
        shas_recovered: recovered.len(),
        prune_commits_encountered: prune_commits,
        elapsed_secs: elapsed,
    })
}
fn fmt_ms(secs: f64) -> String {
    format!("{:.1} ms", secs * 1000.0)
}

fn fmt_us(secs: f64) -> String {
    format!("{:.2} µs", secs * 1_000_000.0)
}
pub fn run(n_commits: usize) -> Result<()> {
    println!(
        "\n{}gmeta history-walker benchmark{}  —  {}{} commits to generate{}",
        BOLD, RESET, CYAN, n_commits, RESET,
    );
    println!(
        "{}rules: 95% introduce / 5% modify, 1–200 values/commit (low-weighted), prune at >{} values (keep {}){}\n",
        DIM, PRUNE_THRESHOLD, PRUNE_KEEP, RESET,
    );

    // Temp bare repo — gix uses on-disk ODB (no mempack equivalent).
    let tmp_path: PathBuf = std::env::temp_dir().join(format!(
        "gmeta-history-walker-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ));
    std::fs::create_dir_all(&tmp_path)?;
    let repo = gix::init_bare(&tmp_path)?;
    println!("{}repo: {} (on-disk ODB){}", DIM, tmp_path.display(), RESET);

    println!("\n{}generating {} commits…{}", BOLD, n_commits, RESET);

    let mut rng: u64 = 0xdeadbeef_cafebabe;
    let gen = generate_history(&repo, n_commits, &mut rng)?;

    let tip_oid = *gen
        .commit_chain
        .last()
        .ok_or_else(|| anyhow::anyhow!("commit chain is empty after generation"))?;

    println!(
        "\n{}generation complete{} in {}{}{}",
        BOLD,
        RESET,
        GREEN,
        fmt_ms(gen.elapsed_secs),
        RESET
    );
    println!(
        "  {}normal commits{}  {}{}{}",
        DIM, RESET, YELLOW, gen.n_normal_commits, RESET
    );
    println!(
        "  {}prune commits{}   {}{}{}",
        DIM, RESET, MAGENTA, gen.n_prune_commits, RESET
    );
    println!(
        "  {}unique SHAs{}     {}{}{}  (ground truth)",
        DIM, RESET, CYAN, gen.total_shas_written, RESET
    );
    println!(
        "  {}live at tip{}     {}{}{}",
        DIM, RESET, DIM, gen.live_count_at_tip, RESET
    );
    println!(
        "  {}tip commit{}      {}{}{}",
        DIM,
        RESET,
        DIM,
        &tip_oid.to_string()[..12],
        RESET
    );

    // Run git gc to pack loose objects (replaces the mempack flush).
    print!("\n{}packing objects (git gc)…{}", BOLD, RESET);
    let _ = std::io::stdout().flush();
    let t_pack = Instant::now();

    let gc_status = std::process::Command::new("git")
        .args(["-C", &tmp_path.to_string_lossy()])
        .args(["gc", "--quiet"])
        .status();

    let pack_secs = t_pack.elapsed().as_secs_f64();

    // Measure pack size
    let pack_dir = tmp_path.join("objects").join("pack");
    let mut pack_bytes: u64 = 0;
    if pack_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&pack_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map(|e| e == "pack").unwrap_or(false) {
                    pack_bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }

    if gc_status.is_ok() {
        println!(
            " {}done{}  {}{:.2} MB{} in {}{}{}",
            BOLD,
            RESET,
            MAGENTA,
            pack_bytes as f64 / 1_048_576.0,
            RESET,
            GREEN,
            fmt_ms(pack_secs),
            RESET,
        );
    } else {
        println!(" {}skipped{} (git gc not available)", DIM, RESET);
    }

    // Point refs/meta/local at the tip commit.
    repo.reference(
        "refs/meta/local/main",
        tip_oid,
        gix::refs::transaction::PreviousValue::Any,
        "history-walker: generation complete",
    )?;

    print!("\n{}walking history from tip…{}", BOLD, RESET);
    let _ = std::io::stdout().flush();

    let walk = walk_history(&repo, tip_oid)?;

    println!(
        " {}done{}  in {}{}{}",
        BOLD,
        RESET,
        GREEN,
        fmt_ms(walk.elapsed_secs),
        RESET
    );
    println!("\n{}walk results{}", BOLD, RESET);
    println!(
        "  {}commits visited{}        {}{}{}",
        DIM, RESET, CYAN, walk.commits_visited, RESET
    );
    println!(
        "  {}prune commits seen{}     {}{}{}",
        DIM, RESET, MAGENTA, walk.prune_commits_encountered, RESET
    );
    println!(
        "  {}SHAs recovered{}         {}{}{}",
        DIM, RESET, YELLOW, walk.shas_recovered, RESET
    );

    // Correctness check.
    let expected = gen.total_shas_written;
    let recovered = walk.shas_recovered;
    let match_str = if recovered == expected {
        format!("{}✓ exact match ({} SHAs){}", GREEN, recovered, RESET)
    } else {
        let diff = recovered as isize - expected as isize;
        format!(
            "{}✗ mismatch: expected {} got {} ({:+}) — check walk logic{}",
            RED, expected, recovered, diff, RESET
        )
    };
    println!("  {}correctness{}            {}", DIM, RESET, match_str);

    println!("\n{}timing summary{}", BOLD, RESET);
    let gen_per_commit = gen.elapsed_secs / gen.commit_chain.len() as f64;
    let walk_per_commit = walk.elapsed_secs / walk.commits_visited.max(1) as f64;
    println!(
        "  {}generation{}   {} total  {}({} / commit){}",
        DIM,
        RESET,
        fmt_ms(gen.elapsed_secs),
        DIM,
        fmt_us(gen_per_commit),
        RESET,
    );
    println!(
        "  {}pack{}          {} total  {}({:.2} MB){}",
        DIM,
        RESET,
        fmt_ms(pack_secs),
        DIM,
        pack_bytes as f64 / 1_048_576.0,
        RESET,
    );
    println!(
        "  {}walk{}          {} total  {}({} / commit){}",
        DIM,
        RESET,
        fmt_ms(walk.elapsed_secs),
        DIM,
        fmt_us(walk_per_commit),
        RESET,
    );
    let total = gen.elapsed_secs + pack_secs + walk.elapsed_secs;
    println!(
        "  {}total{}         {}{}{}",
        DIM,
        RESET,
        GREEN,
        fmt_ms(total),
        RESET
    );

    println!(
        "\n{}note:{} temp repo kept at {}{}{}\n",
        DIM,
        RESET,
        CYAN,
        tmp_path.display(),
        RESET,
    );

    Ok(())
}
