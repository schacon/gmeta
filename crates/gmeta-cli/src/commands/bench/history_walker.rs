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
use git2::{Buf, Oid, Repository};
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
///   P(count ≤ 10)  ≈ 55 %
///   P(count ≤ 50)  ≈ 87 %
///   P(count ≤ 100) ≈ 96 %
fn sample_change_count(rng: &mut u64) -> usize {
    let raw = xorshift(rng);
    // Map to [0, 1)
    let u = (raw >> 11) as f64 / (1u64 << 53) as f64;
    // Concave power curve
    let count = (200.0 * u.powf(2.5)).ceil() as usize;
    count.clamp(1, 200)
}
#[derive(Default)]
struct Dir {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeMap<String, Dir>,
}

fn insert_path(dir: &mut Dir, parts: &[&str], content: Vec<u8>) {
    if parts.len() == 1 {
        dir.files.insert(parts[0].to_string(), content);
    } else {
        let child = dir.dirs.entry(parts[0].to_string()).or_default();
        insert_path(child, &parts[1..], content);
    }
}

fn build_dir(repo: &Repository, dir: &Dir) -> Result<Oid> {
    let mut tb = repo.treebuilder(None)?;
    for (name, content) in &dir.files {
        let blob_oid = repo.blob(content)?;
        tb.insert(name, blob_oid, 0o100644)?;
    }
    for (name, child) in &dir.dirs {
        let child_oid = build_dir(repo, child)?;
        tb.insert(name, child_oid, 0o040000)?;
    }
    Ok(tb.write()?)
}

/// Build a full tree from a flat path→content map.
fn build_full_tree(repo: &Repository, files: &BTreeMap<String, Vec<u8>>) -> Result<Oid> {
    let mut root = Dir::default();
    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }
    build_dir(repo, &root)
}

/// Incremental tree update: reuse unchanged subtrees by OID, rewrite only the
/// spine of paths that changed (same algorithm as fanout_bench).
fn build_tree_incremental(
    repo: &Repository,
    base_oid: Oid,
    changed: &[(&str, Vec<u8>)],
) -> Result<Oid> {
    let mut by_top: BTreeMap<&str, Vec<(Vec<&str>, Vec<u8>)>> = BTreeMap::new();
    let mut root_files: Vec<(&str, Vec<u8>)> = Vec::new();

    for (path, content) in changed {
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() == 1 {
            root_files.push((parts[0], content.clone()));
        } else {
            by_top
                .entry(parts[0])
                .or_default()
                .push((parts[1..].to_vec(), content.clone()));
        }
    }

    let base_tree = repo.find_tree(base_oid)?;
    let mut root_tb = repo.treebuilder(Some(&base_tree))?;

    for (name, content) in root_files {
        let blob_oid = repo.blob(content.as_slice())?;
        root_tb.insert(name, blob_oid, 0o100644)?;
    }

    for (top, children) in &by_top {
        let existing_sub: Option<Oid> = base_tree.get_name(top).and_then(|e| {
            if e.kind() == Some(git2::ObjectType::Tree) {
                Some(e.id())
            } else {
                None
            }
        });
        let new_sub = apply_incremental_subtree(repo, existing_sub, children)?;
        root_tb.insert(top, new_sub, 0o040000)?;
    }

    Ok(root_tb.write()?)
}

fn apply_incremental_subtree(
    repo: &Repository,
    base_oid: Option<Oid>,
    entries: &[(Vec<&str>, Vec<u8>)],
) -> Result<Oid> {
    let mut by_next: BTreeMap<&str, Vec<(Vec<&str>, Vec<u8>)>> = BTreeMap::new();
    let mut leaf_files: Vec<(&str, Vec<u8>)> = Vec::new();

    for (parts, content) in entries {
        if parts.len() == 1 {
            leaf_files.push((parts[0], content.clone()));
        } else {
            by_next
                .entry(parts[0])
                .or_default()
                .push((parts[1..].to_vec(), content.clone()));
        }
    }

    let base_tree_opt = base_oid.and_then(|oid| repo.find_tree(oid).ok());
    let mut tb = match &base_tree_opt {
        Some(t) => repo.treebuilder(Some(t))?,
        None => repo.treebuilder(None)?,
    };

    for (name, content) in leaf_files {
        let blob_oid = repo.blob(content.as_slice())?;
        tb.insert(name, blob_oid, 0o100644)?;
    }

    for (next, children) in &by_next {
        let existing = base_tree_opt
            .as_ref()
            .and_then(|t| t.get_name(next))
            .and_then(|e| {
                if e.kind() == Some(git2::ObjectType::Tree) {
                    Some(e.id())
                } else {
                    None
                }
            });
        let child_oid = apply_incremental_subtree(repo, existing, children)?;
        tb.insert(next, child_oid, 0o040000)?;
    }

    Ok(tb.write()?)
}

/// Tree path for a commit-SHA target's metadata blob.
/// Layout mirrors the real gmeta serialization: commit/{first2}/{sha}/bench/__value
/// The commit SHA is encoded in the path; the blob holds arbitrary metadata.
fn value_path(commit_sha: &str) -> String {
    format!("commit/{}/{}/bench/__value", &commit_sha[..2], commit_sha)
}

/// Write a commit object (no ref update — caller tracks the OID).
fn write_commit(
    repo: &Repository,
    tree_oid: Oid,
    parent_oid: Option<Oid>,
    msg: &str,
) -> Result<Oid> {
    let tree = repo.find_tree(tree_oid)?;
    let sig = git2::Signature::new("bench", "bench@bench", &git2::Time::new(0, 0))?;
    let parents: Vec<git2::Commit> = parent_oid
        .map(|oid| repo.find_commit(oid))
        .transpose()?
        .into_iter()
        .collect();
    let parent_refs: Vec<&git2::Commit> = parents.iter().collect();
    Ok(repo.commit(None, &sig, &sig, msg, &tree, &parent_refs)?)
}
struct GenerationStats {
    n_normal_commits: usize,
    n_prune_commits: usize,
    /// Total distinct SHAs ever written (the ground truth the walker must recover)
    total_shas_written: usize,
    /// OIDs of every commit in order (index 0 = root, last = tip)
    commit_chain: Vec<Oid>,
    /// How many values were live at tip
    live_count_at_tip: usize,
    elapsed_secs: f64,
}

fn generate_history(repo: &Repository, n_commits: usize, rng: &mut u64) -> Result<GenerationStats> {
    // Each entry represents a commit SHA (the target).  The path encodes the
    // commit SHA; the blob content is a metadata string (not another SHA).
    // "Modify" updates the blob content while the path (and therefore the commit
    // SHA) stays the same.
    //
    // live_paths: ordered Vec of active tree paths — O(1) random index access.
    // live_meta:  path → current metadata string (for incremental tree writes).
    // insertion_serial: path → serial, for prune eviction ordering.
    let mut live_paths: Vec<String> = Vec::new();
    let mut live_meta: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut insertion_serial: BTreeMap<String, u64> = BTreeMap::new();
    let mut serial: u64 = 0;

    // Ground truth: all commit SHAs that were *introduced* (path added).
    // Modifications don't add a new commit SHA — they update existing metadata.
    let mut all_commit_shas: BTreeSet<String> = BTreeSet::new();

    // Counter for generating unique commit SHAs (used as tree path keys).
    let mut sha_counter: u64 = 0;

    // Simple metadata value generator — just a small counter string so blob
    // content changes on modify without burning another fake commit SHA.
    let mut meta_counter: u64 = 0;
    let next_meta = |c: &mut u64| -> Vec<u8> {
        *c += 1;
        format!("meta-value-{}", c).into_bytes()
    };

    let mut commit_chain: Vec<Oid> = Vec::with_capacity(n_commits);
    let mut parent_oid: Option<Oid> = None;
    let mut current_tree_oid: Option<Oid> = None;
    let mut n_normal = 0usize;
    let mut n_prune = 0usize;

    let t0 = Instant::now();
    let progress_interval = (n_commits / 40).max(1); // ~40 ticks across the run

    for commit_idx in 0..n_commits {
        let is_prune = live_paths.len() >= PRUNE_THRESHOLD;

        let tree_oid = if is_prune {
            // Keep PRUNE_KEEP newest entries by insertion serial.
            // Sort live paths by serial, evict the oldest.
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
            // Rebuild live_paths without evicted entries (swap-remove safe here
            // since order within live_paths doesn't matter for correctness).
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
                // The path (commit SHA) is unchanged; only blob content changes.
                let do_modify = !live_paths.is_empty() && (xorshift(rng) % 100 < 5);

                if do_modify {
                    // O(1) random pick via index into Vec
                    let idx = (xorshift(rng) as usize) % live_paths.len();
                    let path = live_paths[idx].clone();
                    let new_content = next_meta(&mut meta_counter);
                    live_meta.insert(path.clone(), new_content.clone());
                    // Insertion serial unchanged — this target isn't "newer".
                    changed.push((path, new_content));
                } else {
                    // Introduce a new commit SHA target with initial metadata.
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
/// SHA encoded in the path of every *Added* blob (new targets only — Modified
/// means metadata update on an already-known target, Deleted means a prune
/// removed an entry we already counted).  Prune commits are skipped entirely
/// since they introduce no new commit SHA targets.
fn walk_history(repo: &Repository, tip_oid: Oid) -> Result<WalkStats> {
    let mut recovered: BTreeSet<String> = BTreeSet::new();
    let mut commits_visited = 0usize;
    let mut prune_commits = 0usize;

    let t0 = Instant::now();

    let mut current_oid = tip_oid;
    loop {
        let commit = repo.find_commit(current_oid)?;
        commits_visited += 1;

        let is_prune = commit.message().unwrap_or("").contains("pruned: true");
        if is_prune {
            prune_commits += 1;
            // Prune commits only remove entries from the tree; they introduce
            // no new commit SHA targets.  Skip the diff entirely.
        } else {
            let current_tree = commit.tree()?;
            let parent_tree: Option<git2::Tree> = commit.parent(0).ok().and_then(|p| p.tree().ok());

            let mut diff_opts = git2::DiffOptions::new();
            let diff = repo.diff_tree_to_tree(
                parent_tree.as_ref(),
                Some(&current_tree),
                Some(&mut diff_opts),
            )?;

            // Only Added deltas represent newly introduced commit SHA targets.
            // The commit SHA lives in the path: commit/{first2}/{sha}/k/bench/__value
            // Extract it as the third path component (index 2).
            diff.foreach(
                &mut |delta, _progress| {
                    if delta.status() == git2::Delta::Added {
                        if let Some(path) = delta.new_file().path() {
                            let parts: Vec<&str> = path.to_str().unwrap_or("").split('/').collect();
                            // path layout: commit / {first2} / {full_sha} / k / bench / __value
                            if parts.len() >= 3 && parts[0] == "commit" {
                                let sha = parts[2].to_string();
                                if sha.len() == 40 {
                                    recovered.insert(sha);
                                }
                            }
                        }
                    }
                    true
                },
                None,
                None,
                None,
            )?;
        }

        match commit.parent_id(0) {
            Ok(parent_oid) => current_oid = parent_oid,
            Err(_) => break,
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

    // Temp bare repo with an in-memory ODB backend.
    //
    // By default libgit2 writes every blob and tree as a loose object file
    // (open + zlib-compress + fdatasync + close), which costs ~1-5 ms per
    // object on macOS.  With hundreds of objects per commit that dominates the
    // benchmark completely.
    //
    // Adding a mempack backend at priority 1000 (above the default loose=1 and
    // pack=2 backends) intercepts all writes and keeps them in memory.  The
    // loose backend is still registered but never reached for writes.  All
    // reads also hit mempack first, so tree lookups stay fast.
    let tmp_path: PathBuf = std::env::temp_dir().join(format!(
        "gmeta-history-walker-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ));
    std::fs::create_dir_all(&tmp_path)?;
    let repo = Repository::init_bare(&tmp_path)?;
    let odb = repo.odb()?;
    let mempack = odb.add_new_mempack_backend(1000)?;
    println!(
        "{}repo: {} (mempack ODB — objects in memory){}",
        DIM,
        tmp_path.display(),
        RESET
    );

    println!("\n{}generating {} commits…{}", BOLD, n_commits, RESET);

    let mut rng: u64 = 0xdeadbeef_cafebabe;
    let gen = generate_history(&repo, n_commits, &mut rng)?;

    let tip_oid = *gen.commit_chain.last().unwrap();

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

    print!("\n{}flushing mempack to packfile…{}", BOLD, RESET);
    let _ = std::io::stdout().flush();
    let t_pack = Instant::now();

    let mut pack_buf = Buf::new();
    mempack.dump(&repo, &mut pack_buf)?;
    let pack_bytes = pack_buf.len();
    let mut packwriter = odb.packwriter()?;
    std::io::Write::write_all(&mut packwriter, &pack_buf)?;
    packwriter.commit()?;
    // Now that objects are in a real packfile, mempack is no longer needed.
    mempack.reset()?;

    // Point refs/meta/local at the tip commit and make HEAD a symbolic ref to it.
    repo.reference(
        "refs/meta/local/main",
        tip_oid,
        true,
        "history-walker: generation complete",
    )?;
    repo.set_head("refs/meta/local/main")?;

    let pack_secs = t_pack.elapsed().as_secs_f64();
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
    // Modifications update blob content on an existing path — the commit SHA
    // (encoded in the path) is unchanged and was already counted when first
    // introduced.  The walk collects only Added deltas, so recovered should
    // exactly equal the number of distinct commit SHAs introduced.
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
        "  {}pack flush{}   {} total  {}({:.2} MB){}",
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
