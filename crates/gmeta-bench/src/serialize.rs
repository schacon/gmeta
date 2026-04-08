//! Serialize benchmark.
//!
//! Creates a temporary bare git repo with a fresh gmeta SQLite database,
//! then runs N rounds of: insert 10–1000 random keys -> serialize to git ref.
//! Reports per-round and aggregate timing, plus git ODB statistics (loose
//! objects, blob/tree counts, total size).
//!
//! Usage:  gmeta serialize-bench [--rounds N]   (default N = 10)

use anyhow::{Context, Result};
use gix::prelude::ObjectIdExt;
use std::io::Write;
use std::time::Instant;

use gmeta_core::db::Store;
use gmeta_core::tree_paths;
use gmeta_core::types::{Target, TargetType, ValueType};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const BLUE: &str = "\x1b[34m";
const MAGENTA: &str = "\x1b[35m";

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    /// Uniform in [lo, hi] inclusive.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

fn fake_sha(rng: &mut Rng) -> String {
    let a = rng.next();
    let b = rng.next();
    let c = rng.next();
    format!("{:016x}{:016x}{:08x}", a, b, c as u32)
}

fn fake_value(rng: &mut Rng, min_len: usize, max_len: usize) -> String {
    let len = rng.range(min_len as u64, max_len as u64) as usize;
    (0..len)
        .map(|_| {
            let idx = rng.range(0, 61) as u8;
            (match idx {
                0..=25 => b'a' + idx,
                26..=51 => b'A' + (idx - 26),
                _ => b'0' + (idx - 52),
            }) as char
        })
        .collect()
}
struct RoundStats {
    round: usize,
    keys_inserted: usize,
    insert_secs: f64,
    serialize_secs: f64,
    cumulative_keys: usize,
}
struct OdbStats {
    loose_blobs: usize,
    loose_trees: usize,
    loose_commits: usize,
    loose_other: usize,
    loose_total_bytes: u64,
}

/// Count ODB statistics by walking the loose object directory and classifying
/// each object using gix.
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn count_odb_stats(repo_path: &std::path::Path) -> Result<OdbStats> {
    let obj_dir = repo_path.join("objects");
    let mut blobs = 0usize;
    let mut trees = 0usize;
    let mut commits = 0usize;
    let mut other = 0usize;
    let mut total_bytes = 0u64;

    let repo = gix::open(repo_path)?;

    for entry in std::fs::read_dir(&obj_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s.len() == 2 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            for sub in std::fs::read_dir(entry.path())? {
                let sub = sub?;
                total_bytes += sub.metadata()?.len();
                let sub_name = sub.file_name();
                let full_hex = format!("{}{}", s, sub_name.to_string_lossy());
                if let Ok(oid) = gix::ObjectId::from_hex(full_hex.as_bytes()) {
                    if let Ok(obj) = oid.attach(&repo).object() {
                        match obj.kind {
                            gix::object::Kind::Blob => blobs += 1,
                            gix::object::Kind::Tree => trees += 1,
                            gix::object::Kind::Commit => commits += 1,
                            _ => other += 1,
                        }
                    } else {
                        other += 1;
                    }
                } else {
                    other += 1;
                }
            }
        }
    }

    Ok(OdbStats {
        loose_blobs: blobs,
        loose_trees: trees,
        loose_commits: commits,
        loose_other: other,
        loose_total_bytes: total_bytes,
    })
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn fmt_ms(secs: f64) -> String {
    if secs < 0.001 {
        format!("{:.1} µs", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.1} ms", secs * 1000.0)
    } else {
        format!("{:.2} s", secs)
    }
}

/// Serialize all metadata from the database into a git commit on the given ref.
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn do_serialize(repo: &gix::Repository, db: &Store, ref_name: &str) -> Result<()> {
    let metadata_entries = db.get_all_metadata()?;

    if metadata_entries.is_empty() {
        return Ok(());
    }

    let tree_oid = build_bench_tree(repo, &metadata_entries)?;

    let sig = gix::actor::Signature {
        name: "bench".into(),
        email: "bench@bench".into(),
        time: gix::date::Time::new(0, 0),
    };

    let parent = repo
        .find_reference(ref_name)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(gix::Id::detach);
    let parents: Vec<gix::ObjectId> = parent.into_iter().collect();

    let commit = gix::objs::Commit {
        message: "bench serialize".into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: parents.into(),
        extra_headers: Default::default(),
    };

    let commit_oid = repo.write_object(&commit)?.detach();
    repo.reference(
        ref_name,
        commit_oid,
        gix::refs::transaction::PreviousValue::Any,
        "bench serialize",
    )?;

    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
    db.set_last_materialized(now)?;

    Ok(())
}

/// Simplified tree builder for the benchmark -- handles string values only
/// (which is all we insert in this bench).
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn build_bench_tree(
    repo: &gix::Repository,
    metadata_entries: &[gmeta_core::db::types::SerializableEntry],
) -> Result<gix::ObjectId> {
    use gmeta_core::types::Target;
    use std::collections::BTreeMap;

    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for e in metadata_entries {
        if e.value_type != ValueType::String {
            continue;
        }
        let target = if e.target_type == TargetType::Project {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", e.target_type, e.target_value))?
        };

        let full_path = tree_paths::tree_path(&target, &e.key)?;
        if e.is_git_ref {
            let oid = gix::ObjectId::from_hex(e.value.as_bytes())?;
            let blob = oid.attach(repo).object()?.into_blob();
            files.insert(full_path, blob.data.clone());
        } else {
            let raw_value: String =
                serde_json::from_str(&e.value).unwrap_or_else(|_| e.value.clone());
            files.insert(full_path, raw_value.into_bytes());
        }
    }

    build_tree_from_paths(repo, &files)
}

/// Build a nested Git tree from flat file paths using the gix tree editor.
fn build_tree_from_paths(
    repo: &gix::Repository,
    files: &std::collections::BTreeMap<String, Vec<u8>>,
) -> Result<gix::ObjectId> {
    let mut editor = repo.empty_tree().edit()?;
    for (path, content) in files {
        let blob_id = repo.write_blob(content)?.detach();
        editor.upsert(path, gix::objs::tree::EntryKind::Blob, blob_id)?;
    }
    Ok(editor.write()?.detach())
}
pub fn run(rounds: usize) -> Result<()> {
    let mut rng = Rng(0xdeadbeef_cafebabe);

    println!(
        "\n{}gmeta serialize benchmark{}  —  {}{} rounds{}",
        BOLD, RESET, CYAN, rounds, RESET
    );

    // Create temp directory
    let tmp_path = std::env::temp_dir().join(format!(
        "gmeta-serialize-bench-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ));
    std::fs::create_dir_all(&tmp_path).context("failed to create temp dir")?;
    let repo_path = tmp_path.join("repo.git");

    println!("{}repo: {}{}", DIM, repo_path.display(), RESET);

    // Init bare repo
    let repo = gix::init_bare(&repo_path)?;

    // Open gmeta database inside the bare repo
    let db_path = repo_path.join("gmeta.sqlite");
    let db = Store::open(&db_path)?;

    let ref_name = "refs/meta/local/main";
    let mut total_keys = 0usize;
    let mut round_stats: Vec<RoundStats> = Vec::with_capacity(rounds);

    // Print table header
    println!(
        "\n  {}round  keys   cumulative  insert       serialize    total{}",
        DIM, RESET
    );
    println!("  {}", "─".repeat(70));

    let wall_t0 = Instant::now();

    for round in 1..=rounds {
        // Pick a random number of keys between 10 and 1000
        let n_keys = rng.range(10, 1000) as usize;

        // Insert keys into the database
        let t_insert = Instant::now();
        let timestamp_base =
            time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;

        for i in 0..n_keys {
            let sha = fake_sha(&mut rng);
            let key_segments = rng.range(1, 3);
            let key: String = (0..key_segments)
                .map(|_| fake_value(&mut rng, 3, 12))
                .collect::<Vec<_>>()
                .join(":");
            let value = fake_value(&mut rng, 10, 200);
            let json_value = serde_json::to_string(&value)?;

            let target = Target::from_parts(TargetType::Commit, Some(sha));
            db.set(
                &target,
                &key,
                &json_value,
                &ValueType::String,
                "bench@bench",
                timestamp_base + i as i64,
            )?;
        }
        let insert_secs = t_insert.elapsed().as_secs_f64();

        total_keys += n_keys;

        // Serialize to git ref
        let t_serialize = Instant::now();
        do_serialize(&repo, &db, ref_name)?;
        let serialize_secs = t_serialize.elapsed().as_secs_f64();

        let total_secs = insert_secs + serialize_secs;

        // Print row
        println!(
            "  {}{:>5}{}  {}{:>5}{}  {}{:>10}{}  {}{}{}  {}{}{}  {}{}{}",
            BOLD,
            round,
            RESET,
            CYAN,
            n_keys,
            RESET,
            CYAN,
            total_keys,
            RESET,
            DIM,
            fmt_pad(&fmt_ms(insert_secs), 12),
            RESET,
            YELLOW,
            fmt_pad(&fmt_ms(serialize_secs), 12),
            RESET,
            GREEN,
            fmt_pad(&fmt_ms(total_secs), 10),
            RESET,
        );
        let _ = std::io::stdout().flush();

        round_stats.push(RoundStats {
            round,
            keys_inserted: n_keys,
            insert_secs,
            serialize_secs,
            cumulative_keys: total_keys,
        });
    }

    let wall_secs = wall_t0.elapsed().as_secs_f64();

    println!("\n{}── Summary ──{}", BOLD, RESET);
    println!("  total keys:       {}{}{}", CYAN, total_keys, RESET);
    println!(
        "  wall time:        {}{}{}",
        GREEN,
        fmt_ms(wall_secs),
        RESET
    );

    let total_insert: f64 = round_stats.iter().map(|r| r.insert_secs).sum();
    let total_serialize: f64 = round_stats.iter().map(|r| r.serialize_secs).sum();
    println!(
        "  total insert:     {}{}{}",
        DIM,
        fmt_ms(total_insert),
        RESET
    );
    println!(
        "  total serialize:  {}{}{}",
        YELLOW,
        fmt_ms(total_serialize),
        RESET
    );

    // Per-key averages
    let avg_insert_per_key = total_insert / total_keys as f64;
    let avg_serialize_per_key = total_serialize / total_keys as f64;
    println!(
        "  avg insert/key:   {}{}{}",
        DIM,
        fmt_ms(avg_insert_per_key),
        RESET
    );
    println!(
        "  avg serialize/key:{}{}{}",
        YELLOW,
        fmt_ms(avg_serialize_per_key),
        RESET
    );

    // Serialize time trend (first vs last round)
    if round_stats.len() >= 2 {
        let first = &round_stats[0];
        let last = round_stats
            .last()
            .ok_or_else(|| anyhow::anyhow!("round_stats is empty"))?;
        let first_per_key = first.serialize_secs / first.keys_inserted as f64;
        let last_per_key = last.serialize_secs / last.keys_inserted as f64;
        let slowdown = last.serialize_secs / first.serialize_secs;
        println!("\n  {}serialize scaling:{}", BOLD, RESET);
        println!(
            "    round 1:  {} ({} keys, {}/key)",
            fmt_ms(first.serialize_secs),
            first.keys_inserted,
            fmt_ms(first_per_key),
        );
        println!(
            "    round {}:  {} ({} keys, {}/key, {} cumulative)",
            last.round,
            fmt_ms(last.serialize_secs),
            last.keys_inserted,
            fmt_ms(last_per_key),
            last.cumulative_keys,
        );
        let color = if slowdown > 5.0 {
            RED
        } else if slowdown > 2.0 {
            YELLOW
        } else {
            GREEN
        };
        println!("    slowdown: {}{:.1}x{}", color, slowdown, RESET);
    }

    println!("\n{}── Git ODB ──{}", BOLD, RESET);

    let odb = count_odb_stats(&repo_path)?;
    let total_objects = odb.loose_blobs + odb.loose_trees + odb.loose_commits + odb.loose_other;

    println!("  loose objects:    {}{}{}", CYAN, total_objects, RESET);
    println!("    blobs:          {}{}{}", BLUE, odb.loose_blobs, RESET);
    println!("    trees:          {}{}{}", BLUE, odb.loose_trees, RESET);
    println!("    commits:        {}{}{}", BLUE, odb.loose_commits, RESET);
    if odb.loose_other > 0 {
        println!("    other:          {}{}{}", DIM, odb.loose_other, RESET);
    }
    println!(
        "  total ODB size:   {}{}{}",
        MAGENTA,
        fmt_bytes(odb.loose_total_bytes),
        RESET
    );

    // Ratio of trees to blobs
    if odb.loose_blobs > 0 {
        let ratio = odb.loose_trees as f64 / odb.loose_blobs as f64;
        println!("  tree/blob ratio:  {}{:.2}{}", DIM, ratio, RESET);
    }

    // Bytes per key
    if total_keys > 0 {
        let bytes_per_key = odb.loose_total_bytes as f64 / total_keys as f64;
        println!("  ODB bytes/key:    {}{:.0} B{}", DIM, bytes_per_key, RESET);
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&tmp_path);
    println!(
        "\n{}temp repo removed ({}){}",
        DIM,
        tmp_path.display(),
        RESET
    );

    Ok(())
}

fn fmt_pad(s: &str, width: usize) -> String {
    if s.len() >= width {
        s.to_string()
    } else {
        format!("{}{}", s, " ".repeat(width - s.len()))
    }
}
