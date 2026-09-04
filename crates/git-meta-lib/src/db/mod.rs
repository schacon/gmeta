//! SQLite storage internals used by the CLI and session implementation.

mod config;
mod lists;
mod metadata;
mod promised;
mod publish;
mod schema;
mod sets;
mod sync;
mod tombstones;
mod tree_stats;
/// Named return types for database query methods.
pub mod types;
mod value_ops;

// Modules that exist solely for the CLI to consume via the `internal` feature.
// Compiling them only when the feature is enabled keeps the lib build free of
// dead-code warnings while still exporting the methods to the CLI.
#[cfg(feature = "internal")]
mod prune;
#[cfg(feature = "internal")]
mod stats;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rusqlite::{params, Connection};

use crate::error::{Error, Result};

use crate::list_value::{encode_entries, ListEntry};
/// Global counter for generating unique savepoint names.
static SAVEPOINT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The time to wait when the database is locked before giving up.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const COLLECTION_LOG_VALUE: &str = "[]";

/// Applies performance and correctness pragmas to a freshly opened SQLite connection.
///
/// Settings applied:
/// - **WAL journal mode** — enables concurrent readers during writes.
/// - **synchronous = NORMAL** — fsyncs only at critical moments (safe with WAL).
/// - **wal_autocheckpoint = 1000** — checkpoints after ~1 MB of WAL growth.
/// - **foreign_keys = ON** — enforces foreign key constraints.
/// - **busy_timeout** — waits up to [`BUSY_TIMEOUT`] when the database is locked.
///
/// # Errors
///
/// Returns an error if any pragma or the busy-timeout call fails.
fn configure_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA wal_autocheckpoint = 1000;
         PRAGMA foreign_keys = ON;",
    )?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    Ok(())
}

/// SQLite-backed metadata database with optional git repository for git-ref blobs.
#[derive(Debug)]
pub struct Store {
    pub(crate) conn: Connection,
    /// Optional git repository for reading and writing git-ref string blobs.
    pub(crate) repo: Option<gix::Repository>,
}

impl Store {
    /// Open a store at `path` without an associated git repository.
    ///
    /// Library consumers should use [`Session`](crate::Session), which constructs
    /// a store via [`Store::open_with_repo`].
    #[cfg(feature = "internal")]
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        configure_connection(&conn)?;
        let db = Store { conn, repo: None };
        schema::run_migrations(&db.conn)?;
        Ok(db)
    }

    /// Open a store at `path` with an associated Git repository.
    ///
    /// The repository is used for metadata values that reference Git blobs and
    /// for operations that need to read or write the git-meta exchange refs.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQLite database cannot be opened, connection
    /// pragmas fail, or schema migrations cannot be applied.
    pub fn open_with_repo(path: &Path, repo: gix::Repository) -> Result<Self> {
        let conn = Connection::open(path)?;
        configure_connection(&conn)?;
        let db = Store {
            conn,
            repo: Some(repo),
        };
        schema::run_migrations(&db.conn)?;
        Ok(db)
    }

    #[cfg(test)]
    /// Open an in-memory store for tests.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory SQLite connection cannot be created,
    /// configured, or migrated.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        configure_connection(&conn)?;
        let db = Store { conn, repo: None };
        schema::run_migrations(&db.conn)?;
        Ok(db)
    }

    /// Create a nestable savepoint on the connection.
    ///
    /// Unlike `unchecked_transaction()` (which issues `BEGIN DEFERRED` and
    /// cannot nest), this uses SQLite's `SAVEPOINT` statement, which nests
    /// correctly inside other savepoints and inside batch transactions.
    fn savepoint(&self) -> Result<AutoSavepoint<'_>> {
        AutoSavepoint::new(&self.conn)
    }
}

/// RAII guard for a SQLite savepoint created via raw SQL.
///
/// Rolls back on drop unless [`commit()`](Self::commit) is called.
/// Uses unique names so multiple savepoints can nest.
struct AutoSavepoint<'a> {
    conn: &'a Connection,
    name: String,
    committed: bool,
}

impl<'a> AutoSavepoint<'a> {
    fn new(conn: &'a Connection) -> Result<Self> {
        let id = SAVEPOINT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("git_meta_sp_{id}");
        conn.execute_batch(&format!("SAVEPOINT {name}"))?;
        Ok(Self {
            conn,
            name,
            committed: false,
        })
    }

    fn commit(mut self) -> Result<()> {
        self.conn.execute_batch(&format!("RELEASE {}", self.name))?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for AutoSavepoint<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self
                .conn
                .execute_batch(&format!("ROLLBACK TO {}", self.name));
            let _ = self.conn.execute_batch(&format!("RELEASE {}", self.name));
        }
    }
}

#[derive(Debug, Clone)]
struct ListRow {
    rowid: i64,
    value: String,
    timestamp: i64,
}

fn load_list_entries_by_metadata_id(
    conn: &Connection,
    repo: Option<&gix::Repository>,
    metadata_id: i64,
) -> Result<Vec<ListEntry>> {
    let mut stmt = conn.prepare(
        "SELECT value, timestamp, is_git_ref
         FROM list_values
         WHERE metadata_id = ?1
         ORDER BY timestamp",
    )?;
    let rows = stmt.query_map(params![metadata_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, bool>(2)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (value, timestamp, is_git_ref) = row?;
        let resolved = resolve_blob(repo, &value, is_git_ref)?;
        entries.push(ListEntry {
            value: resolved,
            timestamp,
        });
    }
    Ok(entries)
}

fn load_list_rows_by_metadata_id(conn: &Connection, metadata_id: i64) -> Result<Vec<ListRow>> {
    let mut stmt = conn.prepare(
        "SELECT rowid, value, timestamp
         FROM list_values
         WHERE metadata_id = ?1
         ORDER BY timestamp",
    )?;
    let rows = stmt.query_map(params![metadata_id], |row| {
        Ok(ListRow {
            rowid: row.get::<_, i64>(0)?,
            value: row.get::<_, String>(1)?,
            timestamp: row.get::<_, i64>(2)?,
        })
    })?;

    let mut entries = Vec::new();
    for row in rows {
        entries.push(row?);
    }
    Ok(entries)
}

fn encode_list_entries_by_metadata_id(
    conn: &Connection,
    repo: Option<&gix::Repository>,
    metadata_id: i64,
) -> Result<String> {
    let entries = load_list_entries_by_metadata_id(conn, repo, metadata_id)?;
    encode_entries(&entries)
}

fn load_set_values_by_metadata_id_tx(
    conn: &Connection,
    metadata_id: i64,
) -> Result<std::collections::BTreeMap<String, (String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT member_id, value, timestamp FROM set_values WHERE metadata_id = ?1 ORDER BY member_id",
    )?;
    let rows = stmt.query_map(params![metadata_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let mut result = std::collections::BTreeMap::new();
    for row in rows {
        let (member_id, value, timestamp) = row?;
        result.insert(member_id, (value, timestamp));
    }
    Ok(result)
}

fn encode_set_values_by_metadata_id(conn: &Connection, metadata_id: i64) -> Result<String> {
    let mut stmt =
        conn.prepare("SELECT value FROM set_values WHERE metadata_id = ?1 ORDER BY value")?;
    let rows = stmt.query_map(params![metadata_id], |row| row.get::<_, String>(0))?;
    let mut values = Vec::new();
    for row in rows {
        values.push(row?);
    }
    Ok(serde_json::to_string(&values)?)
}

fn normalize_set_values(raw: &str) -> Result<Vec<String>> {
    let values: Vec<String> = serde_json::from_str(raw)?;
    let mut set = std::collections::BTreeSet::new();
    for value in values {
        set.insert(value);
    }
    Ok(set.into_iter().collect())
}

/// Resolve a stored value: if `is_git_ref` is true, read the blob content from the repo.
fn resolve_blob(repo: Option<&gix::Repository>, value: &str, is_git_ref: bool) -> Result<String> {
    if !is_git_ref {
        return Ok(value.to_string());
    }
    let Some(repo) = repo else {
        return Ok(value.to_string()); // no repo, return OID as-is
    };
    let oid =
        gix::ObjectId::from_hex(value.as_bytes()).map_err(|e| Error::Other(format!("{e}")))?;
    let blob = repo
        .find_blob(oid)
        .map_err(|e| Error::Other(format!("{e}")))?;
    Ok(String::from_utf8_lossy(&blob.data).into_owned())
}

fn escape_like_pattern(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '%' => out.push_str("\\%"),
            '_' => out.push_str("\\_"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::tree::model::{Key, TreeValue};
    use crate::types::{Target, TargetType, ValueType};

    fn commit_target(sha: &str) -> Target {
        Target::parse(&format!("commit:{sha}")).unwrap()
    }

    #[test]
    fn test_set_and_get() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "agent:model",
            "\"claude-4.6\"",
            &ValueType::String,
            "test@test.com",
            1000,
        )
        .unwrap();
        let result = db.get(&target, "agent:model").unwrap();
        assert_eq!(
            result,
            Some(types::MetadataValue {
                value: "\"claude-4.6\"".to_string(),
                value_type: ValueType::String,
                is_git_ref: false
            })
        );
    }

    #[test]
    fn test_set_upsert() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "key",
            "\"v1\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &target,
            "key",
            "\"v2\"",
            &ValueType::String,
            "a@b.com",
            2000,
        )
        .unwrap();
        let result = db.get(&target, "key").unwrap();
        assert_eq!(
            result,
            Some(types::MetadataValue {
                value: "\"v2\"".to_string(),
                value_type: ValueType::String,
                is_git_ref: false
            })
        );
    }

    #[test]
    fn test_get_all_with_prefix() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "agent:model",
            "\"claude\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &target,
            "agent:provider",
            "\"anthropic\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &target,
            "other",
            "\"val\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let results = db.get_all(&target, Some("agent")).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_get_all_with_prefix_escapes_like_wildcards() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "a%:literal",
            "\"match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &target,
            "abc:anything",
            "\"should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &target,
            "a_:literal",
            "\"underscore-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &target,
            "ab:anything",
            "\"underscore-should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let percent_results = db.get_all(&target, Some("a%")).unwrap();
        let percent_keys: Vec<String> = percent_results.into_iter().map(|r| r.key).collect();
        assert_eq!(percent_keys, vec!["a%:literal".to_string()]);

        let underscore_results = db.get_all(&target, Some("a_")).unwrap();
        let underscore_keys: Vec<String> = underscore_results.into_iter().map(|r| r.key).collect();
        assert_eq!(underscore_keys, vec!["a_:literal".to_string()]);
    }

    #[test]
    fn test_get_all_with_prefix_escapes_backslash() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            r"agent\name:model",
            "\"match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &target,
            "agentxname:model",
            "\"should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let results = db.get_all(&target, Some(r"agent\name")).unwrap();
        let keys: Vec<String> = results.into_iter().map(|r| r.key).collect();
        assert_eq!(keys, vec![r"agent\name:model".to_string()]);
    }

    #[test]
    fn test_get_all_with_target_prefix_for_paths() {
        let db = Store::open_in_memory().unwrap();
        let src_git = Target::path("src/git");
        let src_metrics = Target::path("src/metrics");
        let src_obs = Target::path("src/observability");
        let srcx_metrics = Target::path("srcx/metrics");

        db.set(
            &src_git,
            "owner",
            "\"schacon\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &src_metrics,
            "owner",
            "\"kiril\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &src_obs,
            "owner",
            "\"caleb\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &srcx_metrics,
            "owner",
            "\"should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let src_target = Target::path("src");
        let results = db
            .get_all_with_target_prefix(&src_target, true, Some("owner"))
            .unwrap();
        let rows: Vec<(String, String)> = results
            .into_iter()
            .map(|r| (r.target_value, r.key))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("src/git".to_string(), "owner".to_string()),
                ("src/metrics".to_string(), "owner".to_string()),
                ("src/observability".to_string(), "owner".to_string()),
            ]
        );
    }

    #[test]
    fn test_rm() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "key",
            "\"val\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        assert!(db.remove(&target, "key", "a@b.com", 2000).unwrap());
        assert_eq!(db.get(&target, "key").unwrap(), None);
    }

    #[test]
    fn test_rm_creates_tombstone() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "key",
            "\"val\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        assert!(db.remove(&target, "key", "a@b.com", 2000).unwrap());

        let tombstones = db.get_all_tombstones().unwrap();
        assert_eq!(tombstones.len(), 1);
        assert_eq!(
            tombstones[0],
            types::TombstoneRecord {
                target_type: TargetType::Commit,
                target_value: "abc123".to_string(),
                key: "key".to_string(),
                timestamp: 2000,
                email: "a@b.com".to_string(),
            }
        );
    }

    #[test]
    fn test_clear_key_prefix_removes_matching_keys_across_targets() {
        let db = Store::open_in_memory().unwrap();
        let first = commit_target("abc123");
        let second = commit_target("def456");
        for (target, key, value) in [
            (&first, "agent:model", "\"claude\""),
            (&first, "agent:provider", "\"anthropic\""),
            (&first, "other:key", "\"kept\""),
            (&second, "agent:model", "\"claude\""),
        ] {
            db.set(target, key, value, &ValueType::String, "a@b.com", 1000)
                .unwrap();
        }

        let removed = db.clear_key_prefix("agent", "a@b.com", 2000).unwrap();

        assert_eq!(removed, 3);
        assert_eq!(db.get(&first, "agent:model").unwrap(), None);
        assert_eq!(db.get(&first, "agent:provider").unwrap(), None);
        assert_eq!(db.get(&second, "agent:model").unwrap(), None);
        assert!(db.get(&first, "other:key").unwrap().is_some());
        assert_eq!(db.get_all_tombstones().unwrap().len(), 3);
    }

    #[test]
    fn test_clear_key_prefix_escapes_like_wildcards() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        for key in ["a%:literal", "abc:anything", "a_:literal", "ab:anything"] {
            db.set(&target, key, "\"val\"", &ValueType::String, "a@b.com", 1000)
                .unwrap();
        }

        let removed = db.clear_key_prefix("a%", "a@b.com", 2000).unwrap();

        assert_eq!(removed, 1);
        assert_eq!(db.get(&target, "a%:literal").unwrap(), None);
        assert!(db.get(&target, "abc:anything").unwrap().is_some());
        assert!(db.get(&target, "a_:literal").unwrap().is_some());
        assert!(db.get(&target, "ab:anything").unwrap().is_some());
    }

    #[test]
    fn test_set_clears_tombstone() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "key",
            "\"v1\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        assert!(db.remove(&target, "key", "a@b.com", 2000).unwrap());
        assert_eq!(db.get_all_tombstones().unwrap().len(), 1);

        db.set(
            &target,
            "key",
            "\"v2\"",
            &ValueType::String,
            "a@b.com",
            3000,
        )
        .unwrap();

        assert_eq!(db.get_all_tombstones().unwrap().len(), 0);
        let result = db.get(&target, "key").unwrap();
        assert_eq!(
            result,
            Some(types::MetadataValue {
                value: "\"v2\"".to_string(),
                value_type: ValueType::String,
                is_git_ref: false
            })
        );
    }

    #[test]
    fn test_list_push() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.list_push(&target, "tags", "first", "a@b.com", 1000)
            .unwrap();
        db.list_push(&target, "tags", "second", "a@b.com", 2000)
            .unwrap();
        let entry = db.get(&target, "tags").unwrap().unwrap();
        assert_eq!(entry.value_type, ValueType::List);
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["first", "second"]);
    }

    #[test]
    fn test_list_push_logs_compact_collection_value() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.list_push(&target, "tags", "first", "a@b.com", 1000)
            .unwrap();
        db.list_push(&target, "tags", "second", "a@b.com", 2000)
            .unwrap();

        let logged_value: String = db
            .conn
            .query_row(
                "SELECT value FROM metadata_log
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                   AND key = 'tags' AND operation = 'push'
                 ORDER BY timestamp DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(logged_value, COLLECTION_LOG_VALUE);
    }

    #[test]
    fn test_apply_edits() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "events",
            "\"legacy\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set_add(&target, "event-hashes", "hash-0", "a@b.com", 1000)
            .unwrap();
        let summary = crate::MetaValue::String("new summary".to_string());
        let entries = vec![
            ListEntry {
                value: "first".to_string(),
                timestamp: 1000,
            },
            ListEntry {
                value: "second".to_string(),
                timestamp: 1001,
            },
        ];
        let members = vec!["hash-1".to_string(), "hash-2".to_string()];

        db.apply_edits(
            &target,
            [
                crate::MetaEdit::set_value("summary", &summary),
                crate::MetaEdit::list_append("events", &entries),
                crate::MetaEdit::set_add("event-hashes", &members),
            ],
            "a@b.com",
            2000,
        )
        .unwrap();

        let events = db.get(&target, "events").unwrap().unwrap();
        let hashes = db.get(&target, "event-hashes").unwrap().unwrap();

        assert_eq!(db.get_value(&target, "summary").unwrap(), Some(summary));
        assert_eq!(
            crate::list_value::list_values_from_json(&events.value).unwrap(),
            vec!["legacy", "first", "second"]
        );
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&hashes.value).unwrap(),
            vec!["hash-0", "hash-1", "hash-2"]
        );

        let logged_hashes: String = db
            .conn
            .query_row(
                "SELECT value FROM metadata_log
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                   AND key = 'event-hashes' AND operation = 'set_add'
                 ORDER BY timestamp DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(logged_hashes, COLLECTION_LOG_VALUE);
    }

    #[test]
    fn test_apply_edits_rolls_back_on_error() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "event-hashes",
            "\"not-a-set\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        let summary = crate::MetaValue::String("should roll back".to_string());
        let entries = vec![ListEntry {
            value: "first".to_string(),
            timestamp: 1000,
        }];
        let members = vec!["hash-1".to_string()];

        assert!(db
            .apply_edits(
                &target,
                [
                    crate::MetaEdit::set_value("summary", &summary),
                    crate::MetaEdit::list_append("events", &entries),
                    crate::MetaEdit::set_add("event-hashes", &members),
                ],
                "a@b.com",
                2000,
            )
            .is_err());

        assert_eq!(db.get_value(&target, "summary").unwrap(), None);
        assert_eq!(db.get_value(&target, "events").unwrap(), None);
        assert_eq!(
            db.get_value(&target, "event-hashes").unwrap(),
            Some(crate::MetaValue::String("not-a-set".to_string()))
        );

        let batch_log_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM metadata_log
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                   AND timestamp = 2000",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(batch_log_count, 0);
    }

    #[test]
    fn test_publish_local_rejects_promised_set_member_destination() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set_add(&target, "local:sessions", "s1", "a@b.com", 1000)
            .unwrap();
        db.insert_promised(&target, "sessions", &ValueType::Set, None)
            .unwrap();
        let members = vec!["s1".to_string()];

        let result = db.publish_local(
            &target,
            [crate::LocalPublish::set_members(
                "local:sessions",
                "sessions",
                &members,
            )],
            "a@b.com",
            2000,
        );

        assert!(result.is_err());
        let Some(crate::MetaValue::Set(local)) = db.get_value(&target, "local:sessions").unwrap()
        else {
            panic!("expected source set");
        };
        assert!(local.contains("s1"));
    }

    #[test]
    fn test_publish_local_rejects_promised_set_member_source() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.insert_promised(&target, "local:sessions", &ValueType::Set, None)
            .unwrap();
        let members = vec!["s1".to_string()];

        let result = db.publish_local(
            &target,
            [crate::LocalPublish::set_members(
                "local:sessions",
                "sessions",
                &members,
            )],
            "a@b.com",
            2000,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_publish_local_rejects_promised_key_prefix_rows() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.insert_promised(&target, "local:summary:title", &ValueType::String, None)
            .unwrap();

        let result = db.publish_local(
            &target,
            [crate::LocalPublish::key_prefix("local:summary", "summary")],
            "a@b.com",
            2000,
        );

        assert!(result.is_err());

        let db = Store::open_in_memory().unwrap();
        db.set(
            &target,
            "local:summary:title",
            "\"draft\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.insert_promised(&target, "summary:title", &ValueType::String, None)
            .unwrap();

        let result = db.publish_local(
            &target,
            [crate::LocalPublish::key_prefix("local:summary", "summary")],
            "a@b.com",
            2000,
        );

        assert!(result.is_err());
        assert!(db
            .get_value(&target, "local:summary:title")
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_publish_local_preserves_collection_backing_rows() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        let list_value = crate::list_value::encode_entries(&[ListEntry {
            value: "first".to_string(),
            timestamp: 1000,
        }])
        .unwrap();
        db.set(
            &target,
            "local:session:events",
            &list_value,
            &ValueType::List,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set_add(&target, "local:session:tags", "review", "a@b.com", 1001)
            .unwrap();

        let list_metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                 AND key = 'local:session:events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let set_metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                 AND key = 'local:session:tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        db.publish_local(
            &target,
            [crate::LocalPublish::key_prefix("local:session", "session")],
            "a@b.com",
            2000,
        )
        .unwrap();

        let published_list_metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                 AND key = 'session:events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let published_set_metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                 AND key = 'session:tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(published_list_metadata_id, list_metadata_id);
        assert_eq!(published_set_metadata_id, set_metadata_id);

        let list_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![list_metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(list_rows, 1);

        let set_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM set_values WHERE metadata_id = ?1",
                params![set_metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(set_rows, 1);
    }

    #[test]
    fn test_publish_local_does_not_make_local_set_tombstones_public() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set_add(&target, "local:sessions", "s1", "a@b.com", 1000)
            .unwrap();
        db.set_remove(&target, "local:sessions", "s1", "a@b.com", 1001)
            .unwrap();
        db.set_add(&target, "local:sessions", "s2", "a@b.com", 1002)
            .unwrap();

        db.publish_local(
            &target,
            [crate::LocalPublish::key_prefix(
                "local:sessions",
                "sessions",
            )],
            "a@b.com",
            2000,
        )
        .unwrap();

        let public_set_tombstones: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM tombstones
                 WHERE tombstone_type = 'set_member'
                 AND target_type = 'commit' AND target_value = 'abc123'
                 AND key = 'sessions'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(public_set_tombstones, 0);

        let local_set_tombstones: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM tombstones
                 WHERE tombstone_type = 'set_member'
                 AND target_type = 'commit' AND target_value = 'abc123'
                 AND key = 'local:sessions'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(local_set_tombstones, 1);

        let Some(crate::MetaValue::Set(public)) = db.get_value(&target, "sessions").unwrap() else {
            panic!("expected published set");
        };
        assert!(!public.contains("s1"));
        assert!(public.contains("s2"));
    }

    #[test]
    fn test_publish_local_rolls_back_logs_and_tombstones() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "local:session:title",
            "\"draft\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        let members = vec!["s1".to_string()];

        let result = db.publish_local(
            &target,
            [
                crate::LocalPublish::key_prefix("local:session", "session"),
                crate::LocalPublish::set_members("local:missing", "sessions", &members),
            ],
            "a@b.com",
            2000,
        );

        assert!(result.is_err());
        let publish_log_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM metadata_log WHERE timestamp = 2000",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(publish_log_count, 0);
        let publish_tombstone_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM tombstones WHERE timestamp = 2000",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(publish_tombstone_count, 0);
    }

    #[test]
    fn test_apply_edits_applies_same_key_edits_in_order() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        let first = crate::MetaValue::String("first".to_string());
        let final_value = crate::MetaValue::String("final".to_string());
        let entries = vec![ListEntry {
            value: "second".to_string(),
            timestamp: 1000,
        }];

        db.apply_edits(
            &target,
            [
                crate::MetaEdit::set_value("events", &first),
                crate::MetaEdit::list_append("events", &entries),
            ],
            "a@b.com",
            2000,
        )
        .unwrap();
        db.apply_edits(
            &target,
            [
                crate::MetaEdit::list_append("notes", &entries),
                crate::MetaEdit::set_value("notes", &final_value),
            ],
            "a@b.com",
            3000,
        )
        .unwrap();

        let Some(crate::MetaValue::List(events)) = db.get_value(&target, "events").unwrap() else {
            panic!("expected events to be a list");
        };
        assert_eq!(
            events
                .iter()
                .map(|entry| entry.value.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        assert_eq!(
            db.get_value(&target, "notes").unwrap(),
            Some(crate::MetaValue::String("final".to_string()))
        );
    }

    #[test]
    fn test_apply_edits_keeps_large_list_entries_materialized() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = gix::init(dir.path()).unwrap();
        let db = Store::open_with_repo(&dir.path().join("git-meta.sqlite"), repo).unwrap();
        let target = commit_target("abc123");
        let large_value = "x".repeat(2048);
        let entries = vec![ListEntry {
            value: large_value.clone(),
            timestamp: 1000,
        }];

        db.apply_edits(
            &target,
            [crate::MetaEdit::list_append("events", &entries)],
            "a@b.com",
            2000,
        )
        .unwrap();

        let metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata WHERE target_type = 'commit' AND target_value = 'abc123' AND key = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let (stored_value, is_git_ref): (String, bool) = db
            .conn
            .query_row(
                "SELECT value, is_git_ref FROM list_values
                 WHERE metadata_id = ?1
                 ORDER BY rowid
                 LIMIT 1",
                params![metadata_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored_value, large_value);
        assert!(!is_git_ref);
    }

    #[test]
    fn test_list_push_converts_git_ref_string_to_materialized_list() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = gix::init(dir.path()).unwrap();
        let blob_oid = repo
            .write_blob("large string".as_bytes())
            .unwrap()
            .to_string();
        let db = Store::open_with_repo(&dir.path().join("git-meta.sqlite"), repo).unwrap();
        let target = commit_target("abc123");

        db.set_with_git_ref(
            &target,
            "events",
            &blob_oid,
            &ValueType::String,
            "a@b.com",
            1000,
            true,
        )
        .unwrap();
        db.list_push(&target, "events", "next", "a@b.com", 2000)
            .unwrap();

        let entry = db.get(&target, "events").unwrap().unwrap();
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["large string", "next"]);
        assert!(!entry.is_git_ref);
        let git_ref_rows: i64 = db
            .conn
            .query_row(
                "SELECT COALESCE(SUM(is_git_ref), 0) FROM list_values",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(git_ref_rows, 0);
    }

    #[test]
    fn test_apply_tree_does_not_rewrite_same_large_git_ref_string() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = gix::init(dir.path()).unwrap();
        let db = Store::open_with_repo(&dir.path().join("git-meta.sqlite"), repo).unwrap();
        let key = Key {
            target_type: TargetType::Commit,
            target_value: "abc123".to_string(),
            key: "body".to_string(),
        };
        let large_value = "x".repeat(2048);
        let values = BTreeMap::from([(key, TreeValue::String(large_value))]);

        db.apply_tree(
            &values,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            "a@b.com",
            1000,
        )
        .unwrap();
        db.apply_tree(
            &values,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            "a@b.com",
            2000,
        )
        .unwrap();

        let log_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM metadata_log
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                   AND key = 'body' AND operation = 'set'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(log_rows, 1);
    }

    #[test]
    fn test_apply_tree_rematerializes_git_ref_list_entries() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = gix::init(dir.path()).unwrap();
        let blob_oid = repo
            .write_blob("large entry".as_bytes())
            .unwrap()
            .to_string();
        let db = Store::open_with_repo(&dir.path().join("git-meta.sqlite"), repo).unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "events",
            &crate::list_value::encode_entries(&[ListEntry {
                value: "large entry".to_string(),
                timestamp: 1000,
            }])
            .unwrap(),
            &ValueType::List,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.conn
            .execute(
                "UPDATE list_values SET value = ?1, is_git_ref = 1",
                params![blob_oid],
            )
            .unwrap();

        let values = BTreeMap::from([(
            Key {
                target_type: TargetType::Commit,
                target_value: "abc123".to_string(),
                key: "events".to_string(),
            },
            TreeValue::List(vec![("1000-entry".to_string(), "large entry".to_string())]),
        )]);
        db.apply_tree(
            &values,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            "a@b.com",
            2000,
        )
        .unwrap();

        let git_ref_rows: i64 = db
            .conn
            .query_row(
                "SELECT COALESCE(SUM(is_git_ref), 0) FROM list_values",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(git_ref_rows, 0);
    }

    #[test]
    fn test_set_list_stores_rows_in_list_values_table() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = gix::init(dir.path()).unwrap();
        let db = Store::open_with_repo(&dir.path().join("git-meta.sqlite"), repo).unwrap();
        let target = commit_target("abc123");
        let large_value = "x".repeat(2048);
        let list_value = crate::list_value::encode_entries(&[
            ListEntry {
                value: large_value.clone(),
                timestamp: 1000,
            },
            ListEntry {
                value: "b".to_string(),
                timestamp: 1001,
            },
        ])
        .unwrap();
        db.set(
            &target,
            "tags",
            &list_value,
            &ValueType::List,
            "a@b.com",
            2000,
        )
        .unwrap();

        let metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata WHERE target_type = 'commit' AND target_value = 'abc123' AND key = 'tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let list_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(list_rows, 2);
        let git_ref_rows: i64 = db
            .conn
            .query_row(
                "SELECT COALESCE(SUM(is_git_ref), 0) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(git_ref_rows, 0);

        let entry = db.get(&target, "tags").unwrap().unwrap();
        assert_eq!(entry.value_type, ValueType::List);
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec![large_value, "b".to_string()]);

        let logged_value: String = db
            .conn
            .query_row(
                "SELECT value FROM metadata_log
                 WHERE target_type = 'commit' AND target_value = 'abc123'
                   AND key = 'tags' AND operation = 'set'
                 ORDER BY timestamp DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(logged_value, COLLECTION_LOG_VALUE);
    }

    #[test]
    fn test_set_list_replaces_existing_list_rows() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "tags",
            r#"[{"value":"a","timestamp":1000},{"value":"b","timestamp":1001}]"#,
            &ValueType::List,
            "a@b.com",
            2000,
        )
        .unwrap();
        db.set(
            &target,
            "tags",
            r#"[{"value":"c","timestamp":3000}]"#,
            &ValueType::List,
            "a@b.com",
            4000,
        )
        .unwrap();

        let metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata WHERE target_type = 'commit' AND target_value = 'abc123' AND key = 'tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let list_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(list_rows, 1);

        let entry = db.get(&target, "tags").unwrap().unwrap();
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["c"]);
    }

    #[test]
    fn test_list_push_converts_string() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "key",
            "\"original\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.list_push(&target, "key", "appended", "a@b.com", 2000)
            .unwrap();
        let entry = db.get(&target, "key").unwrap().unwrap();
        assert_eq!(entry.value_type, ValueType::List);
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["original", "appended"]);
    }

    #[test]
    fn test_list_pop() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.list_push(&target, "tags", "a", "a@b.com", 1000).unwrap();
        db.list_push(&target, "tags", "b", "a@b.com", 2000).unwrap();
        db.list_pop(&target, "tags", "b", "a@b.com", 3000).unwrap();
        let entry = db.get(&target, "tags").unwrap().unwrap();
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["a"]);
    }

    #[test]
    fn test_apply_tombstone_removes_list_values_rows() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.list_push(&target, "tags", "a", "a@b.com", 1000).unwrap();
        db.list_push(&target, "tags", "b", "a@b.com", 2000).unwrap();

        let metadata_id: i64 = db
            .conn
            .query_row(
                "SELECT rowid FROM metadata WHERE target_type = 'commit' AND target_value = 'abc123' AND key = 'tags'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let before_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(before_count, 2);

        db.apply_tombstone(&target, "tags", "user@example.com", 3000)
            .unwrap();

        let after_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(after_count, 0);
        assert_eq!(db.get(&target, "tags").unwrap(), None);
    }

    #[test]
    fn test_authorship() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");
        db.set(
            &target,
            "key",
            "\"val\"",
            &ValueType::String,
            "user@example.com",
            42000,
        )
        .unwrap();
        let auth = db.get_authorship(&target, "key").unwrap().unwrap();
        assert_eq!(auth.email, "user@example.com");
        assert_eq!(auth.timestamp, 42000);
    }

    #[test]
    fn test_sync_state() {
        let db = Store::open_in_memory().unwrap();
        assert_eq!(db.get_last_materialized().unwrap(), None);
        db.set_last_materialized(5000).unwrap();
        assert_eq!(db.get_last_materialized().unwrap(), Some(5000));
    }

    #[test]
    fn test_last_timestamp_stored_and_returned() {
        let db = Store::open_in_memory().unwrap();
        let target = commit_target("abc123");

        // set stores the timestamp
        db.set(
            &target,
            "key",
            "\"val\"",
            &ValueType::String,
            "a@b.com",
            5000,
        )
        .unwrap();
        let entries = db.get_all_metadata().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].last_timestamp, 5000);

        // upsert updates the timestamp
        db.set(
            &target,
            "key",
            "\"val2\"",
            &ValueType::String,
            "a@b.com",
            9000,
        )
        .unwrap();
        let entries = db.get_all_metadata().unwrap();
        assert_eq!(entries[0].last_timestamp, 9000);

        // list_push stores the timestamp
        db.list_push(&target, "tags", "first", "a@b.com", 11000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.key == "tags").unwrap();
        assert_eq!(tags.last_timestamp, 11000);

        // list_pop updates the timestamp
        db.list_push(&target, "tags", "second", "a@b.com", 12000)
            .unwrap();
        db.list_pop(&target, "tags", "second", "a@b.com", 13000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.key == "tags").unwrap();
        assert_eq!(tags.last_timestamp, 13000);
    }
}
