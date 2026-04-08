mod lists;
mod metadata;
mod promised;
mod prune;
mod schema;
mod sets;
mod stats;
mod sync;
mod tombstones;
/// Named return types for database query methods.
pub mod types;
mod value_ops;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rusqlite::{params, Connection};

use crate::error::{Error, Result};

use crate::list_value::{encode_entries, ListEntry};
use crate::types::GIT_REF_THRESHOLD;

/// Global counter for generating unique savepoint names.
static SAVEPOINT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The time to wait when the database is locked before giving up.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

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

/// SQLite-backed metadata database with optional git repository for blob resolution.
pub struct Store {
    pub(crate) conn: Connection,
    /// Optional git repository for resolving git-ref list item blobs on read.
    pub(crate) repo: Option<gix::Repository>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        configure_connection(&conn)?;
        let db = Store { conn, repo: None };
        schema::run_migrations(&db.conn)?;
        Ok(db)
    }

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
        let name = format!("gmeta_sp_{id}");
        conn.execute_batch(&format!("SAVEPOINT {name}"))?;
        Ok(Self {
            conn,
            name,
            committed: false,
        })
    }

    fn commit(mut self) -> Result<()> {
        self.committed = true;
        self.conn.execute_batch(&format!("RELEASE {}", self.name))?;
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

fn load_list_entries_by_metadata_id_tx(
    conn: &Connection,
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

    // No repo available inside a transaction context; git-ref items remain as OID strings.
    // This path is only used for merge/pop operations that compare values — callers that
    // need resolved content should use the non-tx variant with a repo.
    let mut entries = Vec::new();
    for row in rows {
        let (value, timestamp, _is_git_ref) = row?;
        entries.push(ListEntry { value, timestamp });
    }
    Ok(entries)
}

fn load_list_rows_by_metadata_id_tx(conn: &Connection, metadata_id: i64) -> Result<Vec<ListRow>> {
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

/// Store `value` as a git blob if it exceeds GIT_REF_THRESHOLD, otherwise return as-is.
/// Returns (stored_value, is_git_ref).
fn blob_if_large(repo: Option<&gix::Repository>, value: &str) -> Result<(String, bool)> {
    if value.len() > GIT_REF_THRESHOLD {
        if let Some(repo) = repo {
            let oid = repo
                .write_blob(value.as_bytes())
                .map_err(|e| Error::Other(format!("{e}")))?
                .detach();
            return Ok((oid.to_string(), true));
        }
    }
    Ok((value.to_string(), false))
}

/// Resolve a stored value: if `is_git_ref` is true, read the blob content from the repo.
fn resolve_blob(repo: Option<&gix::Repository>, value: &str, is_git_ref: bool) -> Result<String> {
    if !is_git_ref {
        return Ok(value.to_string());
    }
    let repo = match repo {
        Some(r) => r,
        None => return Ok(value.to_string()), // no repo, return OID as-is
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
    use crate::types::{TargetType, ValueType};

    #[test]
    fn test_set_and_get() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "agent:model",
            "\"claude-4.6\"",
            &ValueType::String,
            "test@test.com",
            1000,
        )
        .unwrap();
        let result = db
            .get(&TargetType::Commit, "abc123", "agent:model")
            .unwrap();
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
        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"v1\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"v2\"",
            &ValueType::String,
            "a@b.com",
            2000,
        )
        .unwrap();
        let result = db.get(&TargetType::Commit, "abc123", "key").unwrap();
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
        db.set(
            &TargetType::Commit,
            "abc123",
            "agent:model",
            "\"claude\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "agent:provider",
            "\"anthropic\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "other",
            "\"val\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let results = db
            .get_all(&TargetType::Commit, "abc123", Some("agent"))
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_get_all_with_prefix_escapes_like_wildcards() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "a%:literal",
            "\"match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "abc:anything",
            "\"should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "a_:literal",
            "\"underscore-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "ab:anything",
            "\"underscore-should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let percent_results = db
            .get_all(&TargetType::Commit, "abc123", Some("a%"))
            .unwrap();
        let percent_keys: Vec<String> = percent_results.into_iter().map(|r| r.key).collect();
        assert_eq!(percent_keys, vec!["a%:literal".to_string()]);

        let underscore_results = db
            .get_all(&TargetType::Commit, "abc123", Some("a_"))
            .unwrap();
        let underscore_keys: Vec<String> = underscore_results.into_iter().map(|r| r.key).collect();
        assert_eq!(underscore_keys, vec!["a_:literal".to_string()]);
    }

    #[test]
    fn test_get_all_with_prefix_escapes_backslash() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            r"agent\name:model",
            "\"match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "agentxname:model",
            "\"should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let results = db
            .get_all(&TargetType::Commit, "abc123", Some(r"agent\name"))
            .unwrap();
        let keys: Vec<String> = results.into_iter().map(|r| r.key).collect();
        assert_eq!(keys, vec![r"agent\name:model".to_string()]);
    }

    #[test]
    fn test_get_all_with_target_prefix_for_paths() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Path,
            "src/git",
            "owner",
            "\"schacon\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Path,
            "src/metrics",
            "owner",
            "\"kiril\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Path,
            "src/observability",
            "owner",
            "\"caleb\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set(
            &TargetType::Path,
            "srcx/metrics",
            "owner",
            "\"should-not-match\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();

        let results = db
            .get_all_with_target_prefix(&TargetType::Path, "src", true, Some("owner"))
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
        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"val\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        assert!(db
            .remove(&TargetType::Commit, "abc123", "key", "a@b.com", 2000)
            .unwrap());
        assert_eq!(db.get(&TargetType::Commit, "abc123", "key").unwrap(), None);
    }

    #[test]
    fn test_rm_creates_tombstone() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"val\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        assert!(db
            .remove(&TargetType::Commit, "abc123", "key", "a@b.com", 2000)
            .unwrap());

        let tombstones = db.get_all_tombstones().unwrap();
        assert_eq!(tombstones.len(), 1);
        assert_eq!(
            tombstones[0],
            types::TombstoneRecord {
                target_type: "commit".to_string(),
                target_value: "abc123".to_string(),
                key: "key".to_string(),
                timestamp: 2000,
                email: "a@b.com".to_string(),
            }
        );
    }

    #[test]
    fn test_set_clears_tombstone() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"v1\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        assert!(db
            .remove(&TargetType::Commit, "abc123", "key", "a@b.com", 2000)
            .unwrap());
        assert_eq!(db.get_all_tombstones().unwrap().len(), 1);

        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"v2\"",
            &ValueType::String,
            "a@b.com",
            3000,
        )
        .unwrap();

        assert_eq!(db.get_all_tombstones().unwrap().len(), 0);
        let result = db.get(&TargetType::Commit, "abc123", "key").unwrap();
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
        db.list_push(
            &TargetType::Commit,
            "abc123",
            "tags",
            "first",
            "a@b.com",
            1000,
        )
        .unwrap();
        db.list_push(
            &TargetType::Commit,
            "abc123",
            "tags",
            "second",
            "a@b.com",
            2000,
        )
        .unwrap();
        let entry = db
            .get(&TargetType::Commit, "abc123", "tags")
            .unwrap()
            .unwrap();
        assert_eq!(entry.value_type, ValueType::List);
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["first", "second"]);
    }

    #[test]
    fn test_set_list_stores_rows_in_list_values_table() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "tags",
            r#"[{"value":"a","timestamp":1000},{"value":"b","timestamp":1001}]"#,
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

        let entry = db
            .get(&TargetType::Commit, "abc123", "tags")
            .unwrap()
            .unwrap();
        assert_eq!(entry.value_type, ValueType::List);
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["a", "b"]);
    }

    #[test]
    fn test_set_list_replaces_existing_list_rows() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "tags",
            r#"[{"value":"a","timestamp":1000},{"value":"b","timestamp":1001}]"#,
            &ValueType::List,
            "a@b.com",
            2000,
        )
        .unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
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

        let entry = db
            .get(&TargetType::Commit, "abc123", "tags")
            .unwrap()
            .unwrap();
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["c"]);
    }

    #[test]
    fn test_list_push_converts_string() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"original\"",
            &ValueType::String,
            "a@b.com",
            1000,
        )
        .unwrap();
        db.list_push(
            &TargetType::Commit,
            "abc123",
            "key",
            "appended",
            "a@b.com",
            2000,
        )
        .unwrap();
        let entry = db
            .get(&TargetType::Commit, "abc123", "key")
            .unwrap()
            .unwrap();
        assert_eq!(entry.value_type, ValueType::List);
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["original", "appended"]);
    }

    #[test]
    fn test_list_pop() {
        let db = Store::open_in_memory().unwrap();
        db.list_push(&TargetType::Commit, "abc123", "tags", "a", "a@b.com", 1000)
            .unwrap();
        db.list_push(&TargetType::Commit, "abc123", "tags", "b", "a@b.com", 2000)
            .unwrap();
        db.list_pop(&TargetType::Commit, "abc123", "tags", "b", "a@b.com", 3000)
            .unwrap();
        let entry = db
            .get(&TargetType::Commit, "abc123", "tags")
            .unwrap()
            .unwrap();
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["a"]);
    }

    #[test]
    fn test_apply_tombstone_removes_list_values_rows() {
        let db = Store::open_in_memory().unwrap();
        db.list_push(&TargetType::Commit, "abc123", "tags", "a", "a@b.com", 1000)
            .unwrap();
        db.list_push(&TargetType::Commit, "abc123", "tags", "b", "a@b.com", 2000)
            .unwrap();

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

        db.apply_tombstone(
            &TargetType::Commit,
            "abc123",
            "tags",
            "user@example.com",
            3000,
        )
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
        assert_eq!(db.get(&TargetType::Commit, "abc123", "tags").unwrap(), None);
    }

    #[test]
    fn test_authorship() {
        let db = Store::open_in_memory().unwrap();
        db.set(
            &TargetType::Commit,
            "abc123",
            "key",
            "\"val\"",
            &ValueType::String,
            "user@example.com",
            42000,
        )
        .unwrap();
        let auth = db
            .get_authorship(&TargetType::Commit, "abc123", "key")
            .unwrap()
            .unwrap();
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

        // set stores the timestamp
        db.set(
            &TargetType::Commit,
            "abc123",
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
            &TargetType::Commit,
            "abc123",
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
        db.list_push(
            &TargetType::Commit,
            "abc123",
            "tags",
            "first",
            "a@b.com",
            11000,
        )
        .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.key == "tags").unwrap();
        assert_eq!(tags.last_timestamp, 11000);

        // list_pop updates the timestamp
        db.list_push(
            &TargetType::Commit,
            "abc123",
            "tags",
            "second",
            "a@b.com",
            12000,
        )
        .unwrap();
        db.list_pop(
            &TargetType::Commit,
            "abc123",
            "tags",
            "second",
            "a@b.com",
            13000,
        )
        .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.key == "tags").unwrap();
        assert_eq!(tags.last_timestamp, 13000);
    }
}
