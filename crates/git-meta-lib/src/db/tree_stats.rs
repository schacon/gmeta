//! Cached per-tree key counts and sizes.
//!
//! Auto-prune needs the size of the serialized tree on every serialize, and
//! measuring it by walking costs more as the tree grows — exactly when it is
//! asked most often. Git trees are content-addressed, so a measurement keyed by
//! tree OID can never go stale, and an incremental serialize reuses most of its
//! subtrees unchanged: only the subtrees along a changed path need remeasuring.

use rusqlite::{params, OptionalExtension};

use super::Store;
use crate::error::Result;

impl Store {
    /// Cached key count for a tree, if it has been measured before.
    pub fn tree_key_count(&self, tree_oid: &gix::oid) -> Result<Option<u64>> {
        self.tree_stat("key_count", tree_oid)
    }

    /// Cached blob-byte total for a tree, if it has been measured before.
    pub fn tree_byte_size(&self, tree_oid: &gix::oid) -> Result<Option<u64>> {
        self.tree_stat("byte_size", tree_oid)
    }

    /// Record a tree's key count.
    pub fn set_tree_key_count(&self, tree_oid: &gix::oid, count: u64, now: i64) -> Result<()> {
        self.set_tree_stat("key_count", tree_oid, count, now)
    }

    /// Record a tree's blob-byte total.
    pub fn set_tree_byte_size(&self, tree_oid: &gix::oid, bytes: u64, now: i64) -> Result<()> {
        self.set_tree_stat("byte_size", tree_oid, bytes, now)
    }

    /// Drop cache rows untouched since `cutoff_ms`.
    ///
    /// Every serialize introduces new tree OIDs along the paths it changed, so
    /// this table grows with history. The entries are a pure cache: dropping
    /// them costs a remeasure, never correctness.
    pub fn prune_tree_stats_before(&self, cutoff_ms: i64) -> Result<u64> {
        let removed = self.conn.execute(
            "DELETE FROM tree_stats WHERE last_used < ?1",
            params![cutoff_ms],
        )?;
        Ok(removed as u64)
    }

    /// Rows currently held in the cache.
    pub fn count_tree_stats(&self) -> Result<u64> {
        let count = self
            .conn
            .query_row("SELECT COUNT(*) FROM tree_stats", [], |row| {
                row.get::<_, i64>(0)
            })?;
        Ok(count as u64)
    }

    fn tree_stat(&self, column: &str, tree_oid: &gix::oid) -> Result<Option<u64>> {
        // `column` is never caller-supplied: both call sites pass a literal.
        let sql = format!("SELECT {column} FROM tree_stats WHERE tree_oid = ?1");
        let value: Option<Option<i64>> = self
            .conn
            .query_row(&sql, params![tree_oid.to_string()], |row| row.get(0))
            .optional()?;
        Ok(value.flatten().and_then(|v| u64::try_from(v).ok()))
    }

    fn set_tree_stat(&self, column: &str, tree_oid: &gix::oid, value: u64, now: i64) -> Result<()> {
        let sql = format!(
            "INSERT INTO tree_stats (tree_oid, {column}, last_used) VALUES (?1, ?2, ?3)
             ON CONFLICT(tree_oid) DO UPDATE SET {column} = ?2, last_used = ?3"
        );
        self.conn.execute(
            &sql,
            params![
                tree_oid.to_string(),
                i64::try_from(value).unwrap_or(i64::MAX),
                now
            ],
        )?;
        Ok(())
    }
}
