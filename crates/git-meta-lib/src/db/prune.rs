//! Prune helpers exposed via the `internal` feature for the CLI's `prune` and
//! `import` commands. None of these methods are used elsewhere in the library,
//! so the entire module is gated on the feature at its declaration site.

use rusqlite::params;

use crate::error::Result;

use super::Store;

impl Store {
    /// Count metadata rows that would be pruned (non-project, older than cutoff).
    pub fn count_metadata_before(&self, cutoff_ms: i64) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM metadata
             WHERE target_type != 'project' AND last_timestamp < ?1",
            params![cutoff_ms],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Count list_values rows that would be pruned (older than cutoff, non-project parent).
    pub fn count_list_values_before(&self, cutoff_ms: i64) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM list_values
             WHERE timestamp < ?1
               AND metadata_id IN (
                   SELECT rowid FROM metadata WHERE target_type != 'project'
               )",
            params![cutoff_ms],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Count tombstone rows that would be pruned (non-project, older than cutoff).
    pub fn count_tombstones_before(&self, cutoff_ms: i64) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM tombstones
             WHERE tombstone_type = 'metadata'
               AND target_type != 'project' AND timestamp < ?1",
            params![cutoff_ms],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Count set tombstone rows that would be pruned (non-project, older than cutoff).
    pub fn count_set_tombstones_before(&self, cutoff_ms: i64) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM tombstones
             WHERE tombstone_type = 'set_member'
               AND target_type != 'project' AND timestamp < ?1",
            params![cutoff_ms],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Count log entries that would be pruned (non-project, older than cutoff).
    pub fn count_log_entries_before(&self, cutoff_ms: i64) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM metadata_log
             WHERE target_type != 'project' AND timestamp < ?1",
            params![cutoff_ms],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Count metadata rows that will survive pruning.
    pub fn count_metadata_remaining(&self, cutoff_ms: i64) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM metadata
             WHERE target_type = 'project' OR last_timestamp >= ?1",
            params![cutoff_ms],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Count list_values rows that will survive pruning.
    pub fn count_list_values_remaining(&self, cutoff_ms: i64) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM list_values
             WHERE timestamp >= ?1
                OR metadata_id IN (
                    SELECT rowid FROM metadata WHERE target_type = 'project'
                )",
            params![cutoff_ms],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Prune metadata and associated child rows older than cutoff.
    ///
    /// Deletes list_values and set_values for pruned metadata rows,
    /// then deletes old list entries from surviving lists,
    /// then deletes the metadata rows themselves.
    /// Returns the number of metadata rows deleted.
    pub fn prune_metadata_before(&self, cutoff_ms: i64) -> Result<u64> {
        // Delete list_values for metadata rows being pruned
        self.conn.execute(
            "DELETE FROM list_values
             WHERE metadata_id IN (
                 SELECT rowid FROM metadata
                 WHERE target_type != 'project' AND last_timestamp < ?1
             )",
            params![cutoff_ms],
        )?;

        // Delete set_values for metadata rows being pruned
        self.conn.execute(
            "DELETE FROM set_values
             WHERE metadata_id IN (
                 SELECT rowid FROM metadata
                 WHERE target_type != 'project' AND last_timestamp < ?1
             )",
            params![cutoff_ms],
        )?;

        // Delete old list entries from lists that survive
        self.conn.execute(
            "DELETE FROM list_values
             WHERE timestamp < ?1
               AND metadata_id IN (
                   SELECT rowid FROM metadata WHERE target_type != 'project'
               )",
            params![cutoff_ms],
        )?;

        // Delete the metadata rows themselves
        let deleted = self.conn.execute(
            "DELETE FROM metadata
             WHERE target_type != 'project' AND last_timestamp < ?1",
            params![cutoff_ms],
        )?;

        Ok(deleted as u64)
    }

    /// Prune tombstone rows older than cutoff. Returns the number deleted.
    pub fn prune_tombstones_before(&self, cutoff_ms: i64) -> Result<u64> {
        let deleted = self.conn.execute(
            "DELETE FROM tombstones
             WHERE tombstone_type = 'metadata'
               AND target_type != 'project' AND timestamp < ?1",
            params![cutoff_ms],
        )?;
        Ok(deleted as u64)
    }

    /// Prune set tombstone rows older than cutoff. Returns the number deleted.
    pub fn prune_set_tombstones_before(&self, cutoff_ms: i64) -> Result<u64> {
        let deleted = self.conn.execute(
            "DELETE FROM tombstones
             WHERE tombstone_type = 'set_member'
               AND target_type != 'project' AND timestamp < ?1",
            params![cutoff_ms],
        )?;
        Ok(deleted as u64)
    }

    /// Prune log entries older than cutoff. Returns the number deleted.
    pub fn prune_log_before(&self, cutoff_ms: i64) -> Result<u64> {
        let deleted = self.conn.execute(
            "DELETE FROM metadata_log
             WHERE target_type != 'project' AND timestamp < ?1",
            params![cutoff_ms],
        )?;
        Ok(deleted as u64)
    }

    /// Load the set of trail IDs that have already been imported.
    ///
    /// Used by the import command to skip already-imported trails.
    pub fn imported_trail_ids(&self) -> Result<std::collections::HashSet<String>> {
        let mut ids = std::collections::HashSet::new();
        let mut stmt = self.conn.prepare(
            "SELECT value FROM metadata WHERE key = 'review:trail-id' AND target_type = 'branch'",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let val = row?;
            if let Ok(s) = serde_json::from_str::<String>(&val) {
                ids.insert(s);
            }
        }
        Ok(ids)
    }
}
