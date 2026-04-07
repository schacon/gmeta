use rusqlite::params;

use crate::error::Result;

use super::{escape_like_pattern, Db};
use crate::types::TargetType;

impl Db {
    /// Get value size distribution as a histogram.
    /// Buckets are powers-of-2 byte ranges for inline (non-git-ref) string values,
    /// plus a separate count for git-ref values (size unknown without blob lookup).
    /// Returns (buckets, git_ref_count) where buckets is Vec<(label, count)>.
    pub fn stats_value_size_histogram(&self) -> Result<(Vec<(String, u64)>, u64)> {
        // Fetch all inline string value lengths
        let mut stmt = self.conn.prepare(
            "SELECT LENGTH(value) FROM metadata WHERE is_git_ref = 0 AND value_type = 'string'",
        )?;
        let lengths: Vec<usize> = stmt
            .query_map([], |row| row.get::<_, i64>(0))?
            .filter_map(|r| r.ok())
            .map(|n| n as usize)
            .collect();

        let git_ref_count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM metadata WHERE is_git_ref = 1",
            [],
            |row| row.get(0),
        )?;

        // Buckets: <64B, 64B-1KB, 1KB-4KB, 4KB-16KB, 16KB-64KB, 64KB+
        let boundaries: &[(usize, &str)] = &[
            (64, "<64B"),
            (1024, "64B\u{2013}1KB"),
            (4096, "1KB\u{2013}4KB"),
            (16384, "4KB\u{2013}16KB"),
            (65536, "16KB\u{2013}64KB"),
            (usize::MAX, "64KB+"),
        ];

        let mut counts = vec![0u64; boundaries.len()];
        for len in &lengths {
            for (i, (limit, _)) in boundaries.iter().enumerate() {
                if len < limit {
                    counts[i] += 1;
                    break;
                }
            }
        }

        let buckets = boundaries
            .iter()
            .zip(counts.iter())
            .map(|((_, label), count)| (label.to_string(), *count))
            .collect();

        Ok((buckets, git_ref_count))
    }

    /// Get total count of values stored in SQLite vs as git blob refs.
    /// Returns (sqlite_count, git_ref_count).
    pub fn stats_storage_counts(&self) -> Result<(u64, u64)> {
        let sqlite_count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM metadata WHERE is_git_ref = 0",
            [],
            |row| row.get(0),
        )?;
        let git_ref_count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM metadata WHERE is_git_ref = 1",
            [],
            |row| row.get(0),
        )?;
        Ok((sqlite_count, git_ref_count))
    }

    /// Get counts grouped by target_type and key.
    /// Returns (target_type, key, count).
    pub fn stats_by_target_type_and_key(&self) -> Result<Vec<(String, String, u64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, key, COUNT(*) as cnt
             FROM metadata
             GROUP BY target_type, key
             ORDER BY target_type, cnt DESC, key",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u64>(2)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all unique (target_type, target_value, key) triples.
    pub fn get_all_keys(&self) -> Result<Vec<(String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key FROM metadata WHERE is_promised = 0 ORDER BY target_type, target_value, key",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Find distinct target_values that start with `prefix` for a given target_type.
    /// Returns at most `limit` matches.
    pub fn find_target_values_by_prefix(
        &self,
        target_type: &TargetType,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let escaped = escape_like_pattern(prefix);
        let pattern = format!("{}%", escaped);
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT target_value FROM metadata
             WHERE target_type = ?1 AND target_value LIKE ?2 ESCAPE '\\'
             ORDER BY target_value
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(
            params![target_type.as_str(), pattern, limit as i64],
            |row| row.get::<_, String>(0),
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}
