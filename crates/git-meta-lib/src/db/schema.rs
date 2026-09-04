use rusqlite::Connection;

use crate::error::Result;

/// Current schema version.
const SCHEMA_VERSION: i32 = 5;

/// Run all pending migrations on the database.
pub(super) fn run_migrations(conn: &Connection) -> Result<()> {
    let version: i32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;

    if version < 1 {
        conn.execute_batch(MIGRATION_1)?;
        conn.pragma_update(None, "user_version", 1)?;
    }
    if version < 2 {
        conn.execute_batch(MIGRATION_2)?;
        conn.pragma_update(None, "user_version", 2)?;
    }
    if version < 3 {
        conn.execute_batch(MIGRATION_3)?;
        conn.pragma_update(None, "user_version", 3)?;
    }
    if version < 4 {
        conn.execute_batch(MIGRATION_4)?;
        conn.pragma_update(None, "user_version", 4)?;
    }
    if version < 5 {
        conn.execute_batch(MIGRATION_5)?;
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    }

    Ok(())
}

/// Migration 1: Full baseline schema with unified tombstones and indexes.
const MIGRATION_1: &str = "
CREATE TABLE IF NOT EXISTS metadata (
    target_type TEXT NOT NULL,
    target_value TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    value_type TEXT NOT NULL,
    last_timestamp INTEGER NOT NULL DEFAULT 0,
    is_git_ref INTEGER NOT NULL DEFAULT 0,
    is_promised INTEGER NOT NULL DEFAULT 0,
    UNIQUE(target_type, target_value, key)
);

CREATE TABLE IF NOT EXISTS list_values (
    metadata_id INTEGER NOT NULL,
    value TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    is_git_ref INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS set_values (
    metadata_id INTEGER NOT NULL,
    member_id TEXT NOT NULL,
    value TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    UNIQUE(metadata_id, member_id)
);

CREATE TABLE IF NOT EXISTS tombstones (
    tombstone_type TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_value TEXT NOT NULL,
    key TEXT NOT NULL,
    entry_id TEXT NOT NULL DEFAULT '',
    value TEXT NOT NULL DEFAULT '',
    timestamp INTEGER NOT NULL,
    email TEXT NOT NULL,
    UNIQUE(tombstone_type, target_type, target_value, key, entry_id)
);

CREATE TABLE IF NOT EXISTS metadata_log (
    target_type TEXT NOT NULL,
    target_value TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    value_type TEXT NOT NULL,
    operation TEXT NOT NULL,
    email TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_materialized INTEGER
);

INSERT OR IGNORE INTO sync_state (id, last_materialized) VALUES (1, NULL);

CREATE INDEX IF NOT EXISTS idx_metadata_target ON metadata(target_type, target_value, key);
CREATE INDEX IF NOT EXISTS idx_tombstones_target ON tombstones(tombstone_type, target_type, target_value, key);
CREATE INDEX IF NOT EXISTS idx_metadata_log_lookup ON metadata_log(target_type, target_value, key);
CREATE INDEX IF NOT EXISTS idx_list_values_metadata_timestamp ON list_values(metadata_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_set_values_metadata ON set_values(metadata_id);
";

/// Migration 2: Track rows imported from non-primary metadata refs.
const MIGRATION_2: &str = "
ALTER TABLE metadata ADD COLUMN source_ref TEXT;
CREATE INDEX IF NOT EXISTS idx_metadata_source_ref ON metadata(source_ref);
";

/// Migration 3: Index `metadata_log` by timestamp.
///
/// Incremental serialization asks for everything written since the last
/// materialization marker. Without this index that predicate cannot seek, so
/// every serialize scans a table that gains a row per key write and is never
/// pruned in normal operation — making publish cost grow with the total number
/// of writes a repository has ever seen.
const MIGRATION_3: &str = "
CREATE INDEX IF NOT EXISTS idx_metadata_log_timestamp
    ON metadata_log(timestamp, target_type, target_value, key);
";

/// Migration 4: Cache per-tree key counts and sizes.
///
/// Auto-prune has to know how large the serialized tree is on every
/// serialize, and walking it to find out costs more as it grows — exactly
/// when it is asked most often. Git trees are content-addressed, so a count
/// keyed by tree OID never goes stale, and an incremental serialize reuses
/// most of its subtrees unchanged: only the subtrees on a changed path need
/// recounting.
const MIGRATION_4: &str = "
CREATE TABLE IF NOT EXISTS tree_stats (
    tree_oid TEXT PRIMARY KEY,
    key_count INTEGER,
    byte_size INTEGER,
    last_used INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tree_stats_last_used ON tree_stats(last_used);
";

/// Migration 5: Index metadata by recency.
///
/// Auto-prune keeps the most recently modified keys down to a floor. Without
/// this index that means reading and sorting every row in the store, so the
/// cost of publishing a bounded tree would grow with everything ever written.
/// With it, the rows arrive in the order the prune wants and it can stop as
/// soon as the floor is reached.
const MIGRATION_5: &str = "
CREATE INDEX IF NOT EXISTS idx_metadata_recency
    ON metadata(last_timestamp DESC, target_type, target_value, key);
";
