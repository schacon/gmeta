use anyhow::Result;
use rusqlite::Connection;

/// Current schema version.
const SCHEMA_VERSION: i32 = 1;

/// Run all pending migrations on the database.
pub(super) fn run_migrations(conn: &Connection) -> Result<()> {
    let version: i32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;

    if version < 1 {
        conn.execute_batch(MIGRATION_1)?;
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
CREATE INDEX IF NOT EXISTS idx_list_values_metadata ON list_values(metadata_id);
CREATE INDEX IF NOT EXISTS idx_set_values_metadata ON set_values(metadata_id);
";
