# gmeta — Scaling Issues

## Scaling Analysis

The following identifies where the current architecture will break down under load. Scenarios to consider:

- **Wide**: 100k commits, each with 5 keys = 500k metadata entries
- **Deep lists**: lists with 10k–100k items (e.g., chat transcripts, CI results)
- **High churn**: frequent mutations across many keys
- **Multi-user**: many collaborators exchanging metadata

### S1. Serialize is always O(total entries), never incremental

`serialize.rs:27-38` — Despite the "incremental" check, both code paths call `db.get_all_metadata()` which loads every row in the database into a Vec. With 500k entries, this means:

- Full table scan of `metadata`
- ~500k rows deserialized into Rust tuples
- ~500k `serde_json::from_str` calls to decode each value
- A `BTreeMap<String, Vec<u8>>` built with 500k+ entries (more for lists — each list item is a separate entry)
- Every value written as an individual `repo.blob()` call (500k+ ODB writes)
- The entire nested `Dir` tree rebuilt from scratch in memory

Even if you changed a single key, you pay the cost of the entire dataset. The `get_modified_since` check on line 29 only short-circuits when *nothing* changed — otherwise it falls through to the full rebuild.

**Impact**: Serialize time grows linearly with total metadata, regardless of how much actually changed. At 500k entries this will take seconds; at millions it could take minutes.

**Fix direction**: True incremental serialization — read the existing tree from the previous commit, apply only the changed entries using `TreeBuilder::new(repo, Some(&existing_tree))`, and write only modified subtrees.

### S2. Materialize loads all blob contents into memory

`materialize.rs:440-467` — `collect_blobs` recursively walks the entire Git tree and reads every blob's content into a `BTreeMap<String, Vec<u8>>`. For the three-way merge path, this happens **three times** (base, local, remote), holding all three in memory simultaneously.

With 500k entries averaging 100 bytes each, that's ~150MB for three copies. With large list items (chat transcripts, etc.) this could easily reach gigabytes.

**Impact**: Memory usage is 3x the total size of all metadata values. OOM risk with large datasets.

**Fix direction**: Stream the tree walk, comparing entries on-the-fly without materializing the entire tree. Or at minimum, only load blobs for entries that differ between trees.

### S3. metadata_log grows unbounded with no indexes

The `metadata_log` table gets a new row for every `set`, `push`, `pop`, and `rm` operation. There is no pruning, no retention policy, and **no indexes** on the table.

Queries that hit this table:
- `get_modified_since` (`db.rs:327`) — `WHERE ml.timestamp > ?1` with a JOIN to metadata: **full table scan**
- `get_authorship` (`db.rs:163`) — `ORDER BY timestamp DESC LIMIT 1`: **full table scan per key**
- `get_locally_modified_keys` (`db.rs:365`) — `WHERE timestamp > ?1`: **full table scan**

After 1M operations, every authorship lookup scans all 1M rows. `git meta get --json --with-authorship` for a target with 50 keys triggers 50 sequential full-table scans.

**Impact**: Authorship queries become O(log_size) per key. Get-with-authorship becomes O(keys × log_size). At 1M+ log entries, `git meta get --with-authorship` for a single target could take seconds.

**Fix direction**: Add indexes:
```sql
CREATE INDEX idx_log_key ON metadata_log(target_type, target_value, key, timestamp DESC);
CREATE INDEX idx_log_timestamp ON metadata_log(timestamp);
```
Add a log retention/pruning policy (e.g., keep only the last N entries per key, or entries newer than X).

### S4. update_db_from_tree does per-row SELECT + writes with no batching

`materialize.rs:135-162` — For each entry in the merged tree, the code:
1. Calls `serde_json::to_string` to encode the value
2. Calls `db.get()` — a SELECT query — to check if it already exists
3. If different, calls `db.set()` — which writes metadata + log in one transaction

With 500k entries, that's 500k SELECTs + up to 1M INSERTs, and still up to 500k transactions (one per changed key). This is better than statement-level autocommit, but transaction-per-row still pays heavy commit/fsync overhead.

**Impact**: Materialize of a large remote dataset is still extremely slow — potentially minutes at 500k entries due to transaction-per-row overhead.

**Fix direction**: Wrap the entire materialize DB update in a single transaction (`BEGIN`/`COMMIT`). This alone could give a 100x+ speedup for bulk inserts. Consider using prepared statements and batching.

### S5. Git object proliferation

Each serialize creates:
- 1 blob per string entry
- 1 blob per list item
- 1 tree object per directory node in the nested path structure
- 1 commit

With 500k entries and a deep path structure (`commit/13/0ae/{sha}/agent/model`), that's roughly 500k blobs + ~200k tree objects per serialize. Git's content-addressable storage deduplicates identical content, so unchanged values reuse existing blob OIDs. But the tree objects are all recreated in memory and written, even if they haven't changed.

Over time, with frequent serializations, the loose object count grows rapidly. `git gc` / `git repack` will need to run to keep the repository healthy.

**Impact**: Repository size grows, `git gc` becomes expensive, clone/fetch times increase. The `refs/meta/*` history accumulates commits even for trivial changes.

**Fix direction**: Use `TreeBuilder` with existing tree as base to avoid recreating unchanged subtrees. Consider squashing meta history periodically (since the spec says only current values matter for exchange).

### S6. List items stored as a single JSON blob in SQLite

Each `list_push` (`db.rs:204-251`):
1. SELECT the current value (entire JSON array as a string)
2. `serde_json::from_str` — parse the entire array
3. Push one element
4. `serde_json::to_string` — re-serialize the entire array
5. UPDATE the row with the new string

For a list with 50k items averaging 200 bytes each, that's ~10MB of JSON parsed and re-serialized on every single push. `list_pop` has the same problem, plus a linear scan to find the value to remove.

**Impact**: `list_push` degrades from O(1) to O(n) in list size. A 100k-item list could take hundreds of milliseconds per push.

**Fix direction**: Store list items in a separate table with one row per item, or use SQLite's JSON functions for in-place array modification.

### S7. No SQLite performance tuning

The code opens SQLite with default settings. Missing optimizations:
- **No WAL mode**: Default journal mode uses rollback journals, which serialize readers and writers. `PRAGMA journal_mode=WAL` would allow concurrent reads during writes.
- **No synchronous tuning**: Default `synchronous=FULL` fsyncs on every commit. `synchronous=NORMAL` with WAL is safe and much faster.
- **No page cache sizing**: Default cache is small. `PRAGMA cache_size=-8000` (8MB) would help with large datasets.
- **No busy timeout**: Multiple processes (e.g., concurrent `git meta set` calls) will get immediate `SQLITE_BUSY` errors instead of retrying.

**Impact**: Poor throughput under any concurrent access. Unnecessary I/O overhead for every operation.

### S8. find_remote_refs iterates ALL refs in the repository

`materialize.rs:357` — `repo.references()` returns an iterator over **every ref** in the repository (branches, tags, notes, stashes, everything). It then filters by prefix with string comparison.

In a large monorepo with 50k+ refs (branches, tags, PRs), this iterates all of them to find the handful under `refs/meta/`.

**Impact**: Materialize startup time scales with total ref count, not meta ref count.

**Fix direction**: Use `repo.references_glob("refs/meta/*")` or the equivalent prefix-filtered iterator from libgit2.

### S9. No pagination for git meta get

`git meta get <target>` with no key returns all key/value pairs for a target. If a target has thousands of keys, this dumps everything to stdout in one shot. The JSON variant builds the entire nested structure in memory before printing.

**Impact**: Unusable output for targets with many keys. Memory spike for JSON rendering.

**Fix direction**: Add `--limit`/`--offset` options or a streaming output mode.

### S10. Quadratic behavior in three-way merge key collection

`materialize.rs:189-192` — Collects all keys from base, local, and remote into a `BTreeMap`. While BTreeMap insertion is O(log n), the three chains allocate and compare composite keys `(String, String, String)`. With 500k entries across three maps, this creates up to 1.5M key comparisons with string cloning.

The merge itself then does 3 lookups per key (one into each BTreeMap), so with 500k unique keys that's 1.5M BTreeMap lookups.

**Impact**: Merge CPU time grows as O(n log n) in total entries. Manageable but not cheap — the constant factor is high due to string allocation and comparison.

### Summary table

| Issue | Threshold | Symptom |
|-------|-----------|---------|
| S1. Full serialize | >50k entries | Serialize takes seconds, wastes I/O |
| S2. Full tree in memory | >100k entries or large values | High memory usage, OOM |
| S3. Unindexed log | >100k log rows | Authorship queries slow to seconds |
| S4. Unbatched DB writes | >10k entries | Materialize takes minutes |
| S5. Object proliferation | >100k entries, frequent serialize | Repo bloat, slow gc |
| S6. List as JSON blob | >1k items per list | Push/pop degrades to O(n) |
| S7. No SQLite tuning | Any concurrent access | SQLITE_BUSY errors, slow writes |
| S8. All-refs iteration | >10k total refs in repo | Slow materialize startup |
| S9. No pagination | >1k keys per target | Unusable output |
| S10. Merge key collection | >100k entries | Merge takes seconds |
