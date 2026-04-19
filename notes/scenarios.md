# gmeta refs transfer scenarios

This document lists 12 interesting scenarios for exchanging metadata through gmeta refs, and what the current project does in each case.

## 1) Simple (from notes.md)

User A sets a key, serializes, and pushes metadata refs. User B fetches and materializes, then updates that key and adds another key, serializes, and pushes. User A then adds a third key, fetches, and materializes.

How gmeta handles it:
- Materialize merges remote metadata into `refs/<namespace>/local` and updates SQLite.
- Non-conflicting keys from both sides are preserved.
- If both sides touched the same string key before convergence, the later metadata commit timestamp wins.

## 2) Custom namespace / ref routing

One repo uses a non-default namespace (for example `meta.namespace=notes`) or wants to materialize from only one remote metadata ref.

How gmeta handles it:
- Serialize writes to `refs/<namespace>/local`.
- Materialize scans `refs/<namespace>/*` (excluding `local`).
- `git meta materialize <remote>` narrows to `refs/<namespace>/<remote>`.

## 3) No-op materialize cases

A user runs materialize when no remote metadata refs exist, or when local and remote metadata refs already point at the same commit.

How gmeta handles it:
- If no refs are found: prints `no remote metadata refs found` and exits.
- If a remote ref is already identical: reports it is already up to date and skips work.

## 4) Fast-forward with only additive/updated remote data

Local metadata history is an ancestor of the remote metadata ref, and remote only added/updated values.

How gmeta handles it:
- Uses fast-forward strategy.
- Applies remote tree values into SQLite.
- Moves local metadata ref directly to the remote commit.

## 5) Fast-forward where remote removed keys

Remote removed keys and serialized tombstones.

How gmeta handles it:
- Tombstones are materialized into SQLite via `apply_tombstone`.
- Existing local values for those keys are deleted.
- Tombstone metadata (`timestamp`, `email`) is preserved.

## 6) Legacy or partial trees (missing tombstones / unexpected paths)

Remote metadata was created by older tooling (missing tombstones) or contains entries that do not match expected key/value path shapes.

How gmeta handles it:
- For legacy missing tombstones in fast-forward: if a key existed in previous local tree but is absent in incoming tree, gmeta removes it locally.
- Unrecognized or invalid paths/blob payloads are ignored during parsing instead of aborting the whole materialization.

## 7) Both sides modified the same string key

In a three-way merge, local and remote both changed the same existing string key.

How gmeta handles it:
- Conflict is resolved by commit timestamp: later metadata commit wins.
- If timestamps tie, local side wins (`remote_timestamp > local_timestamp` is required for remote to win).
- Conflict is reported in `--dry-run` output.

## 8) Concurrent add of the same key

The key did not exist in base, and both local and remote independently added it with different values.

How gmeta handles it:
- Treated as a conflict (`reason=concurrent-add`).
- Resolved with the same timestamp rule used for string/type conflicts.
- Selected value is materialized to SQLite.

## 9) One side removed, the other side modified

A key exists in base; one side deletes it while the other side edits it.

How gmeta handles it:
- Modified value wins over removal.
- The key remains as a value in merged state (not tombstoned).
- Conflict reason is reported as local-modified-remote-removed or remote-modified-local-removed.

## 10) Concurrent list edits

Both sides changed a list key (for example both pushed entries, or one pushed while the other changed list contents).

How gmeta handles it:
- List/list conflicts merge by union of list entry names (`timestamp-hash`).
- Exact duplicate entry names de-duplicate naturally.
- If the same entry name exists with different content (rare collision), local entry wins because remote uses `or_insert`.

## 11) No common ancestor between metadata histories

Two users initialized metadata refs independently and later exchange refs.

How gmeta handles it:
- Materialize switches to `two-way-no-common-ancestor` strategy.
- Union of keys is kept.
- For overlapping conflicts (value vs value, value vs tombstone, etc.), remote state wins.
- Reported explicitly in output and dry-run conflict details.

## 12) Dry-run safety before applying transfer

A user wants to inspect what materialize would do before changing refs/SQLite.

How gmeta handles it:
- `git meta materialize --dry-run` prints strategy, conflict resolutions, and planned SQLite operations.
- It does not mutate SQLite.
- It does not move `refs/<namespace>/local`.
