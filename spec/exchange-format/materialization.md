# Materialization and merge workflow

This document describes the exchange-level rules for turning serialized metadata commits back into local current state.

For higher-level coordination with a version control system's push and pull operations, see [Workflow](../implementation/workflow.md).

## Materialize responsibilities

When materializing a remote metadata head, gmeta must:

1. merge that head into local metadata history
2. resolve merge conflicts according to value-type semantics
3. update the local SQLite database with newly visible values and tombstones
4. record when materialization succeeded

Per-type conflict rules are defined in:

- [Strings](./strings.md)
- [Lists](./lists.md)
- [Sets](./sets.md)

## High-level scenarios

### 1. Initial sync

If the local system has no metadata yet:

- walk the tree at the incoming metadata head
- materialize all visible values into SQLite
- record that metadata commit as materialized

No metadata history walk is required beyond the current tree.

### 2. Multiple start points

If local metadata exists and someone else already pushed an independently initialized metadata history:

- serialize local shareable state
- perform a baseless two-way merge with the remote tree
- for overlapping keys, remote wins
- write a new metadata commit with the remote head as parent
- retry if the remote advanced before push completed

This allows eventual convergence while keeping history linear.

### 3. Fast-forward update

If the incoming metadata ref is a fast-forward from the last materialized point:

- diff the old materialized tree against the new tree
- apply only changed paths to SQLite
- add or update visible values
- apply tombstones
- record the new materialized commit

This is the common efficient update path.

### 4. Both sides mutated data

If local data changed and remote data also changed:

- serialize local current shareable state
- merge the remote tree into it
- resolve conflicts according to per-type semantics
- write a new metadata commit from the merged tree
- retry against newer remote heads until a fast-forward push succeeds

## No common ancestor merge

If local and remote metadata histories have no common ancestor, materialize uses a two-way merge instead of a three-way merge.

Policy:

1. union non-conflicting keys from both sides
2. for overlapping keys or value-vs-tombstone conflicts, remote wins
3. retain non-overlapping keys from both sides

`gmeta materialize --dry-run` should report this explicitly, for example:

- `strategy=two-way-no-common-ancestor`

and should print key-level conflict decisions.

## Removal handling

Deletion is explicit.

During materialization:

- whole-key tombstones remove keys locally and are recorded in SQLite
- per-entry or per-member tombstones are applied according to the collection type

### Legacy compatibility

During fast-forward materialization, implementations may also detect keys that existed in the previous local tree but are missing from the incoming tree and remove them locally for compatibility with old metadata histories that predate tombstones.

This should be treated as a compatibility bridge, not the long-term deletion model.

## Serialization after materialization

After a successful materialization, later serialization can use local bookkeeping to update only values changed since the last materialized point.

This is an optimization, not a semantic requirement.

## Metadata history shape

Unlike source-code history, metadata history does not need rich branch/merge topology.

The preferred shape is linear history created by repeatedly:

- merging in memory
- writing a new commit
- fast-forward pushing

The main goal is convergence on current metadata state, not preserving meaningful branch structure.
