# Lists

Lists in git-meta are append-friendly multi-value collections.

They are best thought of as an ordered append log of string entries, not as a general-purpose mutable array.

Examples:

- agent transcript jsonl chunks
- review comments
- a sequence of notes appended over time

## Intended semantics

The current list design is optimized for the most common concurrent operation: one or more users append new values to the same logical list.

We serialize lists as many small blobs rather than one JSON array blob so we can use normal Git tree merging tooling to avoid array-level conflicts.

## Serialized tree shape

For a list value, entries are written under:

`[base]/<key segments>/__list/<entry-id>`

Where `<entry-id>` is of the form:

`<timestamp-ms>-<content-hash-prefix>`

Examples:

- `branch/06/sc-branch-1-deadbeef/agent/chat/__list/1771232450203-23c0f`
- `path/src/metrics/__target__/comments/__list/1771232450203-23c0f`

Each blob contains one string entry.

If multiple values are inserted in one command, start from one base millisecond timestamp and increment by 1 for each additional entry so local order is preserved.

## Ordering

List order is defined by sorting `<entry-id>` lexically, which is equivalent to sorting by:

1. timestamp ascending
2. hash suffix as a stable tie-breaker

This gives deterministic ordering after merges.

## Duplicate values

Lists allow duplicate string values.

If the same text is appended twice at different times, both entries remain.

This is intentional and differentiates lists from sets.

## Tombstones

### Whole-key removal

If the entire list key is removed, write a tombstone at:

`[base]/__tombstones/<key segments>/__deleted`

This means the logical list key has been removed.

### Entry removal

Removing individual entries in a list after the fact can be done by adding a tombstone to individual entry ids.

`[base]/<key segments>/__tombstones/<entry-id>/__deleted`

Where the `__deleted` blob is the original contents of the entry.

Because `__tombstones` is shared across collection types, serialize and materialize must ignore incompatible child tombstones for the current key type. For example, if a key currently materializes as a list, only tombstones that are valid list entry identifiers should affect the visible list state; incompatible child tombstones should be preserved but ignored.
