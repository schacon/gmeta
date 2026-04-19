# Strings

Strings are the simplest git-meta value type.

A string represents one current scalar value for a `(target, key)` pair.

Examples:

- `agent:model = claude-4.6`
- `owner = schacon`
- `review:signoff = schacon@gmail.com`

## Summary

- One current value per key
- Serialized as a single terminal blob
- Removal uses a key tombstone
- Concurrent updates resolve by last-writer-wins

## Serialized tree shape

For a string value, write the string to:

`[base]/<key segments>/__value`

Examples:

- `commit/13/<full-target>/agent/model/__value`
- `path/src/metrics/__target__/owner/__value`

The blob contents are the raw string value.

## Tombstones

If a string key is removed, serialize a tombstone at:

`[base]/__tombstones/<key segments>/__deleted`

The tombstone blob should be the same blob as the `__value` was. This way Git reuses the blob and we can do content comparisons if needed in cases of conflict.

When a string is set again after deletion, the tombstone is cleared locally and omitted from the next serialization.

## Merge semantics

Merging trees can technically use either `ours` or `theirs` strategies, but one value should win out. Projects or individuals can determine which they prefer.

The default in our implementation is `ours`, so that both values are in the history and reverting is possible.
