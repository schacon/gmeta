# Serialization

This document describes how to serialize metadata into Git primatives that can be transferred via normal Git remote protocols.

[Interactive demo of the serialization format](../other/example-serialize.html)

## Commit model

Serialization writes a Git commit whose tree contains the current shareable metadata state.

- tree pointer
- author identity
- author date / commit date
- ancestry for incremental materialization and merging

## Tree root layout

The base tree path for a target is target-type dependent:

- `commit` → `<target-type>/<first2-of-sha>/<full-target-value>`
- `branch` / `change-id` → `<target-type>/<fanout>/<full-target-value>`
- `path` → `path/<escaped path segments...>/__target__`
- `project` → `project`

Fanout is target-type dependent:

- for `commit`, use the first 2 characters of the commit SHA
- for `path`, do not hash the target value; serialize the path segments directly
- for anything else (`branch`, `change-id`), use the first 2 hexadecimal characters of the SHA-1 hash of the target value (ie, the branch name or change-id value)

Examples:

- `commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/...`
- `branch/06/sc-branch-1-deadbeef/...`
- `path/src/metrics/__target__/owner/__value`

For `path` targets, a reserved separator component `__target__` marks the end of the target path and the beginning of key segments.
If a serialized path segment would begin with `__`, it must be escaped by prefixing it with `~`.
To keep this reversible, a path segment beginning with `~` should also be escaped by prefixing it with `~`.
Examples:

- raw path segment `__generated` → serialized as `~__generated`
- raw path segment `~scratch` → serialized as `~~scratch`

This keeps commit paths readable, preserves human-readable path targets, and still avoids ambiguity between serialized path targets and metadata structure.

## Key path layout

Under the target base path, key segments are serialized directly as path components.

Metadata structure begins when a reserved `__*` component is encountered.

Examples:

- string: `<base>/agent/model/__value`
- list: `<base>/agent/chat/__list/<entry-id>`
- whole-key tombstone: `<base>/__tombstones/agent/model/__deleted`

## Per-type layouts

Per-type layouts are defined in:

- [Strings](./strings.md)
- [Lists](./lists.md)
- [Sets](./sets.md)

## Key path reservation

Keys are serialized directly under the target base path.

Any path component beginning with `__` is reserved for git-meta structural paths such as:

- `__value`
- `__list`
- `__set`
- `__tombstones`
- `__target__` (used only as the separator inside serialized `path` targets)

This means user keys occupy normal path segments and metadata structure begins when a reserved `__*` path component is encountered.

## Path target encoding

`path` targets are serialized using their raw path segments rather than a hash-derived fanout prefix.

Shape:

`path/<escaped path segments...>/__target__/...`

Rules:

- each `/`-separated path component becomes one tree path component
- `__target__` marks the end of the target path and the beginning of key segments
- if a path segment begins with `__`, escape it by prefixing the segment with `~`
- if a path segment begins with `~`, also escape it by prefixing the segment with `~`

Examples:

- `path:src/metrics` → `path/src/metrics/__target__/...`
- `path:src/__generated/file.rs` → `path/src/~__generated/file.rs/__target__/...`
- `path:src/~scratch/file.rs` → `path/src/~~scratch/file.rs/__target__/...`

## Explicit deletion

Exchange must not assume that missing paths mean deletion.

Reasons:

- sparse or pruned trees may omit data
- multiple metadata refs may represent different subsets
- absence should not be ambiguous

Intentional deletion is represented by explicit tombstones.

A single reserved `__tombstones` namespace is used for both whole-key and child-level deletions. Child tombstones are interpreted relative to the current key type. Serialize and materialize must ignore incompatible child tombstones for the current type rather than treating them as errors or as deletions for another collection model.
