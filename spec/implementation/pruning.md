# Auto-pruning

This document describes how to configure automatic pruning rules that are evaluated during serialization.

## Overview

Auto-pruning allows a project to declare rules that trigger a prune commit automatically after serialization. Rules are stored as ordinary project-level metadata under the `meta:prune:` key namespace. Because project metadata is never itself pruned and travels with the metadata ref, all collaborators share the same pruning policy.

Auto-pruning is a **high-water / low-water** rule. When the serialized tree grows past a configured maximum, it is cut back to the corresponding minimum by dropping the least recently modified keys, and then left alone until it grows past the maximum again.

Age is deliberately not part of auto-pruning. A retention window cannot promise to bring a tree under a size limit — if every key is recent, nothing is dropped — so a tree over its limit would attempt a prune on every single serialize and never come down. Date-based pruning is available as the manual `git meta prune --since`, where the caller chooses the window and can see what it removes.

## Configuration keys

All keys are stored as `string` values on the `project` target. At least one of `meta:prune:max-keys` or `meta:prune:max-size` must be set for auto-pruning to activate.

### `meta:prune:max-keys`

An integer. When the number of metadata keys in the serialized tree exceeds this value, a prune is triggered.

Example: `10000`

### `meta:prune:min-keys`

An integer. The key count to prune back down to. Must be below `max-keys`.

If `max-keys` is set without `min-keys`, the minimum defaults to half of the maximum (never less than 1).

Example: `5000`

### `meta:prune:max-size`

A size threshold. When the total size of all blobs in the serialized tree exceeds this value, a prune is triggered.

Accepts human-friendly suffixes (`512k`, `10m`, `1g`).

Example: `50m`

### `meta:prune:min-size`

The total size to prune back down to. Must be below `max-size`. Defaults to half of `max-size`.

Accepts the same suffixes.

Example: `25m`

The gap between a maximum and its minimum is what makes auto-pruning affordable: it buys room for further growth, so pruning happens once every so often rather than on every serialize. A minimum close to its maximum will prune often; a distant one prunes rarely but discards more each time.

Note that the maximum counts every key in the tree, including the project-level configuration keys themselves, while the minimum applies only to the keys that can be dropped. Set `max-keys` comfortably above the number of `meta:` config keys a project holds.

## What is kept

When a prune runs, entries are ordered by their last modification time, most recent first, and kept until a floor is reached:

- **Project metadata is always retained** and does not count against a floor. It holds the prune rules themselves, so dropping it would discard the policy.
- Remaining keys are retained most-recently-modified first, stopping at `min-keys` and/or `min-size`, whichever is reached first.
- Ties are broken by target type, target value, then key, so the same input always produces the same tree.
- **Tombstones** are retained when they are at least as recent as the oldest surviving key. Older ones describe removals in a part of the tree that is no longer published.

A key is kept or dropped whole. A list that survives keeps all of its entries, however old the individual entries are; a list that is dropped is dropped entirely. If a single append-only list grows without bound, the size trigger will eventually drop the key that holds it.

## Evaluation during serialization

Serialization proceeds as follows when auto-prune rules are configured:

1. Normal tree serialization produces a commit as usual.
2. Read all `meta:prune:*` keys from the project metadata in SQLite.
3. If no maximum is configured, stop.
4. Evaluate each configured trigger against the just-written commit's tree:
   - `max-keys`: count the metadata keys in the tree.
   - `max-size`: total the size of all blob objects reachable from the tree.
5. If any trigger is exceeded, rebuild the tree from the entries that survive the retention rules above.
6. If the rebuilt tree differs from the one just written, it is committed on top, with the `git-meta: auto-prune` commit message.

If no trigger fires, serialization produces a single commit as before.

### Measurement caching

Counting keys and totalling blob sizes means walking the serialized tree, and that cost grows exactly as the tree does — while the check runs on every serialize. Implementations should cache these measurements keyed by tree object ID.

Tree object IDs are content hashes, so a cached measurement can never go stale. An incremental serialize rewrites only the subtrees along the paths it changed and reuses every other subtree by ID, so a cached walk costs what changed rather than what exists.

The reference implementation stores these in a `tree_stats` table alongside the metadata, and treats them as a pure cache: discarding rows costs a re-measurement, never correctness.

## Manual pruning

`git meta prune` prunes by date and is never triggered automatically:

```
git meta prune --since 90d          # drop keys not modified in the last 90 days
git meta prune --since 2025-01-01   # drop keys not modified since a date
git meta prune --dry-run --since 6m # report what would go
```

If `--since` is omitted, the project's `meta:prune:since` value is used as a default when one is set. That key has no effect on auto-pruning.

`git meta local-prune --since <window>` does the same for the local SQLite store. Pruning the tree does not shrink the database: the rows remain and a later serialize would republish them. Publishing a smaller tree durably means running both.

## The `git meta config` command

A convenience command for managing project-level `meta:*` keys.

```
git meta config <key> <value>       # set a config key
git meta config <key>               # get a config key
git meta config --list              # list all meta:* config keys
git meta config --unset <key>       # remove a config key
```

Examples:

```
git meta config meta:prune:max-keys 10000
git meta config meta:prune:min-keys 5000
git meta config meta:prune:max-size 50m
git meta config meta:prune:min-size 25m

git meta config meta:prune:max-keys
# → 10000

git meta config --list
# → meta:prune:max-keys = 10000
# → meta:prune:min-keys = 5000
# → meta:prune:max-size = 50m
# → meta:prune:min-size = 25m

git meta config --unset meta:prune:max-keys
```

Under the hood, `git meta config` operates on the `project` target with string values:

- `git meta config <key> <value>` is equivalent to `git meta set project <key> <value>`
- `git meta config <key>` is equivalent to `git meta get project <key>`
- `git meta config --list` queries all project keys matching `meta:*`
- `git meta config --unset <key>` is equivalent to `git meta rm project <key>`

All keys set via `git meta config` must start with `meta:`. This keeps the config namespace distinct from user-defined project metadata.

## Interaction with manual prune

Running `git meta prune --since <window>` manually remains supported and is the only way to prune by date. Auto-pruning does not interfere — if a manual prune has brought the tree below the maximums, auto-pruning will not trigger.

## Interaction with materialization

Auto-prune rules materialized from a remote are immediately effective for subsequent local serializations. There is no special merge handling — the rules are ordinary string values and follow standard last-writer-wins conflict resolution.
