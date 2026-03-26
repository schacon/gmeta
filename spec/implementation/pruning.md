# Auto-pruning

This document describes how to configure automatic pruning rules that are evaluated during serialization.

## Overview

Auto-pruning allows a project to declare rules that trigger a prune commit automatically after serialization. Rules are stored as ordinary project-level metadata under the `meta:prune:` key namespace. Because project metadata is never itself pruned and travels with the metadata ref, all collaborators share the same pruning policy.

## Configuration keys

All keys are stored as `string` values on the `project` target.

### `meta:prune:since`

Required. The retention window used when pruning is triggered.

Accepts ISO-8601 dates (`2025-01-01`) or relative durations (`90d`, `6m`, `1y`).

This value is passed to the prune operation as the `--since` parameter.

### `meta:prune:max-keys`

Optional. An integer threshold. When the total number of metadata keys in the serialized tree exceeds this value, a prune is triggered.

Example: `10000`

### `meta:prune:max-size`

Optional. A size threshold. When the total size of all blobs in the serialized tree exceeds this value, a prune is triggered.

Accepts human-friendly suffixes (`512k`, `10m`, `1g`).

Example: `50m`

### `meta:prune:min-size`

Optional. Passed through to the prune operation as `--min-size`. Target subtrees smaller than this threshold are kept in full regardless of age.

Accepts human-friendly suffixes (`512k`, `10m`).

Example: `512k`

At least one of `meta:prune:max-keys` or `meta:prune:max-size` must be set alongside `meta:prune:since` for auto-pruning to activate. If neither trigger key is present, no auto-pruning occurs even if `meta:prune:since` is set.

## Evaluation during serialization

Serialization proceeds as follows when auto-prune rules are configured:

1. Normal tree serialization produces a commit as usual.
2. Read all `meta:prune:*` keys from the project metadata in SQLite.
3. If rules are incomplete (no `since`, or neither `max-keys` nor `max-size`), stop.
4. Evaluate each trigger against the just-written commit's tree:
   - `max-keys`: count the total number of metadata keys (distinct target+key pairs) in the tree.
   - `max-size`: compute the total size of all blob objects reachable from the tree.
5. If any trigger exceeds its threshold, run a prune using `since` and `min-size` from the rules.
6. The prune creates a second commit on top of the serialization commit, with the standard prune commit message format.

If no trigger fires, serialization produces a single commit as before.

## The `gmeta config` command

A convenience command for managing project-level `meta:*` keys.

```
gmeta config <key> <value>       # set a config key
gmeta config <key>               # get a config key
gmeta config --list              # list all meta:* config keys
gmeta config --unset <key>       # remove a config key
```

Examples:

```
gmeta config meta:prune:since 90d
gmeta config meta:prune:max-keys 10000
gmeta config meta:prune:max-size 50m
gmeta config meta:prune:min-size 512k

gmeta config meta:prune:since
# → 90d

gmeta config --list
# → meta:prune:since = 90d
# → meta:prune:max-keys = 10000
# → meta:prune:max-size = 50m
# → meta:prune:min-size = 512k

gmeta config --unset meta:prune:max-keys
```

Under the hood, `gmeta config` operates on the `project` target with string values:

- `gmeta config <key> <value>` is equivalent to `gmeta set project <key> <value>`
- `gmeta config <key>` is equivalent to `gmeta get project <key>`
- `gmeta config --list` queries all project keys matching `meta:*`
- `gmeta config --unset <key>` is equivalent to `gmeta rm project <key>`

All keys set via `gmeta config` must start with `meta:`. This keeps the config namespace distinct from user-defined project metadata.

## Interaction with manual prune

Running `gmeta prune` manually remains supported. Auto-pruning does not interfere — if a manual prune was recently run and the tree is already below thresholds, auto-pruning will not trigger.

## Interaction with materialization

Auto-prune rules materialized from a remote are immediately effective for subsequent local serializations. There is no special merge handling — the rules are ordinary string values and follow standard last-writer-wins conflict resolution.
