# Serialize filters

This document describes how metadata keys can be excluded from serialization or routed to alternative refs during the serialize step.

This allows the user to split up project metadata into different references for exchange - for example, public versus corporate data.

## Design

### Local-only keys via the `meta:local:` namespace

Any key whose first segments are `meta:local` is **never serialized** to any ref. They are only available in the local storage.

Examples:

- `meta:local:scratch`
- `meta:local:editor:cursor`
- `meta:local:build:last-status`
- `meta:local:ai:draft-summary`

The `meta:local:` prefix is a hard rule enforced by the serializer. No filter configuration is needed to make it work. Keys in this namespace are silently skipped during serialize and are never written into any git tree.

### Filter rules

Users can define filter rules that control serialization behavior. Filter rules are stored as set members on the **project** target under either `meta:filter` or `meta:local:filter`. The `meta:filter` rules are shared (corporate rules), the local ones are not (personal rules).

Each set member is a rule string with the format:

```
<action> <pattern> [<destination>]
```

#### Actions

| Action    | Meaning                                                                                                                 |
| --------- | ----------------------------------------------------------------------------------------------------------------------- |
| `exclude` | Never serialize matching keys to any ref. They remain local-only.                                                       |
| `route`   | Serialize matching keys to a named secondary ref instead of the default local ref. Requires a `<destination>` argument. |

#### Patterns

Patterns match against the full key string using a simple glob-like syntax:

| Syntax  | Meaning                                                                                 |
| ------- | --------------------------------------------------------------------------------------- |
| `*`     | Matches any sequence of characters within a single key segment (between `:` delimiters) |
| `**`    | Matches any number of complete key segments (including zero)                            |
| literal | Exact segment match                                                                     |

Segments are delimited by `:`.

Examples:

| Pattern          | Matches                                            | Does not match                     |
| ---------------- | -------------------------------------------------- | ---------------------------------- |
| `draft:*`        | `draft:summary`, `draft:notes`                     | `draft:ai:summary`                 |
| `draft:**`       | `draft:summary`, `draft:ai:summary`, `draft:x:y:z` | `notes:draft:x`                    |
| `agent:*:prompt` | `agent:claude:prompt`                              | `agent:prompt`, `agent:x:y:prompt` |
| `myteam:**`      | `myteam:anything:at:any:depth`                     | `yourteam:x`                       |
| `wip`            | `wip`                                              | `wip:notes`                        |

#### Route destination

The `route` action takes a third argument specifying the destination name. The destination becomes a sub-ref under the local ref:

```
refs/meta/local/<destination>
```

For example, `route myteam:** private` serializes matching keys to `refs/meta/local/private`.

Each destination ref is a separate commit/tree that contains only the keys routed to it, using the same tree structure as the primary ref. Each can be pushed independently (e.g. to a personal remote or a different refspec).

If a `route` rule matches, the key is **excluded** from the primary `refs/meta/local/main` tree and **included** only in its destination ref.

If the namespace config is set (e.g. `meta.namespace = foo`), a destination `private` becomes `refs/foo/local/private`.

Multiple route rules can target different destinations. Keys from all rules sharing the same destination are collected into a single ref.

#### Multiple Routes

The `<destination>` can be a comma delimited list of destinations and the matching key values will be written to all of them.

### Rule evaluation

Rules are evaluated in order of specificity:

1. `meta:local:` keys are never serialized regardless of any filter rules.
2. Filter rules are evaluated.
   - Filters in `meta:local:filter` are evaluated first, then `meta:filter` rules.
   - If multiple rules match the same key:
     - any `exclude` match applies and key value is not serialized anywhere
     - otherwise, _all_ `route` rules apply
3. Keys that match no rule are serialized to the primary ref as usual.

## Serialize algorithm changes

The current serialize flow is:

1. Read all metadata from SQLite (`get_all_metadata`)
2. Read all tombstones
3. Build git tree entries for each key
4. Commit to `refs/meta/local`

The new flow becomes:

1. Read all metadata from SQLite
2. Read all tombstones
3. Read filter rules from `meta:local:filter` on the project target
4. For each key:
   - If key starts with `meta:local:` -> skip entirely
   - If key matches an `exclude` rule -> skip entirely
   - If key matches a `route` rule -> add to the tree builder for that rule's destination
   - Otherwise -> add to the primary tree builder
5. Apply the same skip/route logic to tombstones
6. Commit the primary tree to `refs/meta/local/main`
7. For each destination that has entries, commit its tree to `refs/meta/local/<destination>`

## CLI surface

No new commands are needed. Filter rules are managed with the existing `set` commands:

```sh
# Add a filter rule
gmeta set:add project meta:filter "exclude draft:**"
gmeta set:add project meta:local:filter "route myteam:** private"
gmeta set:add project meta:local:filter "route acme:** vendor"

# View current filter rules
gmeta get project meta:local:filter

# Remove a filter rule
gmeta set:rm project meta:filter "exclude draft:**"
```

## Examples

### Keep draft notes local

```sh
gmeta set:add project meta:local:filter "exclude draft:**"
gmeta add commit:abc123 draft:summary "WIP: still thinking about this"
gmeta serialize   # draft:summary is not in the git tree
```

### Route personal annotations to a separate ref

```sh
gmeta set:add project meta:local:filter "route myname:** mine"
gmeta set commit:abc123 myname:review-note "looks good but check error handling"
gmeta serialize   # review-note goes to refs/meta/local/mine, not refs/meta/local/main
```

### Route different namespaces to different refs

```sh
gmeta set:add project meta:local:filter "route myname:** mine"
gmeta set:add project meta:local:filter "route acme:** vendor"
gmeta serialize   # myname:* keys go to refs/meta/local/mine
                  # acme:* keys go to refs/meta/local/vendor
```

### Always-local scratch space

```sh
gmeta set commit:abc123 meta:local:cursor-pos "line 42"
gmeta serialize   # meta:local:cursor-pos is never serialized, no filter needed
```

## Non-goals

- Per-target filtering (all filters apply globally across targets)
- Regex patterns (glob syntax is sufficient)
- Filter rules on non-project targets
- Nested destination refs (destination is a single name, not a path)
