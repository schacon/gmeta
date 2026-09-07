# CLI surface

This document describes the intended command-line interface at the project level.

## Core commands

### Set a value

```bash
git meta set <target> <key> <value>
```

`git meta set` always writes a string value. List and set values are mutated
through their own dedicated verbs (see below).

Examples:

```bash
git meta set commit:314e7f0fa7 agent:model claude-4.6
git meta set path:src/metrics review:status approved
```

### Get values

```bash
git meta get <target> [<key>]
```

Behavior:

- with only `<target>`, show all keys for the target
- with `<target> <key>`, show the exact key if present
- with `<target> <partial-key>`, show matching subkeys

### Remove a key

```bash
git meta rm <target> <key>
```

This removes the logical key and records a whole-key tombstone.

## List operations

```bash
git meta list:push <target> <key> <value>
git meta list:pop <target> <key> <value>
```

Notes:

- `list:push` appends a new list entry
- `list:pop` requires explicit exchange semantics before implementation is finalized; see [Lists](../exchange-format/lists.md)
- if a string key is pushed to as a list, the implementation may convert it to a list

## Set operations

```bash
git meta set:add <target> <key> <value>
git meta set:rm <target> <key> <value>
```

Notes:

- `set:add` creates the set if it does not already exist
- `set:add` is idempotent for an existing member
- `set:rm` removes a single member and records a member tombstone

## Exchange commands

### Serialize

```bash
git meta serialize [--force-full]
```

Writes a new metadata commit for the current local shareable state.
By default this may use incremental dirty-target detection. `--force-full`
rebuilds the serialized tree from the complete local SQLite state.

### Materialize

```bash
git meta materialize [<remote>]
```

Behavior:

- if `<remote>` is given, materialize from that remote metadata ref
- if omitted, inspect all known metadata remotes and materialize them

### Dry-run materialize

```bash
git meta materialize --dry-run [<remote>]
```

Useful for reporting the merge strategy and conflict decisions without applying them.

## Sync commands

`push`, `pull`, and `sync` are the porcelain that bundle a local
serialize/materialize with the network step against a configured
metadata remote. They are the commands a project uses day-to-day;
`serialize` and `materialize` are the underlying plumbing.

### Push

```bash
git meta push [<remote>]
git meta push [<remote>] --readme
```

Behavior:

- serializes any pending local metadata changes (equivalent to
  `git meta serialize`) and pushes the resulting metadata commit on
  `refs/<namespace>/main` to the named remote
- if `<remote>` is omitted, the first configured metadata remote is used
- `--readme` instead pushes a starter `README.md` commit to
  `refs/heads/main` on the metadata remote, and only if that branch does
  not already exist (no force push). This is purely cosmetic — it gives
  the metadata repository something human-readable when browsed on the
  forge — and is independent of the metadata refs themselves

### Pull

```bash
git meta pull [<remote>]
```

Behavior:

- fetches `refs/<namespace>/main` from the remote into that remote's
  configured tracking ref and runs the equivalent of `git meta
  materialize` to merge the new history into the local SQLite database
- primary metadata remotes track `refs/<namespace>/remotes/main`; side
  metadata remotes track `refs/<namespace>/remotes/<name>/main`
- if `<remote>` is omitted, a primary configured metadata remote is used
  in preference to side remotes
- prints `Already up-to-date.` when the remote tip is unchanged

### Sync

```bash
git meta sync [<remote>]
```

Behavior:

- pulls remote metadata first, which fetches `refs/<namespace>/main`,
  serializes local metadata for merge, and materializes remote changes
  into the local SQLite database
- pushes the merged local metadata afterward, serializing again and
  pushing `refs/<namespace>/local/main` to `refs/<namespace>/main`
- if the push is rejected because the remote advanced, fetches and
  materializes the new remote state, reserializes locally, rewrites the
  local metadata commit on top of the remote tip, and retries
- if `<remote>` is omitted, all configured metadata remotes are pulled
  first and then all configured metadata remotes are pushed

## Setup and configuration

These commands wire a project up to a metadata remote, or undo that
wiring. They never write or read metadata values directly; they only
manage the local state needed by the sync and exchange commands above.

### Setup

```bash
git meta setup
```

Initializes a metadata remote for the current repository from a
project-local `.git-meta` file at the repository root.

The file is YAML with a required `url` key and an optional `depth`:

```yaml
url: git@github.com:org/project-meta.git
depth: 100000
```

Unknown keys are ignored, so future versions can add more project-local
setup fields without breaking older clients.

`depth` asks new clones to fetch only that many metadata commits. A
metadata history has one commit per publish and most of it describes keys
that pruning has already dropped from the tip, so a long-lived project can
suggest a recent slice rather than years of history. A clone that starts
shallow can reach further back later with `git meta deepen`. Omit `depth`
for the whole history; `depth: 0` is an error rather than a request for
nothing.

Behavior:

- reads `.git-meta` from the repository work tree
- runs the equivalent of `git meta remote add <url> --init`, so on a
  fresh metadata remote the command will also create
  `refs/<namespace>/local/main` with a starter README and push it to
  `refs/<namespace>/main`
- when it initializes a metadata store that has no auto-prune rules,
  configures defaults of `meta:prune:max-keys 10000` and
  `meta:prune:min-keys 5000`, so the published tree stays a size a clone
  can afford. An existing policy is never overwritten.
- intended to be the one command a teammate runs after cloning to opt
  in to the project's metadata exchange

### Deepen a shallow metadata history

```bash
git meta deepen [--depth <n>]
```

Fetches further back into a metadata history that was cloned shallow, and
indexes the keys those commits publish so they become fetchable.

Keys published before a shallow boundary are not merely unfetched: the
commits naming them were never transferred, so history indexing never saw
them and they are absent from the local index entirely. Deepening is what
makes them known.

With `--depth`, fetches that many more commits. Without it, fetches the
whole remaining history. On a history that is already complete, reports
that and does nothing.

### Add a remote

```bash
git meta remote add <url> [--name <name>] [--namespace <namespace>] [--init]
```

Configures a new metadata remote for the current repository.

Behavior:

- writes git config entries for the remote: `remote.<name>.url`,
  `remote.<name>.fetch`, `remote.<name>.meta = true`, plus
  partial-clone/promisor settings so blobs are fetched on demand
- the first metadata remote is primary and fetches to
  `refs/<ns>/remotes/main`; if a primary metadata remote already exists,
  the new remote is marked with `remote.<name>.metaside = true` and
  fetches to `refs/<ns>/remotes/<name>/main`
- inspects the remote with `git ls-remote` to check that
  `refs/<ns>/main` exists, and on success runs an initial blobless
  fetch and `materialize` so values are immediately readable
- if no metadata refs are present on the remote:
  - with `--init`, creates (or reuses) `refs/<ns>/local/main` with a
    starter README commit and pushes it to `refs/<ns>/main` on the
    remote
  - on an interactive terminal without `--init`, prompts the user for
    confirmation and behaves as if `--init` had been passed when they
    accept
  - in non-interactive contexts (CI, pipes), bails with an actionable
    hint to re-run with `--init`
- if metadata refs are found under a different namespace, bails with a
  hint to re-run with `--namespace=<that-namespace>`

Defaults:

- `--name`: `meta`
- `--namespace`: from the `meta.namespace` git config, falling back to
  `meta`

The companion verbs `git meta remote list` and
`git meta remote remove <name>` are also available for inspecting and
removing configured metadata remotes.

### Teardown

```bash
git meta teardown
```

Removes the local git meta state from the repository.

Behavior:

- deletes the SQLite database at `.git/git-meta.sqlite`
- deletes every reference under `refs/<namespace>/`, including local,
  serialized, and remote-tracking refs

This does **not** touch any remote, and is intended for cases like
switching namespaces, recovering from local corruption, or cleanly
uninstalling git meta from a checkout. After teardown,
`git meta setup` (or `git meta remote add`) re-installs the local
state.

## Target syntax

Targets use the syntax documented in [Targets and keys](../exchange-format/targets.md).

Examples:

```bash
commit:<sha>
change-id:<uuid>
branch:<name-or-uuid>
path:src/metrics
project
```

## Value encoding

`git meta set` only writes string values. To populate or mutate list and set
values, use the dedicated verbs:

- `git meta list:push <target> <key> <value>` to append a list entry
- `git meta list:pop <target> <key> <value>` to drop a list entry
- `git meta set:add <target> <key> <value>` to add a set member
- `git meta set:rm <target> <key> <value>` to remove a set member

## Output modes

`git meta get` may support:

- human-readable tabular output
- `--json`
- `--with-authorship` for author/timestamp metadata in JSON mode

The output model is documented in [Output](./output.md).
