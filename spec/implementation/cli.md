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
git meta serialize
```

Writes a new metadata commit for the current local shareable state.

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
