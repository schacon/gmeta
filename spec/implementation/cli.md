# CLI surface

This document describes the intended command-line interface at the project level.

## Core commands

### Set a value

```bash
gmeta set [-t <type>] <target> <key> <value>
```

If `-t` is not given, the value type defaults to `string`.

Examples:

```bash
gmeta set commit:314e7f0fa7 agent:model claude-4.6
gmeta set -t list branch:sc-branch-1-deadbeef agent:chat '["hello", "world"]'
gmeta set -t set path:src/metrics owners '["schacon", "caleb"]'
```

### Get values

```bash
gmeta get <target> [<key>]
```

Behavior:

- with only `<target>`, show all keys for the target
- with `<target> <key>`, show the exact key if present
- with `<target> <partial-key>`, show matching subkeys

### Remove a key

```bash
gmeta rm <target> <key>
```

This removes the logical key and records a whole-key tombstone.

## List operations

```bash
gmeta list:push <target> <key> <value>
gmeta list:pop <target> <key> <value>
```

Notes:

- `list:push` appends a new list entry
- `list:pop` requires explicit exchange semantics before implementation is finalized; see [Lists](../exchange-format/lists.md)
- if a string key is pushed to as a list, the implementation may convert it to a list

## Set operations

These are not yet implemented, but are the natural command shape if sets are adopted:

```bash
gmeta set:add <target> <key> <value>
gmeta set:rm <target> <key> <value>
```

## Exchange commands

### Serialize

```bash
gmeta serialize
```

Writes a new metadata commit for the current local shareable state.

### Materialize

```bash
gmeta materialize [<remote>]
```

Behavior:

- if `<remote>` is given, materialize from that remote metadata ref
- if omitted, inspect all known metadata remotes and materialize them

### Dry-run materialize

```bash
gmeta materialize --dry-run [<remote>]
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

### String input

Without `-t`, `<value>` is interpreted as a string.

### List input

For `-t list`, `<value>` should be a JSON array of strings.

### Set input

For `-t set`, `<value>` should be a JSON array of unique strings.

## Output modes

`gmeta get` may support:

- human-readable tabular output
- `--json`
- `--with-authorship` for author/timestamp metadata in JSON mode

The output model is documented in [Output](./output.md).
